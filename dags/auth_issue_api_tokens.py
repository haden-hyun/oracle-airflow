"""
KIS 토큰 사전 발급 + API 키 만료·설정 정합성 점검 DAG (매일 06:50 KST)

- generate_tokens: 사용 중 KIS credential 토큰 확보 (TOKEN_ISSUE_ENABLED=false 면 기존 파일 존재 검증만)
- check_key_expiry: 만료 규칙·account_master 정합성·Upbit API 대조 → 알릴 것 있으면 Discord, 없으면 skip
- 두 태스크는 의존성 없음 (점검 실패가 토큰 체인을 막지 않음)
"""

from datetime import date, datetime, timedelta

from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException

from alerts import discord_failure_callback, discord_recovery_callback
from asset_flow.runtime import KST, kst_run_end

DAG_DOC = """
### 목적
사용 중인 KIS credential 의 토큰을 06:50 에 미리 확보하고, API 키 만료·설정 정합성을 점검한다.

| Task | 내용 |
|---|---|
| `generate_tokens` | `TokenManager.ensure_all` — credential 별 발급·실패 격리. 발급 금지 모드에선 유효 토큰 존재만 검증 |
| `check_key_expiry` | D-14·D-7, D-3 부터 매일, 구간 내 월요일 알림 + account_master 대조 + Upbit 만료일 대조 |

> 환율·시세·배당 DAG 의 `wait_for_token` 은 tokens.json 의 유효 토큰 존재로 판정한다(발급은 이 DAG 만) → `check_key_expiry` 실패와 무관.
"""


@dag(
    dag_id="auth_issue_api_tokens",
    description="매일 06:50 KIS 토큰 확보 + API 키 만료 점검",
    doc_md=DAG_DOC,
    schedule="50 6 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=KST),
    catchup=False,
    default_args={"owner": "haejun", "retries": 2, "retry_delay": timedelta(minutes=3),
                  "on_failure_callback": discord_failure_callback},
    on_success_callback=discord_recovery_callback,
    tags=["auth", "token", "setup", "daily"],
    params={"as_of": Param(None, type=["null", "string"], format="date",
                           description="만료 점검 기준일(YYYY-MM-DD, KST). 비우면 실행일")},
)
def auth_issue_api_tokens():
    """
    Purpose: DAG 구성 (generate_tokens ∥ check_key_expiry, 의존성 없음)
    Parameter: 없음
    Input: 없음
    Output: 없음
    """

    @task(task_id="generate_tokens")
    def generate_tokens() -> list[str]:
        """
        Purpose: 사용 중 KIS credential 토큰 확보 (발급 금지 모드면 유효 토큰 존재만 검증)
        Parameter: 없음
        Input: secrets 파일, TOKEN_DIR, env TOKEN_ISSUE_ENABLED, (허용 시) KIS 발급 API
        Output: 확보된 credential id 리스트 (실패 id 있으면 예외 — 성공분은 저장됨)
        """
        from asset_flow.secrets_config import load_secrets
        from asset_flow.tokens import TokenManager

        cfg = load_secrets()
        return TokenManager(cfg).ensure_all(c.id for c in cfg.credentials_in_use() if c.provider == "kis")

    @task(task_id="check_key_expiry", retries=1, retry_delay=timedelta(minutes=2))
    def check_key_expiry() -> list[str]:
        """
        Purpose: API 키 만료·설정 정합성 점검 후 필요 시 Discord 알림
        Parameter: 없음
        Input: Param as_of, secrets 파일, config 파일, account.account_master, Upbit /v1/api_keys
        Output: 알림한 항목 이름 리스트. 알릴 것 없으면 AirflowSkipException, 전송 실패면 RuntimeError(재시도)
        """
        from alerts import make_payload, post_discord
        from asset_flow import db_manager as db
        from asset_flow.app_config import load_app_config
        from asset_flow.clients.upbit_client import UpbitApiClient
        from asset_flow.ops import key_expiry as ke
        from asset_flow.secrets_config import load_secrets

        ctx = get_current_context()
        as_of = date.fromisoformat(ctx["params"]["as_of"]) if ctx["params"].get("as_of") else kst_run_end(ctx).date()
        cfg, app = load_secrets(), load_app_config()
        rule = app.credential_check
        items = ke.evaluate(cfg, as_of, rule)
        for item in items:
            if item.provider == "upbit":
                cred = cfg.credential(item.name)
                api_date, err = ke.upbit_expire_on(UpbitApiClient(cred.app_key, cred.app_secret), cred.app_key)
                ke.apply_upbit_crosscheck(item, api_date, err, as_of, rule)
        master = db.get_account_master(db.asset_engine())
        if issues := ke.config_issues(cfg, app, master):
            items.append(ke.config_item(issues))
        # master 에만 있는 활성 계좌(예: 수집 중단 계좌)는 알림 없이 건수만 기록 — 계좌번호 미출력
        master_only = set(master) - {a.account_code for a in cfg.accounts}
        print(f"[key_expiry] account_master 에만 있는 활성 계좌 {len(master_only)}건")
        selected = ke.select(items, as_of, rule)
        days = {i.name: i.days_left for i in items}
        print(f"[key_expiry] 기준일={as_of} 남은일수={days} 알림={[i.name for i in selected]}")
        if not selected:
            raise AirflowSkipException(f"알릴 항목 없음 (기준일 {as_of})")
        if not post_discord(make_payload(ke.build_embed(selected, as_of))):
            raise RuntimeError("Discord 전송 실패 — 재시도")
        return [i.name for i in selected]

    generate_tokens()
    check_key_expiry()


auth_issue_api_tokens()
