"""
배당 일정·수령 원장 수집·적재 DAG (매일 07:20 KST)

- 국내: HHKDB669102C0(종목 일정) + CTRGA011R(계좌 원장, 계좌별 매핑) / 미국: CTRGT011R(금액) + HHDFS78330900(일정)
- 미국은 계좌 원장 API 가 없어 asset_daily 수량으로 추정 원장(is_estimated) 생성
- 적재: PK UPSERT + last_seen_date (DELETE 없음) — 늦은 run·재실행·백필 모두 멱등, 늦은 run skip 규칙 미적용
- 선행: 토큰(데이터 센서) / 미국 추정만 T−1 asset_daily(데이터 센서)
"""

from datetime import datetime, timedelta

from airflow.sdk import Param, dag, get_current_context, task

from alerts import discord_failure_callback, discord_recovery_callback
from asset_flow.assets import DIVIDEND_EVENT, DIVIDEND_RECEIPT, mark
from asset_flow.finalize import fail_if_upstream_failed
from asset_flow.runtime import KST, kst_run_end

DAG_DOC = """
### 목적
보유(이력) 종목의 배당·분배 일정(`market.dividend_event`)과 계좌별 수령 원장(`account.dividend_receipt`)을 매일 갱신한다.

| Task | 내용 |
|---|---|
| `get_windows` | 조회 창 (정기: 일정 T−400~+60/120, 원장 T−100 / 수동 Param `start_date`·`end_date` = 백필) |
| `wait_for_token` | 시세용 KIS 키 유효 토큰 대기 (데이터 센서). 이 DAG 는 어떤 키도 발급하지 않음 |
| `list_targets` | 조회 종목: `market.product`(없으면 `asset_daily`) 400일 보유 이력 ∪ 원장 종목 |
| `ingest_kr_events` / `ingest_us_events` | 종목별 일정 조회 → UPSERT (종목 단위 실패 격리, 실패 있으면 성공분 적재 후 태스크 실패) |
| `list_receipt_accounts` | 원장 조회 계좌 **id** (config `dividend.receipt_kinds`) |
| `ingest_receipts` (매핑, 이름=계좌 id) | 계좌 자기 키로 원장 조회 → UPSERT. 0건 = 정상 |
| `wait_for_asset_daily` | 위탁(kis_stock) 계좌의 T−1 `asset_daily` 존재 대기 |
| `estimate_us_receipts` | 미국 일정 × 락일 직전 수량 → 추정 원장 (원천징수 15%, 지급일 환율) |
| `refresh_dashboard_views` | config `dividend.refresh_dashboard_views=true` 일 때만 MV 갱신 (기본 skip) |
| `fail_if_upstream_failed` | 앞 단계 실패가 있으면 run 실패 |

### 불변식
- DELETE 없음 — 정정된 옛 PK 행은 `last_seen_date` 로 대시보드가 거른다
- XCom·로그에 계좌번호 없음 (계좌 id·종목 코드·건수만)
"""


@dag(
    dag_id="account_ingest_dividend",
    description="매일 07:20 배당 일정·수령 원장 수집·적재 (KIS 4종)",
    doc_md=DAG_DOC,
    schedule="20 7 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=KST),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "haejun", "retries": 1, "retry_delay": timedelta(minutes=5),
                  "on_failure_callback": discord_failure_callback},
    on_success_callback=discord_recovery_callback,
    tags=["account", "market", "dividend", "KIS", "daily", "ingestion"],
    params={
        "start_date": Param(None, type=["null", "string"], format="date",
                            description="백필 시작일 (비우면 정기 창). 전체 백필 = 2000-01-01"),
        "end_date": Param(None, type=["null", "string"], format="date",
                          description="백필 종료일 (비우면 정기 창의 끝: 일정 T+60/120, 원장 T)"),
    },
)
def account_ingest_dividend():
    """
    Purpose: DAG 구성 (창 → 토큰 센서 → 일정 2종·계좌별 원장 → asset_daily 센서 → 미국 추정 → 마무리)
    Parameter: 없음
    Input: 없음
    Output: 없음
    """
    sensor = {"poke_interval": 30, "timeout": 1800, "mode": "reschedule"}     # 07:00 자산 DAG 지연 대비 30분
    retry = {"retries": 3, "retry_delay": timedelta(seconds=20), "retry_exponential_backoff": 2.0}

    @task(task_id="get_windows")
    def get_windows() -> dict:
        """
        Purpose: 조회 창 계산 (늦은 정기 run 도 skip 하지 않음 — 기간 이력 API)
        Parameter: 없음
        Input: 태스크 컨텍스트 (run 시각·Param), config 파일
        Output: run_windows 결과 dict (날짜 문자열만)
        """
        from asset_flow.pipeline.dispatch import Deps
        from asset_flow.pipeline.dividend import run_windows

        ctx = get_current_context()
        win = run_windows(kst_run_end(ctx).to_date_string(), ctx["params"], Deps.load())
        print(f"조회 창: {win}")
        return win

    @task.sensor(task_id="wait_for_token", **sensor)
    def wait_for_token() -> bool:
        """
        Purpose: 시세용 KIS 키 유효 토큰 대기 (직접 발급하지 않음)
        Parameter: 없음
        Input: secrets 파일, tokens.json
        Output: 유효 토큰이 있으면 True (30분 초과 시 센서 실패 + 알림)
        """
        from asset_flow.secrets_config import load_secrets
        from asset_flow.tokens import TokenManager

        cfg = load_secrets()
        return TokenManager(cfg).has_valid_token(cfg.services.exchange_rate_credential)

    @task(task_id="list_targets")
    def list_targets(win: dict) -> dict:
        """
        Purpose: 배당 조회 대상 종목
        Parameter:
            - win: get_windows 결과
        Input: market.product 또는 account.asset_daily, account.dividend_receipt, config extra_kr_codes
        Output: {"kr": [코드], "us": [[티커, 거래소]], "source"} (수십 건, XCom 1KB 미만). 한쪽이 비면 예외
        """
        from asset_flow import db_manager as db
        from asset_flow.app_config import load_app_config

        targets = db.get_dividend_targets(db.asset_engine(), win["held_since"])
        targets["kr"] = sorted(set(targets["kr"]) | set(load_app_config().dividend.extra_kr_codes))
        print(f"대상 ({targets['source']}): 국내 {len(targets['kr'])} · 미국 {len(targets['us'])}")
        if not targets["kr"] or not targets["us"]:          # 조용한 누락 방지 (빈 product·조회 오류)
            raise RuntimeError(f"배당 조회 대상 비어 있음 — 국내 {len(targets['kr'])} · 미국 {len(targets['us'])} "
                               f"({targets['source']})")
        return targets

    @task(task_id="ingest_kr_events", outlets=[DIVIDEND_EVENT], **retry)
    def ingest_kr_events(targets: dict, win: dict) -> dict:
        """
        Purpose: 국내 일정 수집·적재
        Parameter:
            - targets: list_targets 결과
            - win: get_windows 결과
        Input: secrets·config, 토큰, KIS HHKDB669102C0, 휴장일
        Output: {"codes", "rows", "failed"} (+ dividend_event UPSERT). 일부 실패면 성공분 적재 후 예외
        """
        from asset_flow.pipeline.dispatch import Deps
        from asset_flow.pipeline.dividend import ingest_kr_events as run

        result = run(targets["kr"], win, Deps.load())
        mark(DIVIDEND_EVENT, market="KR", base_date=win["base_date"], rows=result["rows"])
        return result

    @task(task_id="ingest_us_events", outlets=[DIVIDEND_EVENT], **retry)
    def ingest_us_events(targets: dict, win: dict) -> dict:
        """
        Purpose: 미국 일정(금액 + 권리종합) 수집·적재
        Parameter:
            - targets: list_targets 결과
            - win: get_windows 결과
        Input: secrets·config, 토큰, KIS CTRGT011R·HHDFS78330900
        Output: {"symbols", "rows", "failed"} (+ dividend_event UPSERT). 일부 실패면 성공분 적재 후 예외
        """
        from asset_flow.pipeline.dispatch import Deps
        from asset_flow.pipeline.dividend import ingest_us_events as run

        result = run(targets["us"], win, Deps.load())
        mark(DIVIDEND_EVENT, market="US", base_date=win["base_date"], rows=result["rows"])
        return result

    @task(task_id="list_receipt_accounts")
    def list_receipt_accounts() -> list[str]:
        """
        Purpose: 원장 조회 계좌 id 목록
        Parameter: 없음
        Input: secrets 파일, config dividend.receipt_kinds
        Output: 정렬된 계좌 id 리스트 (0개면 예외 → 알림)
        """
        from asset_flow.app_config import load_app_config
        from asset_flow.secrets_config import load_secrets

        ids = sorted(a.id for a in load_secrets().active_accounts(*load_app_config().dividend.receipt_kinds))
        if not ids:
            raise RuntimeError("원장 조회 대상 계좌 없음 — secrets accounts·config dividend.receipt_kinds 확인")
        print(f"원장 대상 {len(ids)}개 계좌: {ids}")
        return ids

    @task(task_id="ingest_receipts", outlets=[DIVIDEND_RECEIPT], map_index_template="{{ account_id }}",
          max_active_tis_per_dagrun=2, **retry)
    def ingest_receipts(account_id: str, win: dict) -> dict:
        """
        Purpose: 계좌 1개 국내 수령 원장 수집·적재
        Parameter:
            - account_id: secrets 계좌 id
            - win: get_windows 결과
        Input: secrets·config, 토큰, KIS CTRGA011R
        Output: {"account_id", "rows"} (+ dividend_receipt UPSERT). 0건도 성공
        """
        from asset_flow.pipeline.dispatch import Deps
        from asset_flow.pipeline.dividend import ingest_receipts as run

        get_current_context()["account_id"] = account_id     # map_index_template 표시명
        rows = run(account_id, win, Deps.load())
        mark(DIVIDEND_RECEIPT, account_id=account_id, base_date=win["base_date"], rows=rows)
        return {"account_id": account_id, "rows": rows}

    @task.sensor(task_id="wait_for_asset_daily", **sensor)
    def wait_for_asset_daily(win: dict) -> bool:
        """
        Purpose: 위탁 계좌 T−1 asset_daily 적재 대기 (미국 추정 수량)
        Parameter:
            - win: get_windows 결과
        Input: secrets 파일, account.asset_daily
        Output: kis_stock 계좌 모두 기준일 행이 있으면 True (30분 초과 시 센서 실패 → 추정 생략 + 알림)
        """
        from asset_flow import db_manager as db
        from asset_flow.secrets_config import load_secrets

        codes = [a.account_code for a in load_secrets().active_accounts("kis_stock")]
        return db.has_asset_rows(db.asset_engine(), win["standard_date"], codes)

    @task(task_id="estimate_us_receipts", outlets=[DIVIDEND_RECEIPT])
    def estimate_us_receipts(win: dict) -> int:
        """
        Purpose: 미국 추정 수령 원장 생성·적재
        Parameter:
            - win: get_windows 결과
        Input: secrets·config, dividend_event(US)·asset_daily·exchange_rate_daily
        Output: 적재 행 수 (+ dividend_receipt UPSERT, is_estimated=true)
        """
        from asset_flow.pipeline.dispatch import Deps
        from asset_flow.pipeline.dividend import estimate_us_receipts as run

        rows = run(win, Deps.load())
        mark(DIVIDEND_RECEIPT, kind="US_ESTIMATE", base_date=win["base_date"], rows=rows)
        return rows

    @task(task_id="refresh_dashboard_views", trigger_rule="all_done", retries=1, retry_delay=timedelta(seconds=30))
    def refresh_dashboard_views() -> None:
        """
        Purpose: 대시보드 MV 갱신 (config dividend.refresh_dashboard_views=true 일 때만)
        Parameter: 없음
        Input: config dividend.refresh_dashboard_views, dashboard.refresh_procedure
        Output: 없음 (+ 프로시저 CALL). false 면 AirflowSkipException
        """
        from airflow.sdk.exceptions import AirflowSkipException

        from asset_flow import db_manager as db
        from asset_flow.app_config import load_app_config

        app = load_app_config()
        if not app.dividend.refresh_dashboard_views:
            raise AirflowSkipException("dividend.refresh_dashboard_views=false — 배당은 아직 MV 입력 아님")
        db.call_procedure(db.asset_engine(), app.dashboard.refresh_procedure)

    win = get_windows()
    token = wait_for_token()
    targets = list_targets(win)
    token >> targets
    kr, us = ingest_kr_events(targets, win), ingest_us_events(targets, win)
    accounts = list_receipt_accounts()
    token >> accounts
    receipts = ingest_receipts.partial(win=win).expand(account_id=accounts)
    estimate = estimate_us_receipts(win)
    [us, wait_for_asset_daily(win)] >> estimate
    last = [kr, receipts, estimate]
    last >> refresh_dashboard_views()
    last >> fail_if_upstream_failed()


account_ingest_dividend()
