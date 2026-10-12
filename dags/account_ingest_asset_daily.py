"""
전 계좌 자산 수집·적재 DAG (매일 07:00 KST)

- 계좌 목록은 런타임에 secrets 에서 읽어 동적 매핑 (collect_account × 활성 계좌 id)
- 계좌당 한 태스크가 수집 → 0건이면 실패 → (standard_date, account_code) DELETE+INSERT
- 선행: 기준일 환율·펀드 기준가가 DB 에 있을 것 (데이터 센서) / 적재: config tables.asset_daily
"""

from datetime import datetime, timedelta

from airflow.sdk import dag, get_current_context, task

from alerts import discord_failure_callback, discord_recovery_callback
from asset_flow.assets import ASSET_DAILY, mark
from asset_flow.finalize import finalize
from asset_flow.runtime import KST, standard_dates

DAG_DOC = """
### 목적
secrets 의 활성 계좌별 T-1 잔고·평가금액을 `account.asset_daily` 에 적재하는 메인 파이프라인.

| Task | 내용 |
|---|---|
| `get_standard_date` | standard_date(T-1, 적재 레이블) / base_date(T, Upbit 캔들 경계) |
| `wait_for_exchange_rate` / `wait_for_fund_price_daily` | 기준일 환율 4종·필요 펀드 기준가가 DB 에 생길 때까지 대기 (30초 간격, 최대 10분) |
| `list_accounts` | secrets 활성 계좌 **id** 목록 (XCom 에 계좌번호 없음) |
| `collect_account` (매핑, 이름=계좌 id) | kind 별 수집 → 0건이면 실패 → 계좌·기준일 스코프 DELETE+INSERT |
| `refresh_dashboard_views` | 앞 결과와 무관하게 대시보드 MV 갱신 프로시저 CALL (일부 계좌 실패여도 나머지 반영) |
| `fail_if_upstream_failed` | 앞 단계 실패가 있으면 run 실패 처리 (알림은 실패 태스크가 이미 보냄) |

### 불변식
- 계좌당 writer 1개 (account_code 유일성은 secrets 로더가 보장)
- 빈 결과는 DELETE 전에 예외 → 실패한 계좌의 기존 적재분 보존, 다른 계좌는 계속
- 선행 판정은 상류 run 이 아니라 데이터 존재 → 수동 재실행·상류 수동 실행에도 동작
- 계좌 추가·해지는 secrets 만 수정 (`enabled: false`) — DAG 구조·버전 불변
"""


@dag(
    dag_id="account_ingest_asset_daily",
    description="매일 07:00 전 계좌 자산 수집·적재 (KIS/Upbit API + 수기 원장)",
    doc_md=DAG_DOC,
    schedule="0 7 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=KST),
    catchup=False,
    default_args={"owner": "haejun", "retries": 1, "retry_delay": timedelta(minutes=5),
                  "on_failure_callback": discord_failure_callback},
    on_success_callback=discord_recovery_callback,
    tags=["account", "asset", "portfolio", "KIS", "Upbit", "daily", "ingestion"],
)
def account_ingest_asset_daily():
    """
    Purpose: DAG 구성 (기준일 → 데이터 센서 2 → 계좌별 매핑 수집 → 대시보드 갱신)
    Parameter: 없음
    Input: 없음
    Output: 없음
    """
    def market_gaps(dates: dict, part: str) -> list[str]:
        """
        Purpose: 기준일 시장 데이터 결측 목록
        Parameter:
            - dates: get_standard_date 결과
            - part: "fx" 또는 "fund"
        Input: secrets·config 파일, market 테이블
        Output: 결측 통화 또는 펀드코드 리스트 (비면 준비 완료)
        """
        from asset_flow import db_manager as db
        from asset_flow.app_config import load_app_config
        from asset_flow.pipeline.dispatch import required_fund_codes
        from asset_flow.secrets_config import load_secrets
        from asset_flow.transformers.kis_transformer import EXCHANGE_CURRENCIES

        d = dates["standard_date"]
        funds = required_fund_codes(load_secrets(), load_app_config()) if part == "fund" else []
        currencies = EXCHANGE_CURRENCIES if part == "fx" else []
        missing = db.get_market_data_gaps(db.asset_engine(), d, funds, currencies)[part]
        print(f"[{d}] {part} 결측: {missing or '없음'}")
        return missing

    sensor = {"poke_interval": 30, "timeout": 600, "mode": "reschedule"}

    @task.sensor(task_id="wait_for_exchange_rate", **sensor)
    def wait_for_exchange_rate(dates: dict) -> bool:
        """
        Purpose: 기준일 환율 4종 적재 대기
        Parameter:
            - dates: get_standard_date 결과
        Input: market.exchange_rate_daily
        Output: 준비되면 True (10분 초과 시 센서 실패 → 수집 중단 + 알림)
        """
        return not market_gaps(dates, "fx")

    @task.sensor(task_id="wait_for_fund_price_daily", **sensor)
    def wait_for_fund_price_daily(dates: dict) -> bool:
        """
        Purpose: 기준일 펀드 기준가 적재 대기 (연금저축·수기 펀드 계좌가 쓰는 코드만)
        Parameter:
            - dates: get_standard_date 결과
        Input: secrets·config 파일, market.fund_price_daily
        Output: 준비되면 True (10분 초과 시 센서 실패 → 수집 중단 + 알림)
        """
        return not market_gaps(dates, "fund")

    @task(task_id="get_standard_date", multiple_outputs=False)
    def get_standard_date() -> dict:
        """
        Purpose: 기준일 계산
        Parameter: 없음
        Input: 태스크 컨텍스트
        Output: {"standard_date": T-1, "base_date": T}
        """
        return standard_dates(get_current_context())

    @task(task_id="list_accounts")
    def list_accounts() -> list[str]:
        """
        Purpose: 수집 대상 활성 계좌 id 목록
        Parameter: 없음
        Input: secrets 파일
        Output: 정렬된 계좌 id 리스트 (로드 실패·활성 계좌 0개면 예외 → 실패 알림)
        """
        from asset_flow.secrets_config import load_secrets

        ids = sorted(a.id for a in load_secrets().active_accounts())
        if not ids:
            raise RuntimeError("활성 계좌 없음 — secrets accounts 확인")
        print(f"수집 대상 {len(ids)}개 계좌: {ids}")
        return ids

    @task(
        task_id="collect_account",
        outlets=[ASSET_DAILY],
        multiple_outputs=False,
        map_index_template="{{ account_id }}",
        max_active_tis_per_dagrun=4,
        retries=3,
        retry_delay=timedelta(seconds=20),
        retry_exponential_backoff=2.0,
    )
    def collect_account(account_id: str, dates: dict) -> dict:
        """
        Purpose: 계좌 1개 수집·적재
        Parameter:
            - account_id: secrets 계좌 id
            - dates: get_standard_date 결과
        Input: secrets·config 파일, 토큰 저장소, kind 별 API·업무 DB
        Output: {"account_id", "rows"} (+ asset_daily 쓰기). 0건·API 오류면 예외
        """
        from asset_flow.pipeline.dispatch import Deps, collect_account as run

        get_current_context()["account_id"] = account_id     # map_index_template 표시명
        rows = run(account_id, dates, Deps.load())
        mark(ASSET_DAILY, standard_date=dates["standard_date"], account_id=account_id, rows=rows)
        return {"account_id": account_id, "rows": rows}

    dates = get_standard_date()
    collected = collect_account.partial(dates=dates).expand(account_id=list_accounts())
    [wait_for_exchange_rate(dates), wait_for_fund_price_daily(dates)] >> collected
    finalize(collected)


account_ingest_asset_daily()
