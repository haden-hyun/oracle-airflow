"""
펀드 기준가(NAV) 크롤링·적재 DAG (매일 06:55 KST)

- 대상 = config crawl_codes ∪ 활성 계좌 펀드 ∪ 수기 유형 펀드 ∪ Param extra_fund_codes (런타임 계산)
- fundguide.net 을 Playwright Chromium 으로 크롤링 (동시 1개), 실패 코드는 건너뜀
- 적재: config tables.fund_price_daily / 후행: account_ingest_asset_daily
"""

from datetime import datetime, timedelta

from airflow.sdk import Param, dag, get_current_context, task

from alerts import discord_failure_callback, discord_recovery_callback
from asset_flow.assets import FUND_PRICE, mark
from asset_flow.runtime import KST, standard_dates

DAG_DOC = """
### 목적
연금저축·IRP·DC 펀드의 T-1 기준가를 크롤링해 `market.fund_price_daily` 에 적재한다.

| Task | 내용 |
|---|---|
| `get_standard_date` | 기준일 T-1 (KST) |
| `resolve_fund_codes` | config ∪ secrets 계좌 펀드 ∪ 수기 유형 펀드 ∪ `extra_fund_codes` |
| `crawl_fund_prices` | 코드별 크롤링 → 실패 코드만 1회 재크롤링 (그래도 실패면 `[SKIP]` 후 제외) |
| `upload_fund_prices` | 기준일 + 수집된 코드만 DELETE 후 INSERT (부분 수집이 기존 행을 지우지 않음, 0건이면 생략) |
| `check_fund_coverage` | 기준일 DB 에 기준가가 없는 코드가 있으면 실패 → Discord 알림 (재시도 없음) |
"""


@dag(
    dag_id="market_ingest_fund_price_daily",
    description="매일 06:55 펀드 기준가 크롤링·적재 (fundguide.net)",
    doc_md=DAG_DOC,
    schedule="55 6 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=KST),
    catchup=False,
    default_args={"owner": "haejun", "retries": 1, "retry_delay": timedelta(minutes=5),
                  "on_failure_callback": discord_failure_callback},
    on_success_callback=discord_recovery_callback,
    tags=["market", "fund", "NAV", "crawling", "daily", "ingestion"],
    params={"extra_fund_codes": Param([], type="array", items={"type": "string"},
                                      description="추가로 크롤링할 펀드 표준코드")},
)
def market_ingest_fund_price_daily():
    """
    Purpose: DAG 구성 (기준일·대상 코드 → 크롤링 → 적재)
    Parameter: 없음
    Input: 없음
    Output: 없음
    """

    @task(task_id="get_standard_date")
    def get_standard_date() -> str:
        """
        Purpose: 기준일 T-1 (KST)
        Parameter: 없음
        Input: 태스크 컨텍스트
        Output: YYYY-MM-DD
        """
        return standard_dates(get_current_context())["standard_date"]

    @task(task_id="resolve_fund_codes")
    def resolve_fund_codes() -> list[str]:
        """
        Purpose: 크롤링 대상 펀드코드 계산
        Parameter: 없음
        Input: Param extra_fund_codes, secrets 파일, config 파일
        Output: 정렬된 펀드코드 리스트
        """
        from asset_flow.app_config import load_app_config
        from asset_flow.pipeline.dispatch import fund_codes_to_crawl
        from asset_flow.secrets_config import load_secrets

        extra = get_current_context()["params"]["extra_fund_codes"]
        return fund_codes_to_crawl(load_secrets(), load_app_config(), extra)

    @task(task_id="crawl_fund_prices", max_active_tis_per_dag=1)
    def crawl_fund_prices(standard_date: str, codes: list[str]) -> list:
        """
        Purpose: 펀드 기준가 크롤링 (Chromium 동시 1개, 실패 코드 1회 재시도)
        Parameter:
            - standard_date: 기준일
            - codes: 펀드코드 리스트
        Input: fundguide.net
        Output: fund_price_daily 레코드 리스트 (재시도 후에도 실패한 코드는 빠짐)
        """
        from asset_flow.crawler.fund_crawler import crawl_fund_prices as crawl

        records = crawl(codes, standard_date)
        if missing := sorted(set(codes) - {r["product_code"] for r in records}):
            print(f"재크롤링: {missing}")        # 일시 장애 대비
            records += crawl(missing, standard_date)
        print(f"크롤링 완료: {len(records)}건 / {len(codes)}개 코드")
        return records

    @task(task_id="upload_fund_prices", outlets=[FUND_PRICE])
    def upload_fund_prices(standard_date: str, records: list) -> int:
        """
        Purpose: 기준가 적재 (기준일 + 코드 단위 멱등, 0건이면 생략)
        Parameter:
            - standard_date: 기준일
            - records: 크롤링 레코드
        Input: config tables.fund_price_daily
        Output: 적재 행 수 (+ DB 쓰기, 쓰기 가드 적용)
        """
        from asset_flow import db_manager as db
        from asset_flow.app_config import load_app_config

        table = load_app_config().tables.fund_price_daily
        n = db.replace_daily_rows(db.asset_engine(), table.db_schema, table.table_name, standard_date, records,
                                  key_column="product_code")
        mark(FUND_PRICE, standard_date=standard_date, rows=n)
        return n

    @task(task_id="check_fund_coverage", retries=0)
    def check_fund_coverage(standard_date: str, codes: list[str]) -> None:
        """
        Purpose: 기준일 기준가 누락 검사 (크롤링 실패를 상류에서 바로 알림)
        Parameter:
            - standard_date: 기준일
            - codes: 수집 대상 펀드코드
        Input: market.fund_price_daily
        Output: 없음. 누락 코드가 있으면 RuntimeError
        """
        from asset_flow import db_manager as db

        if missing := db.get_market_data_gaps(db.asset_engine(), standard_date, codes, [])["fund"]:
            raise RuntimeError(f"[{standard_date}] 기준가 미수집 {len(missing)}개: {missing} — 사이트 확인 후 수동 재실행")
        print(f"[{standard_date}] 기준가 {len(codes)}개 모두 적재됨")

    std_date = get_standard_date()
    codes = resolve_fund_codes()
    upload_fund_prices(std_date, crawl_fund_prices(std_date, codes)) >> check_fund_coverage(std_date, codes)


market_ingest_fund_price_daily()
