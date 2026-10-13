"""
펀드 기준가(NAV) 수집·적재 DAG (매일 06:55 KST)

- 대상 = config crawl_codes ∪ 활성 계좌 펀드 ∪ 수기 유형 펀드 ∪ Param extra_fund_codes (런타임 계산)
- 주 경로: KOFIA 전자공시 기준가 (T-1 이하 최신 = 휴일 이월). 없거나 지연된 코드만 fundguide 크롤링 (config)
- 적재: config tables.fund_price_daily / 후행: account_ingest_asset_daily
"""

from datetime import datetime, timedelta

from airflow.sdk import Param, dag, get_current_context, task

from alerts import discord_failure_callback, discord_recovery_callback
from asset_flow.assets import FUND_PRICE, mark
from asset_flow.runtime import KST, standard_dates

DAG_DOC = """
### 목적
연금저축·IRP·DC 펀드의 T-1 기준가를 KOFIA 에서 받아 `market.fund_price_daily` 에 적재한다.

| Task | 내용 |
|---|---|
| `get_standard_date` | 기준일 T-1 (KST) |
| `resolve_fund_codes` | config ∪ secrets 계좌 펀드 ∪ 수기 유형 펀드 ∪ `extra_fund_codes` |
| `collect_fund_prices` | 코드별 KOFIA 조회 → 값 없음·지연 코드만 fundguide 크롤링 (`funds.crawler_fallback`) |
| `upload_fund_prices` | 기준일 + 수집된 코드만 DELETE 후 INSERT (부분 수집이 기존 행을 지우지 않음, 0건이면 생략) |
| `check_fund_coverage` | 기준일 DB 에 기준가가 없는 코드가 있으면 실패 → Discord 알림 (재시도 없음) |
| `compare_with_fundguide` | 전환 검증: KOFIA 값과 크롤러 값이 다르면 실패 알림 (`funds.shadow_compare`, 꺼지면 skip) |
"""


@dag(
    dag_id="market_ingest_fund_price_daily",
    description="매일 06:55 펀드 기준가 수집·적재 (KOFIA, 대체 fundguide)",
    doc_md=DAG_DOC,
    schedule="55 6 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=KST),
    catchup=False,
    default_args={"owner": "haejun", "retries": 1, "retry_delay": timedelta(minutes=5),
                  "on_failure_callback": discord_failure_callback},
    on_success_callback=discord_recovery_callback,
    tags=["market", "fund", "NAV", "KOFIA", "daily", "ingestion"],
    params={"extra_fund_codes": Param([], type="array", items={"type": "string"},
                                      description="추가로 수집할 펀드 표준코드")},
)
def market_ingest_fund_price_daily():
    """
    Purpose: DAG 구성 (기준일·대상 코드 → 수집 → 적재 → 누락 점검 · 크롤러 대조)
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
        Purpose: 수집 대상 펀드코드 계산
        Parameter: 없음
        Input: Param extra_fund_codes, secrets 파일, config 파일
        Output: 정렬된 펀드코드 리스트
        """
        from asset_flow.app_config import load_app_config
        from asset_flow.pipeline.dispatch import fund_codes_to_crawl
        from asset_flow.secrets_config import load_secrets

        extra = get_current_context()["params"]["extra_fund_codes"]
        return fund_codes_to_crawl(load_secrets(), load_app_config(), extra)

    @task(task_id="collect_fund_prices")
    def collect_fund_prices(standard_date: str, codes: list[str]) -> dict:
        """
        Purpose: 기준가 수집 (KOFIA → 빠진 코드만 fundguide 크롤링)
        Parameter:
            - standard_date: 기준일
            - codes: 펀드코드 리스트
        Input: KOFIA, (대체) fundguide.net, market.fund_price_daily(이름)·trading_holiday, config funds
        Output: {"records": fund_price_daily 레코드, "source": {코드: 출처}} — 끝내 빠진 코드는 records 에 없음
        """
        from datetime import date, timedelta

        from asset_flow import db_manager as db
        from asset_flow.app_config import load_app_config
        from asset_flow.clients.kofia_client import KofiaClient
        from asset_flow.crawler.fund_crawler import crawl_fund_prices as crawl
        from asset_flow.pipeline.fund_nav import LOOKBACK_DAYS, collect_fund_prices as collect

        engine, std = db.asset_engine(), date.fromisoformat(standard_date)
        hol = db.get_holiday_sets(engine, (std - timedelta(days=LOOKBACK_DAYS)).isoformat(), standard_date)["KR"]
        fallback = crawl if load_app_config().funds.crawler_fallback else None
        records, source = collect(codes, standard_date, db.get_fund_names(engine, codes), hol, KofiaClient(), fallback)
        print(f"[{standard_date}] 수집 {len(records)}/{len(codes)}: {source}")
        return {"records": records, "source": source}

    @task(task_id="upload_fund_prices", outlets=[FUND_PRICE])
    def upload_fund_prices(standard_date: str, records: list) -> int:
        """
        Purpose: 기준가 적재 (기준일 + 코드 단위 멱등, 0건이면 생략)
        Parameter:
            - standard_date: 기준일
            - records: 수집 레코드
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
            raise RuntimeError(f"[{standard_date}] 기준가 미수집 {len(missing)}개: {missing} — KOFIA 공시 확인 후 수동 재실행")
        print(f"[{standard_date}] 기준가 {len(codes)}개 모두 적재됨")

    @task(task_id="compare_with_fundguide", retries=0, max_active_tis_per_dag=1)
    def compare_with_fundguide(standard_date: str, collected: dict) -> None:
        """
        Purpose: 전환 검증 — KOFIA 적재값과 fundguide 크롤링 값 대조
        Parameter:
            - standard_date: 기준일
            - collected: collect_fund_prices 결과
        Input: config funds.shadow_compare, fundguide.net
        Output: 없음. 스위치 off 면 skip, 값이 다르면 RuntimeError → 알림 (크롤링 실패 코드는 비교 생략)
        """
        from airflow.sdk.exceptions import AirflowSkipException

        from asset_flow.app_config import load_app_config
        from asset_flow.crawler.fund_crawler import crawl_fund_prices as crawl
        from asset_flow.pipeline.fund_nav import compare_sources

        if not load_app_config().funds.shadow_compare:
            raise AirflowSkipException("funds.shadow_compare=false")
        kofia = [r for r in collected["records"] if collected["source"][r["product_code"]].startswith("KOFIA")]
        crawled = crawl([r["product_code"] for r in kofia], standard_date)
        if diff := compare_sources(kofia, crawled):
            raise RuntimeError(f"[{standard_date}] KOFIA ≠ fundguide {len(diff)}건: {diff}")
        print(f"[{standard_date}] 대조 {len(crawled)}/{len(kofia)}개 일치 (크롤링 실패 {len(kofia) - len(crawled)})")

    std_date = get_standard_date()
    codes = resolve_fund_codes()
    collected = collect_fund_prices(std_date, codes)
    upload_fund_prices(std_date, collected["records"]) >> [check_fund_coverage(std_date, codes),
                                                           compare_with_fundguide(std_date, collected)]


market_ingest_fund_price_daily()
