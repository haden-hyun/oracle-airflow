"""
상품 목록 · 일별 시세 빈 구간 채우기 DAG (매일 07:10 KST)

- asset_daily 비현금 상품 → market.product upsert (is_tracked = 보유 중 또는 매도 후 n일, config)
- 창: min(마지막 적재일+1, 최근 5거래일 시작) ~ T-1, 신규면 T-365 ~ T-1
- 소스: KIS 국내(FHKST03010100)·미국(HHDFS76240000) 원주가, 업비트 일봉, KOFIA 펀드 기준가(스위치, 빈 날짜만)
- 선행: 토큰·T-1 asset_daily 데이터 센서 / 늦은 run skip 없음 (이력 API — 과거를 덮지 않음) / 토큰 발급 안 함
"""

from datetime import datetime, timedelta

from airflow.sdk import Param, dag, get_current_context, task

from alerts import discord_failure_callback, discord_recovery_callback
from asset_flow.assets import (ASSET_DAILY, FUND_PRICE, PRODUCT, PRODUCT_PRICE, SOURCE_KIS, SOURCE_KOFIA, SOURCE_UPBIT,
                               TRADING_HOLIDAY, mark)
from asset_flow.finalize import fail_if_upstream_failed
from asset_flow.runtime import KST, kst_run_end

PRICE_SOURCES = ["KIS_KR_DAILY", "KIS_US_DAILY", "UPBIT_DAY"]

DAG_DOC = """
### 목적
보유 이력이 있는 상품의 일봉을 1년치 유지한다 (`market.product_price_daily`, 펀드는 `market.fund_price_daily` 빈 날짜).

| Task | 내용 |
|---|---|
| `get_dates` | 실행일 T·상한 T-1 (KST, 늦은 run 도 skip 안 함) |
| `wait_for_token` / `wait_for_asset_daily` | 시세용 KIS 토큰 · T-1 asset_daily(KIS·업비트 활성 계좌 전부) 대기 |
| `upsert_products` | asset_daily → `market.product` (이름·보유구간·is_tracked) |
| `plan_fetch` | 상품별 수집 창 (신규 1년, 증분, 최근 5거래일 재수집, 재매수 최대 1년 · 펀드는 빈 거래일 있을 때만) |
| `ingest_prices` (소스별) | 수집 → 원주가 upsert (상품 단위 격리, 실패 상품이 있으면 마지막에 실패) |
| `ingest_fund_nav` | KOFIA 기준가 → fund_price_daily 빈 날짜만 (창에 빈 거래일이 있을 때만 호출 — 평소엔 06:55 가 채워 0회. config `price_ingest.fund_nav_enabled=false` 면 skip) |
| `check_tracked_coverage` | 추적 종목이 3거래일 넘게 뒤처지면 실패·알림 |
| `fail_if_upstream_failed` | 앞 단계 실패가 있으면 run 실패 (마지막 점검 성공이 실패를 가리지 않게) |

수동: `product_codes`(비우면 추적 종목) · `start_date` · `end_date` · `full_refresh`
"""


@dag(
    dag_id="market_ingest_product_price_daily",
    description="매일 07:10 상품 목록·일별 시세 빈 구간 채우기 (KIS·Upbit·KOFIA)",
    doc_md=DAG_DOC,
    schedule="10 7 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=KST),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "haejun", "retries": 1, "retry_delay": timedelta(minutes=5),
                  "on_failure_callback": discord_failure_callback},
    on_success_callback=discord_recovery_callback,
    tags=["market", "price", "KIS", "Upbit", "KOFIA", "daily", "ingestion"],
    params={
        "product_codes": Param([], type="array", items={"type": "string"},
                               description="비우면 추적 종목 전부, 지정하면 이 코드만 (매도 종목 백필 가능)"),
        "start_date": Param("", type="string", description="YYYY-MM-DD, 비우면 자동 (빈 구간)"),
        "end_date": Param("", type="string", description="YYYY-MM-DD, 비우면 T-1"),
        "full_refresh": Param(False, type="boolean", description="적재분 무시하고 창 전체 재수집"),
    },
)
def market_ingest_product_price_daily():
    """
    Purpose: DAG 구성 (센서 → 상품 upsert → 계획 → 소스별 수집 → 커버리지 점검 → 실패 전파)
    Parameter: 없음
    Input: 없음
    Output: 없음
    """
    sensor = {"poke_interval": 30, "timeout": 1800, "mode": "reschedule"}

    @task.sensor(task_id="wait_for_token", **sensor)
    def wait_for_token() -> bool:
        """
        Purpose: 시세용 KIS 키의 유효 토큰 대기 (직접 발급하지 않음)
        Parameter: 없음
        Input: secrets 파일, tokens.json
        Output: 유효 토큰이 있으면 True (30분 초과 시 센서 실패 + 알림)
        """
        from asset_flow.secrets_config import load_secrets
        from asset_flow.tokens import TokenManager

        cfg = load_secrets()
        return TokenManager(cfg).has_valid_token(cfg.services.exchange_rate_credential)

    @task(task_id="get_dates")
    def get_dates() -> dict:
        """
        Purpose: 실행일 T 와 상한 T-1 (KST, 늦은 run 도 skip 하지 않음)
        Parameter: 없음
        Input: 태스크 컨텍스트
        Output: {"base_date": T, "end": T-1} (YYYY-MM-DD)
        """
        t = kst_run_end(get_current_context())
        return {"base_date": t.to_date_string(), "end": t.subtract(days=1).to_date_string()}

    @task.sensor(task_id="wait_for_asset_daily", inlets=[ASSET_DAILY], **sensor)
    def wait_for_asset_daily(dates: dict) -> bool:
        """
        Purpose: T-1 asset_daily 적재 대기 (API 수집 계좌 전부 — 신규 종목 누락 방지)
        Parameter:
            - dates: get_dates 결과
        Input: secrets 파일, account.asset_daily
        Output: 활성 API 수집 계좌(수기 제외)가 모두 기준일 행을 가지면 True (30분 초과 시 실패 + 알림)
        """
        from asset_flow import db_manager as db
        from asset_flow.secrets_config import MANUAL_KINDS, load_secrets

        codes = [a.account_code for a in load_secrets().active_accounts() if a.kind not in MANUAL_KINDS]
        return db.has_asset_rows(db.asset_engine(), dates["end"], codes)

    @task(task_id="upsert_products", inlets=[ASSET_DAILY], outlets=[PRODUCT])
    def upsert_products(dates: dict) -> dict:
        """
        Purpose: asset_daily 상품 → market.product
        Parameter:
            - dates: get_dates 결과
        Input: account.asset_daily, config price_ingest
        Output: {"total", "tracked", "new", "changed"} (+ DB 쓰기)
        """
        from asset_flow import db_manager as db
        from asset_flow.app_config import load_app_config

        res = db.upsert_products(db.asset_engine(), dates["base_date"],
                                 load_app_config().price_ingest.tracked_after_sale_days)
        mark(PRODUCT, data_date=dates["end"], rows=res["changed"], base_date=dates["base_date"], **res)
        print(f"상품 {res}")
        return res

    @task(task_id="plan_fetch", inlets=[TRADING_HOLIDAY])
    def plan_fetch(dates: dict) -> dict:
        """
        Purpose: 상품별 수집 창 계산
        Parameter:
            - dates: get_dates 결과
        Input: market.product·시세 테이블, market.trading_holiday, config, Param
        Output: {price_source: [{"product_code","start","end","is_new"}]} (수 KB)
        """
        from asset_flow.pipeline.price_plan import build_plan

        return build_plan(dates, get_current_context()["params"])

    @task(task_id="ingest_prices", inlets=[SOURCE_KIS, SOURCE_UPBIT], outlets=[PRODUCT_PRICE],
          map_index_template="{{ source }}", max_active_tis_per_dagrun=3)
    def ingest_prices(source: str, plan: dict, dates: dict) -> dict:
        """
        Purpose: 소스 1개 수집·적재 (상품 단위 격리)
        Parameter:
            - source: KIS_KR_DAILY | KIS_US_DAILY | UPBIT_DAY
            - plan: plan_fetch 결과
            - dates: get_dates 결과
        Input: 소스 API, 업무 DB
        Output: {"rows","products","failed"} (+ DB 쓰기). 실패 상품이 있으면 RuntimeError
        """
        from asset_flow.pipeline.price_plan import run_source

        get_current_context()["source"] = source
        res = run_source(source, plan.get(source, []))
        mark(PRODUCT_PRICE, data_date=dates["end"], rows=res["rows"], source=source, products=res["products"],
             end=dates["end"])
        if res["failed"]:
            raise RuntimeError(f"[{source}] 실패 {len(res['failed'])}개: {res['failed']}")
        return res

    @task(task_id="ingest_fund_nav", inlets=[SOURCE_KOFIA], outlets=[FUND_PRICE])
    def ingest_fund_nav(plan: dict, dates: dict) -> dict:
        """
        Purpose: KOFIA 기준가 → fund_price_daily 빈 날짜만 (기존 행 불변)
        Parameter:
            - plan: plan_fetch 결과
            - dates: get_dates 결과
        Input: config price_ingest.fund_nav_enabled, dis.kofia.or.kr, 업무 DB
        Output: {"rows","products","failed"} (+ DB INSERT). 스위치 off 면 skip, 실패 펀드가 있으면 RuntimeError
        """
        from airflow.sdk.exceptions import AirflowSkipException

        from asset_flow.app_config import load_app_config
        from asset_flow.pipeline.price_plan import run_source

        if not load_app_config().price_ingest.fund_nav_enabled:
            raise AirflowSkipException("price_ingest.fund_nav_enabled=false")
        res = run_source("FUND_NAV", plan.get("FUND_NAV", []))
        mark(FUND_PRICE, data_date=dates["end"], rows=res["rows"], source="KOFIA", end=dates["end"])
        if res["failed"]:
            raise RuntimeError(f"[FUND_NAV] 실패: {res['failed']}")
        return res

    @task(task_id="check_tracked_coverage", inlets=[TRADING_HOLIDAY], retries=0, trigger_rule="all_done")
    def check_tracked_coverage(dates: dict) -> None:
        """
        Purpose: 추적 종목 최신성 점검 (거래정지·상장폐지·소스 이상 조기 발견)
        Parameter:
            - dates: get_dates 결과
        Input: market.product, market.product_price_daily, market.trading_holiday
        Output: 없음. 3거래일 넘게 뒤처진 종목이 있으면 RuntimeError → 알림
        """
        from asset_flow.pipeline.price_plan import stale_tracked

        if stale := stale_tracked(dates):
            raise RuntimeError(f"시세 지연 {len(stale)}개: {stale}")

    dates = get_dates()
    plan = plan_fetch(dates)
    [wait_for_token(), wait_for_asset_daily(dates)] >> upsert_products(dates) >> plan
    done = [ingest_prices.partial(plan=plan, dates=dates).expand(source=PRICE_SOURCES), ingest_fund_nav(plan, dates)]
    check = check_tracked_coverage(dates)
    done >> check
    [*done, check] >> fail_if_upstream_failed()


market_ingest_product_price_daily()
