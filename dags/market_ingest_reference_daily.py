"""
시장 기준 데이터 수집·적재 DAG (매일 06:55 KST) — 환율 · 펀드 기준가 · 주요 지수

- 보유 자산과 무관한 시장 정보를 TaskGroup 3개로 독립 수집 (그룹 사이 의존 없음 — 한 그룹 실패가 다른 그룹을 막지 않음)
- 공통 선행은 KIS 토큰 대기 하나 (환율·지수만, 펀드는 KOFIA)
- 늦은 run skip 은 그룹별: 환율·펀드는 적용(현재값 API), 지수는 미적용(이력 API — 과거를 덮지 않음)
- 후행: account_ingest_asset_daily (07:00, 환율·기준가 데이터 센서), 시세·배당 DAG
- 토큰 발급 안 함 (06:50 auth_issue_api_tokens 발급분 사용)
"""

from datetime import datetime, timedelta

from airflow.sdk import Param, dag, get_current_context, task, task_group

from alerts import discord_failure_callback, discord_recovery_callback
from asset_flow.assets import (EXCHANGE_RATE, FUND_PRICE, MARKET_INDEX, MV_COST_BASIS_KRW, MV_POSITION_DAILY,
                               SOURCE_FUNDGUIDE, SOURCE_KIS, SOURCE_KOFIA, TRADING_HOLIDAY, mark)
from asset_flow.runtime import KST, kst_run_end, standard_dates

GROUPS = ["exchange_rate", "fund_price", "market_index"]
KIS_GROUPS = {"exchange_rate", "market_index"}

DAG_DOC = """
### 목적
보유 자산과 무관한 시장 기준 데이터를 매일 적재한다. 그룹끼리는 서로 기다리지 않는다.

| 그룹 | 적재 | 내용 |
|---|---|---|
| `exchange_rate` | `market.exchange_rate_daily` | USD·JPY·GBP·EUR 원화 환율 T-1 (달력일, 주말 = 직전 고시). 수동 `start_date` 면 이력 백필(빈 날만) |
| `fund_price` | `market.fund_price_daily` | 연금저축·IRP·DC 펀드 T-1 기준가 (KOFIA, 없거나 지연된 코드만 fundguide) |
| `market_index` | `market.index_daily` | 코스피·코스닥·S&P 500·나스닥 종합 일봉 (거래일만, 최근 5거래일 재수집, 신규면 from_date 부터) |

| Task | 내용 |
|---|---|
| `wait_for_token` | 시세용 KIS 키 유효 토큰 대기 (환율·지수 선행, 둘 다 미선택이면 skip) |
| `exchange_rate.get_standard_date` | 기준일 T-1 (늦은 정기 run · 백필 run 이면 skip) |
| `exchange_rate.fetch/transform/upload_exchange_rates` | KIS 현재값 → 교차 환산 → 기준일 DELETE 후 INSERT |
| `exchange_rate.backfill_exchange_rates` | `start_date`~`end_date` 이력 → 달력일 이월 → 빈 날만 INSERT (기존 행 불변) |
| `fund_price.*` | 기준일 · 대상 코드 → 수집 → 적재 → 누락 점검 · 크롤러 대조 |
| `market_index.plan_index_windows` | 지수별 창 (KR 상한 T-1, US 상한 = 마감된 마지막 미국 날짜) |
| `market_index.collect_index_prices` | KIS 이력 조회 → 창 끝 거래일 행 검증 (없으면 실패·재시도) |
| `market_index.upload_index_prices` | PK upsert (값이 바뀐 행만 update_datetime 갱신) |
| `market_index.refresh_dashboard_views` | config `reference_ingest.market_index.refresh_dashboard_views=true` 일 때만 |

수동: `groups`(실행할 그룹) · `start_date`/`end_date`(환율 백필 · 지수 창) · `extra_fund_codes`
"""


def skip_unless_selected(group: str) -> None:
    """
    Purpose: 수동 run 의 groups Param 에 없는 그룹이면 skip (그룹 첫 태스크에서 호출 → 하류 전체 skip)
    Parameter:
        - group: 그룹 id
    Input: 태스크 컨텍스트 params
    Output: 없음. 미선택이면 AirflowSkipException, start_date 인데 그룹이 여럿이면 ValueError
    """
    from airflow.sdk.exceptions import AirflowSkipException

    params = get_current_context()["params"]
    if group not in params["groups"]:
        raise AirflowSkipException(f"groups Param 에 {group} 없음")
    if params["start_date"] and len(params["groups"]) > 1:
        raise ValueError("start_date 는 그룹 하나만 골라 실행 (환율 백필과 지수 재수집이 같은 날짜를 쓰지 않게)")


@dag(
    dag_id="market_ingest_reference_daily",
    description="매일 06:55 시장 기준 데이터 수집·적재 (환율 · 펀드 기준가 · 주요 지수)",
    doc_md=DAG_DOC,
    schedule="55 6 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=KST),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "haejun", "retries": 1, "retry_delay": timedelta(minutes=5),
                  "on_failure_callback": discord_failure_callback},
    on_success_callback=discord_recovery_callback,
    tags=["market", "exchange-rate", "fund", "index", "KIS", "KOFIA", "daily", "ingestion"],
    params={
        "groups": Param(GROUPS, type="array", items={"type": "string", "enum": GROUPS}, minItems=1, uniqueItems=True,
                        description="실행할 그룹 (수동 백필은 하나만 고르기)"),
        "start_date": Param("", type="string",
                            description="YYYY-MM-DD. 환율: 지정하면 이 날부터 이력 백필(빈 날만) / 지수: 창 시작"),
        "end_date": Param("", type="string", description="YYYY-MM-DD, 비우면 T-1 (지수 US 는 마감된 마지막 날)"),
        "extra_fund_codes": Param([], type="array", items={"type": "string"},
                                  description="추가로 수집할 펀드 표준코드"),
    },
)
def market_ingest_reference_daily():
    """
    Purpose: DAG 구성 (토큰 대기 → 환율 · 지수 그룹, 펀드 그룹은 독립)
    Parameter: 없음
    Input: 없음
    Output: 없음
    """

    @task.sensor(task_id="wait_for_token", poke_interval=30, timeout=600, mode="reschedule")
    def wait_for_token() -> bool:
        """
        Purpose: 시세용 KIS 키의 유효 토큰 대기 (직접 발급하지 않음)
        Parameter: 없음
        Input: secrets 파일, tokens.json, params groups
        Output: 유효 토큰이 있으면 True (10분 초과 시 센서 실패 + 알림). 환율·지수 둘 다 미선택이면 skip
        """
        from airflow.sdk.exceptions import AirflowSkipException

        from asset_flow.secrets_config import load_secrets
        from asset_flow.tokens import TokenManager

        if not KIS_GROUPS & set(get_current_context()["params"]["groups"]):
            raise AirflowSkipException("KIS 를 쓰는 그룹 미선택")
        cfg = load_secrets()
        return TokenManager(cfg).has_valid_token(cfg.services.exchange_rate_credential)

    def kis_client():
        """
        Purpose: 시세용 KIS 클라이언트 (기존 토큰)
        Parameter: 없음
        Input: secrets 파일, 토큰 저장소
        Output: KISApiClient
        """
        from asset_flow.clients.kis_client import KISApiClient
        from asset_flow.secrets_config import kis_client_config, load_secrets
        from asset_flow.tokens import TokenManager

        cfg = load_secrets()
        cred = cfg.credential(cfg.services.exchange_rate_credential)
        return KISApiClient(TokenManager(cfg).token_for(cred.id), kis_client_config(cred))

    @task_group(group_id="exchange_rate")
    def exchange_rate(token) -> None:
        """
        Purpose: 환율 그룹 (매일 T-1 현재값 · 수동 이력 백필)
        Parameter:
            - token: wait_for_token 태스크
        Input: 없음
        Output: 없음
        """

        @task(task_id="get_standard_date")
        def get_standard_date() -> str:
            """
            Purpose: 기준일 T-1 (KST) — 백필 run 이면 매일 경로 skip (현재값이 과거 기준일을 덮지 않게)
            Parameter: 없음
            Input: 태스크 컨텍스트
            Output: YYYY-MM-DD. 미선택·백필 run·늦은 정기 run 이면 AirflowSkipException
            """
            from airflow.sdk.exceptions import AirflowSkipException

            skip_unless_selected("exchange_rate")
            if get_current_context()["params"]["start_date"]:
                raise AirflowSkipException("start_date 지정 = 백필 run → 매일 경로 skip")
            return standard_dates(get_current_context())["standard_date"]

        @task(task_id="fetch_exchange_rates", inlets=[SOURCE_KIS], retries=3, retry_delay=timedelta(seconds=20),
              retry_exponential_backoff=2.0)
        def fetch_exchange_rates(standard_date: str) -> list:
            """
            Purpose: KIS 환율 원시 응답 조회
            Parameter:
                - standard_date: 기준일
            Input: secrets 파일, 토큰 저장소, KIS API
            Output: 통화별 원시 JSON 리스트
            """
            from asset_flow.config.kis import KIS

            return kis_client().get_exchange_rate(standard_date, KIS.EXCHANGE_RATE_PRODUCT_CODES)

        @task(task_id="transform_exchange_rates")
        def transform_exchange_rates(standard_date: str, raw_list: list) -> list:
            """
            Purpose: 원시 응답 → 원화 기준 레코드
            Parameter:
                - standard_date: 기준일
                - raw_list: fetch_exchange_rates 결과
            Input: 없음
            Output: EXCHANGE_RATE_COLUMNS 레코드 리스트
            """
            from asset_flow.transformers.kis_transformer import transform_exchange_rate

            return transform_exchange_rate(raw_list, standard_date).to_dict("records")

        @task(task_id="upload_exchange_rates", outlets=[EXCHANGE_RATE])
        def upload_exchange_rates(standard_date: str, records: list) -> int:
            """
            Purpose: 환율 적재 (기준일 단위 멱등)
            Parameter:
                - standard_date: 기준일
                - records: 변환 레코드
            Input: config tables.exchange_rate_daily
            Output: 적재 행 수 (+ DB 쓰기, 쓰기 가드 적용)
            """
            from asset_flow import db_manager as db
            from asset_flow.app_config import load_app_config

            table = load_app_config().tables.exchange_rate_daily
            n = db.replace_daily_rows(db.asset_engine(), table.db_schema, table.table_name, standard_date, records)
            mark(EXCHANGE_RATE, data_date=standard_date, rows=n, standard_date=standard_date)
            return n

        @task(task_id="backfill_exchange_rates", inlets=[SOURCE_KIS], outlets=[EXCHANGE_RATE], retries=2,
              retry_delay=timedelta(seconds=30))
        def backfill_exchange_rates() -> int:
            """
            Purpose: 환율 이력 백필 — start_date~end_date 달력일, 빈 날만 추가 (수동 run 전용)
            Parameter: 없음
            Input: params start_date·end_date, KIS API
            Output: 추가 행 수 (+ DB INSERT ... DO NOTHING). start_date 없으면 skip, end 는 T-1 로 제한.
                    이력에 빈 날이 있으면 RuntimeError
            """
            from airflow.sdk.exceptions import AirflowSkipException

            from asset_flow import db_manager as db
            from asset_flow.pipeline.reference_data import collect_exchange_rate_history

            skip_unless_selected("exchange_rate")
            params = get_current_context()["params"]
            if not params["start_date"]:
                raise AirflowSkipException("start_date 없음 = 매일 run")
            cap = standard_dates(get_current_context())["standard_date"]
            end = min(params["end_date"] or cap, cap)       # T 이후는 이월값이 미래 날짜를 채우므로 금지
            if params["start_date"] > end:
                raise ValueError(f"start_date {params['start_date']} > end {end} (상한 T-1)")
            records = collect_exchange_rate_history(kis_client(), params["start_date"], end)
            n = db.insert_missing_exchange_rates(db.asset_engine(), records)
            mark(EXCHANGE_RATE, data_date=end, rows=n, data_from=params["start_date"], data_to=end,
                 start=params["start_date"], end=end)
            print(f"환율 백필 {params['start_date']}~{end}: 이력 {len(records)}행 중 {n}행 추가 (나머지는 기존 행)")
            return n

        std_date = get_standard_date()
        raw_list = fetch_exchange_rates(std_date)
        token >> [raw_list, backfill_exchange_rates()]
        upload_exchange_rates(std_date, transform_exchange_rates(std_date, raw_list))

    @task_group(group_id="fund_price")
    def fund_price() -> None:
        """
        Purpose: 펀드 기준가 그룹 (KOFIA 주 경로, 빠진 코드만 fundguide)
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
            Output: YYYY-MM-DD. 미선택·늦은 정기 run 이면 AirflowSkipException
            """
            skip_unless_selected("fund_price")
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

        @task(task_id="collect_fund_prices", inlets=[SOURCE_KOFIA, SOURCE_FUNDGUIDE, TRADING_HOLIDAY])
        def collect_fund_prices(standard_date: str, codes: list[str]) -> dict:
            """
            Purpose: 기준가 수집 (KOFIA → 빠진 코드만 fundguide 크롤링)
            Parameter:
                - standard_date: 기준일
                - codes: 펀드코드 리스트
            Input: KOFIA, (대체) fundguide.net, market.fund_price_daily(이름)·trading_holiday, config funds
            Output: {"records": fund_price_daily 레코드, "source": {코드: 출처}} — 끝내 빠진 코드는 records 에 없음
            """
            from datetime import date

            from asset_flow import db_manager as db
            from asset_flow.app_config import load_app_config
            from asset_flow.clients.kofia_client import KofiaClient
            from asset_flow.crawler.fund_crawler import crawl_fund_prices as crawl
            from asset_flow.pipeline.fund_nav import LOOKBACK_DAYS, collect_fund_prices as collect

            engine, std = db.asset_engine(), date.fromisoformat(standard_date)
            hol = db.get_holiday_sets(engine, (std - timedelta(days=LOOKBACK_DAYS)).isoformat(), standard_date)["KR"]
            fallback = crawl if load_app_config().funds.crawler_fallback else None
            records, source = collect(codes, standard_date, db.get_fund_names(engine, codes), hol, KofiaClient(),
                                      fallback)
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
            mark(FUND_PRICE, data_date=standard_date, rows=n, standard_date=standard_date)
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
                raise RuntimeError(f"[{standard_date}] 기준가 미수집 {len(missing)}개: {missing} — "
                                   f"KOFIA 공시 확인 후 수동 재실행")
            print(f"[{standard_date}] 기준가 {len(codes)}개 모두 적재됨")

        @task(task_id="compare_with_fundguide", inlets=[SOURCE_FUNDGUIDE], retries=0, max_active_tis_per_dag=1)
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

    @task_group(group_id="market_index")
    def market_index(token) -> None:
        """
        Purpose: 주요 지수 그룹 (거래일만, 최근 n 거래일 재수집, 신규면 from_date 부터)
        Parameter:
            - token: wait_for_token 태스크
        Input: 없음
        Output: 없음
        """

        @task(task_id="plan_index_windows", inlets=[TRADING_HOLIDAY])
        def plan_index_windows() -> dict:
            """
            Purpose: 지수별 수집 창 (늦은 run 도 skip 안 함 — 이력 API)
            Parameter: 없음
            Input: market.index_daily, market.trading_holiday, config reference_ingest.market_index, params
            Output: {"windows": [{"index_code","market","start","end"}], "holiday_from": YYYY-MM-DD}. 미선택이면 skip
            """
            from datetime import date

            from asset_flow import db_manager as db
            from asset_flow.app_config import load_app_config
            from asset_flow.pipeline import reference_data as rd

            skip_unless_selected("market_index")
            ctx = get_current_context()
            base, cfg = kst_run_end(ctx).date(), load_app_config().reference_ingest.market_index
            p_start = ctx["params"]["start_date"]
            hol_from = (min(cfg.from_date, date.fromisoformat(p_start)) if p_start else cfg.from_date)
            hol_from = (hol_from - timedelta(days=40)).isoformat()
            engine = db.asset_engine()
            holidays = db.get_holiday_sets(engine, hol_from, base.isoformat())
            windows = rd.plan_index_windows(db.get_index_last_dates(engine), base, holidays, cfg.from_date,
                                            cfg.refetch_trading_days, ctx["params"])
            print(f"T={base} 창: {windows}")
            return {"windows": windows, "holiday_from": hol_from}

        @task(task_id="collect_index_prices", inlets=[SOURCE_KIS, TRADING_HOLIDAY], retries=3,
              retry_delay=timedelta(seconds=30), retry_exponential_backoff=2.0)
        def collect_index_prices(plan: dict) -> list:
            """
            Purpose: 지수 이력 조회 → index_daily 레코드 (창 끝 거래일 행 검증)
            Parameter:
                - plan: plan_index_windows 결과
            Input: KIS API, market.trading_holiday
            Output: INDEX_COLUMNS 레코드 리스트 (지수 4개 × 최대 1년 ≈ 1천 행). 창 끝 거래일 행이 없으면 RuntimeError
            """
            from asset_flow import db_manager as db
            from asset_flow.pipeline import reference_data as rd

            if not plan["windows"]:
                return []
            last_end = max(w["end"] for w in plan["windows"])
            holidays = db.get_holiday_sets(db.asset_engine(), plan["holiday_from"], last_end)
            return rd.collect_index_prices(kis_client(), plan["windows"], holidays)

        @task(task_id="upload_index_prices", outlets=[MARKET_INDEX])
        def upload_index_prices(plan: dict, records: list) -> int:
            """
            Purpose: 지수 일봉 upsert
            Parameter:
                - plan: plan_index_windows 결과 (이벤트 메모용)
                - records: collect_index_prices 결과
            Input: 없음
            Output: 넘긴 행 수 (+ market.index_daily 쓰기, 쓰기 가드 적용)
            """
            from asset_flow import db_manager as db

            n = db.upsert_index_prices(db.asset_engine(), records)
            if ws := plan["windows"]:
                start, end = min(w["start"] for w in ws), max(w["end"] for w in ws)
                mark(MARKET_INDEX, data_date=end, rows=n, data_from=start, data_to=end, end=end)
            else:                                            # 창 없음 = 이미 최신 → T-1 기준 변경 없음
                t_1 = kst_run_end(get_current_context()).subtract(days=1).to_date_string()
                mark(MARKET_INDEX, data_date=t_1, rows=n)
            print(f"지수 {n}행 upsert: {[(w['index_code'], w['start'], w['end']) for w in plan['windows']]}")
            return n

        @task(task_id="refresh_dashboard_views", retries=1, retry_delay=timedelta(seconds=30),
              outlets=[MV_POSITION_DAILY, MV_COST_BASIS_KRW])
        def refresh_dashboard_views() -> None:
            """
            Purpose: 대시보드 MV 갱신 (config reference_ingest.market_index.refresh_dashboard_views=true 일 때만)
            Parameter: 없음
            Input: config reference_ingest.market_index.refresh_dashboard_views, dashboard.refresh_procedure
            Output: 없음 (+ 프로시저 CALL, MV Asset 이벤트). 스위치 off 면 skip
            """
            from airflow.sdk.exceptions import AirflowSkipException

            from asset_flow import db_manager as db
            from asset_flow.app_config import load_app_config

            app = load_app_config()
            if not app.reference_ingest.market_index.refresh_dashboard_views:
                raise AirflowSkipException("refresh_dashboard_views=false — 지수는 아직 MV 입력 아님")
            db.call_procedure(db.asset_engine(), app.dashboard.refresh_procedure)

        plan = plan_index_windows()
        records = collect_index_prices(plan)
        token >> records
        upload_index_prices(plan, records) >> refresh_dashboard_views()

    token = wait_for_token()
    exchange_rate(token)
    fund_price()
    market_index(token)


market_ingest_reference_daily()
