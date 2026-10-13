"""
환율(USD·JPY·GBP·EUR) 수집·적재 DAG (매일 06:55 KST)

- KIS FHKST03030100 으로 T-1 환율 조회, GBP·EUR 는 원/달러로 교차 환산
- 키: secrets services.exchange_rate_credential / 적재: config tables.exchange_rate_daily
- 선행: auth_issue_api_tokens.generate_tokens / 후행: account_ingest_asset_daily, account_ingest_dividend(미국 추정)
"""

from datetime import datetime, timedelta

from airflow.sdk import dag, get_current_context, task

from alerts import discord_failure_callback, discord_recovery_callback
from asset_flow.assets import EXCHANGE_RATE, mark
from asset_flow.runtime import KST, standard_dates

DAG_DOC = """
### 목적
USD·JPY·GBP·EUR 환율을 원화 기준으로 `market.exchange_rate_daily` 에 적재한다 (해외주식 원화 환산용).

| Task | 내용 |
|---|---|
| `wait_for_token` | 환율용 KIS 키의 유효 토큰이 tokens.json 에 생길 때까지 대기 (06:50 토큰 DAG 발급분) |
| `get_standard_date` | 기준일 T-1 (KST) |
| `fetch_exchange_rates` | KIS 원시 환율 조회 |
| `transform_exchange_rates` | 원화 기준 교차 환산 |
| `upload_exchange_rates` | 기준일 DELETE 후 INSERT |
"""


@dag(
    dag_id="market_ingest_exchange_rate_daily",
    description="매일 06:55 환율 수집·적재 (USD, JPY, GBP, EUR)",
    doc_md=DAG_DOC,
    schedule="55 6 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=KST),
    catchup=False,
    default_args={"owner": "haejun", "retries": 1, "retry_delay": timedelta(minutes=5),
                  "on_failure_callback": discord_failure_callback},
    on_success_callback=discord_recovery_callback,
    tags=["market", "exchange-rate", "KIS", "daily", "ingestion"],
)
def market_ingest_exchange_rate_daily():
    """
    Purpose: DAG 구성 (토큰 대기 → 기준일 → 조회 → 변환 → 적재)
    Parameter: 없음
    Input: 없음
    Output: 없음
    """

    @task.sensor(task_id="wait_for_token", poke_interval=30, timeout=600, mode="reschedule")
    def wait_for_token() -> bool:
        """
        Purpose: 환율용 KIS 키의 유효 토큰 대기 (직접 발급하지 않음)
        Parameter: 없음
        Input: secrets 파일, tokens.json
        Output: 유효 토큰이 있으면 True (10분 초과 시 센서 실패 + 알림)
        """
        from asset_flow.secrets_config import load_secrets
        from asset_flow.tokens import TokenManager

        cfg = load_secrets()
        return TokenManager(cfg).has_valid_token(cfg.services.exchange_rate_credential)


    @task(task_id="get_standard_date")
    def get_standard_date() -> str:
        """
        Purpose: 기준일 T-1 (KST)
        Parameter: 없음
        Input: 태스크 컨텍스트
        Output: YYYY-MM-DD
        """
        return standard_dates(get_current_context())["standard_date"]

    @task(task_id="fetch_exchange_rates", retries=3, retry_delay=timedelta(seconds=20), retry_exponential_backoff=2.0)
    def fetch_exchange_rates(standard_date: str) -> list:
        """
        Purpose: KIS 환율 원시 응답 조회
        Parameter:
            - standard_date: 기준일
        Input: secrets 파일, 토큰 저장소, KIS API
        Output: 통화별 원시 JSON 리스트
        """
        from asset_flow.clients.kis_client import KISApiClient
        from asset_flow.config.kis import KIS
        from asset_flow.secrets_config import kis_client_config, load_secrets
        from asset_flow.tokens import TokenManager

        cfg = load_secrets()
        cred = cfg.credential(cfg.services.exchange_rate_credential)
        kis = KISApiClient(TokenManager(cfg).token_for(cred.id), kis_client_config(cred))
        return kis.get_exchange_rate(standard_date, KIS.EXCHANGE_RATE_PRODUCT_CODES)

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
        mark(EXCHANGE_RATE, standard_date=standard_date, rows=n)
        return n

    std_date = get_standard_date()
    raw_list = fetch_exchange_rates(std_date)
    wait_for_token() >> raw_list
    upload_exchange_rates(std_date, transform_exchange_rates(std_date, raw_list))


market_ingest_exchange_rate_daily()
