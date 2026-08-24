"""
환율 데이터 수집 및 DB 적재 DAG

keywords: exchange rate, KIS, USD, JPY, GBP, EUR, market data, PostgreSQL

매일 06:55 KST에 실행되어 한국투자증권 API(FHKST03030100)에서 전일(T-1) 기준
USD·JPY·GBP·EUR 환율을 수집하고 market.exchange_rate_daily 테이블에 적재한다.
GBP·EUR는 KIS API가 달러 기준으로 반환하므로, 원/달러 기준가를 곱해 원화 기준으로 교차 환산한다.

스케줄: 매일 06:55 KST (cron: '55 6 * * *')
의존성: make_token (ExternalTaskSensor)
후행 DAG: fetch_asset_daily (ExternalTaskSensor로 연결)
적재 테이블: market.exchange_rate_daily
"""

from airflow.decorators import task, dag
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from callbacks import slack_failure_callback, slack_recovery_callback
import pendulum
import pandas as pd
from sqlalchemy import text

kst = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'haejun',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': slack_failure_callback,
}

DAG_DOC = """
### 목적
USD·JPY·GBP·EUR 환율을 매일 수집해 원화 기준으로 통일 저장한다.
`fetch_asset_daily`가 해외주식 평가금액을 원화로 환산할 때 이 값을 쓴다.

### Pipeline
1. `get_standard_date`: 실행일 기준 T-1(전일) 계산
2. `fetch_exchange_rates`: KIS API로 4개 통화쌍 원시 데이터 수집
3. `transform_exchange_rates`: GBP·EUR는 KIS가 달러 기준으로 주므로,
   원/달러 기준가를 곱해 원화 기준으로 교차 환산
4. `upload_exchange_rates`: 해당 날짜 기존 행 DELETE 후 INSERT (멱등 적재)

### Task
| Task | 내용 |
|---|---|
| `wait_for_token` | `make_token` 완료 대기 |
| `get_standard_date` | 기준일(T-1) 계산 |
| `fetch_exchange_rates` | KIS API 원시 환율 수집 |
| `transform_exchange_rates` | 원화 기준 교차 환산 |
| `upload_exchange_rates` | DB 적재 |
"""


@dag(
    dag_id='fetch_exchange_rate',
    default_args=default_args,
    description='매일 06:55 환율 정보 수집 및 DB 적재 (USD, JPY, GBP, EUR)',
    doc_md=DAG_DOC,
    schedule='55 6 * * *',
    start_date=datetime(2024, 1, 1, tzinfo=kst),
    catchup=False,
    tags=['market', 'exchange-rate', 'KIS', 'daily', 'ingestion'],
    on_success_callback=slack_recovery_callback,
    params={
        'exchange_rate': Param({
            'schema': 'market',
            'table_name': 'exchange_rate_daily',
        }),
    },
)
def fetch_exchange_rate_dag():

    wait_for_token = ExternalTaskSensor(
        task_id='wait_for_token',
        external_dag_id='make_token',
        external_task_id='generate_tokens',
        execution_delta=timedelta(minutes=5),
        timeout=600,
        poke_interval=30,
        mode='poke',
    )

    @task(task_id='get_standard_date')
    def get_standard_date() -> str:
        """
        Airflow data_interval_end 기준 전일 날짜(T-1) 반환

        Returns:
            standard_date: 기준일자 문자열 (YYYY-MM-DD)
        """
        context = get_current_context()
        utc_end = context.get('data_interval_end')
        kst_end = utc_end.in_timezone("Asia/Seoul")
        standard_date = (kst_end - relativedelta(days=1)).strftime('%Y-%m-%d')
        print(f"기준일자: [{standard_date}]")
        return standard_date

    @task(task_id='fetch_exchange_rates', retries=3, retry_delay=timedelta(seconds=20), retry_exponential_backoff=True)
    def fetch_exchange_rates(standard_date: str) -> list:
        """
        KIS API에서 USD·JPY·GBP·EUR 환율 원시 데이터 수집

        KIS.EXCHANGE_RATE_PRODUCT_CODES에 정의된 4개 통화쌍을 순차 조회한다.

        Args:
            standard_date: 기준일자 (YYYY-MM-DD, T-1)

        Returns:
            list: 통화별 KIS API 원시 JSON 응답 리스트
        """
        from asset_flow.clients.kis_client import KISApiClient
        from asset_flow.managers.token_manager import TokenManager
        from asset_flow.config.kis import KIS
        from airflow.models import Variable

        tokens = TokenManager().GetTokens()
        config = Variable.get('KIS_STOCK', deserialize_json=True)

        kis = KISApiClient(tokens['KIS_STOCK'], config)
        raw_list = kis.get_exchange_rate(standard_date, KIS.EXCHANGE_RATE_PRODUCT_CODES)

        print(f"환율 수집 완료: {len(raw_list)}개 통화")
        return raw_list

    @task(task_id='transform_exchange_rates')
    def transform_exchange_rates(standard_date: str, raw_list: list) -> list:
        """
        환율 원시 응답 → EXCHANGE_RATE_COLUMNS 레코드 변환

        GBP·EUR 교차 환산(달러 기준 → 원화 기준) 포함.

        Args:
            standard_date: 기준일자 (YYYY-MM-DD)
            raw_list: fetch_exchange_rates() 결과 원시 JSON 리스트

        Returns:
            list: EXCHANGE_RATE_COLUMNS 스키마의 dict 레코드 리스트
        """
        from asset_flow.transformers.kis_transformer import transform_exchange_rate

        df = transform_exchange_rate(raw_list, standard_date)
        if df.empty:
            print("변환된 환율 데이터가 없습니다.")
            return []

        print(f"변환 완료: {len(df)}건")
        return df.to_dict('records')

    @task(task_id='upload_exchange_rates')
    def upload_exchange_rates(standard_date: str, records: list) -> None:
        """
        변환된 환율 레코드를 market.exchange_rate_daily 테이블에 적재

        기존 standard_date 행을 먼저 삭제(DELETE)한 뒤 신규 삽입(INSERT)하여 멱등성을 보장한다.

        Args:
            standard_date: 기준일자 (YYYY-MM-DD) — DELETE 조건 및 로그 출력용
            records: transform_exchange_rates() 결과 레코드 리스트
        """
        if not records:
            print("적재할 환율 데이터가 없습니다.")
            return

        context = get_current_context()
        params = context.get('params')['exchange_rate']
        schema = params['schema']
        table_name = params['table_name']
        full_table_name = f"{schema}.{table_name}"

        df = pd.DataFrame(records)

        pg_hook = PostgresHook(postgres_conn_id='postgres_asset')
        engine = pg_hook.get_sqlalchemy_engine()

        with engine.begin() as conn:
            conn.execute(
                text(f"DELETE FROM {full_table_name} WHERE standard_date = :std_date"),
                {"std_date": standard_date},
            )
            df.to_sql(
                name=table_name,
                con=conn,
                schema=schema,
                if_exists='append',
                index=False,
            )

        print(f"[{standard_date}] 환율 데이터 적재 완료: {len(df)}건")

    # 의존성 구성
    std_date = get_standard_date()
    raw_list = fetch_exchange_rates(std_date)
    records = transform_exchange_rates(std_date, raw_list)

    wait_for_token >> raw_list
    upload_exchange_rates(std_date, records)


fetch_exchange_rate_dag()
