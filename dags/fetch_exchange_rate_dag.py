from airflow.decorators import task, dag
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pendulum
import pandas as pd
from sqlalchemy import text

kst = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'haejun',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id='fetch_exchange_rate',
    default_args=default_args,
    description='매일 06:55 환율 정보 수집 및 DB 적재 (USD, JPY, GBP, EUR)',
    schedule='55 6 * * *',
    start_date=datetime(2024, 1, 1, tzinfo=kst),
    catchup=False,
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
        context = get_current_context()
        utc_end = context.get('data_interval_end')
        kst_end = utc_end.in_timezone("Asia/Seoul")
        standard_date = (kst_end - relativedelta(days=1)).strftime('%Y-%m-%d')
        print(f"기준일자: [{standard_date}]")
        return standard_date

    @task(task_id='fetch_exchange_rates')
    def fetch_exchange_rates(standard_date: str) -> list:
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
        from asset_flow.transformers.kis_transformer import transform_exchange_rate

        df = transform_exchange_rate(raw_list, standard_date)
        if df.empty:
            print("변환된 환율 데이터가 없습니다.")
            return []

        print(f"변환 완료: {len(df)}건")
        return df.to_dict('records')

    @task(task_id='upload_exchange_rates')
    def upload_exchange_rates(standard_date: str, records: list) -> None:
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
