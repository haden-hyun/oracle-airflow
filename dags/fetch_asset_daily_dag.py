from airflow.decorators import task, dag, task_group
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
    dag_id='fetch_asset_daily',
    default_args=default_args,
    description='매일 07:00 자산 현황 수집 및 DB 적재 (해외주식/국내주식/ISA/연금저축/CMA)',
    schedule='0 7 * * *',
    start_date=datetime(2024, 1, 1, tzinfo=kst),
    catchup=False,
    params={
        'daily_asset': Param({
            'schema': 'account',
            'table_name': 'asset_daily',
        }),
        'pension_fund_code': Param(
            'K553W5E17401',
            description='연금저축 펀드 표준코드',
        ),
    },
)
def fetch_asset_daily_dag():

    wait_for_exchange_rate = ExternalTaskSensor(
        task_id='wait_for_exchange_rate',
        external_dag_id='fetch_exchange_rate',
        external_task_id='upload_exchange_rates',
        execution_delta=timedelta(minutes=5),
        timeout=600,
        poke_interval=30,
        mode='poke',
    )

    wait_for_fund_price_daily = ExternalTaskSensor(
        task_id='wait_for_fund_price_daily',
        external_dag_id='fetch_fund_price_daily',
        external_task_id='upload_fund_prices',
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

    @task_group(group_id='get_daily_asset_group')
    def get_daily_asset_group(standard_date: str):

        @task(task_id='get_overseas_stock')
        def get_overseas_stock(standard_date: str) -> list:
            from asset_flow.clients.kis_client import KISApiClient
            from asset_flow.managers.token_manager import TokenManager
            from asset_flow.transformers.kis_transformer import transform_overseas_balance
            from airflow.models import Variable

            try:
                tokens = TokenManager().GetTokens()
                config = Variable.get('KIS_STOCK', deserialize_json=True)
                kis = KISApiClient(tokens['KIS_STOCK'], config)
                raw = kis.get_overseas_balance()
                df = transform_overseas_balance(raw, standard_date, config)
                if df.empty:
                    print("해외주식 데이터 없음")
                    return []
                print(f"해외주식 수집: {len(df)}건")
                return df.to_dict('records')
            except Exception as e:
                print(f"[ERROR] 해외주식 조회 실패: {e}")
                return []

        @task(task_id='get_domestic_stock')
        def get_domestic_stock(standard_date: str) -> list:
            from asset_flow.clients.kis_client import KISApiClient
            from asset_flow.managers.token_manager import TokenManager
            from asset_flow.transformers.kis_transformer import transform_domestic_balance
            from airflow.models import Variable

            try:
                tokens = TokenManager().GetTokens()
                config = Variable.get('KIS_STOCK', deserialize_json=True)
                kis = KISApiClient(tokens['KIS_STOCK'], config)
                raw = kis.get_domestic_balance()
                df = transform_domestic_balance(raw, standard_date, config)
                if df.empty:
                    print("국내주식 데이터 없음")
                    return []
                print(f"국내주식 수집: {len(df)}건")
                return df.to_dict('records')
            except Exception as e:
                print(f"[ERROR] 국내주식 조회 실패: {e}")
                return []

        @task(task_id='get_isa_stock')
        def get_isa_stock(standard_date: str) -> list:
            from asset_flow.clients.kis_client import KISApiClient
            from asset_flow.managers.token_manager import TokenManager
            from asset_flow.transformers.kis_transformer import transform_domestic_balance
            from airflow.models import Variable

            try:
                tokens = TokenManager().GetTokens()
                config = Variable.get('KIS_ISA', deserialize_json=True)
                kis = KISApiClient(tokens['KIS_ISA'], config)
                raw = kis.get_domestic_balance()
                df = transform_domestic_balance(raw, standard_date, config)
                if df.empty:
                    print("ISA 주식 데이터 없음")
                    return []
                print(f"ISA 주식 수집: {len(df)}건")
                return df.to_dict('records')
            except Exception as e:
                print(f"[ERROR] ISA 주식 조회 실패: {e}")
                return []

        @task(task_id='get_pension_assets')
        def get_pension_assets(standard_date: str) -> list:
            from asset_flow.clients.kis_client import KISApiClient
            from asset_flow.managers.token_manager import TokenManager
            from asset_flow.managers.db_manager import get_fund_price
            from asset_flow.transformers.kis_transformer import (
                transform_pension_fund_balance,
                transform_domestic_balance,
            )
            from airflow.models import Variable

            try:
                tokens = TokenManager().GetTokens()
                config = Variable.get('KIS_PENSION', deserialize_json=True)
                pension_fund_code = get_current_context().get('params')['pension_fund_code']

                pg_hook = PostgresHook(postgres_conn_id='postgres_asset')
                engine = pg_hook.get_sqlalchemy_engine()
                fund_price, product_name = get_fund_price(engine, pension_fund_code, standard_date)

                kis = KISApiClient(tokens['KIS_PENSION'], config)

                account_balance_raw = kis.get_account_balance()
                pension_fund_df = transform_pension_fund_balance(
                    account_balance_raw,
                    standard_date=standard_date,
                    config=config,
                    product_code=pension_fund_code,
                    fund_price=fund_price,
                    product_name=product_name,
                )

                domestic_raw = kis.get_domestic_balance()
                pension_stock_df = transform_domestic_balance(
                    domestic_raw, standard_date, config,
                )

                pension_df = pd.concat([pension_fund_df, pension_stock_df], ignore_index=True)
                if pension_df.empty:
                    print("연금저축 데이터 없음")
                    return []
                print(
                    f"연금저축 수집: {len(pension_df)}건 "
                    f"(펀드: {len(pension_fund_df)}, 주식: {len(pension_stock_df)})"
                )
                return pension_df.to_dict('records')
            except Exception as e:
                print(f"[ERROR] 연금저축 조회 실패: {e}")
                return []

        @task(task_id='get_cma_cash')
        def get_cma_cash(standard_date: str) -> list:
            from asset_flow.clients.kis_client import KISApiClient
            from asset_flow.managers.token_manager import TokenManager
            from asset_flow.transformers.kis_transformer import transform_cma_cash_balance
            from airflow.models import Variable

            try:
                tokens = TokenManager().GetTokens()
                cma_config = Variable.get('KIS_CMA', deserialize_json=True)
                # CMA는 ISA 토큰 공유, 계좌 설정만 KIS_CMA 사용
                kis = KISApiClient(tokens['KIS_ISA'], cma_config)
                raw = kis.get_account_balance()
                df = transform_cma_cash_balance(raw, standard_date, cma_config)
                if df.empty:
                    print("CMA 현금 데이터 없음")
                    return []
                print(f"CMA 현금 수집: {len(df)}건")
                return df.to_dict('records')
            except Exception as e:
                print(f"[ERROR] CMA 현금 조회 실패: {e}")
                return []

        @task(task_id='get_upbit_assets')
        def get_upbit_assets(standard_date: str) -> list:
            from asset_flow.clients.upbit_client import UpbitApiClient
            from asset_flow.managers.token_manager import TokenManager
            from asset_flow.transformers.upbit_transformer import transform_upbit_balance
            from airflow.models import Variable

            try:
                tokens = TokenManager().GetTokens()
                config = Variable.get('UPBIT', deserialize_json=True)

                upbit = UpbitApiClient(tokens['UPBIT'])
                balance_raw = upbit.get_balance()
                market_code_raw = upbit.get_market_codes()

                markets = ['KRW-' + b['currency'] for b in balance_raw if b['currency'] != 'KRW']
                price_raw = upbit.get_current_prices(markets) if markets else []

                df = transform_upbit_balance(
                    balance_raw, market_code_raw, price_raw,
                    standard_date=standard_date,
                    account_code=config.get('account', 'UPBIT'),
                    account_name=config.get('type', '업비트'),
                )
                if df.empty:
                    print("Upbit 데이터 없음")
                    return []
                print(f"Upbit 수집: {len(df)}건")
                return df.to_dict('records')
            except Exception as e:
                print(f"[ERROR] Upbit 조회 실패: {e}")
                return []

        return [
            get_overseas_stock(standard_date),
            get_domestic_stock(standard_date),
            get_isa_stock(standard_date),
            get_pension_assets(standard_date),
            get_cma_cash(standard_date),
            get_upbit_assets(standard_date),
        ]

    @task(task_id='upload_asset_data')
    def upload_asset_data(standard_date: str, asset_data_list: list) -> None:
        if not asset_data_list or all(not data for data in asset_data_list):
            print("적재할 자산 데이터가 없습니다.")
            return

        all_dfs = [pd.DataFrame(records) for records in asset_data_list if records]
        if not all_dfs:
            print("적재할 자산 데이터가 없습니다.")
            return

        final_df = pd.concat(all_dfs, ignore_index=True)

        context = get_current_context()
        params = context.get('params')['daily_asset']
        schema = params['schema']
        table_name = params['table_name']
        full_table_name = f"{schema}.{table_name}"

        pg_hook = PostgresHook(postgres_conn_id='postgres_asset')
        engine = pg_hook.get_sqlalchemy_engine()

        with engine.begin() as conn:
            conn.execute(
                text(f"DELETE FROM {full_table_name} WHERE standard_date = :std_date"),
                {"std_date": standard_date},
            )
            final_df.to_sql(
                name=table_name,
                con=conn,
                schema=schema,
                if_exists='append',
                index=False,
            )

        print(f"[{standard_date}] 자산 데이터 적재 완료: {len(final_df)}건")

    # 의존성 구성
    std_date = get_standard_date()

    # 센서 → get_standard_date 연결: get_daily_asset_group은 std_date에 의존하므로 체인 연결됨
    [wait_for_exchange_rate, wait_for_fund_price_daily] >> std_date

    collected_assets = get_daily_asset_group(std_date)
    upload_asset_data(std_date, collected_assets)


fetch_asset_daily_dag()
