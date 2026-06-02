"""
전체 자산 현황 수집 및 DB 적재 DAG

keywords: asset, portfolio, KIS, Upbit, stock, fund, CMA, crypto, PostgreSQL, daily snapshot

매일 07:00 KST에 실행되어 6개 자산 유형의 전일(T-1) 기준 잔고를 수집하고
account.asset_daily 테이블에 통합 적재한다.

수집 자산:
    - 해외주식 (KIS_STOCK, TR: TTTS3012R)
    - 국내주식 (KIS_STOCK, TR: TTTC8434R)
    - ISA 주식 (KIS_ISA, TR: TTTC8434R)
    - 연금저축 펀드 + 주식 (KIS_PENSION, TR: CTRP6548R + TTTC8434R)
    - CMA 현금 (KIS_ISA/KIS_CMA, TR: CTRP6548R)
    - 가상자산 (Upbit, /v1/accounts + /v1/candles/days)

스케줄: 매일 07:00 KST (cron: '0 7 * * *')
의존성: fetch_exchange_rate, fetch_fund_price_daily (ExternalTaskSensor)
적재 테이블: account.asset_daily
기준일 처리:
    - standard_date (T-1): 데이터 기준일, DB 적재 레이블
    - base_date (T):       Upbit 일 캔들 to 경계값 — base_date 00:00:00 이전 캔들 = T-1 종가
"""

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
    tags=['account', 'asset', 'portfolio', 'KIS', 'Upbit', 'daily', 'ingestion'],
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
    def get_standard_date() -> dict:
        """
        Airflow data_interval_end 기준 날짜 계산

        Returns:
            dict:
                standard_date (str): T-1일 (YYYY-MM-DD) — 데이터 기준일, DB 적재 레이블
                base_date (str):     T일 (YYYY-MM-DD)   — Upbit 일 캔들 to 경계값
        """
        context = get_current_context()
        utc_end = context.get('data_interval_end')
        kst_end = utc_end.in_timezone("Asia/Seoul")
        standard_date = (kst_end - relativedelta(days=1)).strftime('%Y-%m-%d')  # T-1: 데이터 기준일
        base_date = kst_end.strftime('%Y-%m-%d')                                # T:   Upbit 캔들 to 경계
        print(f"기준일자: [{standard_date}] / 실행일자: [{base_date}]")
        return {"standard_date": standard_date, "base_date": base_date}

    @task_group(group_id='get_daily_asset_group')
    def get_daily_asset_group(dates: dict):

        @task(task_id='get_overseas_stock')
        def get_overseas_stock(dates: dict) -> list:
            from asset_flow.clients.kis_client import KISApiClient
            from asset_flow.managers.token_manager import TokenManager
            from asset_flow.transformers.kis_transformer import transform_overseas_balance
            from airflow.models import Variable

            try:
                tokens = TokenManager().GetTokens()
                config = Variable.get('KIS_STOCK', deserialize_json=True)
                kis = KISApiClient(tokens['KIS_STOCK'], config)
                raw = kis.get_overseas_balance()
                df = transform_overseas_balance(raw, dates["standard_date"], config)
                if df.empty:
                    print("해외주식 데이터 없음")
                    return []
                print(f"해외주식 수집: {len(df)}건")
                return df.to_dict('records')
            except Exception as e:
                print(f"[ERROR] 해외주식 조회 실패: {e}")
                return []

        @task(task_id='get_domestic_stock')
        def get_domestic_stock(dates: dict) -> list:
            from asset_flow.clients.kis_client import KISApiClient
            from asset_flow.managers.token_manager import TokenManager
            from asset_flow.transformers.kis_transformer import transform_domestic_balance
            from airflow.models import Variable

            try:
                tokens = TokenManager().GetTokens()
                config = Variable.get('KIS_STOCK', deserialize_json=True)
                kis = KISApiClient(tokens['KIS_STOCK'], config)
                raw = kis.get_domestic_balance()
                df = transform_domestic_balance(raw, dates["standard_date"], config)
                if df.empty:
                    print("국내주식 데이터 없음")
                    return []
                print(f"국내주식 수집: {len(df)}건")
                return df.to_dict('records')
            except Exception as e:
                print(f"[ERROR] 국내주식 조회 실패: {e}")
                return []

        @task(task_id='get_isa_stock')
        def get_isa_stock(dates: dict) -> list:
            from asset_flow.clients.kis_client import KISApiClient
            from asset_flow.managers.token_manager import TokenManager
            from asset_flow.transformers.kis_transformer import transform_domestic_balance
            from airflow.models import Variable

            try:
                tokens = TokenManager().GetTokens()
                config = Variable.get('KIS_ISA', deserialize_json=True)
                kis = KISApiClient(tokens['KIS_ISA'], config)
                raw = kis.get_domestic_balance()
                df = transform_domestic_balance(raw, dates["standard_date"], config)
                if df.empty:
                    print("ISA 주식 데이터 없음")
                    return []
                print(f"ISA 주식 수집: {len(df)}건")
                return df.to_dict('records')
            except Exception as e:
                print(f"[ERROR] ISA 주식 조회 실패: {e}")
                return []

        @task(task_id='get_pension_assets')
        def get_pension_assets(dates: dict) -> list:
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
                fund_price, product_name = get_fund_price(engine, pension_fund_code, dates["standard_date"])

                kis = KISApiClient(tokens['KIS_PENSION'], config)

                account_balance_raw = kis.get_account_balance()
                pension_fund_df = transform_pension_fund_balance(
                    account_balance_raw,
                    standard_date=dates["standard_date"],
                    config=config,
                    product_code=pension_fund_code,
                    fund_price=fund_price,
                    product_name=product_name,
                )

                domestic_raw = kis.get_domestic_balance()
                pension_stock_df = transform_domestic_balance(
                    domestic_raw, dates["standard_date"], config,
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
        def get_cma_cash(dates: dict) -> list:
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
                df = transform_cma_cash_balance(raw, dates["standard_date"], cma_config)
                if df.empty:
                    print("CMA 현금 데이터 없음")
                    return []
                print(f"CMA 현금 수집: {len(df)}건")
                return df.to_dict('records')
            except Exception as e:
                print(f"[ERROR] CMA 현금 조회 실패: {e}")
                return []

        @task(task_id='get_upbit_assets')
        def get_upbit_assets(dates: dict) -> list:
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
                # price_raw = upbit.get_current_prices(markets) if markets else []  # 실시간 현재가 (T-1 기준일 불일치로 대체)
                price_raw = upbit.get_daily_candles(markets, to_date=dates["base_date"]) if markets else []

                df = transform_upbit_balance(
                    balance_raw, market_code_raw, price_raw,
                    standard_date=dates["standard_date"],
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
            get_overseas_stock(dates),
            get_domestic_stock(dates),
            get_isa_stock(dates),
            get_pension_assets(dates),
            get_cma_cash(dates),
            get_upbit_assets(dates),
        ]

    @task(task_id='upload_asset_data')
    def upload_asset_data(dates: dict, asset_data_list: list) -> None:
        """
        수집된 전 자산 레코드를 account.asset_daily 테이블에 통합 적재

        6개 서브태스크 결과를 하나의 DataFrame으로 합산한 뒤,
        기존 standard_date 행을 삭제(DELETE)하고 신규 삽입(INSERT)하여 멱등성을 보장한다.

        Args:
            dates: get_standard_date() 반환 딕셔너리 {standard_date, base_date}
            asset_data_list: get_daily_asset_group() 결과 — 자산 유형별 레코드 리스트의 리스트
        """
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
                {"std_date": dates["standard_date"]},
            )
            final_df.to_sql(
                name=table_name,
                con=conn,
                schema=schema,
                if_exists='append',
                index=False,
            )

        print(f"[{dates['standard_date']}] 자산 데이터 적재 완료: {len(final_df)}건")

    # 의존성 구성
    dates = get_standard_date()

    # 센서 → get_standard_date 연결: get_daily_asset_group은 dates에 의존하므로 체인 연결됨
    [wait_for_exchange_rate, wait_for_fund_price_daily] >> dates

    collected_assets = get_daily_asset_group(dates)
    upload_asset_data(dates, collected_assets)


fetch_asset_daily_dag()
