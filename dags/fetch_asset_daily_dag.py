"""
전체 자산 현황 수집 및 DB 적재 DAG

keywords: asset, portfolio, KIS, Upbit, stock, fund, CMA, crypto, PostgreSQL, daily snapshot

매일 07:00 KST에 실행되어 10개 자산 유형의 전일(T-1) 기준 잔고를 수집하고
account.asset_daily 테이블에 계좌 단위로 적재한다.

수집 자산:
    - 위탁계좌 주식 (KIS_STOCK, TR: TTTS3012R 해외 + TTTC8434R 국내)
    - ISA 주식 (KIS_ISA, TR: TTTC8434R)
    - 연금저축 펀드 + 주식 (KIS_PENSION, TR: CTRP6548R + TTTC8434R)
    - CMA 현금 (KIS_ISA/KIS_CMA, TR: CTRP6548R)
    - 가상자산 (Upbit, /v1/accounts + /v1/candles/days)
    - IRP·DC 펀드 (account.manual_position_ledger 원장 좌수 × market.fund_price_daily NAV)
    - 적금·주택청약 (account.manual_position_ledger 원장 원금 캐리)

IRP·DC·적금·청약은 API 수집이 아니라 수동 입력 원장(manual_position_ledger,
upsert_manual_position DAG로 월 1회 갱신)을 매일 읽어 파생시킨다.

스케줄: 매일 07:00 KST (cron: '0 7 * * *')
의존성: fetch_exchange_rate, fetch_fund_price_daily (ExternalTaskSensor)
적재 테이블: account.asset_daily
기준일 처리:
    - standard_date (T-1): 데이터 기준일, DB 적재 레이블
    - base_date (T):       Upbit 일 캔들 to 경계값 — base_date 00:00:00 이전 캔들 = T-1 종가

아키텍처:
    계좌 단위 get_X/upload_X 페어. upload_X는 DELETE 스코프를
    (standard_date, account_code)로 좁혀 다른 계좌를 건드리지 않으며,
    실패한 계좌만 단독 재시도/백필할 수 있다.

    [불변식] 한 account_code = 한 get_X/upload_X 페어.
    같은 계좌를 두 upload가 병렬로 쓰면 DELETE 스코프가 겹쳐 서로의 적재분을
    지운다. 위탁계좌의 해외·국내를 get_stock_account 하나로 묶은 이유다.

    [빈 응답] get_X는 0건이면 예외를 던진다. 0건을 성공으로 넘기면 upload_X가
    DELETE만 수행해 기존 적재분이 사라진다.
"""

from airflow.decorators import task, dag, task_group
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from callbacks import slack_failure_callback, slack_recovery_callback
import pendulum
import pandas as pd

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
전 계좌(API 5종 + 원장 기반 4종, 총 10개 자산)의 그날 잔고·평가금액을 계산해
`account.asset_daily`에 적재하는 메인 파이프라인이다.

### Pipeline
1. `wait_for_exchange_rate` / `wait_for_fund_price_daily`: 환율·펀드기준가 DAG 완료 대기
2. `get_standard_date`: 기준일(T-1)과 Upbit 캔들 경계값(T) 계산
3. **조회 단계** (`get_daily_asset_group`, 9개 태스크 병렬): 계좌별로 잔고를 조회해
   `{account_code, records}` 형태로 반환. 하나가 실패해도 나머지는 계속 진행된다
4. **적재 단계** (`upload_asset_group`, 9개 태스크): 계좌별로 `(standard_date, account_code)`
   스코프 DELETE 후 INSERT. 조회가 실패한 계좌는 적재를 건너뛰어 기존 데이터를 보존한다

### Task — 조회(get) / 적재(upload) 페어
| 계좌 | get / upload | 내용 |
|---|---|---|
| 위탁계좌 | `get_stock_account` / `upload_stock_account` | KIS 해외+국내 주식 잔고 |
| ISA | `get_isa_stock` / `upload_isa_stock` | KIS 국내 주식 잔고 |
| 연금저축 | `get_pension_assets` / `upload_pension_assets` | KIS 잔고 + NAV로 좌수 역산(평가금액→좌수) |
| CMA | `get_cma_cash` / `upload_cma_cash` | KIS 계좌잔고 중 현금성 자산 |
| 업비트 | `get_upbit_assets` / `upload_upbit_assets` | Upbit 잔고 + 당일 캔들 |
| IRP | `get_irp_assets` / `upload_irp_assets` | 원장 좌수 × NAV(좌수→평가금액, 연금저축과 반대 방향) |
| DC | `get_dc_assets` / `upload_dc_assets` | IRP와 계산식 동일 |
| 청년미래적금 | `get_savings_assets` / `upload_savings_assets` | 원장의 누적 납입원금을 그대로 캐리 |
| 청년주택드림청약 | `get_housing_assets` / `upload_housing_assets` | 위와 동일 |

### 실패 처리
- 조회 결과가 0건이면 예외를 던진다 — 빈 결과를 그대로 넘기면 적재 단계가 DELETE만
  수행해 기존 데이터가 사라지기 때문이다.
- IRP·DC·연금저축은 그날 NAV가 없으면 직전 값으로 채우지 않고 그 계좌만 실패 처리한다.
- 계좌 하나의 실패가 다른 계좌 적재를 막지 않는다(계좌당 단일 get/upload 페어 불변식).
"""


@dag(
    dag_id='fetch_asset_daily',
    default_args=default_args,
    description='매일 07:00 자산 현황 수집 및 DB 적재 (KIS/Upbit API + IRP·DC·적금·청약 수동 원장)',
    doc_md=DAG_DOC,
    schedule='0 7 * * *',
    start_date=datetime(2024, 1, 1, tzinfo=kst),
    catchup=False,
    tags=['account', 'asset', 'portfolio', 'KIS', 'Upbit', 'daily', 'ingestion'],
    on_success_callback=slack_recovery_callback,
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
    def get_daily_asset_group(dates: dict) -> dict:

        @task(task_id='get_stock_account', retries=3, retry_delay=timedelta(seconds=20), retry_exponential_backoff=True)
        def get_stock_account(dates: dict) -> dict:
            """
            위탁계좌(KIS_STOCK)의 해외주식 + 국내주식 수집

            TR은 다르지만 같은 계좌라 한 태스크로 묶는다(모듈 docstring 불변식 참조).
            """
            from asset_flow.clients.kis_client import KISApiClient
            from asset_flow.managers.token_manager import TokenManager
            from asset_flow.transformers.kis_transformer import (
                transform_overseas_balance,
                transform_domestic_balance,
            )
            from airflow.models import Variable

            tokens = TokenManager().GetTokens()
            config = Variable.get('KIS_STOCK', deserialize_json=True)
            account_code = f"{config['account']}-{config['product_code']}"
            kis = KISApiClient(tokens['KIS_STOCK'], config)

            overseas_df = transform_overseas_balance(
                kis.get_overseas_balance(), dates["standard_date"], config,
            )
            domestic_df = transform_domestic_balance(
                kis.get_domestic_balance(), dates["standard_date"], config,
            )

            stock_df = pd.concat([overseas_df, domestic_df], ignore_index=True)
            if stock_df.empty:
                raise AirflowException("위탁계좌 주식 데이터 없음 — 빈 응답")
            print(
                f"위탁계좌 주식 수집: {len(stock_df)}건 "
                f"(해외: {len(overseas_df)}, 국내: {len(domestic_df)})"
            )
            return {"account_code": account_code, "records": stock_df.to_dict('records')}

        @task(task_id='get_isa_stock', retries=3, retry_delay=timedelta(seconds=20), retry_exponential_backoff=True)
        def get_isa_stock(dates: dict) -> dict:
            from asset_flow.clients.kis_client import KISApiClient
            from asset_flow.managers.token_manager import TokenManager
            from asset_flow.transformers.kis_transformer import transform_domestic_balance
            from airflow.models import Variable

            tokens = TokenManager().GetTokens()
            config = Variable.get('KIS_ISA', deserialize_json=True)
            account_code = f"{config['account']}-{config['product_code']}"
            kis = KISApiClient(tokens['KIS_ISA'], config)
            raw = kis.get_domestic_balance()
            df = transform_domestic_balance(raw, dates["standard_date"], config)
            if df.empty:
                raise AirflowException("ISA 주식 데이터 없음 — 빈 응답")
            print(f"ISA 주식 수집: {len(df)}건")
            return {"account_code": account_code, "records": df.to_dict('records')}

        @task(task_id='get_pension_assets', retries=3, retry_delay=timedelta(seconds=20), retry_exponential_backoff=True)
        def get_pension_assets(dates: dict) -> dict:
            from asset_flow.clients.kis_client import KISApiClient
            from asset_flow.managers.token_manager import TokenManager
            from asset_flow.managers.db_manager import get_fund_price
            from asset_flow.transformers.kis_transformer import (
                transform_pension_fund_balance,
                transform_domestic_balance,
            )
            from airflow.models import Variable

            tokens = TokenManager().GetTokens()
            config = Variable.get('KIS_PENSION', deserialize_json=True)
            account_code = f"{config['account']}-{config['product_code']}"
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
                raise AirflowException("연금저축 데이터 없음 — 빈 응답")
            print(
                f"연금저축 수집: {len(pension_df)}건 "
                f"(펀드: {len(pension_fund_df)}, 주식: {len(pension_stock_df)})"
            )
            return {"account_code": account_code, "records": pension_df.to_dict('records')}

        @task(task_id='get_cma_cash', retries=3, retry_delay=timedelta(seconds=20), retry_exponential_backoff=True)
        def get_cma_cash(dates: dict) -> dict:
            from asset_flow.clients.kis_client import KISApiClient
            from asset_flow.managers.token_manager import TokenManager
            from asset_flow.transformers.kis_transformer import transform_cma_cash_balance
            from airflow.models import Variable

            tokens = TokenManager().GetTokens()
            cma_config = Variable.get('KIS_CMA', deserialize_json=True)
            account_code = f"{cma_config['account']}-{cma_config['product_code']}"
            # CMA는 ISA 토큰 공유, 계좌 설정만 KIS_CMA 사용
            kis = KISApiClient(tokens['KIS_ISA'], cma_config)
            raw = kis.get_account_balance()
            df = transform_cma_cash_balance(raw, dates["standard_date"], cma_config)
            if df.empty:
                raise AirflowException("CMA 현금 데이터 없음 — 빈 응답")
            print(f"CMA 현금 수집: {len(df)}건")
            return {"account_code": account_code, "records": df.to_dict('records')}

        @task(task_id='get_upbit_assets', retries=3, retry_delay=timedelta(seconds=20), retry_exponential_backoff=True)
        def get_upbit_assets(dates: dict) -> dict:
            from asset_flow.clients.upbit_client import UpbitApiClient
            from asset_flow.managers.token_manager import TokenManager
            from asset_flow.transformers.upbit_transformer import transform_upbit_balance
            from airflow.models import Variable

            tokens = TokenManager().GetTokens()
            config = Variable.get('UPBIT', deserialize_json=True)
            account_code = config.get('account', 'UPBIT')

            upbit = UpbitApiClient(tokens['UPBIT'])
            balance_raw = upbit.get_balance()
            market_code_raw = upbit.get_market_codes()

            markets = ['KRW-' + b['currency'] for b in balance_raw if b['currency'] != 'KRW']
            # price_raw = upbit.get_current_prices(markets) if markets else []  # 실시간 현재가 (T-1 기준일 불일치로 대체)
            price_raw = upbit.get_daily_candles(markets, to_date=dates["base_date"]) if markets else []

            df = transform_upbit_balance(
                balance_raw, market_code_raw, price_raw,
                standard_date=dates["standard_date"],
                account_code=account_code,
                account_name=config.get('type', '업비트'),
            )
            if df.empty:
                raise AirflowException("Upbit 데이터 없음 — 빈 응답")
            print(f"Upbit 수집: {len(df)}건")
            return {"account_code": account_code, "records": df.to_dict('records')}

        @task(task_id='get_irp_assets', retries=3, retry_delay=timedelta(seconds=20), retry_exponential_backoff=True)
        def get_irp_assets(dates: dict) -> dict:
            """IRP 계좌 평가 — 원장 좌수 × NAV × 0.001. NAV 없으면 예외."""
            from asset_flow.managers.db_manager import get_manual_positions, get_fund_price
            from asset_flow.transformers.ledger_transformer import transform_fund_position

            pg_hook = PostgresHook(postgres_conn_id='postgres_asset')
            engine = pg_hook.get_sqlalchemy_engine()

            positions = get_manual_positions(engine, dates["standard_date"])
            irp = positions[positions["account_type"] == "IRP"]
            if irp.empty:
                raise AirflowException("IRP 원장 데이터 없음 — 빈 응답")
            row = irp.iloc[0].to_dict()

            fund_price, product_name = get_fund_price(engine, row["product_code"], dates["standard_date"])
            df = transform_fund_position(row, dates["standard_date"], fund_price, product_name)
            print(f"IRP 평가: {len(df)}건")
            return {"account_code": row["account_code"], "records": df.to_dict('records')}

        @task(task_id='get_dc_assets', retries=3, retry_delay=timedelta(seconds=20), retry_exponential_backoff=True)
        def get_dc_assets(dates: dict) -> dict:
            """DC 계좌 평가 — IRP와 계산식 동일(같은 펀드 K55105D43299 공유)."""
            from asset_flow.managers.db_manager import get_manual_positions, get_fund_price
            from asset_flow.transformers.ledger_transformer import transform_fund_position

            pg_hook = PostgresHook(postgres_conn_id='postgres_asset')
            engine = pg_hook.get_sqlalchemy_engine()

            positions = get_manual_positions(engine, dates["standard_date"])
            dc = positions[positions["account_type"] == "DC"]
            if dc.empty:
                raise AirflowException("DC 원장 데이터 없음 — 빈 응답")
            row = dc.iloc[0].to_dict()

            fund_price, product_name = get_fund_price(engine, row["product_code"], dates["standard_date"])
            df = transform_fund_position(row, dates["standard_date"], fund_price, product_name)
            print(f"DC 평가: {len(df)}건")
            return {"account_code": row["account_code"], "records": df.to_dict('records')}

        @task(task_id='get_savings_assets', retries=3, retry_delay=timedelta(seconds=20), retry_exponential_backoff=True)
        def get_savings_assets(dates: dict) -> dict:
            """청년미래적금 평가 — 원금 캐리(CMA와 동일 패턴)"""
            from asset_flow.managers.db_manager import get_manual_positions
            from asset_flow.transformers.ledger_transformer import transform_cash_position

            pg_hook = PostgresHook(postgres_conn_id='postgres_asset')
            engine = pg_hook.get_sqlalchemy_engine()

            positions = get_manual_positions(engine, dates["standard_date"])
            savings = positions[positions["account_type"] == "INSTALLMENT_SAVINGS"]
            if savings.empty:
                raise AirflowException("적금 원장 데이터 없음 — 빈 응답")
            row = savings.iloc[0].to_dict()

            df = transform_cash_position(row, dates["standard_date"])
            print(f"적금 평가: {len(df)}건")
            return {"account_code": row["account_code"], "records": df.to_dict('records')}

        @task(task_id='get_housing_assets', retries=3, retry_delay=timedelta(seconds=20), retry_exponential_backoff=True)
        def get_housing_assets(dates: dict) -> dict:
            """청년주택드림청약 평가 — 원금 캐리(CMA와 동일 패턴)"""
            from asset_flow.managers.db_manager import get_manual_positions
            from asset_flow.transformers.ledger_transformer import transform_cash_position

            pg_hook = PostgresHook(postgres_conn_id='postgres_asset')
            engine = pg_hook.get_sqlalchemy_engine()

            positions = get_manual_positions(engine, dates["standard_date"])
            housing = positions[positions["account_type"] == "HOUSING_SUBSCRIPTION"]
            if housing.empty:
                raise AirflowException("청약 원장 데이터 없음 — 빈 응답")
            row = housing.iloc[0].to_dict()

            df = transform_cash_position(row, dates["standard_date"])
            print(f"청약 평가: {len(df)}건")
            return {"account_code": row["account_code"], "records": df.to_dict('records')}

        return {
            'stock': get_stock_account(dates),
            'isa_stock': get_isa_stock(dates),
            'pension': get_pension_assets(dates),
            'cma_cash': get_cma_cash(dates),
            'upbit': get_upbit_assets(dates),
            'irp': get_irp_assets(dates),
            'dc': get_dc_assets(dates),
            'savings': get_savings_assets(dates),
            'housing': get_housing_assets(dates),
        }

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def upload_account(dates: dict, payload: dict) -> None:
        """
        단일 계좌의 수집 결과를 account.asset_daily에 스코프된 멱등 적재

        trigger_rule=ALL_DONE: get_X가 최종 실패하면 payload가 None이 되어
        DELETE/INSERT 없이 종료한다(기존 적재분 보존). get_X의 0건 예외도
        이 경로를 타기 위한 것이다 — 0건 payload는 truthy라 가드를 통과한다.

        Args:
            dates: get_standard_date() 반환 딕셔너리 {standard_date, base_date}
            payload: 대응하는 get_X task 반환 딕셔너리 {account_code, records} 또는 None(get 실패 시)
        """
        if not payload:
            print("계좌 조회 실패로 적재 생략")
            return

        from asset_flow.managers.db_manager import delete_and_insert_account_assets

        context = get_current_context()
        daily_asset = context.get('params')['daily_asset']

        pg_hook = PostgresHook(postgres_conn_id='postgres_asset')
        engine = pg_hook.get_sqlalchemy_engine()

        count = delete_and_insert_account_assets(
            engine=engine,
            schema=daily_asset['schema'],
            table_name=daily_asset['table_name'],
            standard_date=dates["standard_date"],
            account_code=payload["account_code"],
            records=payload["records"],
        )
        print(f"[{dates['standard_date']}] {payload['account_code']} 적재 완료: {count}건")

    @task_group(group_id='upload_asset_group')
    def upload_asset_group(dates: dict, get_results: dict):
        upload_account.override(task_id='upload_stock_account')(dates, get_results['stock'])
        upload_account.override(task_id='upload_isa_stock')(dates, get_results['isa_stock'])
        upload_account.override(task_id='upload_pension_assets')(dates, get_results['pension'])
        upload_account.override(task_id='upload_cma_cash')(dates, get_results['cma_cash'])
        upload_account.override(task_id='upload_upbit_assets')(dates, get_results['upbit'])
        upload_account.override(task_id='upload_irp_assets')(dates, get_results['irp'])
        upload_account.override(task_id='upload_dc_assets')(dates, get_results['dc'])
        upload_account.override(task_id='upload_savings_assets')(dates, get_results['savings'])
        upload_account.override(task_id='upload_housing_assets')(dates, get_results['housing'])

    # 의존성 구성
    dates = get_standard_date()

    # 센서 → get_standard_date 연결: get_daily_asset_group은 dates에 의존하므로 체인 연결됨
    [wait_for_exchange_rate, wait_for_fund_price_daily] >> dates

    get_results = get_daily_asset_group(dates)
    upload_asset_group(dates, get_results)


fetch_asset_daily_dag()
