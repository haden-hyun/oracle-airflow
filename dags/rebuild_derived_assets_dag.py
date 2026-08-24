"""
수동 자산(IRP·DC·적금·주택청약) 파생 재계산 백필 DAG

keywords: backfill, rebuild, manual ledger, IRP, DC, installment savings, housing subscription

account.manual_position_ledger를 원천으로 두는 4개 계좌만 날짜 범위를 순회하며
account.asset_daily를 재계산·재적재한다. fetch_asset_daily의 하루치 파생 로직
(get_manual_positions → ledger_transformer → delete_and_insert_account_assets)을
날짜 루프로 감싼 것이고, 로직 자체는 그대로 재사용한다.

KIS·Upbit 계좌는 과거 잔고 조회 API가 없어 백필이 불가능하다 — 이 DAG가 다루는
계좌를 4개로 못박아 그 경계를 강제한다.

쓰는 상황:
    - 좌수·원금 오타 정정 후 과거 asset_daily 재계산
    - 신규 계좌 등록 시 과거분 소급 적재
    - market.fund_price_daily 결측 복구 후 IRP·DC 재계산

스케줄: 없음 (schedule=None) — Airflow UI에서 start_date/end_date 지정해 수동 트리거
의존성: 없음 (원장·NAV가 이미 채워져 있어야 의미 있는 결과가 나온다)
적재 테이블: account.asset_daily (DELETE 스코프: standard_date + account_code)

[결측 처리] 원장에 그 날짜 이전 행이 없으면(계좌 개설 전) 조용히 건너뛴다.
IRP·DC는 NAV가 없는 날짜만 건너뛰고 계속 진행하되, 끝나고 실패 목록과 함께
예외를 던져 태스크를 실패로 표시한다 — 결측을 조용히 덮지 않는다.
"""

from airflow.decorators import task, dag
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from callbacks import slack_failure_callback, slack_recovery_callback
import pendulum

kst = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'haejun',
    'depends_on_past': False,
    'retries': 0,
    'on_failure_callback': slack_failure_callback,
}


def _date_range(start: str, end: str):
    d0 = datetime.strptime(start, '%Y-%m-%d').date()
    d1 = datetime.strptime(end, '%Y-%m-%d').date()
    d = d0
    while d <= d1:
        yield d.strftime('%Y-%m-%d')
        d += timedelta(days=1)


@dag(
    dag_id='rebuild_derived_assets',
    default_args=default_args,
    description='IRP·DC·적금·청약 4계좌 asset_daily 백필/재계산 (날짜 범위 수동 트리거)',
    doc_md=__doc__,
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=kst),
    catchup=False,
    tags=['account', 'manual', 'ledger', 'backfill', 'IRP', 'DC'],
    on_success_callback=slack_recovery_callback,
    params={
        'daily_asset': Param({
            'schema': 'account',
            'table_name': 'asset_daily',
        }),
        'start_date': Param(
            '2026-01-28',
            type='string',
            format='date',
            description='백필 시작일 (asset_daily 전체 최소 standard_date, 통상 이보다 앞당기지 않음)',
        ),
        'end_date': Param(
            datetime.now(kst).strftime('%Y-%m-%d'),
            type='string',
            format='date',
            description='백필 종료일 (포함). 보통 어제까지 — 오늘은 fetch_asset_daily가 처리',
        ),
    },
)
def rebuild_derived_assets_dag():

    @task(task_id='rebuild_irp')
    def rebuild_irp() -> None:
        """IRP asset_daily 재계산 — 좌수 × NAV × 0.001"""
        from asset_flow.managers.db_manager import get_manual_positions, get_fund_price, delete_and_insert_account_assets
        from asset_flow.transformers.ledger_transformer import transform_fund_position

        params = get_current_context().get('params')
        daily_asset = params['daily_asset']

        pg_hook = PostgresHook(postgres_conn_id='postgres_asset')
        engine = pg_hook.get_sqlalchemy_engine()

        processed, skipped, nav_missing = 0, 0, []
        for d in _date_range(params['start_date'], params['end_date']):
            positions = get_manual_positions(engine, d)
            irp = positions[positions["account_type"] == "IRP"]
            if irp.empty:
                skipped += 1
                continue
            row = irp.iloc[0].to_dict()

            fund_price, product_name = get_fund_price(engine, row["product_code"], d)
            try:
                df = transform_fund_position(row, d, fund_price, product_name)
            except ValueError as e:
                print(f"[SKIP] {d}: {e}")
                nav_missing.append(d)
                continue

            delete_and_insert_account_assets(
                engine=engine, schema=daily_asset['schema'], table_name=daily_asset['table_name'],
                standard_date=d, account_code=row["account_code"], records=df.to_dict('records'),
            )
            processed += 1

        print(f"IRP 백필 완료: {processed}일 적재 / {skipped}일 원장없음(정상) / {len(nav_missing)}일 NAV결측")
        if nav_missing:
            raise AirflowException(f"IRP 백필 중 NAV 결측 {len(nav_missing)}일: {nav_missing}")

    @task(task_id='rebuild_dc')
    def rebuild_dc() -> None:
        """DC asset_daily 재계산 — IRP와 계산식 동일, 같은 펀드 K55105D43299 공유"""
        from asset_flow.managers.db_manager import get_manual_positions, get_fund_price, delete_and_insert_account_assets
        from asset_flow.transformers.ledger_transformer import transform_fund_position

        params = get_current_context().get('params')
        daily_asset = params['daily_asset']

        pg_hook = PostgresHook(postgres_conn_id='postgres_asset')
        engine = pg_hook.get_sqlalchemy_engine()

        processed, skipped, nav_missing = 0, 0, []
        for d in _date_range(params['start_date'], params['end_date']):
            positions = get_manual_positions(engine, d)
            dc = positions[positions["account_type"] == "DC"]
            if dc.empty:
                skipped += 1
                continue
            row = dc.iloc[0].to_dict()

            fund_price, product_name = get_fund_price(engine, row["product_code"], d)
            try:
                df = transform_fund_position(row, d, fund_price, product_name)
            except ValueError as e:
                print(f"[SKIP] {d}: {e}")
                nav_missing.append(d)
                continue

            delete_and_insert_account_assets(
                engine=engine, schema=daily_asset['schema'], table_name=daily_asset['table_name'],
                standard_date=d, account_code=row["account_code"], records=df.to_dict('records'),
            )
            processed += 1

        print(f"DC 백필 완료: {processed}일 적재 / {skipped}일 원장없음(정상) / {len(nav_missing)}일 NAV결측")
        if nav_missing:
            raise AirflowException(f"DC 백필 중 NAV 결측 {len(nav_missing)}일: {nav_missing}")

    @task(task_id='rebuild_savings')
    def rebuild_savings() -> None:
        """청년미래적금 asset_daily 재계산 — 원금 캐리"""
        from asset_flow.managers.db_manager import get_manual_positions, delete_and_insert_account_assets
        from asset_flow.transformers.ledger_transformer import transform_cash_position

        params = get_current_context().get('params')
        daily_asset = params['daily_asset']

        pg_hook = PostgresHook(postgres_conn_id='postgres_asset')
        engine = pg_hook.get_sqlalchemy_engine()

        processed, skipped = 0, 0
        for d in _date_range(params['start_date'], params['end_date']):
            positions = get_manual_positions(engine, d)
            savings = positions[positions["account_type"] == "INSTALLMENT_SAVINGS"]
            if savings.empty:
                skipped += 1
                continue
            row = savings.iloc[0].to_dict()

            df = transform_cash_position(row, d)
            delete_and_insert_account_assets(
                engine=engine, schema=daily_asset['schema'], table_name=daily_asset['table_name'],
                standard_date=d, account_code=row["account_code"], records=df.to_dict('records'),
            )
            processed += 1

        print(f"적금 백필 완료: {processed}일 적재 / {skipped}일 원장없음(정상)")

    @task(task_id='rebuild_housing')
    def rebuild_housing() -> None:
        """청년주택드림청약 asset_daily 재계산 — 원금 캐리"""
        from asset_flow.managers.db_manager import get_manual_positions, delete_and_insert_account_assets
        from asset_flow.transformers.ledger_transformer import transform_cash_position

        params = get_current_context().get('params')
        daily_asset = params['daily_asset']

        pg_hook = PostgresHook(postgres_conn_id='postgres_asset')
        engine = pg_hook.get_sqlalchemy_engine()

        processed, skipped = 0, 0
        for d in _date_range(params['start_date'], params['end_date']):
            positions = get_manual_positions(engine, d)
            housing = positions[positions["account_type"] == "HOUSING_SUBSCRIPTION"]
            if housing.empty:
                skipped += 1
                continue
            row = housing.iloc[0].to_dict()

            df = transform_cash_position(row, d)
            delete_and_insert_account_assets(
                engine=engine, schema=daily_asset['schema'], table_name=daily_asset['table_name'],
                standard_date=d, account_code=row["account_code"], records=df.to_dict('records'),
            )
            processed += 1

        print(f"청약 백필 완료: {processed}일 적재 / {skipped}일 원장없음(정상)")

    rebuild_irp()
    rebuild_dc()
    rebuild_savings()
    rebuild_housing()


rebuild_derived_assets_dag()
