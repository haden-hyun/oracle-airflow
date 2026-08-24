"""
수동 자산(IRP·DC·적금·주택청약) 원장 입력 DAG

keywords: manual, ledger, IRP, DC, installment savings, housing subscription, upsert

KIS·Upbit API로 커버되지 않는 자산 네 가지의 포지션을 사람이 앱 화면을 보고
월 1회 직접 입력하는 유일한 쓰기 경로다. 입력값은 account.manual_position_ledger에
Append-only + 효력 기준일(standard_date)로 저장되며, fetch_asset_daily가 매일
이 원장을 읽어 그날의 NAV(IRP·DC)나 원금 캐리(적금·청약)로 asset_daily를 파생시킨다.

스케줄: 없음 (schedule=None) — Airflow UI에서 수동 트리거
의존성: 없음 (IRP·DC 좌수 검증에 market.fund_price_daily를 참조하지만 실패해도 스킵)
적재 테이블: account.manual_position_ledger
"""

from airflow.decorators import task, dag
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from callbacks import slack_failure_callback, slack_recovery_callback
import pendulum
from sqlalchemy import text

kst = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'haejun',
    'depends_on_past': False,
    'retries': 0,
    'on_failure_callback': slack_failure_callback,
}

ACCOUNT_CODE_ENUM = [
    '43904978-29',       # 저축IRP (한국투자증권)
    '266-021-430970',    # 확정기여형DC (신한은행)
    '230-389-0337643',   # 청년미래적금 (신한)
    '339-2472-2043-41',  # 청년주택드림청약 (농협)
]

# 좌수 검증 오차 허용치 — 초과하면 자릿수 오타로 간주해 예외
VALIDATION_TOLERANCE = 0.01


@dag(
    dag_id='upsert_manual_position',
    default_args=default_args,
    description='수동 자산(IRP·DC·적금·청약) 원장 입력 (월 1회 수동 트리거)',
    doc_md=__doc__,
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=kst),
    catchup=False,
    tags=['account', 'manual', 'ledger', 'IRP', 'DC', 'ingestion'],
    on_success_callback=slack_recovery_callback,
    params={
        'account_code': Param(
            '43904978-29',
            type='string',
            enum=ACCOUNT_CODE_ENUM,
            description=(
                '43904978-29 = 저축IRP(한국투자증권) / '
                '266-021-430970 = 확정기여형DC(신한은행) / '
                '230-389-0337643 = 청년미래적금(신한) / '
                '339-2472-2043-41 = 청년주택드림청약(농협)'
            ),
        ),
        'standard_date': Param(
            datetime.now(kst).strftime('%Y-%m-%d'),
            type='string',
            format='date',
            description='효력 기준일. IRP·DC는 결제 완료일, 적금·청약은 납입일',
        ),
        'holding_quantity': Param(
            None,
            type=['null', 'number'],
            description='[IRP·DC 전용] 앱 > 보유수량. 예) 1023351 (좌)',
        ),
        'total_purchase_amount': Param(
            None,
            type=['null', 'number'],
            description='IRP·DC: 앱 > 납입원금. 예) 1750060 / 적금·청약: 지금까지 넣은 원금 총액',
        ),
        'evaluation_amount': Param(
            None,
            type=['null', 'number'],
            description='[IRP·DC 전용·검증용] 앱 > 평가금액. 예) 1793812. 저장되지 않는다',
        ),
    },
)
def upsert_manual_position_dag():

    @task(task_id='validate')
    def validate() -> dict:
        """
        입력값을 검증하고 원장에 저장할 페이로드를 만든다.

        IRP·DC: 좌수 × NAV(standard_date) × multiplier ≈ evaluation_amount 를 확인한다.
        evaluation_amount 미입력이거나 NAV 행이 없으면(크롤링 실패) 검증을 건너뛴다 — 경고만.
        적금·청약: 검증할 것이 없어 통과하되, 납입원금 누계가 직전 원장값보다 감소하면 경고한다.
        """
        from asset_flow.managers.db_manager import get_fund_price

        context = get_current_context()
        p = context['params']
        account_code = p['account_code']
        standard_date = p['standard_date']
        holding_quantity = p['holding_quantity']
        total_purchase_amount = p['total_purchase_amount']
        evaluation_amount = p['evaluation_amount']

        if total_purchase_amount is None:
            raise AirflowException("total_purchase_amount는 필수 입력이다")

        pg_hook = PostgresHook(postgres_conn_id='postgres_asset')
        engine = pg_hook.get_sqlalchemy_engine()

        with engine.connect() as conn:
            account = conn.execute(
                text("""
                    SELECT account_type FROM account.account_master
                    WHERE account_code = :account_code AND is_active
                """),
                {"account_code": account_code},
            ).fetchone()
        if account is None:
            raise AirflowException(f"account_master에 활성 계좌 없음: {account_code}")
        account_type = account.account_type

        manual_config = Variable.get('MANUAL_ASSET_CONFIG', deserialize_json=True)
        account_config = manual_config.get(account_type)
        if account_config is None:
            raise AirflowException(f"MANUAL_ASSET_CONFIG에 account_type '{account_type}' 설정 없음")

        if account_type in ('IRP', 'DC'):
            if holding_quantity is None:
                raise AirflowException(f"{account_type}는 holding_quantity가 필수 입력이다")

            nav, _ = get_fund_price(engine, account_config['product_code'], standard_date)
            if nav is None:
                print(f"[검증 스킵] {standard_date} NAV 없음 — 크롤링 실패로 추정. 좌수 자릿수는 수동으로 재확인할 것")
            elif evaluation_amount is None:
                print("[검증 스킵] evaluation_amount 미입력")
            else:
                expected = holding_quantity * nav * account_config['multiplier']
                diff_rate = abs(expected - evaluation_amount) / evaluation_amount
                if diff_rate > VALIDATION_TOLERANCE:
                    raise AirflowException(
                        f"좌수 검증 실패: 좌수({holding_quantity}) × NAV({nav}) × "
                        f"{account_config['multiplier']} = {expected:,.0f} ≠ "
                        f"입력 평가금액 {evaluation_amount:,.0f} (오차 {diff_rate:.2%})"
                    )
                print(f"[검증 통과] 계산 평가금액 {expected:,.0f} ≈ 입력 평가금액 {evaluation_amount:,.0f} (오차 {diff_rate:.2%})")
        else:
            holding_quantity = None
            with engine.connect() as conn:
                prev = conn.execute(
                    text("""
                        SELECT total_purchase_amount FROM account.manual_position_ledger
                        WHERE account_code = :account_code AND standard_date < :standard_date
                        ORDER BY standard_date DESC LIMIT 1
                    """),
                    {"account_code": account_code, "standard_date": standard_date},
                ).fetchone()
            if prev is not None and total_purchase_amount < float(prev.total_purchase_amount):
                print(
                    f"[경고] 납입원금 누계가 직전 원장값보다 감소함: "
                    f"직전 {float(prev.total_purchase_amount):,.0f} → 입력값 {total_purchase_amount:,.0f}"
                )

        return {
            "standard_date": standard_date,
            "account_code": account_code,
            "product_code": account_config['product_code'],
            "holding_quantity": holding_quantity,
            "total_purchase_amount": total_purchase_amount,
        }

    @task(task_id='upsert')
    def upsert(payload: dict) -> None:
        """
        원장에 UPSERT. 같은 (standard_date, account_code) 재입력은 오타 정정으로 간주해 덮어쓴다.
        """
        pg_hook = PostgresHook(postgres_conn_id='postgres_asset')
        engine = pg_hook.get_sqlalchemy_engine()

        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO account.manual_position_ledger
                        (standard_date, account_code, product_code, holding_quantity, total_purchase_amount)
                    VALUES
                        (:standard_date, :account_code, :product_code, :holding_quantity, :total_purchase_amount)
                    ON CONFLICT (standard_date, account_code) DO UPDATE SET
                        product_code = EXCLUDED.product_code,
                        holding_quantity = EXCLUDED.holding_quantity,
                        total_purchase_amount = EXCLUDED.total_purchase_amount
                """),
                payload,
            )
        print(f"[{payload['standard_date']}] {payload['account_code']} 원장 upsert 완료")

    upsert(validate())


upsert_manual_position_dag()
