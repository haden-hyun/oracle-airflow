"""
수동 자산(IRP·DC·적금·주택청약) 원장 입력 DAG

keywords: manual, ledger, IRP, DC, installment savings, housing subscription, upsert

KIS·Upbit API로 커버되지 않는 자산 네 가지의 포지션을 사람이 앱 화면을 보고
월 1회 직접 입력하는 유일한 쓰기 경로다. 입력값은 account.manual_position_ledger에
Append-only + 효력 기준일(standard_date)로 저장되며, fetch_asset_daily가 매일
이 원장을 읽어 그날의 NAV(IRP·DC)나 원금 캐리(적금·청약)로 asset_daily를 파생시킨다.

스케줄: 없음 (schedule=None) — Airflow UI에서 수동 트리거
의존성: 없음 (IRP·DC 좌수 검증에 market.fund_price_daily를 참조하되, 당일 입력은
      해당 기준가가 아직 없으므로 최신 기준가 대비 자릿수 검증으로 대체한다)
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

# 자릿수 검증(당일 입력) 허용치. 오타는 10배로 튀므로 넓게 잡아도 잡힌다.
SANITY_TOLERANCE = 0.10          # 역산 NAV — 최신 NAV와 며칠치 변동만큼만 벌어진다
PURCHASE_SANITY_TOLERANCE = 0.50  # 역산 매입단가 — 누적 평균이라 시세와 크게 벌어질 수 있다

DAG_DOC = """
### 목적
IRP·DC·적금·청약처럼 API가 없는 자산의 포지션을 사람이 앱 화면을 보고 직접 입력하는
유일한 쓰기 경로다. 입금·결제가 있을 때마다 트리거한다.

### Pipeline
1. `validate`: 입력값 검증
   - IRP·DC(과거 소급): 좌수 × NAV(standard_date) × multiplier와 `evaluation_amount`를
     대조, 오차 1% 초과면 실패
   - IRP·DC(당일 입력): D 기준가는 D+1 06:55에 들어오므로, 역산값(평가금액 또는 매입원금
     ÷ 좌수 ÷ multiplier)을 최신 NAV와 대조해 자릿수 오타만 잡는다
   - 적금·청약: 누적 납입원금이 직전 원장값보다 줄면 경고만
2. `upsert`: 검증된 값을 원장에 저장. 같은 `(standard_date, account_code)`로
   재입력하면 오타 정정으로 간주해 덮어쓴다

### Task
| Task | 내용 |
|---|---|
| `validate` | 계좌·타입 확인 후 IRP·DC는 NAV 대조 또는 자릿수 검증, 적금·청약은 감소 여부만 확인 |
| `upsert` | `account.manual_position_ledger`에 UPSERT |

> Param 사용법은 저장소 루트 `README.md`의 "upsert_manual_position 사용법" 참고.
"""


def _sanity_check(
    engine,
    account_config: dict,
    standard_date: str,
    holding_quantity: float,
    total_purchase_amount: float,
    evaluation_amount: float | None,
) -> None:
    """당일 입력용 자릿수 검증. standard_date NAV가 없을 때만 호출한다.

    D 기준가는 08~09시에 확정되어 D+1 06:55 크롤링으로 들어오므로, 당일 트리거 시점에는
    존재하지 않는다. 대신 좌수에 반비례하는 역산값을 최신 NAV와 대조한다.
    자릿수 오타는 역산값을 10배로 튀게 하므로 NAV가 하루 이틀 낡아도 판정이 뒤집히지 않는다.
    evaluation_amount가 없으면 매입원금으로 대체 검증한다(누적 평균이라 허용치가 넓다).
    """
    if standard_date >= datetime.now(kst).strftime('%Y-%m-%d'):
        print(f"[정상] {standard_date} 기준가는 D+1 06:55 크롤링 예정 — 자릿수 검증으로 대체")
    else:
        print(f"[경고] {standard_date} NAV 결측 — 크롤링 실패 의심. fetch_fund_price_daily 로그 확인")

    with engine.connect() as conn:
        latest = conn.execute(
            text("""
                SELECT standard_price, standard_date FROM market.fund_price_daily
                WHERE product_code = :product_code AND standard_price > 0
                ORDER BY standard_date DESC LIMIT 1
            """),
            {"product_code": account_config['product_code']},
        ).fetchone()
    if latest is None:
        print(f"[검증 불가] {account_config['product_code']} 기준가 이력 없음 — product_code 확인 필요")
        return

    latest_nav = float(latest.standard_price)
    multiplier = account_config['multiplier']
    if evaluation_amount is not None:
        label, actual, tolerance = '역산 NAV', evaluation_amount, SANITY_TOLERANCE
    else:
        label, actual, tolerance = '역산 매입단가', total_purchase_amount, PURCHASE_SANITY_TOLERANCE
    actual = actual / holding_quantity / multiplier

    diff_rate = (actual - latest_nav) / latest_nav
    if abs(diff_rate) > tolerance:
        raise AirflowException(
            f"좌수 자릿수 검증 실패: {label} {actual:,.2f}가 최신 NAV {latest_nav:,.2f}"
            f"({latest.standard_date}) 대비 {diff_rate:+.1%} 괴리. 좌수·금액 자릿수를 확인할 것"
        )
    print(
        f"[자릿수 검증 통과] {label} {actual:,.2f} ≈ 최신 NAV {latest_nav:,.2f} "
        f"({latest.standard_date} 기준, 괴리 {diff_rate:+.1%})"
    )


@dag(
    dag_id='upsert_manual_position',
    default_args=default_args,
    description='수동 자산(IRP·DC·적금·청약) 원장 입력 (월 1회 수동 트리거)',
    doc_md=DAG_DOC,
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
            description=(
                '[IRP·DC 전용·검증용] 앱 > 평가금액. 예) 1793812. 저장되지 않는다. '
                '비우면 매입원금 기반 자릿수 검증으로 대체된다'
            ),
        ),
    },
)
def upsert_manual_position_dag():

    @task(task_id='validate')
    def validate() -> dict:
        """
        입력값을 검증하고 원장에 저장할 페이로드를 만든다.

        IRP·DC: 좌수 × NAV(standard_date) × multiplier ≈ evaluation_amount 를 확인한다.
        NAV 행이 없으면(당일 입력) _sanity_check()로 자릿수만 검증한다.
        evaluation_amount 미입력이면 정밀 검증은 건너뛴다 — 경고만.
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
            if holding_quantity <= 0:
                raise AirflowException(f"holding_quantity는 양수여야 한다: {holding_quantity}")

            nav, _ = get_fund_price(engine, account_config['product_code'], standard_date)
            if nav is None:
                _sanity_check(
                    engine, account_config, standard_date,
                    holding_quantity, total_purchase_amount, evaluation_amount,
                )
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
