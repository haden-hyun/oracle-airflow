"""
수기 자산(IRP·DC·적금·청약) 원장 입력 + asset_daily 재계산 DAG (수동 트리거, 월 1회)

- Param account_id = secrets 수기 계좌 id (계좌번호는 태스크 안에서 secrets 로 해석)
- validate: 날짜 검증 → IRP·DC 는 좌수 × NAV × multiplier 대조(당일이면 자릿수 검증),
  적금·청약은 입금금액 → 누계 계산(또는 누계 직접 입력, 감소는 경고)
- upsert: account.manual_position_ledger UPSERT (XCom 에는 계좌 id 와 입력값만). 적금·청약은 이후 행 누계도 조정
- 대시보드(dashboard_owner 계정)가 REST API 로 트리거 — 권한은 이 DAG 만 (access_control)
- rebuild_from_ledger: 그 계좌만 기준일 ~ 정기 DAG 최종 계산일 재계산 (과거 날짜 입력이 즉시 반영)
- 재계산 뒤 나란히: report_entry(반영 Discord 알림, 앞 단계 실패면 생략, 알림 오류는 run 을 실패시키지 않음)
  · finalize(대시보드 MV 갱신 + 실패 전파)
- validate·upsert·rebuild_from_ledger 모두 정기 적재 시간(06:50~07:30 KST)에 시작하면 실패 (대기 run·clear 재실행 포함)
"""

from datetime import date, datetime

from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException

from alerts import discord_failure_callback, discord_recovery_callback
from asset_flow.assets import ASSET_DAILY, MANUAL_LEDGER, mark
from asset_flow.finalize import finalize
from asset_flow.runtime import KST, kst_today

VALIDATION_TOLERANCE = 0.01          # 과거 NAV 대조 허용 오차
SANITY_TOLERANCE = 0.10              # 역산 NAV vs 최신 NAV (당일 입력)
PURCHASE_SANITY_TOLERANCE = 0.50     # 역산 매입단가 vs 최신 NAV (누적 평균이라 넓게)

DAG_DOC = """
### 목적
API 가 없는 수기 자산의 포지션을 앱 화면을 보고 입력하는 유일한 쓰기 경로.
입력한 기준일부터 이미 계산된 `asset_daily` 까지 바로 재계산한다 (과거 날짜 입력도 그날부터 반영).

| Task | 내용 |
|---|---|
| `validate` | 정기 적재 시간 차단 → 계좌 id 해석·활성 확인 → 날짜 검증 → IRP·DC NAV 대조 또는 자릿수 검증 / 적금·청약 누계 계산·감소 경고. 재계산 예정 범위 로그 |
| `upsert` | 같은 `(standard_date, account_code)` 재입력은 오타 정정으로 덮어씀. 적금·청약은 그날 누계 변화분을 이후 원장 행에 가산 |
| `rebuild_from_ledger` | 그 계좌만 기준일 ~ min(asset_daily 최대 기준일, 어제) 재계산. 범위가 비면 skip, NAV 결측일이 있으면 나머지 적재 후 실패 |
| `report_entry` | 반영 알림: 계좌·기준일·입력값·누계 변화(예적금)·재계산 일수. 앞 단계 실패면 실행 안 함(실패 알림이 따로 감) |
| `refresh_dashboard_views` | 대시보드 MV 갱신 프로시저 CALL → 입력이 화면에 바로 반영 |
| `fail_if_upstream_failed` | 앞 단계 실패가 있으면 run 실패 처리 |

**날짜 검증**
- 미래 날짜, asset_daily 수집 시작일 이전 → 실패
- 기존 원장 날짜 → 정정으로 진행
- 이 계좌 최신 원장일보다 과거(빠뜨린 입력) → `allow_backdate=true` 일 때만 진행

**적금·청약 입력** (둘 중 하나만)
- `deposit_amount`(입금금액): 새 누계 = 기준일 직전 누계 + 입금금액. 같은 날 여러 번 입금은 합계로 입력
- `total_purchase_amount`(누계 직접): 잔액 맞추기. 직전보다 줄면 경고만
- 과거 날짜 입력·정정 → 그날 누계 변화분을 이후 원장 행 전부에 가산

정기 실행(06:50~07:30) 중에 시작한 validate·upsert·rebuild_from_ledger 는 실패한다 (같은 계좌·날짜 동시 쓰기).
동시 실행은 1개 (`max_active_runs=1`) — 직전 누계를 읽고 쓰는 사이 다른 입력이 끼지 않게.
`account_id` 는 secrets 의 수기 계좌 id (예: IRP, DC, INSTALLMENT_SAVINGS, HOUSING_SUBSCRIPTION).
"""


def _sanity_check(engine, product_code: str, multiplier: float, standard_date: str,
                  qty: float, purchase: float, evaluation: float | None) -> None:
    """
    Purpose: 당일 입력 자릿수 검증 — 기준일 NAV 가 아직 없을 때 역산값을 최신 NAV 와 대조
    Parameter:
        - engine: 업무 DB 엔진
        - product_code: 펀드 표준코드
        - multiplier: 좌수 배수
        - standard_date: 효력 기준일
        - qty: 좌수
        - purchase: 납입원금
        - evaluation: 평가금액 (없으면 매입원금으로 대체 검증)
    Input: market.fund_price_daily
    Output: 없음 (괴리가 허용치 초과면 ValueError, 과거 날짜·이력 없음은 경고만)
    """
    from asset_flow.db_manager import get_latest_fund_price

    if standard_date < kst_today().isoformat():
        print(f"[경고] {standard_date} NAV 결측 — 크롤링 실패 의심. 과거 날짜라 자릿수 검증 생략")
        return
    latest = get_latest_fund_price(engine, product_code)
    if latest is None:
        print(f"[검증 불가] {product_code} 기준가 이력 없음 — product_code 확인 필요")
        return
    nav = float(latest.standard_price)
    label, base, tol = ("역산 NAV", evaluation, SANITY_TOLERANCE) if evaluation is not None else (
        "역산 매입단가", purchase, PURCHASE_SANITY_TOLERANCE)
    actual = base / qty / multiplier
    diff = (actual - nav) / nav
    if abs(diff) > tol:
        raise ValueError(f"좌수 자릿수 검증 실패: {label} {actual:,.2f} vs 최신 NAV {nav:,.2f}"
                         f"({latest.standard_date}) {diff:+.1%} 괴리 — 좌수·금액 자릿수 확인")
    print(f"[자릿수 검증 통과] {label} {actual:,.2f} ≈ 최신 NAV {nav:,.2f} ({latest.standard_date}, {diff:+.1%})")


def _resolve(account_id: str):
    """
    Purpose: 계좌 id → 수기 계좌·product_code·multiplier 해석
    Parameter:
        - account_id: secrets 계좌 id
    Input: secrets 파일, config 파일
    Output: (Account, product_code, multiplier). 수기 계좌가 아니면 ValueError (유효 id 목록 포함)
    """
    from asset_flow.app_config import load_app_config
    from asset_flow.pipeline.dispatch import manual_product
    from asset_flow.secrets_config import MANUAL_KINDS, load_secrets

    cfg = load_secrets()
    manual_ids = [a.id for a in cfg.active_accounts(*MANUAL_KINDS)]
    if account_id not in manual_ids:
        raise ValueError(f"수기 계좌 id 가 아님: {account_id!r} (가능: {manual_ids})")
    acct = cfg.account(account_id)
    return (acct, *manual_product(acct, load_app_config()))


@dag(
    dag_id="account_upsert_manual_position_ledger",
    description="수기 자산(IRP·DC·적금·청약) 원장 입력 (수동 트리거)",
    doc_md=DAG_DOC,
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=KST),
    catchup=False,
    max_active_runs=1,
    access_control={"dashboard_owner": {"DAGs": {"can_read", "can_edit"}, "DAG Runs": {"can_read", "can_create"}}},
    default_args={"owner": "haejun", "retries": 0, "on_failure_callback": discord_failure_callback},
    on_success_callback=discord_recovery_callback,
    tags=["account", "manual", "ledger", "ingestion"],
    params={
        "account_id": Param("IRP", type="string", pattern=r"^[A-Z][A-Z0-9_]{1,63}$", section="공통",
                            description="secrets 의 수기 계좌 id (IRP, DC, INSTALLMENT_SAVINGS, HOUSING_SUBSCRIPTION)"),
        "standard_date": Param(None, type=["null", "string"], format="date", section="공통",
                               description="효력 기준일. IRP·DC 는 결제 완료일, 적금·청약은 납입일. 비우면 오늘(KST)"),
        "allow_backdate": Param(False, type="boolean", section="공통",
                                description="최신 원장일보다 과거 날짜(빠뜨린 입력) 허용. 날짜 오타 방지용 확인"),
        "total_purchase_amount": Param(None, type=["null", "number"], section="공통",
                                       description="IRP·DC: 앱 > 납입원금 (필수) / "
                                                   "적금·청약: 누계 직접 입력(잔액 맞추기) — deposit_amount 와 하나만"),
        "holding_quantity": Param(None, type=["null", "number"], section="펀드 (IRP·DC)",
                                  description="앱 > 보유수량 (좌)"),
        "evaluation_amount": Param(None, type=["null", "number"], section="펀드 (IRP·DC)",
                                   description="[검증용] 앱 > 평가금액. 저장 안 됨. 비우면 매입원금 검증"),
        "deposit_amount": Param(None, type=["null", "number"], section="예적금",
                                description="입금금액. 새 누계 = 직전 누계 + 입금금액. 같은 날 여러 번 입금은 합계로 입력"),
    },
)
def account_upsert_manual_position_ledger():
    """
    Purpose: DAG 구성 (validate → upsert → rebuild_from_ledger → [report_entry, 대시보드 갱신])
    Parameter: 없음
    Input: 없음
    Output: 없음
    """

    @task(task_id="validate", multiple_outputs=False)
    def validate() -> dict:
        """
        Purpose: 입력값 검증 후 원장 저장값 생성
        Parameter: 없음
        Input: Params, secrets·config 파일, account_master, asset_daily, fund_price_daily, 원장
        Output: {account_id, standard_date, holding_quantity, total_purchase_amount} (계좌번호 미포함).
                검증 실패 시 ValueError
        """
        from asset_flow import db_manager as db
        from asset_flow.app_config import load_app_config
        from asset_flow.pipeline.ledger_rebuild import cash_total, check_ledger_date, check_run_window, rebuild_end

        check_run_window(datetime.now(KST))
        p = get_current_context()["params"]
        acct, product_code, multiplier = _resolve(p["account_id"])
        standard_date = p["standard_date"] or kst_today().isoformat()
        date.fromisoformat(standard_date)
        qty, purchase, evaluation = p["holding_quantity"], p["total_purchase_amount"], p["evaluation_amount"]
        deposit = p.get("deposit_amount")
        if acct.kind == "manual_fund" and (purchase is None or deposit is not None):
            raise ValueError(f"[{acct.id}] IRP·DC 는 total_purchase_amount 필수, deposit_amount 는 비워야 한다")
        engine = db.asset_engine()
        if acct.account_code not in db.get_account_master(engine):
            raise ValueError(f"[{acct.id}] account_master 에 활성 계좌 없음")

        # 날짜 오타 차단 + 재계산 예정 범위 안내
        table, today, day = load_app_config().tables.asset_daily, kst_today(), date.fromisoformat(standard_date)
        data_start, latest = db.get_asset_date_bounds(engine, table.db_schema, table.table_name)
        mode = check_ledger_date(day, today, data_start, db.get_ledger_dates(engine, acct.account_code),
                                 p["allow_backdate"])
        end = rebuild_end(latest, today)
        plan = f"{day} ~ {end}" if end and day <= end else "없음 (정기 DAG 가 새 원장으로 계산)"
        print(f"[날짜 검증 통과] {mode} / 재계산 예정: {acct.id} {plan}")

        if acct.kind == "manual_fund":
            if qty is None or qty <= 0:
                raise ValueError(f"[{acct.id}] holding_quantity 는 양수 필수 입력이다")
            nav, _ = db.get_fund_price(engine, product_code, standard_date)
            if nav is None:
                _sanity_check(engine, product_code, multiplier, standard_date, qty, purchase, evaluation)
            elif evaluation is None:
                print("[검증 스킵] evaluation_amount 미입력")
            elif evaluation == 0:
                raise ValueError("evaluation_amount는 0이 될 수 없다 — 미입력이면 비워둘 것")
            else:
                expected = qty * nav * multiplier
                diff = abs(expected - evaluation) / evaluation
                if diff > VALIDATION_TOLERANCE:
                    raise ValueError(f"좌수 검증 실패: {qty} × NAV {nav} × {multiplier} = {expected:,.0f} "
                                     f"≠ 입력 평가금액 {evaluation:,.0f} (오차 {diff:.2%})")
                print(f"[검증 통과] 계산 {expected:,.0f} ≈ 입력 {evaluation:,.0f} (오차 {diff:.2%})")
        else:
            prev = db.get_previous_ledger_amount(engine, acct.account_code, standard_date)
            purchase = cash_total(deposit, purchase, prev, qty, evaluation)
            base = "없음(첫 입력)" if prev is None else f"{prev:,.0f}"
            how = f"입금 {deposit:,.0f}" if deposit is not None else "누계 직접 입력"
            print(f"[누계] 직전 {base} → 새 누계 {purchase:,.0f} ({how})")
            if prev is not None and purchase < prev:
                print(f"[경고] 납입원금 누계 감소: 직전 {prev:,.0f} → 입력 {purchase:,.0f}")
        return {"account_id": acct.id, "standard_date": standard_date,
                "holding_quantity": qty, "total_purchase_amount": purchase}

    @task(task_id="upsert", outlets=[MANUAL_LEDGER])
    def upsert(payload: dict) -> dict:
        """
        Purpose: 검증된 값을 원장에 UPSERT (계좌번호·product_code 는 여기서 재해석)
        Parameter:
            - payload: validate 결과
        Input: secrets·config 파일
        Output: 예적금 {old, new, adjusted} / 펀드 {} (+ account.manual_position_ledger 쓰기, 쓰기 가드 적용)
        """
        from asset_flow import db_manager as db
        from asset_flow.pipeline.ledger_rebuild import check_run_window

        check_run_window(datetime.now(KST))
        acct, product_code, _ = _resolve(payload["account_id"])
        result = {}
        row = {k: payload[k] for k in ("standard_date", "holding_quantity", "total_purchase_amount")}
        row |= {"account_code": acct.account_code, "product_code": product_code}
        if acct.kind == "manual_fund":
            db.upsert_ledger(db.asset_engine(), row)
        else:
            # 이 태스크만 clear 해 다시 돌리면 validate 때의 직전 누계로 계산한 값이 남아 있다 → 그사이 바뀌었으면 새로 입력
            deposit = get_current_context()["params"].get("deposit_amount")
            if deposit is not None:
                prev = db.get_previous_ledger_amount(db.asset_engine(), acct.account_code, payload["standard_date"]) or 0
                if abs(prev + deposit - payload["total_purchase_amount"]) > 0.005:
                    raise ValueError(f"직전 누계가 검증 뒤 바뀜 ({prev:,.0f}) — validate 부터 다시 실행하거나 새로 입력")
            old, new, adjusted = db.upsert_cash_ledger(db.asset_engine(), row)
            result = {"old": old, "new": new, "adjusted": adjusted}
            print(f"[누계 반영] 그날 기존 {old:,.0f} → {new:,.0f}, 이후 원장 {adjusted}행 {new - old:+,.0f} 조정")
        mark(MANUAL_LEDGER, standard_date=payload["standard_date"], account_id=acct.id)
        print(f"[{payload['standard_date']}] {acct.id} 원장 upsert 완료")
        return result

    @task(task_id="rebuild_from_ledger", outlets=[ASSET_DAILY])
    def rebuild_from_ledger(payload: dict) -> int:
        """
        Purpose: 입력 계좌 asset_daily 재계산 (기준일 ~ 정기 DAG 최종 계산일)
        Parameter:
            - payload: validate 결과
        Input: secrets·config 파일, 원장, asset_daily, fund_price_daily
        Output: 재적재한 날 수 (+ asset_daily 쓰기). 범위 없음 → skip, NAV 결측일 있으면 나머지 적재 후 RuntimeError
        """
        from asset_flow import db_manager as db
        from asset_flow.pipeline.dispatch import Deps
        from asset_flow.pipeline.ledger_rebuild import check_run_window, rebuild_end, rebuild_range

        deps, account_id = Deps.load(), payload["account_id"]
        table = deps.app.tables.asset_daily
        start = date.fromisoformat(payload["standard_date"])
        end = rebuild_end(db.get_asset_date_bounds(deps.engine, table.db_schema, table.table_name)[1], kst_today())
        if end is None or start > end:
            raise AirflowSkipException(f"재계산 대상 없음 — {start} 부터는 정기 DAG 가 새 원장으로 계산")
        check_run_window(datetime.now(KST))   # 재계산할 날이 있을 때만 (skip 될 오늘 입력은 막지 않음)
        result = rebuild_range(account_id, start, end, deps)
        if result.nav_missing:
            raise RuntimeError(f"[{account_id}] NAV 결측 {len(result.nav_missing)}일: {result.nav_missing} "
                               "— 기준가 보충 후 이 태스크만 재실행 (06:50~07:30 제외)")
        mark(ASSET_DAILY, rebuild=f"{start}~{end}", account_id=account_id, days=result.processed)
        return result.processed

    @task(task_id="report_entry", trigger_rule="none_failed")
    def report_entry(payload: dict, written: dict | None, rebuilt: int | None) -> None:
        """
        Purpose: 반영 내용 Discord 알림 (재계산 skip 이어도 보냄)
        Parameter:
            - payload: validate 결과
            - written: upsert 결과 (예적금 누계 변화)
            - rebuilt: 재계산 일수 (skip 이면 None)
        Input: secrets 파일(계좌 표시명), Params(입금금액·평가금액), Connection discord_alert[_test]
        Output: 없음 (+ Discord 전송). 알림 오류는 로그만 — 원장·재계산이 끝난 run 을 실패로 만들지 않음
        """
        import logging

        from alerts import make_payload, post_discord

        from asset_flow.pipeline.ledger_rebuild import entry_embed

        try:
            ctx = get_current_context()
            acct, _, _ = _resolve(payload["account_id"])
            post_discord(make_payload(entry_embed(acct.display_name, acct.kind, payload, ctx["params"],
                                                  written, rebuilt, ctx["run_id"])))
        except Exception:
            logging.getLogger(__name__).exception("[report_entry] 반영 알림 생략 (원장·재계산은 완료)")

    payload = validate()
    written = upsert(payload)
    rebuilt = rebuild_from_ledger(payload)
    written >> rebuilt
    report_entry(payload, written, rebuilt)
    finalize(rebuilt)


account_upsert_manual_position_ledger()
