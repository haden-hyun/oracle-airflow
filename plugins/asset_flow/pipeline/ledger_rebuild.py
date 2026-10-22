"""
수기 원장 계좌 asset_daily 재계산 (upsert DAG·재적재 DAG 공용)

- check_run_window: 정기 적재(06:50~07:30 KST) 중 실행 차단
- check_ledger_date: 원장 입력 기준일 검증 (미래·수집 이전 차단, 과거 원장 사이 끼워 넣기는 명시 허용)
- cash_total: 적금·청약 입력(입금금액 또는 누계) → 그날 누계
- rebuild_end: 재계산 종료일 = 정기 DAG 가 마지막으로 계산한 날 (어제 이하)
- rebuild_range: 계좌 1개를 날짜 범위로 재계산·재적재
- entry_embed: 원장 반영 알림 embed (upsert DAG 성공 알림)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta

from asset_flow.pipeline.dispatch import HANDLERS, Deps, write_account


@dataclass
class RebuildResult:
    processed: int = 0                                   # 재적재한 날 수
    rows: int = 0                                        # 재적재한 asset_daily 행 수
    skipped: int = 0                                     # 원장 없는 날 (개설 전, 정상)
    nav_missing: list[str] = field(default_factory=list)  # NAV 결측으로 건너뛴 날


SCHEDULED_WINDOW = (time(6, 50), time(7, 30))   # 정기 적재가 같은 계좌·날짜를 쓰는 시간 (KST)


def check_run_window(now: datetime) -> None:
    """
    Purpose: 정기 적재 시간 실행 차단 (대기하다 늦게 시작한 run 포함)
    Parameter:
        - now: 현재 시각 (KST)
    Input: 없음
    Output: 없음. 06:50 이상 07:30 미만이면 ValueError
    """
    start, end = SCHEDULED_WINDOW
    if start <= now.time() < end:
        raise ValueError(f"정기 적재 시간({start:%H:%M}~{end:%H:%M}) 중 — {end:%H:%M} 이후 다시 실행")


def check_ledger_date(standard_date: date, today: date, data_start: date | None,
                      ledger_dates: list[date], allow_backdate: bool) -> str:
    """
    Purpose: 원장 입력 기준일 검증 (날짜 오타 차단)
    Parameter:
        - standard_date: 입력 기준일
        - today: 오늘 (KST)
        - data_start: asset_daily 최소 기준일 (없으면 None)
        - ledger_dates: 이 계좌의 기존 원장 기준일
        - allow_backdate: 최신 원장보다 과거 날짜 입력 허용 여부
    Input: 없음
    Output: "correction"(기존 행 정정) / "backdate"(과거 사이 끼워 넣기) / "new". 규칙 위반이면 ValueError
    """
    if standard_date > today:
        raise ValueError(f"미래 날짜: {standard_date} (오늘 {today})")
    if data_start and standard_date < data_start:
        raise ValueError(f"수집 시작일 {data_start} 이전 날짜: {standard_date} — 연도 오타 확인")
    if standard_date in ledger_dates:
        return "correction"
    latest = max(ledger_dates, default=None)
    if latest and standard_date < latest:
        if not allow_backdate:
            raise ValueError(f"최신 원장일 {latest} 보다 과거 날짜: {standard_date} — "
                             "날짜 오타 확인. 빠뜨린 과거 입력이 맞으면 allow_backdate=true 로 다시 실행")
        return "backdate"
    return "new"


def cash_total(deposit: float | None, total: float | None, prev: float | None,
               qty: float | None, evaluation: float | None) -> float:
    """
    Purpose: 적금·청약 입력 검증 + 그날 누적 납입원금 계산
    Parameter:
        - deposit: 입금금액 (deposit_amount)
        - total: 누계 직접 입력 (total_purchase_amount, 잔액 맞추기)
        - prev: 기준일 미포함 직전 원장 누계 (없으면 None = 첫 입력)
        - qty: holding_quantity (예적금은 비워야 함)
        - evaluation: evaluation_amount (예적금은 비워야 함)
    Input: 없음
    Output: 그날 누계 = 직전 누계 + 입금금액, 또는 직접 입력한 누계. 규칙 위반이면 ValueError
    """
    if qty is not None or evaluation is not None:
        raise ValueError("적금·청약은 holding_quantity·evaluation_amount 를 비워야 한다")
    if (deposit is None) == (total is None):
        raise ValueError("적금·청약은 deposit_amount(입금금액)·total_purchase_amount(누계) 중 하나만 입력한다")
    if deposit is not None:
        if deposit <= 0:
            raise ValueError(f"deposit_amount 는 양수여야 한다: {deposit}")
        return (prev or 0) + deposit
    if total < 0:
        raise ValueError(f"total_purchase_amount 는 음수일 수 없다: {total}")
    return total


def rebuild_end(latest_asset_date: date | None, today: date) -> date | None:
    """
    Purpose: 재계산 종료일 (이미 계산된 날만 고침, 이후는 정기 DAG 가 새 원장으로 계산)
    Parameter:
        - latest_asset_date: asset_daily 최대 기준일 (없으면 None)
        - today: 오늘 (KST)
    Input: 없음
    Output: min(최대 기준일, 어제). asset_daily 가 비면 None
    """
    if latest_asset_date is None:
        return None
    return min(latest_asset_date, today - timedelta(days=1))


def rebuild_range(account_id: str, start: date, end: date, deps: Deps) -> RebuildResult:
    """
    Purpose: 수기 계좌 1개를 start~end 하루씩 재계산·재적재
    Parameter:
        - account_id: secrets 수기 계좌 id
        - start: 시작일 (포함)
        - end: 종료일 (포함)
        - deps: 의존성
    Input: 원장, market.fund_price_daily
    Output: RebuildResult (+ asset_daily 쓰기). NAV 결측일은 건너뛰고 nav_missing 에 모음
    """
    acct = deps.secrets.account(account_id)
    result, day = RebuildResult(), start
    while day <= end:
        d = day.isoformat()
        day += timedelta(days=1)
        try:
            df = HANDLERS[acct.kind](acct, {"standard_date": d}, deps)
        except ValueError as e:                 # NAV 결측 — 이 날짜만 건너뜀
            print(f"[SKIP] {d}: {e}")
            result.nav_missing.append(d)
            continue
        if df.empty:                            # 원장 없음 (계좌 개설 전) — 정상
            result.skipped += 1
            continue
        result.rows += write_account(acct, d, df, deps)
        result.processed += 1
    print(f"[{account_id}] 재계산 {start}~{end}: {result.processed}일 적재 / {result.skipped}일 원장없음 / "
          f"{len(result.nav_missing)}일 NAV결측")
    return result


def entry_embed(name: str, kind: str, payload: dict, params: dict, written: dict,
                rebuilt: int | None, run_id: str) -> dict:
    """
    Purpose: 원장 반영 알림 embed 생성
    Parameter:
        - name: 계좌 표시명 (secrets display_name)
        - kind: manual_fund / manual_cash
        - payload: validate 결과 {account_id, standard_date, holding_quantity, total_purchase_amount}
        - params: DAG Params (deposit_amount·evaluation_amount 원 입력값)
        - written: upsert 결과 (예적금 {old, new, adjusted}, 펀드 {})
        - rebuilt: 재계산 일수 (skip 이면 None)
        - run_id: run id (dashboard__ 로 시작하면 대시보드 입력)
    Input: 없음
    Output: Discord embed dict (계좌번호 없음)
    """
    from alerts import field

    won = lambda v: f"{v:,.0f}원"  # noqa: E731
    fields = [field("계좌", f"{name} (`{payload['account_id']}`)"), field("기준일", payload["standard_date"])]
    if kind == "manual_fund":
        qty = f"{payload['holding_quantity']:,.8f}".rstrip("0").rstrip(".")
        fields += [field("좌수", f"{qty}좌"),
                   field("납입원금", won(payload["total_purchase_amount"]))]
        if params.get("evaluation_amount") is not None:
            fields.append(field("평가금액", won(params["evaluation_amount"])))
    else:
        if (deposit := params.get("deposit_amount")) is not None:
            fields.append(field("납입액", won(deposit)))
        fields.append(field("총 납입액", f"{written['old']:,.0f} → {won(written['new'])}"))
        if written["adjusted"]:
            fields.append(field("이후 기록", f"{written['adjusted']}건 {written['new'] - written['old']:+,.0f}원"))
    fields.append(field("재계산", f"{rebuilt}일" if rebuilt else "내일 아침"))
    fields.append(field("입력", "대시보드" if run_id.startswith("dashboard__") else "Airflow UI"))
    return {"title": f"📝 수기 입력: {name}", "color": 0x43A047,
            "description": "저장 · 재계산 완료" if rebuilt else "저장 완료 · 자산은 내일 아침 반영",
            "fields": fields}
