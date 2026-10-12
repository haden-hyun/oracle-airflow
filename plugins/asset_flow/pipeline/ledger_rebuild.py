"""
수기 원장 계좌 asset_daily 재계산 (upsert DAG·재적재 DAG 공용)

- check_ledger_date: 원장 입력 기준일 검증 (미래·수집 이전 차단, 과거 원장 사이 끼워 넣기는 명시 허용)
- rebuild_end: 재계산 종료일 = 정기 DAG 가 마지막으로 계산한 날 (어제 이하)
- rebuild_range: 계좌 1개를 날짜 범위로 재계산·재적재
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta

from asset_flow.pipeline.dispatch import HANDLERS, Deps, write_account


@dataclass
class RebuildResult:
    processed: int = 0                                   # 재적재한 날 수
    skipped: int = 0                                     # 원장 없는 날 (개설 전, 정상)
    nav_missing: list[str] = field(default_factory=list)  # NAV 결측으로 건너뛴 날


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
        write_account(acct, d, df, deps)
        result.processed += 1
    print(f"[{account_id}] 재계산 {start}~{end}: {result.processed}일 적재 / {result.skipped}일 원장없음 / "
          f"{len(result.nav_missing)}일 NAV결측")
    return result
