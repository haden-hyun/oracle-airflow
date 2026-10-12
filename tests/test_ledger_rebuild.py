import importlib.util
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from asset_flow.pipeline import ledger_rebuild as lr

D = date.fromisoformat
TODAY = D("2026-10-13")
START = D("2026-01-28")
LEDGER = [D("2026-07-15"), D("2026-09-21")]
DAGS = Path(__file__).resolve().parents[1] / "dags"


@pytest.mark.parametrize("day,allow,mode", [
    ("2026-10-02", False, "new"),            # 최신 원장 이후 정상 입력
    ("2026-10-13", False, "new"),            # 오늘
    ("2026-09-21", False, "correction"),     # 기존 행 정정
    ("2026-08-15", True, "backdate"),        # 빠뜨린 과거 입력 (명시 허용)
    ("2026-01-28", True, "backdate"),        # 수집 시작일 당일
])
def test_check_ledger_date_accepts(day, allow, mode):
    assert lr.check_ledger_date(D(day), TODAY, START, LEDGER, allow) == mode


@pytest.mark.parametrize("day,allow,msg", [
    ("2026-10-20", True, "미래 날짜"),           # 월·일 오타
    ("2025-10-02", True, "수집 시작일"),         # 연도 오타 (허용 플래그로도 못 넘김)
    ("2026-01-02", True, "수집 시작일"),
    ("2026-08-15", False, "allow_backdate"),     # 월 오타 의심
])
def test_check_ledger_date_rejects(day, allow, msg):
    with pytest.raises(ValueError, match=msg):
        lr.check_ledger_date(D(day), TODAY, START, LEDGER, allow)


def test_check_ledger_date_first_entry_of_new_account():
    # 원장이 없는 신규 계좌: 수집 기간 안이면 과거 날짜도 그대로 허용
    assert lr.check_ledger_date(D("2026-03-01"), TODAY, START, [], False) == "new"


@pytest.mark.parametrize("latest,end", [
    ("2026-10-11", "2026-10-11"),   # 07:00 전: 어제분 아직 미계산 → 정기 DAG 몫
    ("2026-10-12", "2026-10-12"),   # 07:00 후
    ("2026-10-13", "2026-10-12"),   # 오늘분이 있어도 어제까지만
])
def test_rebuild_end_stops_at_last_computed_day(latest, end):
    assert lr.rebuild_end(D(latest), TODAY) == D(end)


def test_rebuild_end_empty_table():
    assert lr.rebuild_end(None, TODAY) is None


def _deps(kind="manual_fund"):
    """
    Purpose: rebuild_range 용 가짜 의존성
    Parameter:
        - kind: 계좌 kind
    Input: 없음
    Output: secrets.account 만 가진 대역
    """
    acct = SimpleNamespace(id="IRP", kind=kind, account_code="X")
    return SimpleNamespace(secrets=SimpleNamespace(account=lambda _id: acct))


def test_rebuild_range_counts_and_collects_nav_gaps(monkeypatch):
    # 원장 전(빈 DF) · 정상 · NAV 결측이 섞인 구간: 결측일만 모으고 나머지는 적재
    def handler(acct, dates, deps):
        """
        Purpose: 날짜별 결과를 흉내 내는 핸들러
        Parameter:
            - acct: 계좌
            - dates: {standard_date}
            - deps: 의존성
        Input: 없음
        Output: DataFrame (10/01 빈 DF, 10/03 ValueError)
        """
        d = dates["standard_date"]
        if d == "2026-10-01":
            return pd.DataFrame()
        if d == "2026-10-03":
            raise ValueError("fund_price가 없다")
        return pd.DataFrame([{"v": 1}])

    written = []
    monkeypatch.setitem(lr.HANDLERS, "manual_fund", handler)
    monkeypatch.setattr(lr, "write_account", lambda acct, d, df, deps: written.append(d))
    r = lr.rebuild_range("IRP", D("2026-10-01"), D("2026-10-04"), _deps())
    assert (r.processed, r.skipped, r.nav_missing) == (2, 1, ["2026-10-03"])
    assert written == ["2026-10-02", "2026-10-04"]


def test_rebuild_range_empty_when_start_after_end(monkeypatch):
    monkeypatch.setattr(lr, "write_account", lambda *a: pytest.fail("쓰기 없어야 함"))
    r = lr.rebuild_range("IRP", D("2026-10-05"), D("2026-10-04"), _deps())
    assert (r.processed, r.skipped, r.nav_missing) == (0, 0, [])


def _load_dag(name):
    """
    Purpose: DAG 파일을 import 해 DAG 객체 반환
    Parameter:
        - name: DAG 파일명(확장자 제외) = dag_id
    Input: dags/<name>.py
    Output: DAG (@dag 팩토리를 다시 호출해 생성)
    """
    spec = importlib.util.spec_from_file_location(f"_t_{name}", DAGS / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return getattr(mod, name)()


def test_upsert_dag_rebuilds_after_upsert():
    dag = _load_dag("account_upsert_manual_position_ledger")
    t = dag.task_dict
    assert {"validate", "upsert", "rebuild_from_ledger"} <= set(t)
    assert t["rebuild_from_ledger"].upstream_task_ids == {"validate", "upsert"}
    assert dag.params["allow_backdate"] is False
