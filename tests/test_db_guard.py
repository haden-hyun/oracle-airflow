from types import SimpleNamespace

import pytest

from asset_flow import db_manager as db


def _engine(name):
    """
    Purpose: DB 이름만 가진 가짜 엔진
    Parameter:
        - name: 이름
    Input: 없음
    Output: 가짜 Engine
    """
    def begin():
        """
        Purpose: 트랜잭션 진입 시 실패 (가드가 먼저 막는지 확인용)
        Parameter: 없음
        Input: 없음
        Output: 없음 (AssertionError)
        """
        raise AssertionError("가드 전에 DB 를 열면 안 됨")

    return SimpleNamespace(url=SimpleNamespace(database=name), begin=begin)


@pytest.mark.parametrize("env,dbname,allowed", [
    (None, "asset", False), ("test", "asset", False), ("test", "asset_staging", True),
    ("prod", "asset", True), (None, None, False),
])
def test_write_guard(monkeypatch, env, dbname, allowed):
    if env:
        monkeypatch.setenv("ASSET_FLOW_ENV", env)
    else:
        monkeypatch.delenv("ASSET_FLOW_ENV", raising=False)
    if allowed:
        db.ensure_writable(_engine(dbname))
    else:
        with pytest.raises(RuntimeError, match="쓰기 차단"):
            db.ensure_writable(_engine(dbname))


@pytest.mark.parametrize("write", [
    lambda e: db.delete_and_insert_account_assets(e, "account", "asset_daily", "2026-10-11", "X", [{"a": 1}]),
    lambda e: db.replace_daily_rows(e, "market", "fund_price_daily", "2026-10-11", [{"a": 1}]),
    lambda e: db.upsert_ledger(e, {}),
    lambda e: db.replace_trading_holidays(e, "2025-01-01", "2027-12-31", []),
    lambda e: db.call_procedure(e, "dashboard.refresh_views"),
])
def test_all_writers_guarded_before_db(monkeypatch, write):
    monkeypatch.delenv("ASSET_FLOW_ENV", raising=False)
    with pytest.raises(RuntimeError, match="쓰기 차단"):
        write(_engine("asset"))


def test_to_frame_converts_date_columns():
    # pandas 3 + psycopg3: 문자열 날짜는 date 컬럼 INSERT 실패 → 변환 필수
    import datetime as dt

    from asset_flow.db_manager import to_frame

    df = to_frame([{"standard_date": "2026-10-11", "account_code": "X", "total_evaluation_amount": 1.0}])
    assert df.loc[0, "standard_date"] == dt.date(2026, 10, 11)
    assert df.loc[0, "account_code"] == "X"
