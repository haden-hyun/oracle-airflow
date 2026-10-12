from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from airflow.sdk.exceptions import AirflowSkipException

from asset_flow.runtime import kst_run_end, standard_dates

T0 = datetime(2026, 10, 11, 22, 0, tzinfo=timezone.utc)   # 2026-10-12 07:00 KST


def _run(run_after: datetime, run_type: str = "scheduled") -> SimpleNamespace:
    """
    Purpose: 가짜 DagRun
    Parameter:
        - run_after: 예정 시각
        - run_type: run 유형
    Input: 없음
    Output: DagRun 대역
    """
    return SimpleNamespace(run_after=run_after, run_type=run_type)


def test_uses_data_interval_end_when_present():
    ctx = {"data_interval_end": T0, "dag_run": _run(T0)}
    assert standard_dates(ctx, now=T0) == {"standard_date": "2026-10-11", "base_date": "2026-10-12"}


def test_falls_back_to_run_after_for_manual_runs():
    ra = datetime(2026, 10, 11, 21, 50, tzinfo=timezone.utc)
    ctx = {"data_interval_end": None, "dag_run": _run(ra, "manual")}
    assert kst_run_end(ctx).to_date_string() == "2026-10-12"            # 21:50 UTC → 다음 날 06:50 KST
    assert standard_dates(ctx, now=ra + timedelta(days=1))["standard_date"] == "2026-10-11"   # 수동은 지연 무관


def test_kst_boundary_before_midnight():
    ra = datetime(2026, 10, 11, 14, 59, tzinfo=timezone.utc)
    assert standard_dates({"dag_run": _run(ra)}, now=ra) == {"standard_date": "2026-10-10", "base_date": "2026-10-11"}


def test_late_scheduled_run_is_skipped():
    # unpause 직후 따라잡기 run 이 과거 기준일을 현재 잔고로 덮지 않음
    ctx = {"data_interval_end": T0, "dag_run": _run(T0)}
    assert standard_dates(ctx, now=T0 + timedelta(hours=2, minutes=59))["standard_date"] == "2026-10-11"
    with pytest.raises(AirflowSkipException, match="지연 시작"):
        standard_dates(ctx, now=T0 + timedelta(hours=18))
