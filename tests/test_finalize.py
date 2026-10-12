import importlib.util
from pathlib import Path

import pytest
from pydantic import ValidationError

from asset_flow.app_config import Dashboard

DAGS = Path(__file__).resolve().parents[1] / "dags"
WRITERS = {
    "account_ingest_asset_daily": "collect_account",
    "account_upsert_manual_position_ledger": "rebuild_from_ledger",
    "account_rebuild_ledger_assets": "rebuild_account",
}


def _load_dag(name):
    """
    Purpose: DAG 파일을 import 해 DAG 객체 반환
    Parameter:
        - name: DAG 파일명(확장자 제외) = dag_id
    Input: dags/<name>.py
    Output: DAG (@dag 팩토리를 다시 호출해 생성)
    """
    spec = importlib.util.spec_from_file_location(f"_f_{name}", DAGS / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return getattr(mod, name)()


@pytest.mark.parametrize("dag_id,last", WRITERS.items())
def test_writer_dags_end_with_refresh_and_failure_guard(dag_id, last):
    t = _load_dag(dag_id).task_dict
    refresh, guard = t["refresh_dashboard_views"], t["fail_if_upstream_failed"]
    # 앞 단계 실패여도 갱신, 실패는 run 상태로 전파
    assert refresh.upstream_task_ids == {last} and refresh.trigger_rule == "all_done"
    assert guard.upstream_task_ids == {last} and guard.trigger_rule == "one_failed"
    assert not refresh.downstream_task_ids and not guard.downstream_task_ids
    # 중복 알림 없음 (실패한 태스크가 이미 알림)
    assert not guard.on_failure_callback


def test_refresh_procedure_name_validated(app):
    assert app.dashboard.refresh_procedure == "dashboard.refresh_views"
    for bad in ["refresh_views", "dashboard.refresh_views(); DROP TABLE x", "Dashboard.X"]:
        with pytest.raises(ValidationError):
            Dashboard(refresh_procedure=bad)
