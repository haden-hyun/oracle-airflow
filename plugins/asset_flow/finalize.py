"""
적재 DAG 공용 마무리 단계 (finalize = 자산·수기 원장·재적재 DAG, fail_if_upstream_failed 는 시세·배당 DAG 도 사용)

- refresh_dashboard_views: 앞 단계 결과와 무관하게(all_done) 대시보드 MV 갱신 프로시저 CALL
- fail_if_upstream_failed: 앞 단계에 실패가 있으면 run 을 실패로 (마지막 태스크 성공이 run 실패를 가리지 않게)
- 실패 알림은 실패한 태스크가 이미 보냄 → fail_if_upstream_failed 는 알림 없음
"""

from __future__ import annotations

from datetime import timedelta

from airflow.sdk import task
from airflow.sdk.exceptions import AirflowFailException

from asset_flow.assets import MV_COST_BASIS_KRW, MV_POSITION_DAILY


@task(task_id="refresh_dashboard_views", trigger_rule="all_done", retries=1, retry_delay=timedelta(seconds=30),
      outlets=[MV_POSITION_DAILY, MV_COST_BASIS_KRW])
def refresh_dashboard_views() -> None:
    """
    Purpose: 대시보드 MV 갱신 (적재 직후 화면 반영)
    Parameter: 없음
    Input: config dashboard.refresh_procedure
    Output: 없음 (+ 프로시저 CALL: MV 갱신·갱신 기록, MV Asset 이벤트). 실패 시 예외 → 알림
    """
    import time

    from asset_flow import db_manager as db
    from asset_flow.app_config import load_app_config

    name, started = load_app_config().dashboard.refresh_procedure, time.perf_counter()
    db.call_procedure(db.asset_engine(), name)
    print(f"CALL {name}() 완료 ({time.perf_counter() - started:.1f}초)")


@task(task_id="fail_if_upstream_failed", trigger_rule="one_failed", retries=0, on_failure_callback=None)
def fail_if_upstream_failed() -> None:
    """
    Purpose: 앞 단계 실패를 run 상태에 반영 (앞 단계가 모두 성공·skip 이면 이 태스크는 skip)
    Parameter: 없음
    Input: 없음
    Output: 없음. 실행되면 항상 AirflowFailException
    """
    raise AirflowFailException("앞 단계 실패 — 실패 태스크의 알림·로그 확인")


def finalize(upstream) -> None:
    """
    Purpose: 마무리 태스크 2개를 만들어 마지막 적재 태스크 뒤에 연결
    Parameter:
        - upstream: 마지막 적재 태스크 (매핑 태스크 가능)
    Input: 없음
    Output: 없음 (DAG 에 refresh_dashboard_views, fail_if_upstream_failed 추가)
    """
    upstream >> [refresh_dashboard_views(), fail_if_upstream_failed()]
