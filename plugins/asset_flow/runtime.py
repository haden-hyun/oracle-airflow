"""
실행 시점 날짜 헬퍼 (KST 기준)

- kst_run_end(context): data_interval_end → 없으면 dag_run.run_after (3.x 수동 run 대비)
- standard_dates(context): 기준일 T-1 과 Upbit 캔들 경계 T (늦게 시작한 정기 run 은 skip)
- kst_today(): Param 동적 기본값 대신 런타임에 계산하는 오늘(KST)
"""

from __future__ import annotations

import datetime as dt

import pendulum

KST = pendulum.timezone("Asia/Seoul")
MAX_SCHEDULED_DELAY = dt.timedelta(hours=3)   # 초과 지연 정기 run = 따라잡기 run


def kst_run_end(context: dict) -> pendulum.DateTime:
    """
    Purpose: run 의 기준 시각을 KST 로 반환
    Parameter:
        - context: 태스크 컨텍스트 (data_interval_end, dag_run)
    Input: 없음
    Output: KST DateTime (data_interval_end 우선, 없으면 dag_run.run_after)
    """
    end = context.get("data_interval_end") or context["dag_run"].run_after
    return pendulum.instance(end).in_timezone(KST)


def standard_dates(context: dict, now: dt.datetime | None = None) -> dict:
    """
    Purpose: 수집 DAG 공통 기준일 계산 + 늦게 시작한 정기 run 차단
    Parameter:
        - context: 태스크 컨텍스트
        - now: 현재 시각 (테스트용, 기본 시스템 시계)
    Input: 시스템 시계
    Output: {"standard_date": T-1, "base_date": T} (YYYY-MM-DD, KST).
            정기 run 이 예정 시각보다 3시간 넘게 늦으면 AirflowSkipException (하류 적재도 skip)
    """
    end = kst_run_end(context)
    dag_run = context["dag_run"]
    run_type = str(getattr(dag_run.run_type, "value", dag_run.run_type)).lower()
    delay = (now or dt.datetime.now(dt.timezone.utc)) - dag_run.run_after
    # API 는 현재 값만 줌 → 늦은 정기 run 이 과거 기준일을 덮지 않게
    if run_type == "scheduled" and delay > MAX_SCHEDULED_DELAY:
        from airflow.sdk.exceptions import AirflowSkipException

        raise AirflowSkipException(f"정기 run {delay} 지연 시작 — 과거 기준일을 현재 값으로 덮지 않도록 skip "
                                   f"(필요하면 수동 실행)")
    return {"standard_date": end.subtract(days=1).to_date_string(), "base_date": end.to_date_string()}


def kst_today() -> dt.date:
    """
    Purpose: 현재 KST 날짜
    Parameter: 없음
    Input: 시스템 시계
    Output: date
    """
    return pendulum.now(KST).date()
