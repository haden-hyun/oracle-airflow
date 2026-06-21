"""
Airflow DAG 알림 콜백 모듈

사용법:
    from callbacks import slack_failure_callback, slack_recovery_callback

    default_args = {
        'on_failure_callback': slack_failure_callback,
        'on_success_callback': slack_recovery_callback,  # 이전 실패 시에만 알림
    }
"""

import os
import traceback
import requests
from airflow.models import DagRun, Variable
from airflow.utils.state import DagRunState


def _send_slack(payload: dict) -> None:
    url = Variable.get("SLACK_WEBHOOK_URL", default_var="")
    if not url:
        print("[callback] Airflow Variable 'SLACK_WEBHOOK_URL'이 설정되지 않아 알림을 건너뜁니다.")
        return
    resp = requests.post(url, json=payload, timeout=10)
    if resp.status_code != 200:
        print(f"[callback] Slack 전송 실패: {resp.status_code} {resp.text}")


def _build_dag_url(dag_id: str, run_id: str) -> str:
    base_url = os.environ.get("AIRFLOW__WEBSERVER__BASE_URL", "http://localhost:8080")
    return f"{base_url}/dags/{dag_id}/grid?dag_run_id={run_id}"


def _get_error_message(context: dict) -> str:
    exception = context.get("exception")
    if exception:
        lines = traceback.format_exception(type(exception), exception, exception.__traceback__)
        full = "".join(lines)
        # 너무 길면 마지막 3줄만 표시
        short_lines = full.strip().splitlines()
        if len(short_lines) > 6:
            return "\n".join(short_lines[-6:])
        return full.strip()
    return "에러 정보 없음"


def _was_previous_run_failed(dag_id: str, current_run_id: str) -> bool:
    """직전 '동일 run_type' DAG 실행이 실패했는지 확인 (Recovery Alert 판단용)

    수동/ad-hoc(run_type=manual) 트리거는 execution_date가 정기 스케줄 사이에
    끼어들 수 있어, 구분 없이 비교하면 정상적으로 연속 성공 중인 스케줄에도
    엉뚱한 복구 알림이 발생한다. 따라서 현재 run과 동일한 run_type만 비교한다.
    """
    try:
        current_runs = DagRun.find(dag_id=dag_id, run_id=current_run_id)
        if not current_runs:
            return False
        current_run_type = current_runs[0].run_type

        runs = DagRun.find(dag_id=dag_id, state=None)
        same_type_runs = [r for r in runs if r.run_type == current_run_type]

        # 실행 시작 시간 기준 정렬
        sorted_runs = sorted(same_type_runs, key=lambda r: r.execution_date, reverse=True)

        # 현재 run 제외하고 직전 동일 타입 run 찾기
        previous = next((r for r in sorted_runs if r.run_id != current_run_id), None)
        if previous is None:
            return False
        return previous.state == DagRunState.FAILED
    except Exception as e:
        print(f"[callback] 이전 실행 상태 조회 실패: {e}")
        return False


def slack_failure_callback(context: dict) -> None:
    """Task/DAG 실패 시 Slack 알림 (상세 에러 포함)"""
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id = context["run_id"]
    execution_date = context["execution_date"].strftime("%Y-%m-%d %H:%M KST")
    try_number = context["task_instance"].try_number
    max_tries = context["task_instance"].max_tries + 1
    dag_url = _build_dag_url(dag_id, run_id)
    error_msg = _get_error_message(context)

    payload = {
        "attachments": [
            {
                "color": "#E53935",  # 빨강
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": f":red_circle:  DAG 실패: {dag_id}",
                        },
                    },
                    {
                        "type": "section",
                        "fields": [
                            {"type": "mrkdwn", "text": f"*Task*\n`{task_id}`"},
                            {"type": "mrkdwn", "text": f"*시도*\n{try_number} / {max_tries}"},
                            {"type": "mrkdwn", "text": f"*실행 기준일*\n{execution_date}"},
                            {"type": "mrkdwn", "text": f"*Run ID*\n`{run_id}`"},
                        ],
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*에러 메시지*\n```{error_msg}```",
                        },
                    },
                    {
                        "type": "actions",
                        "elements": [
                            {
                                "type": "button",
                                "text": {"type": "plain_text", "text": "Airflow UI에서 확인"},
                                "url": dag_url,
                                "style": "danger",
                            }
                        ],
                    },
                ],
            }
        ]
    }
    _send_slack(payload)


def slack_recovery_callback(context: dict) -> None:
    """직전 실행이 실패했을 때만 성공(복구) 알림 — 매번 성공 알림 없음"""
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]

    if not _was_previous_run_failed(dag_id, run_id):
        return  # 정상 연속 성공 → 무시

    execution_date = context["execution_date"].strftime("%Y-%m-%d %H:%M KST")
    dag_url = _build_dag_url(dag_id, run_id)

    payload = {
        "attachments": [
            {
                "color": "#43A047",  # 초록
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": f":large_green_circle:  DAG 복구: {dag_id}",
                        },
                    },
                    {
                        "type": "section",
                        "fields": [
                            {"type": "mrkdwn", "text": f"*실행 기준일*\n{execution_date}"},
                            {"type": "mrkdwn", "text": f"*상태*\n이전 실패 → 현재 성공 :white_check_mark:"},
                        ],
                    },
                    {
                        "type": "actions",
                        "elements": [
                            {
                                "type": "button",
                                "text": {"type": "plain_text", "text": "Airflow UI에서 확인"},
                                "url": dag_url,
                            }
                        ],
                    },
                ],
            }
        ]
    }
    _send_slack(payload)
