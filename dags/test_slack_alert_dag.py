"""
Slack 실패 알람 테스트 DAG

callbacks.py의 slack_failure_callback / slack_recovery_callback 동작을 검증한다.
테스트 완료 후 삭제해도 무방하다.

Task 구성:
    task_will_succeed  → 항상 성공
    task_will_fail     → 항상 ValueError 발생 (실패 알람 트리거)
"""

from airflow.decorators import task, dag
from callbacks import slack_failure_callback, slack_recovery_callback
from datetime import datetime, timedelta
import pendulum

kst = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'haejun',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    'on_failure_callback': slack_failure_callback,  # task 레벨: 에러 트레이스 포함
}


@dag(
    dag_id='test_slack_alert',
    default_args=default_args,
    description='Slack 실패/복구 알람 콜백 테스트용 DAG',
    doc_md=__doc__,
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=kst),
    catchup=False,
    tags=['test', 'slack', 'alert'],
    on_success_callback=slack_recovery_callback,  # DAG 레벨: DAG 전체 성공 시에만 복구 알람
)
def test_slack_alert_dag():

    @task(task_id='task_will_succeed')
    def task_will_succeed():
        print("이 태스크는 항상 성공합니다.")

    @task(task_id='task_will_fail')
    def task_will_fail():
        raise ValueError("의도된 실패 — Slack 알람 확인용 에러입니다.\n실패 콜백이 정상 작동하면 Slack 메시지가 도착해야 합니다.")

    task_will_succeed() >> task_will_fail()


test_slack_alert_dag()
