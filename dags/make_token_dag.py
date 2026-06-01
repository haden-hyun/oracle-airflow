from airflow.decorators import task, dag
from datetime import datetime, timedelta
import pendulum

kst = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'haejun',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}


@dag(
    dag_id='make_token',
    default_args=default_args,
    description='매일 06:50 KIS/Upbit 토큰 발급 및 파일 저장',
    schedule='50 6 * * *',
    start_date=datetime(2024, 1, 1, tzinfo=kst),
    catchup=False,
)
def make_token_dag():

    @task(task_id='generate_tokens')
    def generate_tokens() -> None:
        from asset_flow.managers.token_manager import TokenManager

        tm = TokenManager()
        tm.TokenGenerator()
        print("토큰 파일 생성 완료")

    generate_tokens()


make_token_dag()
