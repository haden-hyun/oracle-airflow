"""
KIS/Upbit API 토큰 사전 발급 DAG

keywords: token, KIS, Upbit, authentication, OAuth2, JWT

매일 06:50 KST에 실행되어 한국투자증권(KIS) 5개 계좌와 Upbit의 액세스 토큰을
발급하고 날짜별 JSON 파일(data/tokens/YYYYMMDD_token.json)로 저장한다.
이후 실행되는 fetch_exchange_rate, fetch_fund_price_daily, fetch_asset_daily DAG들은
이 파일에서 토큰을 읽어 API를 호출한다.

스케줄: 매일 06:50 KST (cron: '50 6 * * *')
의존성: 없음
후행 DAG: fetch_exchange_rate, fetch_fund_price_daily (ExternalTaskSensor로 연결)
"""

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
    tags=['auth', 'token', 'setup', 'daily'],
)
def make_token_dag():

    @task(task_id='generate_tokens')
    def generate_tokens() -> None:
        """
        KIS/Upbit 토큰 발급 및 파일 저장

        TokenManager.TokenGenerator()를 호출하여 당일 토큰 파일을 생성한다.
        파일이 이미 존재하면 재발급 없이 즉시 종료된다.
        """
        from asset_flow.managers.token_manager import TokenManager

        tm = TokenManager()
        tm.TokenGenerator()
        print("토큰 파일 생성 완료")

    generate_tokens()


make_token_dag()
