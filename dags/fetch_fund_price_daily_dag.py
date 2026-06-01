from airflow.decorators import task, dag
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pendulum
import pandas as pd
from sqlalchemy import text

kst = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'haejun',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id='fetch_fund_price_daily',
    default_args=default_args,
    description='매일 06:55 펀드 기준가 크롤링 및 DB 적재 (fundguide.net)',
    schedule='55 6 * * *',
    start_date=datetime(2024, 1, 1, tzinfo=kst),
    catchup=False,
    params={
        'fund_price': Param({
            'schema': 'market',
            'table_name': 'fund_price_daily',
        }),
        'fund_codes': Param(
            ['K553W5E17401', 'K55105D43299'],
            description='크롤링할 펀드 표준코드 리스트',
        ),
    },
)
def fetch_fund_price_daily_dag():

    @task(task_id='get_standard_date')
    def get_standard_date() -> str:
        context = get_current_context()
        utc_end = context.get('data_interval_end')
        kst_end = utc_end.in_timezone("Asia/Seoul")
        standard_date = (kst_end - relativedelta(days=1)).strftime('%Y-%m-%d')
        print(f"기준일자: [{standard_date}]")
        return standard_date

    @task(task_id='crawl_fund_prices')
    def crawl_fund_prices(standard_date: str) -> list:
        from asset_flow.crawler.fund_crawler import fetch_fund_quote, to_fund_price_record
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

        context = get_current_context()
        fund_codes = context.get('params')['fund_codes']

        records = []
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            browser_ctx = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
                ),
                locale="ko-KR",
            )
            page = browser_ctx.new_page()

            for code in fund_codes:
                try:
                    quote = fetch_fund_quote(page, code)
                    if quote.nav is None:
                        print(f"[SKIP] {code} — nav 없음")
                        continue
                    record = to_fund_price_record(quote.to_dict(), standard_date)
                    records.append(record)
                    print(f"[OK] {code} nav={quote.nav}")
                except PWTimeout:
                    print(f"[TIMEOUT] {code}")
                except Exception as e:
                    print(f"[ERROR] {code}: {e}")

            browser.close()

        print(f"크롤링 완료: {len(records)}건 / {len(fund_codes)}개 코드")
        return records

    @task(task_id='upload_fund_prices')
    def upload_fund_prices(standard_date: str, records: list) -> None:
        if not records:
            print("적재할 펀드 데이터가 없습니다.")
            return

        context = get_current_context()
        params = context.get('params')['fund_price']
        schema = params['schema']
        table_name = params['table_name']
        full_table_name = f"{schema}.{table_name}"

        df = pd.DataFrame(records)

        pg_hook = PostgresHook(postgres_conn_id='postgres_asset')
        engine = pg_hook.get_sqlalchemy_engine()

        with engine.begin() as conn:
            conn.execute(
                text(f"DELETE FROM {full_table_name} WHERE standard_date = :std_date"),
                {"std_date": standard_date},
            )
            df.to_sql(
                name=table_name,
                con=conn,
                schema=schema,
                if_exists='append',
                index=False,
            )

        print(f"[{standard_date}] 펀드 기준가 적재 완료: {len(df)}건")

    # 의존성 구성
    std_date = get_standard_date()
    records = crawl_fund_prices(std_date)
    upload_fund_prices(std_date, records)


fetch_fund_price_daily_dag()
