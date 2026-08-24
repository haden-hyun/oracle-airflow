"""
펀드 기준가 크롤링 및 DB 적재 DAG

keywords: fund, NAV, 기준가, fundguide, Playwright, crawling, market data, PostgreSQL

매일 06:55 KST에 실행되어 fundguide.net에서 연금저축 펀드의 전일(T-1) 기준가(NAV)를
Playwright 헤드리스 브라우저로 크롤링하고 market.fund_price_daily 테이블에 적재한다.
수집된 기준가는 fetch_asset_daily DAG의 연금저축 펀드 좌수 역산 계산에 사용된다.

스케줄: 매일 06:55 KST (cron: '55 6 * * *')
의존성: 없음 (make_token 불필요 — 크롤러는 API 인증 없이 동작)
후행 DAG: fetch_asset_daily (ExternalTaskSensor로 연결)
적재 테이블: market.fund_price_daily
"""

from airflow.decorators import task, dag
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from callbacks import slack_failure_callback, slack_recovery_callback
import pendulum
import pandas as pd
from sqlalchemy import text

kst = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'haejun',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': slack_failure_callback,
}

DAG_DOC = """
### 목적
연금저축·IRP·DC가 투자하는 펀드의 기준가(NAV)를 fundguide.net에서 크롤링한다.
`fetch_asset_daily`가 이 값으로 좌수 역산(연금저축) 또는 평가금액 계산(IRP·DC)을 한다.

### Pipeline
1. `get_standard_date`: 실행일 기준 T-1 계산
2. `crawl_fund_prices`: `fund_codes` 파라미터를 순회하며 Playwright로 기준가 크롤링.
   타임아웃/NAV 없음인 코드는 예외 없이 건너뛴다(`[SKIP]` 로그만 남김)
3. `upload_fund_prices`: 크롤링된 만큼만 DELETE+INSERT (0건이면 적재 없이 종료)

### Task
| Task | 내용 |
|---|---|
| `get_standard_date` | 기준일(T-1) 계산 |
| `crawl_fund_prices` | 펀드코드별 기준가 크롤링, 실패 코드는 스킵 |
| `upload_fund_prices` | DB 적재 |

> 크롤링이 코드 단위로 조용히 스킵되므로, 특정 펀드의 NAV가 그날 결측일 수 있다.
> 다운스트림(`fetch_asset_daily`)이 결측을 어떻게 처리하는지는 해당 DAG 문서 참고.
"""


@dag(
    dag_id='fetch_fund_price_daily',
    default_args=default_args,
    description='매일 06:55 펀드 기준가 크롤링 및 DB 적재 (fundguide.net)',
    doc_md=DAG_DOC,
    schedule='55 6 * * *',
    start_date=datetime(2024, 1, 1, tzinfo=kst),
    catchup=False,
    tags=['market', 'fund', 'NAV', 'crawling', 'daily', 'ingestion'],
    on_success_callback=slack_recovery_callback,
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
        """
        Airflow data_interval_end 기준 전일 날짜(T-1) 반환

        Returns:
            standard_date: 기준일자 문자열 (YYYY-MM-DD)
        """
        context = get_current_context()
        utc_end = context.get('data_interval_end')
        kst_end = utc_end.in_timezone("Asia/Seoul")
        standard_date = (kst_end - relativedelta(days=1)).strftime('%Y-%m-%d')
        print(f"기준일자: [{standard_date}]")
        return standard_date

    @task(task_id='crawl_fund_prices')
    def crawl_fund_prices(standard_date: str) -> list:
        """
        fundguide.net에서 펀드 기준가 크롤링

        Airflow param fund_codes에 정의된 펀드 표준코드 목록을 순회하며
        Playwright 헤드리스 브라우저로 기준가를 수집한다.
        nav가 None이거나 타임아웃이 발생한 코드는 건너뛴다.

        Args:
            standard_date: 기준일자 (YYYY-MM-DD, T-1) — DB 레코드 standard_date 필드에 저장

        Returns:
            list: fund_price_daily 스키마 dict 레코드 리스트 [{standard_date, product_code, product_name, standard_price}]
        """
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
        """
        크롤링된 펀드 기준가 레코드를 market.fund_price_daily 테이블에 적재

        기존 standard_date 행을 먼저 삭제(DELETE)한 뒤 신규 삽입(INSERT)하여 멱등성을 보장한다.

        Args:
            standard_date: 기준일자 (YYYY-MM-DD) — DELETE 조건 및 로그 출력용
            records: crawl_fund_prices() 결과 레코드 리스트
        """
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
