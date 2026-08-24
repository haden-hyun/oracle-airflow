"""
데이터베이스 접근 헬퍼

PostgreSQL 연결 엔진 생성과 업무 도메인별 조회 함수를 제공한다.
Airflow 내부에서는 PostgresHook을 통해 엔진을 주입받으며,
이 모듈의 함수들은 로컬 실행 또는 단위 테스트에서도 직접 사용할 수 있다.
"""

from typing import Optional, Tuple

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def create_db_engine(db_info: dict) -> Engine:
    """
    PostgreSQL 연결 엔진 생성

    Args:
        db_info: {user, password, host, port, database} 접속 정보 딕셔너리

    Returns:
        SQLAlchemy Engine 객체
    """
    url = (
        f"postgresql://{db_info['user']}:{db_info['password']}"
        f"@{db_info['host']}:{db_info['port']}/{db_info['database']}"
    )
    return create_engine(url)


def get_fund_price(
    engine: Engine,
    product_code: str,
    standard_date: str,
) -> Tuple[Optional[float], Optional[str]]:
    """market.fund_price_daily 에서 기준가와 종목명 반환.

    기준가가 없거나 0이면 (None, None) 반환.
    """
    query = text("""
        SELECT standard_price, product_name
        FROM market.fund_price_daily
        WHERE product_code = :product_code
          AND standard_date = :standard_date
        LIMIT 1
    """)
    with engine.connect() as conn:
        row = conn.execute(
            query,
            {"product_code": product_code, "standard_date": standard_date},
        ).fetchone()

    if row is None or not row.standard_price:
        return None, None

    return float(row.standard_price), row.product_name


def get_manual_positions(engine: Engine, standard_date: str) -> pd.DataFrame:
    """기준일 D 시점에 유효한 수동 자산 포지션을 계좌별 1행씩 반환한다.

    수동 자산(IRP·DC·적금·주택청약)은 원장에 월 1회만 행이 생기므로,
    이 함수가 계좌별로 standard_date 이하 중 가장 최근 원장 행을 골라
    "D 시점에 유효한 포지션"을 채워 넣는다. 해지 계좌(is_active=false)는 제외한다.

    조회 결과가 없으면 빈 DataFrame을 반환한다. 예외는 호출자(get_X)가 던진다.
    """
    query = text("""
        SELECT DISTINCT ON (l.account_code)
               l.standard_date, l.account_code, l.product_code,
               l.holding_quantity, l.total_purchase_amount,
               m.account_name, m.account_type
          FROM account.manual_position_ledger l
          JOIN account.account_master m USING (account_code)
         WHERE l.standard_date <= :standard_date
           AND m.is_active
         ORDER BY l.account_code, l.standard_date DESC
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params={"standard_date": standard_date})


def delete_and_insert_account_assets(
    engine: Engine,
    schema: str,
    table_name: str,
    standard_date: str,
    account_code: str,
    records: list,
) -> int:
    """(standard_date, account_code)에 스코프된 멱등 적재.

    해당 계좌·기준일 행만 DELETE 후 records를 INSERT하므로, 계좌 단위
    재시도/백필이 다른 계좌 데이터를 덮어쓰지 않는다.

    DELETE 행 수는 정상 첫 실행이면 0이다. 재실행이 아닌데 0이 아니면 같은
    (standard_date, account_code)에 쓰는 upload가 둘 이상이라는 신호다.

    Returns:
        삽입된 행 수
    """
    full_table_name = f"{schema}.{table_name}"
    with engine.begin() as conn:
        result = conn.execute(
            text(f"DELETE FROM {full_table_name} WHERE standard_date = :std_date AND account_code = :account_code"),
            {"std_date": standard_date, "account_code": account_code},
        )
        if result.rowcount:
            print(f"[재적재] {standard_date} {account_code}: 기존 {result.rowcount}건 삭제 후 재적재")
        if records:
            pd.DataFrame(records).to_sql(
                name=table_name,
                con=conn,
                schema=schema,
                if_exists='append',
                index=False,
            )
    return len(records)
