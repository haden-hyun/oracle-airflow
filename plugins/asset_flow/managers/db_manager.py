"""
데이터베이스 접근 헬퍼

PostgreSQL 연결 엔진 생성과 업무 도메인별 조회 함수를 제공한다.
Airflow 내부에서는 PostgresHook을 통해 엔진을 주입받으며,
이 모듈의 함수들은 로컬 실행 또는 단위 테스트에서도 직접 사용할 수 있다.
"""

from typing import Optional, Tuple

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
