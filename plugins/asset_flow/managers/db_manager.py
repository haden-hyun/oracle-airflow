from typing import Optional, Tuple

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def create_db_engine(db_info: dict) -> Engine:
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
