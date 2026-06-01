import pandas as pd
from typing import List

_NUMERIC_COLS: List[str] = [
    "holding_quantity",
    "unit_purchase_price",
    "unit_market_price",
    "total_purchase_amount",
    "total_evaluation_amount",
    "total_profit_amount",
    "valuation_profit_rate",
    "multiplier",
]


def normalize_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """콤마 제거, float 변환, NaN → 0 처리"""
    result = df.copy()
    for col in _NUMERIC_COLS:
        if col in result.columns:
            result[col] = (
                pd.to_numeric(result[col].astype(str).str.replace(",", ""), errors="coerce")
                .fillna(0)
                .astype(float)
            )
    return result
