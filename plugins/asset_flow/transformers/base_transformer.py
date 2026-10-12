"""
공통 숫자 정규화 유틸리티

- normalize_numeric(): 쉼표·빈 문자열·None·inf 를 float 0 으로 정리 (모든 transformer 공용)
"""

from __future__ import annotations

import numpy as np
import pandas as pd

_NUMERIC_COLS = [
    "holding_quantity", "unit_purchase_price", "unit_market_price", "total_purchase_amount",
    "total_evaluation_amount", "total_profit_amount", "valuation_profit_rate", "multiplier",
]


def normalize_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """
    Purpose: 숫자형 컬럼 일괄 정규화
    Parameter:
        - df: 대상 DataFrame
    Input: 없음
    Output: 복사본 DataFrame (쉼표 제거 → float, inf/NaN → 0, 없는 컬럼은 건너뜀)
    """
    result = df.copy()
    for col in _NUMERIC_COLS:
        if col in result.columns:
            result[col] = (
                pd.to_numeric(result[col].astype(str).str.replace(",", ""), errors="coerce")
                .replace([np.inf, -np.inf], np.nan)
                .fillna(0)
                .astype(float)
            )
    return result
