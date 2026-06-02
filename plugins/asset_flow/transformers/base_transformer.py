"""
공통 데이터 정규화 유틸리티

모든 transformer에서 공유하는 숫자형 컬럼 정규화 함수를 제공한다.
KIS/Upbit API 응답값에 포함된 쉼표(,), 빈 문자열, None 등을
일관된 float 타입으로 변환하고 NaN을 0으로 채운다.
"""

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
    """
    숫자형 컬럼 일괄 정규화

    _NUMERIC_COLS에 정의된 컬럼에 대해 쉼표 제거 → float 변환 → NaN을 0으로 채운다.
    컬럼이 DataFrame에 없으면 건너뛴다.

    Args:
        df: 정규화 대상 DataFrame

    Returns:
        정규화된 DataFrame (원본 복사본)
    """
    result = df.copy()
    for col in _NUMERIC_COLS:
        if col in result.columns:
            result[col] = (
                pd.to_numeric(result[col].astype(str).str.replace(",", ""), errors="coerce")
                .fillna(0)
                .astype(float)
            )
    return result
