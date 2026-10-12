"""
수기 자산(IRP·DC·적금·청약) 원장 행 → asset_daily 변환 (순수 함수)

- transform_fund_position(): 좌수 × NAV × multiplier (multiplier 는 config manual_asset_types)
- transform_cash_position(): 원금 캐리
"""

from __future__ import annotations

import pandas as pd

from asset_flow.config.schemas import BALANCE_COLUMNS
from asset_flow.transformers.base_transformer import normalize_numeric

# 원장은 적금·청약 product_code 를 NULL 로 둠 → asset_daily PK 용 상수 (CMA="CMA" 패턴)
_CASH_PRODUCT_CODE = {
    "INSTALLMENT_SAVINGS": "INSTALLMENT_SAVINGS",
    "HOUSING_SUBSCRIPTION": "HOUSING_SUBSCRIPTION",
}


def transform_fund_position(
    row: dict, standard_date: str, fund_price: float | None, product_name: str | None, multiplier: float
) -> pd.DataFrame:
    """
    Purpose: IRP·DC 원장 행 평가 (좌수 × NAV × multiplier)
    Parameter:
        - row: get_manual_positions() 의 한 계좌 행
        - standard_date: 평가 기준일 (원장 효력일과 다름)
        - fund_price: 기준일 NAV (없으면 None)
        - product_name: 펀드명 (없으면 product_code 로 대체)
        - multiplier: 좌수 단위 배수 (예: 0.001)
    Input: 없음
    Output: BALANCE_COLUMNS 1행 DataFrame (NAV 없으면 ValueError — 직전 값으로 채우지 않음)
    """
    if fund_price is None:
        raise ValueError(f"fund_price가 없다 — {standard_date} 평가 불가 (NAV 결측)")
    qty = float(row["holding_quantity"])
    purchase = float(row["total_purchase_amount"])
    evaluation = qty * fund_price * multiplier
    profit = evaluation - purchase
    df = pd.DataFrame([{
        "standard_date": standard_date,
        "account_code": row["account_code"],
        "account_name": row["account_name"],
        "product_code": row["product_code"],
        "product_name": product_name or row["product_code"],
        "asset_type": "FUND",
        "currency_code": "KRW",
        "exchange_code": "KRX",
        "multiplier": multiplier,
        "holding_quantity": qty,
        "unit_purchase_price": purchase / qty / multiplier,
        "unit_market_price": fund_price,
        "total_purchase_amount": purchase,
        "total_evaluation_amount": evaluation,
        "total_profit_amount": profit,
        "valuation_profit_rate": profit / purchase * 100 if purchase else 0.0,
    }])
    return normalize_numeric(df)[BALANCE_COLUMNS]


def transform_cash_position(row: dict, standard_date: str) -> pd.DataFrame:
    """
    Purpose: 적금·청약 원장 행 평가 (원금 캐리, 손익 0)
    Parameter:
        - row: get_manual_positions() 의 한 계좌 행
        - standard_date: 평가 기준일
    Input: 없음
    Output: BALANCE_COLUMNS 1행 DataFrame
    """
    amount = float(row["total_purchase_amount"])
    df = pd.DataFrame([{
        "standard_date": standard_date,
        "account_code": row["account_code"],
        "account_name": row["account_name"],
        "product_code": _CASH_PRODUCT_CODE[row["account_type"]],
        "product_name": row["account_name"],
        "asset_type": "CASH",
        "currency_code": "KRW",
        "exchange_code": None,
        "multiplier": 1.0,
        "holding_quantity": 1.0,
        "unit_purchase_price": amount,
        "unit_market_price": amount,
        "total_purchase_amount": amount,
        "total_evaluation_amount": amount,
        "total_profit_amount": 0.0,
        "valuation_profit_rate": 0.0,
    }])
    return normalize_numeric(df)[BALANCE_COLUMNS]
