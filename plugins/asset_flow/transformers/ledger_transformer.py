"""
수동 자산(IRP·DC·적금·주택청약) 원장 → asset_daily 변환

account.manual_position_ledger의 계좌별 한 행을 그날의 자산 평가로 변환한다.
API 호출이 없는 순수 함수이며, kis_transformer / upbit_transformer와 대칭이다.

제공 함수:
    - transform_fund_position(): IRP·DC 좌수 × NAV 평가
    - transform_cash_position(): 적금·청약 원금 캐리
"""

import pandas as pd

from asset_flow.config.schemas import BALANCE_COLUMNS
from asset_flow.transformers.base_transformer import normalize_numeric

# 원장은 적금·청약의 product_code를 NULL로 둔다. asset_daily.product_code는
# NOT NULL이자 PK 일부라 상수로 채운다 (CMA의 product_code="CMA"와 같은 패턴).
_CASH_PRODUCT_CODE = {
    "INSTALLMENT_SAVINGS": "INSTALLMENT_SAVINGS",
    "HOUSING_SUBSCRIPTION": "HOUSING_SUBSCRIPTION",
}

_FUND_MULTIPLIER = 0.001


def transform_fund_position(
    row: dict, standard_date: str, fund_price: float, product_name: str | None = None
) -> pd.DataFrame:
    """
    IRP·DC 원장 행 → asset_daily 변환 (좌수 × NAV × 0.001)

    좌수는 원장의 확정값이라 fund_price가 없으면 ValueError — 직전 값으로 채우지 않는다.

    Args:
        row: get_manual_positions() 결과의 한 계좌 행
        standard_date: 평가 기준일(D). 원장 행 자체의 효력일(row["standard_date"])과는 다르다
        fund_price: standard_date 기준 NAV(market.fund_price_daily)
        product_name: 펀드명. 없으면 product_code로 채운다

    Returns:
        BALANCE_COLUMNS 스키마의 단일 행 DataFrame
    """
    if fund_price is None:
        raise ValueError(f"fund_price가 없다 — {row['account_code']} 평가 불가 (NAV 결측)")

    holding_quantity = float(row["holding_quantity"])
    total_purchase_amount = float(row["total_purchase_amount"])

    total_evaluation_amount = holding_quantity * fund_price * _FUND_MULTIPLIER
    unit_purchase_price = total_purchase_amount / holding_quantity / _FUND_MULTIPLIER
    total_profit_amount = total_evaluation_amount - total_purchase_amount
    valuation_profit_rate = (
        total_profit_amount / total_purchase_amount * 100 if total_purchase_amount else 0.0
    )

    df = pd.DataFrame([{
        "standard_date": standard_date,
        "account_code": row["account_code"],
        "account_name": row["account_name"],
        "product_code": row["product_code"],
        "product_name": product_name or row["product_code"],
        "asset_type": "FUND",
        "currency_code": "KRW",
        "exchange_code": "KRX",
        "multiplier": _FUND_MULTIPLIER,
        "holding_quantity": holding_quantity,
        "unit_purchase_price": unit_purchase_price,
        "unit_market_price": fund_price,
        "total_purchase_amount": total_purchase_amount,
        "total_evaluation_amount": total_evaluation_amount,
        "total_profit_amount": total_profit_amount,
        "valuation_profit_rate": valuation_profit_rate,
    }])
    return normalize_numeric(df)[BALANCE_COLUMNS]


def transform_cash_position(row: dict, standard_date: str) -> pd.DataFrame:
    """
    적금·청약 원장 행 → asset_daily 변환 (원금 캐리)

    transform_cma_cash_balance와 같은 패턴 — 수량 개념이 없어 holding_quantity=1,
    unit_*_price=원금으로 고정한다. 이자는 만기 계산이라 손익은 항상 0.

    Args:
        row: get_manual_positions() 결과의 한 계좌 행
        standard_date: 평가 기준일(D)

    Returns:
        BALANCE_COLUMNS 스키마의 단일 행 DataFrame
    """
    total_purchase_amount = float(row["total_purchase_amount"])
    product_code = _CASH_PRODUCT_CODE[row["account_type"]]

    df = pd.DataFrame([{
        "standard_date": standard_date,
        "account_code": row["account_code"],
        "account_name": row["account_name"],
        "product_code": product_code,
        "product_name": row["account_name"],
        "asset_type": "CASH",
        "currency_code": "KRW",
        "exchange_code": None,
        "multiplier": 1.0,
        "holding_quantity": 1.0,
        "unit_purchase_price": total_purchase_amount,
        "unit_market_price": total_purchase_amount,
        "total_purchase_amount": total_purchase_amount,
        "total_evaluation_amount": total_purchase_amount,
        "total_profit_amount": 0.0,
        "valuation_profit_rate": 0.0,
    }])
    return normalize_numeric(df)[BALANCE_COLUMNS]
