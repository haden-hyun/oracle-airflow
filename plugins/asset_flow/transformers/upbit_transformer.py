"""
Upbit API 원시 응답 → 도메인 DataFrame 변환 (순수 함수)

- transform_upbit_balance(): 잔고 + 마켓 한글명 + T-1 일 캔들 종가 병합 (KRW 잔고 제외)
"""

from __future__ import annotations

import pandas as pd

from asset_flow.config.schemas import BALANCE_COLUMNS, UPBIT_RENAME
from asset_flow.transformers.base_transformer import normalize_numeric


def transform_upbit_balance(
    balance_raw: list, market_code_raw: list, price_raw: list, standard_date: str, account_code: str, account_name: str
) -> pd.DataFrame:
    """
    Purpose: Upbit 보유 코인 평가 (수량 × T-1 종가)
    Parameter:
        - balance_raw: /v1/accounts 응답
        - market_code_raw: /v1/market/all 응답
        - price_raw: /v1/candles/days 응답 (T-1 종가)
        - standard_date: 기준일
        - account_code: 계좌 코드 ('UPBIT')
        - account_name: 표시명
    Input: 없음
    Output: BALANCE_COLUMNS DataFrame (보유 코인 없으면 빈 DataFrame)
    """
    balance_df = pd.json_normalize(balance_raw)
    if balance_df.empty:
        return pd.DataFrame()
    holding = (
        balance_df.query('currency != "KRW"')
        .assign(market=lambda df: "KRW-" + df["currency"])
        .reset_index(drop=True)
    )
    if holding.empty:
        return pd.DataFrame()
    market_df = pd.json_normalize(market_code_raw)
    krw_markets = market_df[market_df["market"].str.startswith("KRW-")]
    merged = (
        holding.merge(krw_markets[["market", "korean_name"]], on="market", how="left")
        .merge(pd.json_normalize(price_raw)[["market", "trade_price"]], on="market", how="left")
        .rename(columns=UPBIT_RENAME)
    )
    for col in ["holding_quantity", "unit_purchase_price", "unit_market_price"]:
        merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0.0)
    result = merged.assign(
        standard_date=standard_date, account_code=account_code, account_name=account_name, asset_type="CRYPTO",
        currency_code="KRW", exchange_code="UPBIT", multiplier=1.0,
        total_purchase_amount=lambda df: df["holding_quantity"] * df["unit_purchase_price"],
        total_evaluation_amount=lambda df: df["holding_quantity"] * df["unit_market_price"],
        total_profit_amount=lambda df: df["total_evaluation_amount"] - df["total_purchase_amount"],
        valuation_profit_rate=lambda df: df["total_profit_amount"] / df["total_purchase_amount"] * 100,
    )[BALANCE_COLUMNS]
    return normalize_numeric(result)
