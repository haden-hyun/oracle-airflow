import pandas as pd

from ..config.schemas import UPBIT_RENAME, BALANCE_COLUMNS
from .base_transformer import normalize_numeric


def transform_upbit_balance(
    balance_raw: list,
    market_code_raw: list,
    price_raw: list,
    standard_date: str,
    account_code: str,
    account_name: str,
) -> pd.DataFrame:
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
    price_df = pd.json_normalize(price_raw)

    merged = (
        holding
        .merge(krw_markets[["market", "korean_name"]], on="market", how="left")
        .merge(price_df[["market", "trade_price"]], on="market", how="left")
        .rename(columns=UPBIT_RENAME)
    )

    for col in ["holding_quantity", "unit_purchase_price", "unit_market_price"]:
        merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0.0)

    result = merged.assign(
        standard_date=standard_date,
        account_code=account_code,
        account_name=account_name,
        asset_type="CRYPTO",
        currency_code="KRW",
        exchange_code="UPBIT",
        multiplier=1.0,
        total_purchase_amount=lambda df: df["holding_quantity"] * df["unit_purchase_price"],
        total_evaluation_amount=lambda df: df["holding_quantity"] * df["unit_market_price"],
        total_profit_amount=lambda df: df["total_evaluation_amount"] - df["total_purchase_amount"],
        valuation_profit_rate=lambda df: df["total_profit_amount"] / df["total_purchase_amount"] * 100,
    )[BALANCE_COLUMNS]

    return normalize_numeric(result)
