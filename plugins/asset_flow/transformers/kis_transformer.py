import pandas as pd
from typing import List

from ..config.schemas import KIS_RENAME, BALANCE_COLUMNS, EXCHANGE_RATE_COLUMNS
from .base_transformer import normalize_numeric

_CURRENCY_CODE_MAP = {
    "원/달러": "USD",
    "원/엔": "JPY",
    "원/파운드": "GBP",
    "원/유로": "EUR",
}

_PRICE_COLS: List[str] = [
    "base_rate",
    "previous_day_closing_price",
    "opening_price",
    "highest_price",
    "lowest_price",
]


def transform_domestic_balance(
    raw: dict,
    standard_date: str,
    config: dict,
) -> pd.DataFrame:
    account_code = f"{config['account']}-{config['product_code']}"
    account_name = config["type"]
    df = pd.json_normalize(raw["output1"])
    if df.empty:
        return pd.DataFrame()
    return normalize_numeric(
        df.rename(columns=KIS_RENAME["domestic_balance"])
        .assign(
            standard_date=standard_date,
            account_code=account_code,
            account_name=account_name,
            asset_type="STOCK",
            currency_code="KRW",
            exchange_code="KRX",
            multiplier=1.0,
        )
        [BALANCE_COLUMNS]
    )


def transform_overseas_balance(
    raw: dict,
    standard_date: str,
    config: dict,
) -> pd.DataFrame:
    account_code = f"{config['account']}-{config['product_code']}"
    account_name = config["type"]
    df = pd.json_normalize(raw["output1"])
    if df.empty:
        return pd.DataFrame()
    return normalize_numeric(
        df.rename(columns=KIS_RENAME["overseas_balance"])
        .assign(
            standard_date=standard_date,
            account_code=account_code,
            account_name=account_name,
            asset_type="STOCK",
            multiplier=1.0,
        )
        [BALANCE_COLUMNS]
    )


def transform_pension_fund_balance(
    raw: dict,
    standard_date: str,
    config: dict,
    product_code: str,
    fund_price: float | None,
    product_name: str | None,
) -> pd.DataFrame:
    account_code = f"{config['account']}-{config['product_code']}"
    account_name = config["type"]
    df = pd.json_normalize(raw["output1"])
    # index 1 = "펀드/MMW" 행 (KIS API 응답 순서 고정)
    df = df.loc[[1]].rename(columns=KIS_RENAME["account_balance"])

    df["total_purchase_amount"] = df["total_purchase_amount"].astype(float)
    df["total_evaluation_amount"] = df["total_evaluation_amount"].astype(float)
    df["total_profit_amount"] = df["total_profit_amount"].astype(float)

    df = df.assign(
        standard_date=standard_date,
        account_code=account_code,
        account_name=account_name,
        asset_type="FUND",
        currency_code="KRW",
        exchange_code="KRX",
        product_code=product_code,
        product_name=product_name,
        unit_market_price=fund_price,
        multiplier=0.001,
    )

    if fund_price:
        df["holding_quantity"] = (df["total_evaluation_amount"] / fund_price / 0.001).round(4)
        df["unit_purchase_price"] = (df["total_purchase_amount"] / df["holding_quantity"] / 0.001).round(0)
    else:
        df["holding_quantity"] = 0.0
        df["unit_purchase_price"] = 0.0

    if (df["total_purchase_amount"] > 0).all():
        df["valuation_profit_rate"] = (df["total_profit_amount"] / df["total_purchase_amount"] * 100).round(2)
    else:
        df["valuation_profit_rate"] = 0.0

    return normalize_numeric(df)[BALANCE_COLUMNS]


def transform_cma_cash_balance(
    raw: dict,
    standard_date: str,
    config: dict,
) -> pd.DataFrame:
    account_code = f"{config['account']}-{config['product_code']}"
    account_name = config["type"]
    df = pd.json_normalize(raw["output1"])
    if df.empty:
        return pd.DataFrame()
    # index 14 = "외화단기사채" 행 위치에 CMA 현금 잔고가 반환됨 (KIS API 응답 순서 고정)
    df = df.loc[[14]].rename(columns=KIS_RENAME["account_balance"])

    df["total_purchase_amount"] = df["total_purchase_amount"].astype(float)
    df["total_evaluation_amount"] = df["total_evaluation_amount"].astype(float)
    df["total_profit_amount"] = df["total_profit_amount"].astype(float)

    df = df.assign(
        standard_date=standard_date,
        account_code=account_code,
        account_name=account_name,
        total_purchase_amount=lambda x: x["total_evaluation_amount"],
        asset_type="CASH",
        currency_code="KRW",
        exchange_code="KRX",
        product_code="CMA",
        product_name="CMA",
        unit_market_price=lambda x: x["total_evaluation_amount"],
        multiplier=1.0,
        holding_quantity=1.0,
        unit_purchase_price=lambda x: x["total_evaluation_amount"],
        valuation_profit_rate=0.0,
    )

    return normalize_numeric(df)[BALANCE_COLUMNS]


def transform_exchange_rate(raw_list: list, standard_date: str) -> pd.DataFrame:
    df = pd.concat(
        [pd.json_normalize(r["output1"]) for r in raw_list],
        ignore_index=True,
    )
    df.rename(columns=KIS_RENAME["exchange_rate"], inplace=True)
    df.insert(0, "standard_date", standard_date)
    df[_PRICE_COLS] = df[_PRICE_COLS].astype(float)

    # 달러/파운드, 달러/유로 → 원/파운드, 원/유로 (원/달러 기준 교차 환산)
    usd_krw = df.loc[df["currency_pair_name"] == "원/달러", _PRICE_COLS].values[0]
    cross_mask = df["currency_pair_name"].isin(["달러/파운드", "달러/유로"])

    cross = df[cross_mask].copy()
    cross[_PRICE_COLS] = cross[_PRICE_COLS].multiply(usd_krw, axis=1)
    cross["currency_pair_name"] = cross["currency_pair_name"].str.replace("달러/", "원/")

    result = pd.concat([df[~cross_mask], cross], ignore_index=True)
    result["currency_code"] = result["currency_pair_name"].map(_CURRENCY_CODE_MAP)

    return result[EXCHANGE_RATE_COLUMNS]
