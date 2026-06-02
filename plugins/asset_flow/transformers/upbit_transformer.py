"""
Upbit API 원시 응답 → 도메인 DataFrame 변환

보유 자산(accounts), 마켓 코드(market/all), 일 캔들 종가(candles/days) 세 응답을
조합하여 account.asset_daily 테이블 스키마에 맞는 DataFrame을 생성한다.

KRW 잔고는 제외하고 암호화폐 보유 종목만 처리한다.
평가금액·손익·수익률은 보유수량 × 단가로 직접 계산한다.
"""

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
    """
    Upbit 보유 자산 + 마켓 코드 + 일 캔들 종가 → BALANCE_COLUMNS DataFrame

    세 API 응답을 market 키로 병합하여 종목별 잔고·평가·손익을 계산한다.
    price_raw는 /v1/candles/days 응답(T-1 종가)을 사용하므로 standard_date와 기준일이 일치한다.

    Args:
        balance_raw: /v1/accounts 응답 리스트 (보유 자산)
        market_code_raw: /v1/market/all 응답 리스트 (마켓 코드·한글명)
        price_raw: /v1/candles/days 응답 리스트 (T-1 종가, trade_price 필드 사용)
        standard_date: 기준일자 (YYYY-MM-DD, T-1)
        account_code: 계좌 식별코드 (Airflow Variable UPBIT.account)
        account_name: 계좌명 (Airflow Variable UPBIT.type)

    Returns:
        BALANCE_COLUMNS 스키마의 DataFrame (보유 종목이 없으면 빈 DataFrame)
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
