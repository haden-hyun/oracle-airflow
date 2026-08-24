"""
KIS API 원시 응답 → 도메인 DataFrame 변환

KIS API 각 TR_ID의 응답 JSON을 받아 account.asset_daily / market.exchange_rate_daily
테이블 스키마에 맞는 pandas DataFrame으로 변환한다.
숫자 정규화는 base_transformer.normalize_numeric에 위임한다.

제공 함수:
    - transform_domestic_balance(): 국내주식 잔고 변환 (TTTC8434R output1)
    - transform_overseas_balance(): 해외주식 잔고 변환 (TTTS3012R output1)
    - transform_pension_fund_balance(): 연금저축 펀드 잔고 변환 및 좌수·매입단가 역산 (CTRP6548R output1[1])
    - transform_cma_cash_balance(): CMA 현금 잔고 변환 (CTRP6548R output1[14])
    - transform_exchange_rate(): 환율 변환 및 교차 환산 (FHKST03030100)
"""

import pandas as pd
from typing import List

from asset_flow.config.schemas import KIS_RENAME, BALANCE_COLUMNS, EXCHANGE_RATE_COLUMNS
from asset_flow.transformers.base_transformer import normalize_numeric

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
    """
    국내주식 잔고 원시 응답 → BALANCE_COLUMNS DataFrame

    Args:
        raw: KIS get_domestic_balance() 원시 JSON (output1 키 포함)
        standard_date: 기준일자 (YYYY-MM-DD)
        config: Airflow Variable 계좌 설정 딕셔너리

    Returns:
        BALANCE_COLUMNS 스키마의 DataFrame (보유 종목이 없으면 빈 DataFrame)
    """
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
    """
    해외주식 잔고 원시 응답 → BALANCE_COLUMNS DataFrame

    currency_code / exchange_code는 API 응답(tr_crcy_cd, ovrs_excg_cd)에서 직접 매핑된다.

    Args:
        raw: KIS get_overseas_balance() 원시 JSON (output1 키 포함)
        standard_date: 기준일자 (YYYY-MM-DD)
        config: Airflow Variable 계좌 설정 딕셔너리

    Returns:
        BALANCE_COLUMNS 스키마의 DataFrame (보유 종목이 없으면 빈 DataFrame)
    """
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
    """
    연금저축 펀드 잔고 변환 및 좌수·매입단가 역산

    CTRP6548R output1[1](펀드/MMW 행)에서 평가금액·매입금액을 읽고,
    market.fund_price_daily의 기준가(fund_price)를 사용해 좌수와 매입단가를 역산한다.

    역산 공식:
        holding_quantity   = 평가금액 / fund_price / 0.001
        unit_purchase_price = 매입금액 / holding_quantity / 0.001

    fund_price가 None이면 예외를 던진다. 좌수 자체가 평가금액의 역산 결과라
    NAV 없이는 계산할 방법이 없다 — 직전 값으로 채우지 않는다.

    Args:
        raw: KIS get_account_balance() 원시 JSON
        standard_date: 기준일자 (YYYY-MM-DD)
        config: Airflow Variable 계좌 설정 딕셔너리
        product_code: 펀드 표준코드 (Airflow param pension_fund_code)
        fund_price: market.fund_price_daily에서 조회한 T-1 기준가 (없으면 None)
        product_name: 펀드명 (없으면 None)

    Returns:
        BALANCE_COLUMNS 스키마의 단일 행 DataFrame

    Raises:
        ValueError: fund_price가 None이거나 0일 때
    """
    account_code = f"{config['account']}-{config['product_code']}"
    account_name = config["type"]

    if not fund_price:
        raise ValueError(f"fund_price가 없다 — {account_code} 평가 불가 (NAV 결측)")
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

    df["holding_quantity"] = (df["total_evaluation_amount"] / fund_price / 0.001).round(0)
    df["unit_purchase_price"] = (df["total_purchase_amount"] / df["holding_quantity"] / 0.001).round(0)

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
    """
    CMA 현금 잔고 변환

    CTRP6548R output1[14](외화단기사채 행)에서 평가금액을 읽어 CMA 잔고로 처리한다.
    CMA는 수량 개념이 없으므로 holding_quantity=1, unit_*_price=평가금액으로 고정한다.

    Args:
        raw: KIS get_account_balance() 원시 JSON
        standard_date: 기준일자 (YYYY-MM-DD)
        config: Airflow Variable 계좌 설정 딕셔너리

    Returns:
        BALANCE_COLUMNS 스키마의 단일 행 DataFrame (잔고 없으면 빈 DataFrame)
    """
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
    """
    환율 원시 응답 → EXCHANGE_RATE_COLUMNS DataFrame

    KIS API는 GBP·EUR를 달러 기준(달러/파운드, 달러/유로)으로 반환하므로,
    원/달러 기준가를 곱해 원화 기준(원/파운드, 원/유로)으로 교차 환산한다.

    Args:
        raw_list: KIS get_exchange_rate() 응답 리스트 (통화별 dict)
        standard_date: 기준일자 (YYYY-MM-DD)

    Returns:
        EXCHANGE_RATE_COLUMNS 스키마의 DataFrame (USD, JPY, GBP, EUR 4행)
    """
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
