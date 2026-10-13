"""
일봉·기준가 원시 응답 → 적재 레코드 (순수 함수)

- to_price_frame(source, rows, product_code, currency, end): KIS 국내·해외, 업비트 → PRICE_COLUMNS
- to_fund_frame(rows, product_code, product_name, end): KOFIA → FUND_PRICE_COLUMNS
- 공통: end 이후(장중·당일) 행 제거, 종가 0 이하 제거, 날짜 중복 제거
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from asset_flow.config.schemas import FUND_PRICE_COLUMNS, PRICE_COLUMNS, PRICE_RENAME

_SOURCE_RENAME = {"KIS_KR_DAILY": "domestic_daily_price", "KIS_US_DAILY": "overseas_daily_price",
                  "UPBIT_DAY": "upbit_day_candle"}
_NUM = ["opening_price", "highest_price", "lowest_price", "closing_price", "trading_volume"]


def closed_upbit_candles(rows: list[dict], now: datetime | None = None) -> list[dict]:
    """
    Purpose: 마감된 업비트 일 캔들만 남김 (07:10 KST 에 T-1 캔들은 09:00 마감 전)
    Parameter:
        - rows: get_day_candles 결과
        - now: 기준 시각 (테스트용, 기본 현재 UTC)
    Input: 없음
    Output: candle_date_time_utc + 1일 <= now 인 캔들
    """
    now = now or datetime.now(timezone.utc)
    return [c for c in rows
            if pd.Timestamp(c["candle_date_time_utc"], tz="UTC") + pd.Timedelta(days=1) <= pd.Timestamp(now)]


def to_price_frame(source: str, rows: list[dict], product_code: str, currency: str, end: str) -> pd.DataFrame:
    """
    Purpose: 소스별 원시 일봉 → product_price_daily 레코드
    Parameter:
        - source: KIS_KR_DAILY | KIS_US_DAILY | UPBIT_DAY
        - rows: 원시 행 (업비트는 closed_upbit_candles 통과분)
        - product_code: 적재 키 (asset_daily.product_code)
        - currency: 통화
        - end: 적재 상한일 YYYY-MM-DD (이후 행 제거)
    Input: 없음
    Output: PRICE_COLUMNS DataFrame (standard_date 오름차순, YYYY-MM-DD 문자열). 행 없으면 빈 DataFrame
    """
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=PRICE_COLUMNS)
    if source == "UPBIT_DAY":
        df["standard_date"] = df["candle_date_time_kst"].str[:10]
    df = df.rename(columns=PRICE_RENAME[_SOURCE_RENAME[source]])
    if source != "UPBIT_DAY":
        df["standard_date"] = pd.to_datetime(df["standard_date"], format="%Y%m%d").dt.strftime("%Y-%m-%d")
    for col in _NUM:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", ""), errors="coerce")
    df = df.assign(product_code=product_code, currency_code=currency, source=source)
    df = df[(df["standard_date"] <= end) & (df["closing_price"] > 0)]
    return df.drop_duplicates("standard_date").sort_values("standard_date")[PRICE_COLUMNS].reset_index(drop=True)


def to_fund_frame(rows: list[dict], product_code: str, product_name: str, end: str) -> pd.DataFrame:
    """
    Purpose: KOFIA 기준가 이력 → fund_price_daily 레코드
    Parameter:
        - rows: get_fund_nav_history 결과
        - product_code: 펀드 표준코드
        - product_name: 표시명 (market.product)
        - end: 적재 상한일 YYYY-MM-DD (당일 공시분 제거)
    Input: 없음
    Output: FUND_PRICE_COLUMNS DataFrame (기준가 0 이하 제거)
    """
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=FUND_PRICE_COLUMNS)
    df["standard_date"] = pd.to_datetime(df["standard_date"], format="%Y%m%d").dt.strftime("%Y-%m-%d")
    df["standard_price"] = pd.to_numeric(df["standard_price"], errors="coerce")
    df = df.assign(product_code=product_code, product_name=product_name)
    df = df[(df["standard_date"] <= end) & (df["standard_price"] > 0)]
    return df.drop_duplicates("standard_date").sort_values("standard_date")[FUND_PRICE_COLUMNS].reset_index(drop=True)
