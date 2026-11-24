"""
일봉·기준가 원시 응답 → 적재 레코드 (순수 함수)

- kst_day_ohlc(rows, now): 업비트 60분봉 → KST 일별 OHLCV (종가 = 자정 직전 체결가, 마감된 날만)
- to_price_frame(source, rows, product_code, currency, end): KIS 국내·해외, 업비트(kst_day_ohlc 결과) → PRICE_COLUMNS
- to_fund_frame(rows, product_code, product_name, end): KOFIA → FUND_PRICE_COLUMNS
- 공통: end 이후(장중·당일) 행 제거, 종가 0 이하 제거, 날짜 중복 제거
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone

import pandas as pd

from asset_flow.config.schemas import FUND_PRICE_COLUMNS, PRICE_COLUMNS, PRICE_RENAME

log = logging.getLogger(__name__)

_SOURCE_RENAME = {"KIS_KR_DAILY": "domestic_daily_price", "KIS_US_DAILY": "overseas_daily_price"}
_NUM = ["opening_price", "highest_price", "lowest_price", "closing_price", "trading_volume"]
_KST = timezone(timedelta(hours=9))
_LATE_CLOSE_HOUR = 22                                   # 종가 캔들이 이 시각(KST) 전에 시작하면 경고


def kst_day_ohlc(rows: list[dict], now: datetime | None = None) -> list[dict]:
    """
    Purpose: 업비트 60분봉 → KST 날짜별 OHLCV (d 는 d+1 00:00 KST 가 지난 날만)
    Parameter:
        - rows: get_hour_candles 결과
        - now: 기준 시각 (테스트용, 기본 현재)
    Input: 없음
    Output: [{standard_date, opening_price, highest_price, lowest_price, closing_price, trading_volume}] 날짜 오름차순.
            종가 캔들이 22시 전 시작이면 경고 로그
    """
    today = (now or datetime.now(timezone.utc)).astimezone(_KST).date()
    by_day: dict[str, dict[str, dict]] = {}
    for c in rows:
        by_day.setdefault(c["candle_date_time_kst"][:10], {})[c["candle_date_time_kst"]] = c
    out = []
    for d in sorted(by_day):
        if date.fromisoformat(d) >= today:
            continue
        candles = [by_day[d][k] for k in sorted(by_day[d])]
        last = candles[-1]["candle_date_time_kst"]
        if int(last[11:13]) < _LATE_CLOSE_HOUR:
            log.warning("[%s] %s 종가 캔들 %s 시작 (거래 없는 시간)", candles[-1].get("market"), d, last[11:16])
        out.append({"standard_date": d, "opening_price": candles[0]["opening_price"],
                    "highest_price": max(c["high_price"] for c in candles),
                    "lowest_price": min(c["low_price"] for c in candles),
                    "closing_price": candles[-1]["trade_price"],
                    "trading_volume": round(sum(c["candle_acc_trade_volume"] for c in candles), 8)})
    return out


def to_price_frame(source: str, rows: list[dict], product_code: str, currency: str, end: str) -> pd.DataFrame:
    """
    Purpose: 소스별 원시 일봉 → product_price_daily 레코드
    Parameter:
        - source: KIS_KR_DAILY | KIS_US_DAILY | UPBIT_DAY
        - rows: 원시 행 (업비트는 kst_day_ohlc 결과)
        - product_code: 적재 키 (asset_daily.product_code)
        - currency: 통화
        - end: 적재 상한일 YYYY-MM-DD (이후 행 제거)
    Input: 없음
    Output: PRICE_COLUMNS DataFrame (standard_date 오름차순, YYYY-MM-DD 문자열). 행 없으면 빈 DataFrame
    """
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=PRICE_COLUMNS)
    if source != "UPBIT_DAY":
        df = df.rename(columns=PRICE_RENAME[_SOURCE_RENAME[source]])
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
