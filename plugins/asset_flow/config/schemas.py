"""
출력 스키마와 API 필드명 매핑

- KIS_RENAME / UPBIT_RENAME: API 응답 필드 → 도메인 필드
- BalanceRecord / ExchangeRateRecord: asset_daily / exchange_rate_daily 스키마
- BALANCE_COLUMNS / EXCHANGE_RATE_COLUMNS: DataFrame 컬럼 순서
- DividendEventRecord / DividendReceiptRecord (+ _COLUMNS·_PK): market.dividend_event / account.dividend_receipt
- PRICE_RENAME, PriceRecord / FundPriceRecord (+ _COLUMNS): market.product_price_daily / fund_price_daily
"""

import datetime as dt
from typing import TypedDict

# ── API 필드명 → 도메인 필드명 변환 매핑 ──
KIS_RENAME: dict[str, dict[str, str]] = {
    "account_balance": {
        "pchs_amt": "total_purchase_amount",  # 매입금액
        "evlu_amt": "total_evaluation_amount",  # 평가금액
        "evlu_pfls_amt": "total_profit_amount",  # 평가손익금액
    },
    "exchange_rate": {
        "ovrs_nmix_prdy_vrss": "change_from_previous_day",  # 전일대비
        "prdy_vrss_sign": "change_from_previous_day_sign",  # 전일대비 기호
        "prdy_ctrt": "change_from_previous_day_rate",  # 전일대비 비율
        "ovrs_nmix_prdy_clpr": "previous_day_closing_price",  # 전일 종가
        "acml_vol": "total_volume",  # 누적거래량
        "hts_kor_isnm": "currency_pair_name",  # 통화쌍 이름
        "ovrs_nmix_prpr": "base_rate",  # 매매기준율
        "stck_shrn_iscd": "symbol",  # 심볼
        "prdy_vol": "previous_day_volume",  # 전일 거래량
        "ovrs_prod_oprc": "opening_price",  # 시가
        "ovrs_prod_hgpr": "highest_price",  # 최고가
        "ovrs_prod_lwpr": "lowest_price",  # 최저가
    },
    "domestic_balance": {
        "pdno": "product_code",  # 종목코드
        "prdt_name": "product_name",  # 종목명
        "evlu_pfls_amt": "total_profit_amount",  # 평가손익금액
        "evlu_pfls_rt": "valuation_profit_rate",  # 평가손익비율
        "pchs_avg_pric": "unit_purchase_price",  # 매입단가
        "hldg_qty": "holding_quantity",  # 보유수량
        "pchs_amt": "total_purchase_amount",  # 매입금액
        "evlu_amt": "total_evaluation_amount",  # 평가금액
        "prpr": "unit_market_price",  # 현재가격
    },
    "overseas_balance": {
        "ovrs_pdno": "product_code",  # 종목코드
        "ovrs_item_name": "product_name",  # 종목명
        "frcr_evlu_pfls_amt": "total_profit_amount",  # 평가손익금액
        "evlu_pfls_rt": "valuation_profit_rate",  # 평가손익비율
        "pchs_avg_pric": "unit_purchase_price",  # 매입단가
        "ovrs_cblc_qty": "holding_quantity",  # 보유수량
        "frcr_pchs_amt1": "total_purchase_amount",  # 매입금액
        "ovrs_stck_evlu_amt": "total_evaluation_amount",  # 평가금액
        "now_pric2": "unit_market_price",  # 현재가격
        "tr_crcy_cd": "currency_code",  # 통화코드
        "ovrs_excg_cd": "exchange_code",  # 거래소코드
    },
}

UPBIT_RENAME: dict[str, str] = {
    "currency": "product_code",  # 종목코드
    "korean_name": "product_name",  # 종목명
    "balance": "holding_quantity",  # 보유수량
    "trade_price": "unit_market_price",  # 현재가격
    "avg_buy_price": "unit_purchase_price",  # 매입단가
}


# ── 출력 스키마 정의 (TypedDict) ──
class BalanceRecord(TypedDict):
    standard_date: str
    account_code: str
    account_name: str
    product_code: str
    product_name: str
    asset_type: str
    currency_code: str
    exchange_code: str
    multiplier: float
    holding_quantity: float
    unit_purchase_price: float
    unit_market_price: float
    total_purchase_amount: float
    total_evaluation_amount: float
    total_profit_amount: float
    valuation_profit_rate: float


class ExchangeRateRecord(TypedDict):
    standard_date: str
    symbol: str
    currency_code: str
    currency_pair_name: str
    base_rate: float
    previous_day_closing_price: float
    opening_price: float
    highest_price: float
    lowest_price: float


class DividendEventRecord(TypedDict):
    product_code: str
    record_date: dt.date
    dividend_kind: str              # CASH | SPECIAL
    market: str                     # KR | US
    product_name: str | None
    dividend_label: str | None      # KR divi_kind(결산·분기·반기·'') / US ca_title — 원문
    ex_date: dt.date | None
    pay_date: dt.date | None
    dps_amount: float | None        # NULL = 금액 미정 (US 금액 지연, KR 미확정 0원)
    currency_code: str
    dividend_rate: float | None     # KR divi_rate 원문 (의미 불명, 화면 미사용)
    is_confirmed: bool
    announce_date: dt.date | None
    source: str


class DividendReceiptRecord(TypedDict):
    account_code: str
    product_code: str
    record_date: dt.date
    right_type_code: str            # 32 | 03 | EST03 | EST75
    product_name: str | None
    ex_date: dt.date | None
    pay_date: dt.date | None
    holding_quantity: float
    gross_amount: float | None      # 원통화 세전 (추정 행만 NULL 허용)
    tax_amount: float | None        # 원천 tax_amt 그대로 (ISA 는 미징수 참고값) / 추정 = 세전 × 15%
    currency_code: str
    fx_rate: float | None           # 추정(USD) 행만: 지급일 이하 최신 USD 기준율
    is_estimated: bool
    source: str


# ── 일봉 API 필드 → product_price_daily 컬럼 ──
PRICE_RENAME: dict[str, dict[str, str]] = {
    "domestic_daily_price": {
        "stck_bsop_date": "standard_date",  # 영업일자 YYYYMMDD
        "stck_oprc": "opening_price",       # 시가
        "stck_hgpr": "highest_price",       # 고가
        "stck_lwpr": "lowest_price",        # 저가
        "stck_clpr": "closing_price",       # 종가
        "acml_vol": "trading_volume",       # 누적거래량
    },
    "overseas_daily_price": {
        "xymd": "standard_date",            # 현지 일자 YYYYMMDD
        "open": "opening_price",
        "high": "highest_price",
        "low": "lowest_price",
        "clos": "closing_price",
        "tvol": "trading_volume",
    },
    "upbit_day_candle": {
        "opening_price": "opening_price",
        "high_price": "highest_price",
        "low_price": "lowest_price",
        "trade_price": "closing_price",
        "candle_acc_trade_volume": "trading_volume",
    },
}


class PriceRecord(TypedDict):
    standard_date: str
    product_code: str
    opening_price: float
    highest_price: float
    lowest_price: float
    closing_price: float
    trading_volume: float
    currency_code: str
    source: str


class FundPriceRecord(TypedDict):
    standard_date: str
    product_code: str
    product_name: str
    standard_price: float


# ── pandas 컬럼 선택용 리스트 (TypedDict에서 파생) ──
BALANCE_COLUMNS: list[str] = list(BalanceRecord.__annotations__.keys())
EXCHANGE_RATE_COLUMNS: list[str] = list(ExchangeRateRecord.__annotations__.keys())
DIVIDEND_EVENT_COLUMNS: list[str] = list(DividendEventRecord.__annotations__.keys())
DIVIDEND_RECEIPT_COLUMNS: list[str] = list(DividendReceiptRecord.__annotations__.keys())
DIVIDEND_EVENT_PK: list[str] = ["product_code", "record_date", "dividend_kind"]
DIVIDEND_RECEIPT_PK: list[str] = ["account_code", "product_code", "record_date", "right_type_code"]
PRICE_COLUMNS: list[str] = list(PriceRecord.__annotations__.keys())
FUND_PRICE_COLUMNS: list[str] = list(FundPriceRecord.__annotations__.keys())
