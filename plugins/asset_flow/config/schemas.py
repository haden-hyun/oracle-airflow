from typing import Dict, List, TypedDict

# ── API 필드명 → 도메인 필드명 변환 매핑 ─────────────────────────────────────

KIS_RENAME: Dict[str, Dict[str, str]] = {
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

UPBIT_RENAME: Dict[str, str] = {
    "currency": "product_code",  # 종목코드
    "korean_name": "product_name",  # 종목명
    "balance": "holding_quantity",  # 보유수량
    "trade_price": "unit_market_price",  # 현재가격
    "avg_buy_price": "unit_purchase_price",  # 매입단가
}


# ── 출력 스키마 정의 (TypedDict) ───────────────────────────────────────────────


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


# ── pandas 컬럼 선택용 리스트 (TypedDict에서 파생) ─────────────────────────────

BALANCE_COLUMNS: List[str] = list(BalanceRecord.__annotations__.keys())
EXCHANGE_RATE_COLUMNS: List[str] = list(ExchangeRateRecord.__annotations__.keys())
