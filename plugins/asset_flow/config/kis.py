from typing import Dict, List


class KIS:
    """한국투자증권 API 설정"""

    BASE_URL: str = "https://openapi.koreainvestment.com:9443"

    PATHS: Dict[str, str] = {
        # API명: OAuth 2.0 토큰 발급
        "token": "/oauth2/tokenP",
        # API명: 투자계좌자산현황조회[v1_국내주식-048] → 금현물 및 RP/발행어음 조회용
        "account_balance": "/uapi/domestic-stock/v1/trading/inquire-account-balance",
        # API명: 해외주식 종목/지수/환율기간별시세[v1_해외주식-012] → 환율 조회용
        "exchange_rate": "/uapi/overseas-price/v1/quotations/inquire-daily-chartprice",
        # API명: 주식잔고조회[v1_국내주식-006] → 국내주식 잔고 조회용
        "domestic_balance": "/uapi/domestic-stock/v1/trading/inquire-balance",
        # API명: 해외주식 잔고조회[v1_해외주식-006] → 해외주식 잔고 조회용
        "overseas_balance": "/uapi/overseas-stock/v1/trading/inquire-balance",
    }

    TR_IDS: Dict[str, str] = {
        "exchange_rate": "FHKST03030100",
        "domestic_balance": "TTTC8434R",
        "overseas_balance": "TTTS3012R",
        "account_balance": "CTRP6548R",
    }

    PARAMS: Dict[str, dict] = {
        "account_balance": {
            "INQR_DVSN_1": "",
            "BSPR_BF_DT_APLY_YN": "",
        },
        "exchange_rate": {
            "FID_COND_MRKT_DIV_CODE": "X",
            "FID_PERIOD_DIV_CODE": "D",
        },
        "overseas_balance": {
            "OVRS_EXCG_CD": "NASD",
            "TR_CRCY_CD": "USD",
            "CTX_AREA_FK200": "",
            "CTX_AREA_NK200": "",
        },
        "domestic_balance": {
            "AFHR_FLPR_YN": "N",
            "OFL_YN": "",
            "INQR_DVSN": "01",
            "UNPR_DVSN": "01",
            "FUND_STTL_ICLD_YN": "N",
            "FNCG_AMT_AUTO_RDPT_YN": "N",
            "PRCS_DVSN": "01",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        },
    }

    EXCHANGE_RATE_PRODUCT_CODES: List[str] = ["FX@KRWKFTC", "FX@KRWJS", "FX@GBP", "FX@EUR"]

    # 투자계좌자산현황 응답의 행 순서 (API가 인덱스 없이 순서로 반환)
    ACCOUNT_BALANCE_ROW_NAMES: List[str] = [
        "주식",
        "펀드/MMW",
        "IMA",
        "채권",
        "ELS/DLS",
        "WRAP",
        "신탁/퇴직연금/외화신탁",
        "RP/발행어음",
        "해외주식",
        "해외채권",
        "금현물",
        "CD/CP",
        "단기사채",
        "타사상품",
        "외화단기사채",
        "외화 ELS/DLS",
        "외화",
        "예수금 및 CMA",
        "청약자예수금",
        "합계",
    ]
