"""
한국투자증권(KIS) OpenAPI 상수

- PATHS·TR_IDS·PARAMS: 엔드포인트·거래 ID·고정 쿼리
- EXCHANGE_RATE_PRODUCT_CODES·EXCHANGE_RATE_PAIRS, ACCOUNT_BALANCE_ROW_NAMES(CTRP6548R 행 순서)
- 공통 헤더·연속조회: CUSTTYPE, REQUEST_INTERVAL, CONTINUE_CODES, MAX_PAGES (_request 경로 전용)
- 배당: DIVIDEND_RIGHT_TYPES, OVERSEAS_PRDT_TYPE, US_DIVIDEND_RIGHTS, ICE_DIVIDEND_TITLES
- 시세: PRICE_PAGE_SIZE, OVERSEAS_PRICE_EXCD
- 지수·환율 기간 조회: MARKET_INDEXES, INDEX_SOURCES, CHART_QUERY_PAD_DAYS
"""

class KIS:
    """한국투자증권 API 설정"""

    BASE_URL: str = "https://openapi.koreainvestment.com:9443"

    PATHS: dict[str, str] = {
        # API명: OAuth 2.0 토큰 발급
        "token": "/oauth2/tokenP",
        # API명: 투자계좌자산현황조회[v1_국내주식-048] → 연금저축 펀드·CMA·RP/발행어음 행 조회용
        "account_balance": "/uapi/domestic-stock/v1/trading/inquire-account-balance",
        # API명: 해외주식 종목/지수/환율기간별시세[v1_해외주식-012] → 환율 조회용
        "exchange_rate": "/uapi/overseas-price/v1/quotations/inquire-daily-chartprice",
        # API명: 해외주식 종목/지수/환율기간별시세[v1_해외주식-012] → 미국 지수 일봉 (환율과 같은 API, 시장 구분 N)
        "overseas_index_daily": "/uapi/overseas-price/v1/quotations/inquire-daily-chartprice",
        # API명: 국내주식업종기간별시세(일/주/월/년) [국내주식] 업종/기타 → 코스피·코스닥 일봉 (1회 50행)
        "domestic_index_daily": "/uapi/domestic-stock/v1/quotations/inquire-daily-indexchartprice",
        # API명: 주식잔고조회[v1_국내주식-006] → 국내주식 잔고 조회용
        "domestic_balance": "/uapi/domestic-stock/v1/trading/inquire-balance",
        # API명: 해외주식 잔고조회[v1_해외주식-006] → 해외주식 잔고 조회용
        "overseas_balance": "/uapi/overseas-stock/v1/trading/inquire-balance",
        # API명: 해외주식 체결기준현재잔고[v1_해외주식-008] → 통화별 외화 예수금 (위탁)
        "overseas_present_balance": "/uapi/overseas-stock/v1/trading/inquire-present-balance",
        # API명: 예탁원정보(배당일정)[국내주식-145] → 종목별 주당분배금·지급일 (계좌 불필요)
        "ksd_dividend": "/uapi/domestic-stock/v1/ksdinfo/dividend",
        # API명: 기간별계좌권리현황조회[국내주식-211] → 계좌 분배금·배당 원장 (세전·세금·지급일)
        "period_rights": "/uapi/domestic-stock/v1/trading/period-rights",
        # API명: 해외주식 기간별권리조회[해외주식-052] → 주당 외화배당·확정여부 (계좌 불필요)
        "overseas_period_rights": "/uapi/overseas-price/v1/quotations/period-rights",
        # API명: 해외주식 권리종합[해외주식-050] → 공시일·락일·지급일 (금액 없음, 계좌 불필요)
        "overseas_rights_ice": "/uapi/overseas-price/v1/quotations/rights-by-ice",
        # API명: 국내주식기간별시세(일/주/월/년)[v1_국내주식-016] → 국내 일봉 (1회 100행)
        "domestic_daily_price": "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
        # API명: 해외주식 기간별시세[v1_해외주식-010] → 미국 일봉 (BYMD 이전 100행)
        "overseas_daily_price": "/uapi/overseas-price/v1/quotations/dailyprice",
    }

    TR_IDS: dict[str, str] = {
        "exchange_rate": "FHKST03030100",
        "overseas_index_daily": "FHKST03030100",
        "domestic_index_daily": "FHKUP03500100",
        "domestic_balance": "TTTC8434R",
        "overseas_balance": "TTTS3012R",
        "account_balance": "CTRP6548R",
        "overseas_present_balance": "CTRP6504R",
        "ksd_dividend": "HHKDB669102C0",
        "period_rights": "CTRGA011R",
        "overseas_period_rights": "CTRGT011R",
        "overseas_rights_ice": "HHDFS78330900",
        "domestic_daily_price": "FHKST03010100",
        "overseas_daily_price": "HHDFS76240000",
    }

    PARAMS: dict[str, dict] = {
        "account_balance": {
            "INQR_DVSN_1": "",
            "BSPR_BF_DT_APLY_YN": "Y",  # 전일 기준가 적용 (스냅샷 기준일 일치)
        },
        "exchange_rate": {
            "FID_COND_MRKT_DIV_CODE": "X",
            "FID_PERIOD_DIV_CODE": "D",
        },
        "overseas_index_daily": {
            "FID_COND_MRKT_DIV_CODE": "N",        # N = 해외지수 (환율은 X)
            "FID_PERIOD_DIV_CODE": "D",
        },
        "domestic_index_daily": {
            "FID_COND_MRKT_DIV_CODE": "U",        # U = 업종
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
        "overseas_present_balance": {
            "WCRC_FRCR_DVSN_CD": "02",            # 02 = 외화 기준
            "NATN_CD": "840", "TR_MKET_CD": "00", "INQR_DVSN_CD": "00",   # 미국 · 전체 시장 · 전체
        },
        "ksd_dividend": {"CTS": "", "GB1": "0", "HIGH_GB": ""},           # GB1 0 = 결산·중간 전체
        "period_rights": {
            "INQR_DVSN": "03",                    # 01·02 는 '조회할 자료가 없습니다'
            "CUST_RNCNO25": "", "HMID": "", "RGHT_TYPE_CD": "", "PDNO": "", "PRDT_TYPE_CD": "",
            "CTX_AREA_NK100": "", "CTX_AREA_FK100": "",
        },
        "overseas_period_rights": {
            "INQR_DVSN_CD": "02",                 # 02 = 현지기준일 기준 범위
            "CTX_AREA_NK50": "", "CTX_AREA_FK50": "",
        },
        "overseas_rights_ice": {"NCOD": "US"},
        "domestic_daily_price": {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_PERIOD_DIV_CODE": "D",
            "FID_ORG_ADJ_PRC": "1",               # 1 = 원주가 (0 = 수정주가: 배당락 소급 → asset_daily 와 불일치)
        },
        "overseas_daily_price": {
            "AUTH": "",
            "GUBN": "0",                          # 일
            "MODP": "0",                          # 0 = 원주가 (1 = 배당·분할 소급 조정)
        },
    }

    EXCHANGE_RATE_PRODUCT_CODES: list[str] = ["FX@KRWKFTC", "FX@KRWJS", "FX@GBP", "FX@EUR"]
    # 기간 조회(output2)엔 통화쌍 이름이 없음 → 심볼로 채움. 달러/… 는 원/달러로 교차 환산 대상
    EXCHANGE_RATE_PAIRS: dict[str, str] = {"FX@KRWKFTC": "원/달러", "FX@KRWJS": "원/엔",
                                           "FX@GBP": "달러/파운드", "FX@EUR": "달러/유로"}

    # 투자계좌자산현황 응답의 행 순서 (API가 인덱스 없이 순서로 반환)
    ACCOUNT_BALANCE_ROW_NAMES: list[str] = [
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

    # ── 공통 헤더·연속조회 (_request 경로 전용) ──
    CUSTTYPE: str = "P"                       # 개인
    REQUEST_INTERVAL: float = 0.35            # 같은 클라이언트의 연속 요청 최소 간격(초)
    CONTINUE_CODES: tuple[str, ...] = ("M", "F")   # 응답 헤더 tr_cont: 다음 페이지 있음 (D·E·"" = 끝)
    MAX_PAGES: int = 20                       # 연속조회 무한 루프 방지

    # ── 잔고 재시도 ──
    BALANCE_RETRIES: int = 2                  # 잔고 API rt_cd 오류 시 총 시도 횟수 (TTTC8434R 일시 rt_cd=7 대비)

    # ── 배당 ──
    DIVIDEND_RIGHT_TYPES: tuple[str, ...] = ("32", "03")   # CTRGA011R 32 ETF 분배금, 03 주식 현금배당 (04 등 비현금 제외)
    OVERSEAS_PRDT_TYPE: dict[str, str] = {"NASD": "512", "NYSE": "513", "AMEX": "529"}   # asset_daily.exchange_code → CTRGT011R (529 미확인)
    US_DIVIDEND_RIGHTS: dict[str, str] = {"03": "CASH", "75": "SPECIAL"}                   # CTRGT011R RGHT_TYPE_CD → dividend_type
    ICE_DIVIDEND_TITLES: dict[str, str] = {"현금배당": "CASH", "특별배당": "SPECIAL"}      # HHDFS78330900 ca_title → dividend_type

    # ── 시세 ──
    PRICE_PAGE_SIZE: int = 100                # 국내·해외 일봉 1회 최대 행
    OVERSEAS_PRICE_EXCD: dict[str, str] = {"NASD": "NAS", "NYSE": "NYS", "AMEX": "AMS"}  # asset_daily.exchange_code → 시세 EXCD

    # ── 지수·환율 기간 조회 (FHKUP03500100 · FHKST03030100 output2) ──
    # index_daily.index_code → (시장, KIS FID_INPUT_ISCD). 잘못된 코드도 rt_cd=0 + 0행 (2026-10-17 실조회 확인)
    MARKET_INDEXES: dict[str, tuple[str, str]] = {"KOSPI": ("KR", "0001"), "KOSDAQ": ("KR", "1001"),
                                                  "SPX": ("US", "SPX"), "COMP": ("US", "COMP")}
    INDEX_SOURCES: dict[str, str] = {"KR": "KIS_KR_INDEX", "US": "KIS_US_INDEX"}
    # 1회 응답 상한: 국내 업종 50행, FHKST03030100 은 100행이면서 '요청 구간의 한국 영업일 수'만큼 (최근 쪽만 남김)
    # → 끝 날짜를 받은 최소일 −1 로 당겨 반복하고, 시작일 앞을 이만큼 더 요청해 구간 끝의 한국 연휴에도 행이 오게 함
    CHART_QUERY_PAD_DAYS: int = 14
