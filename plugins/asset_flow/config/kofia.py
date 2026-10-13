"""
금융투자협회 전자공시(KOFIA) 상수

- 펀드 기준가격 조회 XML 서비스 (공개 화면 내부 서비스, 키 없음)
"""


class KOFIA:
    """KOFIA 전자공시 설정"""

    URL: str = "https://dis.kofia.or.kr/proframeWeb/XMLSERVICES/"
    APP_NAME: str = "FS-DIS2"
    FUND_NAV_SERVICE: str = "DISFundStdPrcStutSO"      # 화면: 펀드 > 기준가격
