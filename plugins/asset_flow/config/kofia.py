"""
금융투자협회 전자공시(KOFIA) 상수

- 펀드 기준가격 조회 XML 서비스 (공개 화면 내부 서비스, 키 없음)
"""


class KOFIA:
    """KOFIA 전자공시 설정"""

    URL: str = "https://dis.kofia.or.kr/proframeWeb/XMLSERVICES/"
    APP_NAME: str = "FS-DIS2"
    FUND_NAV_SERVICE: str = "DISFundStdPrcStutSO"      # 화면: 펀드 > 기준가격
    TIMEOUT: tuple[int, int] = (10, 150)               # (접속, 응답) 초 — 비브라우저 UA 응답 ~105초 지연(실측) 대비
    MAX_WORKERS: int = 4                               # 동시 요청 상한 — 지연이 다시 생겨도 펀드 수만큼 늘지 않게
    # 2026-10-17 부터 UA 별 처리 (실측 10-18): Python 기본 UA = 180초 뒤 무응답 끊김, curl = ~105초, 브라우저 = 1초 미만
    USER_AGENT: str = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0 Safari/537.36")
