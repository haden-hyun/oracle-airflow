"""
업비트(Upbit) API 설정 모음

API 기본 URL과 엔드포인트 경로(PATHS)를 중앙 관리한다.

참고:
    - 업비트 API 문서: https://docs.upbit.com
    - 인증 방식: JWT Bearer token (요청별 생성)
"""

from typing import Dict


class UPBIT:
    """업비트 API 설정"""

    BASE_URL: str = "https://api.upbit.com"

    PATHS: Dict[str, str] = {
        "balance": "/v1/accounts",
        "market_code": "/v1/market/all",
        "current_price": "/v1/ticker",
        "daily_candle": "/v1/candles/days",
    }
