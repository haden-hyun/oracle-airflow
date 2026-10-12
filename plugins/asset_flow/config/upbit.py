"""
업비트(Upbit) API 상수

- BASE_URL, PATHS (인증: 요청마다 JWT 서명 — clients/upbit_client.py)
"""

class UPBIT:
    """업비트 API 설정"""

    BASE_URL: str = "https://api.upbit.com"

    PATHS: dict[str, str] = {
        "balance": "/v1/accounts",
        "api_keys": "/v1/api_keys",
        "market_code": "/v1/market/all",
        "daily_candle": "/v1/candles/days",
    }
