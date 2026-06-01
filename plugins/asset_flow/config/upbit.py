from typing import Dict


class UPBIT:
    """업비트 API 설정"""

    BASE_URL: str = "https://api.upbit.com"

    PATHS: Dict[str, str] = {
        "balance": "/v1/accounts",
        "market_code": "/v1/market/all",
        "current_price": "/v1/ticker",
    }
