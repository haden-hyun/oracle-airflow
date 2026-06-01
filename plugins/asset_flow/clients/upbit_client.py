"""
역할: 업비트 API 전용 클라이언트
의존성: base_client, constants
책임:
  - 업비트 API 엔드포인트별 요청 메서드 제공
  - 업비트 인증 헤더 생성
  - 원시 JSON 응답 반환
"""

from typing import Dict, List
from .base_client import BaseApiClient
from ..config.upbit import UPBIT


class UpbitApiClient(BaseApiClient):
    """업비트 API 클라이언트"""

    def __init__(self, token: str):
        super().__init__(base_url=UPBIT.BASE_URL, token=token)

    def _build_headers(self) -> Dict[str, str]:
        """업비트 API 헤더 생성"""
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def get_balance(self) -> Dict:
        """
        보유 자산 조회

        Returns:
            dict: 원시 JSON 응답 (리스트 형태)
        """
        url = self._build_url(UPBIT.PATHS["balance"])
        headers = self._build_headers()

        response = self.safe_request("GET", url, headers=headers)
        return response.json()

    def get_market_codes(self) -> Dict:
        """마켓 코드 조회 (한글명 매핑용)"""
        url = self._build_url(UPBIT.PATHS["market_code"])
        headers = self._build_headers()

        response = self.safe_request("GET", url, headers=headers)
        return response.json()

    def get_current_prices(self, markets: List[str]) -> Dict:
        """
        현재가 조회

        Args:
            markets: 마켓 코드 리스트 ['KRW-BTC', 'KRW-ETH', ...]

        Returns:
            dict: 원시 JSON 응답
        """
        url = self._build_url(UPBIT.PATHS["current_price"])
        headers = self._build_headers()

        params = {"markets": markets}

        response = self.safe_request("GET", url, headers=headers, params=params)
        return response.json()
