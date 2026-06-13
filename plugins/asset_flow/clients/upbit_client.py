"""
업비트(Upbit) API 전용 클라이언트

BaseApiClient를 상속하여 Upbit API JWT 인증 헤더 생성과 각 엔드포인트별 요청 메서드를 제공한다.
응답은 원시 JSON을 그대로 반환하며, 데이터 변환은 upbit_transformer에서 담당한다.

제공 메서드:
    - get_balance(): 보유 자산 조회 (/v1/accounts)
    - get_market_codes(): 마켓 코드 및 한글명 목록 (/v1/market/all)
    - get_daily_candles(): 일 캔들 종가 조회 (/v1/candles/days) — T-1 종가 수집용
"""

from typing import Dict, List
from asset_flow.clients.base_client import BaseApiClient
from asset_flow.config.upbit import UPBIT


class UpbitApiClient(BaseApiClient):
    """업비트 API 클라이언트"""

    def __init__(self, token: str):
        super().__init__(base_url=UPBIT.BASE_URL, token=token)

    def _build_headers(self) -> Dict[str, str]:
        """Upbit API JWT Bearer 인증 헤더 생성"""
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
        """
        마켓 코드 및 한글명 목록 조회 (/v1/market/all)

        Returns:
            list: [{market, korean_name, english_name}, ...] 전체 마켓 목록
        """
        url = self._build_url(UPBIT.PATHS["market_code"])
        headers = self._build_headers()

        response = self.safe_request("GET", url, headers=headers)
        return response.json()

    # def get_current_prices(self, markets: List[str]) -> Dict:
    #     """
    #     현재가 조회 (실시간 ticker)
    #     → standard_date(T-1)와 기준일 불일치로 인해 get_daily_candles()로 대체
    #
    #     Args:
    #         markets: 마켓 코드 리스트 ['KRW-BTC', 'KRW-ETH', ...]
    #
    #     Returns:
    #         dict: 원시 JSON 응답
    #     """
    #     url = self._build_url(UPBIT.PATHS["current_price"])
    #     headers = self._build_headers()
    #     params = {"markets": markets}
    #     response = self.safe_request("GET", url, headers=headers, params=params)
    #     return response.json()

    def get_daily_candles(self, markets: List[str], to_date: str) -> List[Dict]:
        """
        일 캔들 종가 조회 (T-1 종가 고정 수집용)

        Args:
            markets: 마켓 코드 리스트 ['KRW-BTC', 'KRW-ETH', ...]
            to_date: 조회 상한 일자 ('YYYY-MM-DD') — 이 날짜 00:00:00 이전 캔들 1개 반환
                     DAG에서 base_date(T일)를 전달하면 T-1 종가가 반환됨

        Returns:
            list: 마켓별 일 캔들 응답 리스트 [{market, trade_price, candle_date_time_kst, ...}]
        """
        url = self._build_url(UPBIT.PATHS["daily_candle"])
        headers = self._build_headers()
        result = []

        for market in markets:
            params = {
                "market": market,
                "to": f"{to_date}T00:00:00",
                "count": 1,
            }
            response = self.safe_request("GET", url, headers=headers, params=params)
            candles = response.json()
            if candles:
                result.append(candles[0])

        return result
