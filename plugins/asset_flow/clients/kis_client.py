"""
역할: 한국투자증권 API 전용 클라이언트
의존성: base_client, constants
책임:
    - KIS API 엔드포인트별 요청 메서드 제공
    - KIS 인증 헤더 생성
    - 원시 JSON 응답 반환 (변환 로직 없음)
"""

from typing import Dict, List
from .base_client import BaseApiClient
from ..config.kis import KIS


class KISApiClient(BaseApiClient):
    """한국투자증권 API 클라이언트"""

    def __init__(self, token: str, config: Dict[str, str]):
        """
        Args:
            token: 액세스 토큰
            config: 계좌 설정 {'appkey': ..., 'secret': ..., 'account': ...}
        """
        super().__init__(base_url=KIS.BASE_URL, token=token)
        self.config = config

    def _build_headers(self, tr_id: str) -> Dict[str, str]:
        """KIS API 헤더 생성"""
        return {
            "content-type": "application/json",
            "authorization": f"Bearer {self.token}",
            "appKey": self.config["appkey"],
            "appSecret": self.config["secret"],
            "tr_id": tr_id,
        }

    def get_overseas_balance(self) -> Dict:
        """
        해외주식 잔고 조회

        Returns:
            dict: 원시 JSON 응답
        """
        url = self._build_url(KIS.PATHS["overseas_balance"])
        headers = self._build_headers(KIS.TR_IDS["overseas_balance"])

        params = {
            "CANO": self.config["account"],
            "ACNT_PRDT_CD": self.config["product_code"],
        }
        params.update(KIS.PARAMS["overseas_balance"])

        response = self.safe_request("GET", url, headers=headers, params=params)
        return response.json()

    def get_domestic_balance(self) -> Dict:
        """국내주식 잔고 조회"""
        url = self._build_url(KIS.PATHS["domestic_balance"])
        headers = self._build_headers(KIS.TR_IDS["domestic_balance"])

        params = {
            "CANO": self.config["account"],
            "ACNT_PRDT_CD": self.config["product_code"],
        }
        params.update(KIS.PARAMS["domestic_balance"])

        response = self.safe_request("GET", url, headers=headers, params=params)
        return response.json()

    def get_account_balance(self) -> Dict:
        """투자계좌자산현황 조회"""
        url = self._build_url(KIS.PATHS["account_balance"])
        headers = self._build_headers(KIS.TR_IDS["account_balance"])

        params = {
            "CANO": self.config["account"],
            "ACNT_PRDT_CD": self.config["product_code"],
        }
        params.update(KIS.PARAMS["account_balance"])

        response = self.safe_request("GET", url, headers=headers, params=params)
        return response.json()

    def get_exchange_rate(
        self, standard_date: str, product_codes: List[str]
    ) -> List[Dict]:
        """
        환율 조회

        Args:
            standard_date: 기준일 (YYYY-MM-DD)
            product_codes: 상품 코드 리스트 ['FX@KRWKFTC', 'FX@KRWJS', ...]

        Returns:
            List[dict]: 각 상품별 응답 리스트
        """
        url = self._build_url(KIS.PATHS["exchange_rate"])
        headers = self._build_headers(KIS.TR_IDS["exchange_rate"])

        api_date = standard_date.replace("-", "")
        results = []
        for product_code in product_codes:
            params = {
                "FID_INPUT_ISCD": product_code,
                "FID_INPUT_DATE_1": api_date,
                "FID_INPUT_DATE_2": api_date,
            }
            params.update(KIS.PARAMS["exchange_rate"])

            response = self.safe_request("GET", url, headers=headers, params=params)
            results.append(response.json())

        return results
