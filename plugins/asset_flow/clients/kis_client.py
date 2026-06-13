"""
한국투자증권(KIS) OpenAPI 전용 클라이언트

BaseApiClient를 상속하여 KIS API 인증 헤더 생성과 각 TR_ID별 요청 메서드를 제공한다.
응답은 원시 JSON을 그대로 반환하며, 데이터 변환은 kis_transformer에서 담당한다.

제공 메서드:
    - get_overseas_balance(): 해외주식 잔고 (TR: TTTS3012R)
    - get_domestic_balance(): 국내주식 잔고 (TR: TTTC8434R)
    - get_account_balance(): 투자계좌자산현황 — 연금저축·CMA 용도 (TR: CTRP6548R)
    - get_exchange_rate(): 환율 기간별 시세 (TR: FHKST03030100)
"""

from typing import Dict, List
from asset_flow.clients.base_client import BaseApiClient
from asset_flow.config.kis import KIS


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
        """KIS API 공통 인증 헤더 생성 (appKey, appSecret, tr_id 포함)"""
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
        """
        국내주식 잔고 조회 (TR: TTTC8434R)

        Returns:
            dict: 원시 JSON 응답 — output1 리스트에 종목별 잔고 포함
        """
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
        """
        투자계좌자산현황 조회 (TR: CTRP6548R)

        output1은 자산 유형별 고정 순서 행 배열로 반환된다.
        행 인덱스 의미는 KIS.ACCOUNT_BALANCE_ROW_NAMES 참조.
        BSPR_BF_DT_APLY_YN="Y" 적용으로 전일 기준가 기반 평가금액 반환.

        Returns:
            dict: 원시 JSON 응답 — output1[1]=펀드/MMW, output1[14]=외화단기사채(CMA)
        """
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
