"""
한국투자증권(KIS) OpenAPI 클라이언트

- 토큰은 호출부가 TokenManager.token_for() 로 받아 주입
- config 는 기존 Variable 모양 dict (secrets_config.kis_client_config 로 생성)
- 제공: get_overseas_balance / get_domestic_balance / get_account_balance / get_exchange_rate (원시 JSON 반환)
- 계좌번호가 응답 본문(ctx_area_*·acno10)에 평문으로 올 수 있음 → _redact 로 실패 로그·예외에서 가림
"""

from __future__ import annotations

import re

from asset_flow.clients.base_client import BaseApiClient
from asset_flow.config.kis import KIS

# 응답 본문의 계좌번호 필드 (ctx_area_nk100 · ctx_area_fk100 · acno10 · cano) 값
_ACCOUNT_FIELD = re.compile(r'("(?:ctx_area_[a-z0-9]+|acno\d*|cano)"\s*:\s*")[^"]*(")', re.IGNORECASE)


class KISApiClient(BaseApiClient):
    """한국투자증권 API 클라이언트."""

    def __init__(self, token: str, config: dict):
        """
        Purpose: 토큰·계좌 설정 보관
        Parameter:
            - token: KIS 액세스 토큰
            - config: {appkey, secret[, account, product_code, type]}
        Input: 없음
        Output: 없음
        """
        super().__init__(base_url=KIS.BASE_URL)
        self.token = token
        self.config = config

    def _build_headers(self, tr_id: str) -> dict[str, str]:
        """
        Purpose: KIS 공통 인증 헤더 생성
        Parameter:
            - tr_id: 거래 ID
        Input: 없음
        Output: 헤더 dict (authorization, appKey, appSecret, tr_id)
        """
        return {
            "content-type": "application/json",
            "authorization": f"Bearer {self.token}",
            "appKey": self.config["appkey"],
            "appSecret": self.config["secret"],
            "tr_id": tr_id,
        }

    def _redact(self, text: str) -> str:
        """
        Purpose: 실패 로그·예외용 본문에서 계좌번호 가리기
        Parameter:
            - text: 응답 본문
        Input: 없음
        Output: ctx_area_*·acno*·cano 값과 이 클라이언트 계좌번호를 ******** 로 바꾼 문자열
        """
        text = _ACCOUNT_FIELD.sub(r"\1********\2", text)
        account = self.config.get("account")
        return text.replace(account, "********") if account else text

    def _get_account(self, name: str) -> dict:
        """
        Purpose: 계좌 단위 조회 공통 처리 (CANO·상품코드 + 고정 파라미터)
        Parameter:
            - name: KIS.PATHS/TR_IDS/PARAMS 키
        Input: KIS API
        Output: 원시 JSON
        """
        params = {"CANO": self.config["account"], "ACNT_PRDT_CD": self.config["product_code"], **KIS.PARAMS[name]}
        url = self._build_url(KIS.PATHS[name])
        return self.safe_request("GET", url, headers=self._build_headers(KIS.TR_IDS[name]), params=params).json()

    def get_overseas_balance(self) -> dict:
        """
        Purpose: 해외주식 잔고 조회 (TTTS3012R)
        Parameter: 없음
        Input: KIS API
        Output: 원시 JSON (output1 = 종목별 잔고)
        """
        return self._get_account("overseas_balance")

    def get_domestic_balance(self) -> dict:
        """
        Purpose: 국내주식 잔고 조회 (TTTC8434R)
        Parameter: 없음
        Input: KIS API
        Output: 원시 JSON (output1 = 종목별 잔고)
        """
        return self._get_account("domestic_balance")

    def get_account_balance(self) -> dict:
        """
        Purpose: 투자계좌자산현황 조회 (CTRP6548R, 전일 기준가 적용) — 연금저축·CMA 용
        Parameter: 없음
        Input: KIS API
        Output: 원시 JSON (output1[1]=펀드/MMW, output1[14]=외화단기사채(CMA)).
                rt_cd != "0" 이면 ValueError — 미보유로 오인해 기존 적재분을 지우지 않도록
        """
        data = self._get_account("account_balance")
        if data.get("rt_cd") != "0":
            raise ValueError(f"KIS 투자계좌자산현황 조회 실패 (rt_cd={data.get('rt_cd')}): {data.get('msg1')}")
        return data

    def get_exchange_rate(self, standard_date: str, product_codes: list[str]) -> list[dict]:
        """
        Purpose: 환율 기간별 시세 조회 (FHKST03030100, 계좌번호 불필요)
        Parameter:
            - standard_date: 기준일 (YYYY-MM-DD)
            - product_codes: 통화 상품코드 리스트
        Input: KIS API
        Output: 통화별 원시 JSON 리스트
        """
        url = self._build_url(KIS.PATHS["exchange_rate"])
        headers = self._build_headers(KIS.TR_IDS["exchange_rate"])
        day = standard_date.replace("-", "")
        results = []
        for code in product_codes:
            params = {"FID_INPUT_ISCD": code, "FID_INPUT_DATE_1": day, "FID_INPUT_DATE_2": day,
                      **KIS.PARAMS["exchange_rate"]}
            results.append(self.safe_request("GET", url, headers=headers, params=params).json())
        return results
