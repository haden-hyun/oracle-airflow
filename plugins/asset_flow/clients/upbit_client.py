"""
업비트(Upbit) API 클라이언트

- 인증 엔드포인트는 요청마다 JWT 서명 (nonce=uuid4, 토큰 저장·재사용 없음)
- 공개 엔드포인트(market/all, candles)는 인증 헤더 없음
- 제공: get_balance / get_market_codes / get_api_keys / get_kst_closes(잔고 평가) / get_hour_candles(기간, 시세 DAG)
- 가격 기준: 기준일 d = d+1 00:00 KST 직전 체결가 (60분봉, `to` 는 +09:00 명시)
"""

from __future__ import annotations

import logging
import time
import uuid
from datetime import date, timedelta

import jwt

from asset_flow.clients.base_client import BaseApiClient
from asset_flow.config.upbit import UPBIT

log = logging.getLogger(__name__)

STALE_CLOSE_DAYS = 7                                    # 종가 캔들이 T-1 보다 이 일수 넘게 오래되면 상폐·거래정지 의심


class UpbitApiClient(BaseApiClient):
    """업비트 API 클라이언트."""

    def __init__(self, access_key: str, secret_key: str):
        """
        Purpose: 키 보관 (서명은 요청 시점에)
        Parameter:
            - access_key: Upbit access key
            - secret_key: Upbit secret key
        Input: 없음
        Output: 없음
        """
        super().__init__(base_url=UPBIT.BASE_URL)
        self._access_key = access_key
        self._secret_key = secret_key

    def _build_headers(self, auth: bool = True) -> dict[str, str]:
        """
        Purpose: 요청 헤더 생성 — 인증 요청이면 새 nonce 로 JWT 서명
        Parameter:
            - auth: 인증 헤더 포함 여부 (기본 True)
        Input: 없음
        Output: 헤더 dict
        """
        headers = {"Content-Type": "application/json"}
        if auth:
            token = jwt.encode({"access_key": self._access_key, "nonce": str(uuid.uuid4())}, self._secret_key)
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def _get(self, path_key: str, auth: bool, params: dict | None = None):
        """
        Purpose: GET 요청 공통 처리
        Parameter:
            - path_key: UPBIT.PATHS 키
            - auth: 인증 필요 여부
            - params: 쿼리 파라미터 (기본 None)
        Input: Upbit API
        Output: 응답 JSON
        """
        url = self._build_url(UPBIT.PATHS[path_key])
        return self.safe_request("GET", url, headers=self._build_headers(auth), params=params).json()

    def get_balance(self) -> list:
        """
        Purpose: 보유 자산 조회 (/v1/accounts)
        Parameter: 없음
        Input: Upbit API (인증)
        Output: 잔고 리스트
        """
        return self._get("balance", auth=True)

    def get_api_keys(self) -> list:
        """
        Purpose: API 키 목록·만료 시각 조회 (/v1/api_keys)
        Parameter: 없음
        Input: Upbit API (인증)
        Output: [{access_key, expire_at}, ...]
        """
        return self._get("api_keys", auth=True)

    def get_market_codes(self) -> list:
        """
        Purpose: 마켓 코드·한글명 목록 (/v1/market/all)
        Parameter: 없음
        Input: Upbit API (공개)
        Output: [{market, korean_name, english_name}, ...]
        """
        return self._get("market_code", auth=False)

    def get_kst_closes(self, markets: list[str], base_date: str) -> list:
        """
        Purpose: 마켓별 T-1 KST 자정 종가 (60분봉 to=T 00:00+09:00, count 1)
        Parameter:
            - markets: 마켓 코드 리스트 (KRW-BTC 등)
            - base_date: 실행일 T (YYYY-MM-DD)
        Input: Upbit API (공개)
        Output: 마켓별 60분봉 1개 리스트. 캔들이 T-1 이전이면 경고 후 그 캔들(직전 체결가) 사용.
                응답이 비거나 T-1 보다 STALE_CLOSE_DAYS 일 넘게 오래되면 ValueError
        """
        prev = date.fromisoformat(base_date) - timedelta(days=1)
        result = []
        for market in markets:
            params = {"market": market, "to": f"{base_date}T00:00:00+09:00", "count": 1}
            candles = self._get("hour_candle", auth=False, params=params)
            if not candles:
                raise ValueError(f"{market} {prev} 종가 캔들 없음")
            kst = candles[0]["candle_date_time_kst"]
            lag = (prev - date.fromisoformat(kst[:10])).days
            if lag > STALE_CLOSE_DAYS:
                raise ValueError(f"{market} 종가 캔들 {kst} — {prev} 보다 {lag}일 전 (상폐·거래정지 의심)")
            if lag:
                log.warning("[%s] %s 종가 캔들 %s (그 뒤 체결 없음) — 직전 체결가 사용", market, prev, kst)
            result.append(candles[0])
        return result

    def get_hour_candles(self, market: str, start: str, end: str, interval: float = 0.35) -> list[dict]:
        """
        Purpose: 마켓 1개의 KST start~end 60분봉 전부 (count 200 씩 to 를 당겨 반복)
        Parameter:
            - market: KRW-BTC 등
            - start: KST 날짜 YYYY-MM-DD (포함)
            - end: KST 날짜 YYYY-MM-DD (포함)
            - interval: 호출 간격 초 (기본 0.35)
        Input: Upbit API (공개)
        Output: 원시 60분봉 리스트 (최근→과거). 일별 집계·미마감 제거는 kst_day_ohlc
        """
        to = f"{date.fromisoformat(end) + timedelta(days=1)}T00:00:00+09:00"
        rows = []
        while True:
            time.sleep(interval)
            page = self._get("hour_candle", auth=False, params={"market": market, "count": 200, "to": to})
            rows += [c for c in page if start <= c["candle_date_time_kst"][:10] <= end]
            if len(page) < 200 or page[-1]["candle_date_time_kst"] <= f"{start}T00:00:00":
                return rows
            to = f"{page[-1]['candle_date_time_kst']}+09:00"
