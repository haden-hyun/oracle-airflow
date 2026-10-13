"""
업비트(Upbit) API 클라이언트

- 인증 엔드포인트는 요청마다 JWT 서명 (nonce=uuid4, 토큰 저장·재사용 없음)
- 공개 엔드포인트(market/all, candles)는 인증 헤더 없음
- 제공: get_balance / get_market_codes / get_daily_candles / get_api_keys / get_day_candles(기간, 시세 DAG)
"""

from __future__ import annotations

import uuid

import jwt

from asset_flow.clients.base_client import BaseApiClient
from asset_flow.config.upbit import UPBIT


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

    def get_daily_candles(self, markets: list[str], to_date: str) -> list:
        """
        Purpose: 마켓별 T-1 일 캔들 1개 조회
        Parameter:
            - markets: 마켓 코드 리스트 (KRW-BTC 등)
            - to_date: 상한일 (YYYY-MM-DD) — base_date(T) 를 넘기면 T-1 종가
        Input: Upbit API (공개)
        Output: 마켓별 캔들 리스트
        """
        result = []
        for market in markets:
            params = {"market": market, "to": f"{to_date}T00:00:00", "count": 1}
            candles = self._get("daily_candle", auth=False, params=params)
            if candles:
                result.append(candles[0])
        return result

    def get_day_candles(self, market: str, start: str, end: str, interval: float = 0.35) -> list[dict]:
        """
        Purpose: 마켓 1개의 일 캔들 기간 조회 (count 200 씩 to 를 당겨 반복, 공개 API)
        Parameter:
            - market: KRW-BTC 등
            - start: 캔들 시작일(KST) YYYY-MM-DD (포함)
            - end: 캔들 시작일(KST) YYYY-MM-DD (포함)
            - interval: 호출 간격 초 (기본 0.35)
        Input: Upbit API (공개)
        Output: 원시 캔들 리스트 (최근→과거, start~end 만). 미마감 캔들 제거는 transformer 몫
        """
        import time
        from datetime import date, timedelta

        # to 는 시간대 없으면 UTC (2026-10-13 실측). 일봉 d(KST) 시작 = d 00:00 UTC → end+1 00:00 UTC 이전 캔들부터
        to = f"{date.fromisoformat(end) + timedelta(days=1)}T00:00:00"
        rows = []
        while True:
            time.sleep(interval)
            page = self._get("daily_candle", auth=False, params={"market": market, "count": 200, "to": to})
            rows += [c for c in page if start <= c["candle_date_time_kst"][:10] <= end]
            if len(page) < 200 or page[-1]["candle_date_time_kst"][:10] <= start:
                return rows
            to = page[-1]["candle_date_time_utc"]
