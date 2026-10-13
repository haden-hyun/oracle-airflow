"""
API 클라이언트 공통 기반 클래스

- safe_request(): HTTP 요청 실행, 4xx/5xx 는 상태코드·가린 본문만 로그 후 재전파 (쿼리스트링 미노출)
- _redact(): 로그·예외에 남길 본문 가리기 (기본 그대로, 하위 클래스가 계좌번호 등 마스킹)
- _build_url(): base_url + path
- _build_headers(): 하위 클래스가 인증 헤더 구현
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod

import requests

log = logging.getLogger(__name__)


class BaseApiClient(ABC):
    """API 클라이언트 추상 베이스."""

    def __init__(self, base_url: str):
        """
        Purpose: 기본 URL 설정
        Parameter:
            - base_url: API 기본 URL
        Input: 없음
        Output: 없음
        """
        self.base_url = base_url

    @abstractmethod
    def _build_headers(self, *args, **kwargs) -> dict[str, str]:
        """
        Purpose: API 별 인증 헤더 생성 (하위 클래스 구현)
        Parameter:
            - args/kwargs: 하위 클래스 정의
        Input: 없음
        Output: 헤더 dict
        """

    def safe_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        Purpose: HTTP 요청 실행 + 실패 로깅 (헤더·쿼리스트링은 남기지 않고 본문은 _redact 후 300자)
        Parameter:
            - method: HTTP 메서드
            - url: 요청 URL (쿼리 없이 — 쿼리는 params 로)
            - kwargs: requests 인자 (timeout 미지정 시 (5, 30))
        Input: 외부 API
        Output: Response. 실패 시 같은 RequestException 계열을 쿼리스트링(계좌번호) 없는 메시지로 바꿔 재전파
        """
        kwargs.setdefault("timeout", (5, 30))
        try:
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            status = getattr(e.response, "status_code", None)
            body = self._redact(getattr(e.response, "text", "") or "")[:300]
            log.error("[API CALL FAILED] %s %s status=%s body=%s", method, url, status, body)
            # 원 메시지('for url: …?CANO=…')가 traceback·Discord 로 새지 않게 교체 (response 는 보존)
            raise type(e)(f"{type(e).__name__} status={status} {method} {url}", response=e.response) from None

    def _redact(self, text: str) -> str:
        """
        Purpose: 실패 로그·예외에 넣을 응답 본문 가리기 (하위 클래스 재정의)
        Parameter:
            - text: 응답 본문
        Input: 없음
        Output: 가린 문자열 (기본: 그대로)
        """
        return text

    def _build_url(self, path: str) -> str:
        """
        Purpose: 요청 URL 조립
        Parameter:
            - path: API 경로
        Input: 없음
        Output: base_url + path
        """
        return f"{self.base_url}{path}"
