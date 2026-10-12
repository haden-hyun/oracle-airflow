"""
API 클라이언트 공통 기반 클래스

- safe_request(): HTTP 요청 실행, 4xx/5xx 는 상태코드만 로그 후 재전파
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
        Purpose: HTTP 요청 실행 + 실패 로깅 (헤더·본문의 키 값은 로그에 남기지 않음)
        Parameter:
            - method: HTTP 메서드
            - url: 요청 URL
            - kwargs: requests 인자 (timeout 미지정 시 (5, 30))
        Input: 외부 API
        Output: Response (요청 실패 시 RequestException 재전파)
        """
        kwargs.setdefault("timeout", (5, 30))
        try:
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            status = getattr(e.response, "status_code", None)
            body = (getattr(e.response, "text", "") or "")[:300]
            log.error("[API CALL FAILED] %s %s status=%s body=%s", method, url, status, body)
            raise

    def _build_url(self, path: str) -> str:
        """
        Purpose: 요청 URL 조립
        Parameter:
            - path: API 경로
        Input: 없음
        Output: base_url + path
        """
        return f"{self.base_url}{path}"
