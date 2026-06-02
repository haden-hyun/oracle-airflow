"""
모든 API 클라이언트의 공통 기반 클래스

HTTP 요청 실행, 에러 처리, URL 조립 등 API 클라이언트 공통 동작을 추상화한다.
KISApiClient, UpbitApiClient 는 이 클래스를 상속하여 각 API별 인증 헤더와
엔드포인트 메서드를 구현한다.

책임:
    - safe_request(): HTTP 요청 실행 및 4xx/5xx 에러 로깅 후 예외 재전파
    - _build_url(): base_url + path 조합
    - _build_headers(): 추상 메서드 — 하위 클래스에서 인증 헤더를 구현
"""

from abc import ABC, abstractmethod
import requests
from typing import Any, Dict, Optional


class BaseApiClient(ABC):
    """API 클라이언트 추상 베이스 클래스"""

    def __init__(self, base_url: str, token: str):
        self.base_url = base_url
        self.token = token

    @abstractmethod
    def _build_headers(self, **kwargs) -> Dict[str, str]:
        """API별 헤더 생성 (하위 클래스에서 구현)"""
        pass

    def safe_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        안전한 API 요청 실행

        Args:
            method: HTTP 메서드 (GET, POST, etc.)
            url: 요청 URL
            **kwargs: requests 파라미터

        Returns:
            requests.Response

        Raises:
            requests.exceptions.RequestException: API 요청 실패 시
        """
        try:
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()
            return response

        except requests.exceptions.RequestException as e:
            status = getattr(e.response, "status_code", None)
            body = getattr(e.response, "text", None)

            print(f"[API CALL FAILED]")
            print(f"Method: {method}")
            print(f"URL: {url}")
            print(f"Status: {status}")
            print(f"Body: {body}")
            print(f"Error: {e}")

            raise

    def _build_url(self, path: str) -> str:
        """base_url과 경로를 결합하여 완전한 요청 URL 반환"""
        return f"{self.base_url}{path}"
