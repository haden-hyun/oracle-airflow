import logging
from types import SimpleNamespace

import pytest
import requests

import alerts
from asset_flow.clients.kis_client import KISApiClient
from asset_flow.clients.upbit_client import UpbitApiClient

CANO = "12345678"
KIS_CONF = {"appkey": "k" * 8, "secret": "s" * 8, "account": CANO, "product_code": "01"}


def _raise_http(resp):
    """
    Purpose: requests.request 대체 — HTTPError(쿼리스트링에 계좌번호 포함한 원 메시지) 발생
    Parameter:
        - resp: 예외에 실을 응답
    Input: 없음
    Output: 대체 함수
    """
    def boom(method, url, **kw):
        """
        Purpose: HTTP 오류 흉내
        Parameter:
            - method: HTTP 메서드
            - url: URL
            - kw: 요청 인자
        Input: 없음
        Output: 없음 (HTTPError)
        """
        raise requests.exceptions.HTTPError(f"500 Server Error for url: {url}?CANO={CANO}&ACNT_PRDT_CD=01",
                                            response=resp)

    return boom


def test_kis_failure_log_and_exception_hide_account(monkeypatch, caplog):
    body = f'{{"rt_cd":"1","ctx_area_fk100":"03!^{CANO}!^01","output":[{{"acno10":"{CANO}01"}}]}}'
    monkeypatch.setattr("asset_flow.clients.base_client.requests.request",
                        _raise_http(SimpleNamespace(status_code=500, text=body)))
    kis = KISApiClient("tok", KIS_CONF)
    with caplog.at_level(logging.ERROR), pytest.raises(requests.exceptions.HTTPError) as e:
        kis.safe_request("GET", "https://x/uapi", params={"CANO": CANO})
    assert CANO not in caplog.text and CANO not in str(e.value)
    assert "status=500" in str(e.value)


def test_upbit_error_keeps_response_json(monkeypatch):
    # key_expiry 가 e.response.json() 을 읽음 → 메시지 교체 후에도 response 보존
    resp = SimpleNamespace(status_code=404, text="{}", json=lambda: {"error": {"name": 404}})
    monkeypatch.setattr("asset_flow.clients.base_client.requests.request", _raise_http(resp))
    with pytest.raises(requests.exceptions.HTTPError) as e:
        UpbitApiClient("a", "b").get_market_codes()
    assert e.value.response.json() == {"error": {"name": 404}}


def test_alert_text_redacts_secrets_and_db_detail(monkeypatch):
    monkeypatch.setattr(alerts, "_secret_values", lambda: [CANO, "APPKEY123"])
    detail = (f'IntegrityError: Key (account_code)=({CANO}-01) is not present\n'
              f'HTTPError for url: https://x?CANO=99999999&ACNT_PRDT_CD=22 key=APPKEY123')
    out = alerts._error_text({"exception": RuntimeError(detail)})
    assert CANO not in out and "99999999" not in out and "APPKEY123" not in out
    assert "********-01" in out


def test_alert_redact_survives_missing_secrets(monkeypatch):
    monkeypatch.setenv("ASSET_FLOW_SECRETS", "/nonexistent/secrets.yaml")
    assert alerts.redact("x?CANO=11112222") == "x?CANO=********"


def test_legacy_kis_headers_unchanged():
    # 운영 잔고·환율 호출 헤더는 그대로 (custtype·tr_cont 는 배당·시세 조회만)
    h = KISApiClient("tok", KIS_CONF)._build_headers("TTTC8434R")
    assert set(h) == {"content-type", "authorization", "appKey", "appSecret", "tr_id"}
