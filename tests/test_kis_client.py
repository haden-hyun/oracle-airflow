import json

import pytest

from asset_flow.clients.kis_client import KISApiClient, KisApiError

CANO = "12345678"


class FakeResp:
    def __init__(self, body, tr_cont=""):
        """
        Purpose: 가짜 HTTP 응답
        Parameter:
            - body: JSON 본문
            - tr_cont: 응답 헤더 tr_cont
        Input: 없음
        Output: 없음
        """
        self._body, self.headers, self.status_code = body, {"tr_cont": tr_cont}, 200
        self.text = json.dumps(body, ensure_ascii=False)

    def json(self):
        """
        Purpose: 본문 반환
        Parameter: 없음
        Input: 없음
        Output: dict
        """
        return self._body


def _client(monkeypatch, responses, calls):
    """
    Purpose: 응답 큐를 돌려주는 KIS 클라이언트 (HTTP·sleep 없음)
    Parameter:
        - monkeypatch: pytest monkeypatch
        - responses: FakeResp 리스트
        - calls: 요청 (headers, params) 기록 리스트
    Input: 없음
    Output: KISApiClient
    """
    def fake(self, method, url, **kw):
        """
        Purpose: safe_request 대체
        Parameter:
            - self: 클라이언트
            - method: HTTP 메서드
            - url: URL
            - kw: headers·params
        Input: 없음
        Output: FakeResp
        """
        calls.append((dict(kw["headers"]), dict(kw["params"])))
        return responses.pop(0)

    monkeypatch.setattr(KISApiClient, "safe_request", fake)
    monkeypatch.setattr("asset_flow.clients.kis_client.time.sleep", lambda s: None)
    return KISApiClient("tok", {"appkey": "k" * 8, "secret": "s" * 8, "account": CANO, "product_code": "01"})


def test_get_pages_follows_tr_cont_and_ctx(monkeypatch):
    calls = []
    page1 = FakeResp({"rt_cd": "0", "output": [{"a": 1}] * 25, "ctx_area_nk100": "32!^NEXT   ",
                      "ctx_area_fk100": f"03!^{CANO}!^01   "}, tr_cont="M")
    page2 = FakeResp({"rt_cd": "0", "msg_cd": "KIOK0460", "output": [{"a": 2}], "ctx_area_nk100": " ",
                      "ctx_area_fk100": "x"}, tr_cont="D")
    rows = _client(monkeypatch, [page1, page2], calls).get_period_rights("20260101", "20261013")
    assert len(rows) == 26
    (h1, p1), (h2, p2) = calls
    assert (h1["tr_cont"], h2["tr_cont"], h1["custtype"]) == ("", "N", "P")
    assert p1["CTX_AREA_NK100"] == "" and p2["CTX_AREA_NK100"] == "32!^NEXT   "   # 패딩 그대로 되돌려 보냄
    assert p1["INQR_DVSN"] == "03"


def test_no_data_is_ok_and_rt_cd_error_raises(monkeypatch):
    kis = _client(monkeypatch, [FakeResp({"rt_cd": "0", "msg_cd": "KIER2620", "output": []}, tr_cont="D"),
                                FakeResp({"rt_cd": "2", "msg_cd": "OPSQ2000",
                                          "msg1": "ERROR : INPUT INVALID_CHECK_ACNO   "})], [])
    assert kis.get_period_rights("20260101", "20261013") == []
    with pytest.raises(KisApiError, match="OPSQ2000") as e:
        kis.get_period_rights("20260101", "20261013")
    assert CANO not in str(e.value) and e.value.msg1 == "ERROR : INPUT INVALID_CHECK_ACNO"


def test_page_limit(monkeypatch):
    loop = [FakeResp({"rt_cd": "0", "output": [], "ctx_area_nk100": "n", "ctx_area_fk100": "f"}, tr_cont="M")
            for _ in range(3)]
    kis = _client(monkeypatch, loop, [])
    with pytest.raises(RuntimeError, match="3페이지 초과"):
        kis._get_pages("period_rights", {}, "output", ("ctx_area_nk100", "ctx_area_fk100"), max_pages=3)


def test_rights_ice_adds_symbol(monkeypatch):
    kis = _client(monkeypatch, [FakeResp({"rt_cd": "0", "output1": [{"ca_title": "현금배당"}]})], [])
    assert kis.get_overseas_rights_ice("ORCL", "20250101", "20261013") == [{"ca_title": "현금배당", "SYMB": "ORCL"}]


def _kr_rows(start_day, n):
    """
    Purpose: 국내 일봉 원시 행 n 개 (start_day 부터 하루씩 과거로)
    Parameter:
        - start_day: 가장 최근 날짜 YYYYMMDD
        - n: 행 수
    Input: 없음
    Output: 행 리스트
    """
    import datetime as dt

    d = dt.datetime.strptime(start_day, "%Y%m%d")
    return [{"stck_bsop_date": (d - dt.timedelta(days=i)).strftime("%Y%m%d"), "stck_clpr": "1"} for i in range(n)]


def test_domestic_price_pages_by_moving_end_date(monkeypatch):
    calls = []
    pages = [FakeResp({"rt_cd": "0", "output2": _kr_rows("20261012", 100)}),
             FakeResp({"rt_cd": "0", "output2": _kr_rows("20260703", 100)}),
             FakeResp({"rt_cd": "0", "output2": _kr_rows("20260325", 46) + [{"stck_bsop_date": ""}]})]
    rows = _client(monkeypatch, pages, calls).get_domestic_daily_price("476800", "20251013", "20261012")
    assert len(rows) == 246 and len(calls) == 3
    first_min = min(r["stck_bsop_date"] for r in _kr_rows("20261012", 100))      # 2026-07-05
    assert (first_min, calls[1][1]["FID_INPUT_DATE_2"]) == ("20260705", "20260704")  # 최소일 − 1
    assert calls[0][1]["FID_ORG_ADJ_PRC"] == "1" and calls[0][0]["custtype"] == "P"


def test_domestic_price_short_page_stops(monkeypatch):
    calls = []
    _client(monkeypatch, [FakeResp({"rt_cd": "0", "output2": _kr_rows("20261012", 99)})], calls) \
        .get_domestic_daily_price("476800", "20251013", "20261012")
    assert len(calls) == 1


def test_overseas_price_cuts_rows_before_start(monkeypatch):
    rows = [{"xymd": d, "clos": "1"} for d in ("20261013", "20261012", "20261009")]
    kis = _client(monkeypatch, [FakeResp({"rt_cd": "0", "output2": rows})], [])
    out = kis.get_overseas_daily_price("NAS", "AAPL", "20261010", "20261012")
    assert [r["xymd"] for r in out] == ["20261012"]
