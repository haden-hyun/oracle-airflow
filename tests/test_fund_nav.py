import datetime as dt

from asset_flow.pipeline.fund_nav import (collect_fund_prices, compare_sources, expected_nav_date, fetch_all,
                                         nav_for_date)

D = dt.date
HOL = {D(2026, 10, 5), D(2026, 10, 9), D(2025, 12, 31)}          # 대체공휴일 · 한글날 · KRX 연말휴장
ROWS = [{"standard_date": d, "standard_price": p} for d, p in
        [("20251230", "1700.0"), ("20251231", "1725.76"), ("20261002", "2010.0"),
         ("20261006", "2015.0"), ("20261008", "2028.06"), ("20261012", "2030.5"), ("20261013", "2031.0")]]


class FakeKofia:
    def __init__(self, rows=None, fail=()):
        """
        Purpose: KOFIA 클라이언트 대역
        Parameter:
            - rows: 코드별 응답 {code: [행]}
            - fail: 예외를 낼 코드
        Input: 없음
        Output: 없음
        """
        self.rows, self.fail, self.calls = rows or {}, set(fail), []

    def get_fund_nav_history(self, code, start, end):
        """
        Purpose: 이력 조회 흉내
        Parameter:
            - code: 펀드코드
            - start: 시작 YYYYMMDD
            - end: 끝 YYYYMMDD
        Input: 없음
        Output: 행 리스트 (fail 코드면 ConnectionError)
        """
        self.calls.append((code, start, end))
        if code in self.fail:
            raise ConnectionError("down")
        return self.rows.get(code, [])


def test_weekday_uses_same_day_nav():
    assert nav_for_date(ROWS, D(2026, 10, 13), HOL) == (2031.0, D(2026, 10, 13))


def test_weekend_and_holiday_carry_previous_nav():
    # 10-10(토)·10-11(일) ← 10-09 한글날 휴장 → 10-08 기준가
    assert nav_for_date(ROWS, D(2026, 10, 11), HOL) == (2028.06, D(2026, 10, 8))
    assert expected_nav_date(D(2026, 10, 11), HOL) == D(2026, 10, 8)


def test_krx_year_end_holiday_keeps_fund_nav():
    # 12/31 은 KRX 휴장이지만 펀드 기준가는 있음 → 그날 값 사용
    assert nav_for_date(ROWS, D(2025, 12, 31), HOL) == (1725.76, D(2025, 12, 31))


def test_stale_nav_is_missing():
    # 마지막 영업일(10-13) 기준가가 아직 없으면 이월하지 않고 None (공시 지연)
    assert nav_for_date(ROWS[:-1], D(2026, 10, 13), HOL) is None
    assert nav_for_date([], D(2026, 10, 13), HOL) is None


def test_collect_kofia_first():
    kofia = FakeKofia({"A": ROWS})
    recs, src = collect_fund_prices(["A"], "2026-10-11", {"A": "펀드A"}, HOL, kofia, crawl=None)
    assert recs == [{"standard_date": "2026-10-11", "product_code": "A", "product_name": "펀드A",
                     "standard_price": 2028.06}]
    assert src == {"A": "KOFIA 2026-10-08"} and kofia.calls == [("A", "20260921", "20261011")]


def test_collect_falls_back_to_crawler_only_for_missing():
    crawled = []

    def crawl(codes, std):
        """
        Purpose: 크롤러 대역 (요청 코드 기록)
        Parameter:
            - codes: 코드
            - std: 기준일
        Input: 없음
        Output: 레코드
        """
        crawled.extend(codes)
        return [{"standard_date": std, "product_code": c, "product_name": c, "standard_price": 1.0} for c in codes]

    kofia = FakeKofia({"A": ROWS, "B": ROWS[:-1]}, fail={"C"})
    recs, src = collect_fund_prices(["A", "B", "C"], "2026-10-13", {}, HOL, kofia, crawl=crawl)
    assert crawled == ["B", "C"] and len(recs) == 3
    assert src == {"A": "KOFIA 2026-10-13", "B": "fundguide", "C": "fundguide"}
    assert recs[0]["product_name"] == "A"                                         # 이름 없으면 코드


def test_collect_without_fallback_leaves_gap():
    recs, src = collect_fund_prices(["B"], "2026-10-13", {}, HOL, FakeKofia({"B": ROWS[:-1]}), crawl=None)
    assert recs == [] and src["B"].startswith("missing")


def test_compare_sources():
    k = [{"product_code": "A", "standard_price": 2031.0}, {"product_code": "B", "standard_price": 10.0}]
    assert compare_sources(k, [{"product_code": "A", "standard_price": 2031.004}]) == []      # B 크롤링 실패 → 생략
    assert compare_sources(k, [{"product_code": "B", "standard_price": 10.5}]) == ["B KOFIA 10.0 ≠ fundguide 10.5"]


def test_fund_dag_structure():
    from test_finalize import _load_dag

    t = _load_dag("market_ingest_reference_daily").task_dict
    g = "fund_price."
    assert g + "crawl_fund_prices" not in t
    assert t[g + "upload_fund_prices"].upstream_task_ids == {g + "get_standard_date", g + "collect_fund_prices"}
    assert t[g + "compare_with_fundguide"].upstream_task_ids == {g + "get_standard_date", g + "collect_fund_prices",
                                                                 g + "upload_fund_prices"}
    assert t[g + "check_fund_coverage"].upstream_task_ids >= {g + "upload_fund_prices"}


def test_kofia_client_uses_long_timeout(monkeypatch):
    # 비브라우저 UA 는 응답 ~105초 지연·Python UA 는 무응답 (2026-10 실측) — 긴 timeout + 브라우저 UA
    from types import SimpleNamespace

    from asset_flow.clients.kofia_client import KofiaClient
    from asset_flow.config.kofia import KOFIA

    seen = {}
    xml = (b"<message><selectMeta><tmpV1>20261016</tmpV1><tmpV2>1692.99</tmpV2>"
           b"<tmpV12>K55105D43299</tmpV12></selectMeta></message>")

    def fake(method, url, **kw):
        """
        Purpose: requests.request 대역 (인자 기록)
        Parameter:
            - method / url / kw: requests 인자
        Input: 없음
        Output: 고정 XML 응답
        """
        seen.update(kw)
        return SimpleNamespace(content=xml, raise_for_status=lambda: None)

    monkeypatch.setattr("asset_flow.clients.base_client.requests.request", fake)
    rows = KofiaClient().get_fund_nav_history("K55105D43299", "20261012", "20261017", interval=0)
    assert rows == [{"standard_date": "20261016", "standard_price": "1692.99"}]
    assert seen["timeout"] == KOFIA.TIMEOUT and KOFIA.TIMEOUT[1] >= 150
    assert "python" not in seen["headers"]["User-Agent"].lower()            # Python 기본 UA 는 서버가 끊음


def test_fetch_all_runs_concurrently_and_isolates_errors():
    import threading

    gate = threading.Barrier(3, timeout=5)                  # 3개가 동시에 들어와야 통과 (순차면 BrokenBarrierError)

    def fetch(key):
        """
        Purpose: 동시성 확인용 조회 대역
        Parameter:
            - key: 키 (C 는 실패)
        Input: 없음
        Output: 키 소문자 (C 는 ConnectionError)
        """
        gate.wait()
        if key == "C":
            raise ConnectionError("down")
        return key.lower()

    got = fetch_all(fetch, ["A", "B", "C"], workers=3)
    assert got["A"] == "a" and got["B"] == "b" and isinstance(got["C"], ConnectionError)
    assert fetch_all(fetch, []) == {}


def test_ingest_fund_nav_fetches_in_parallel(monkeypatch):
    import threading
    from types import SimpleNamespace

    from asset_flow import db_manager as db
    from asset_flow.pipeline import price_plan
    from asset_flow.pipeline.price_plan import Window, ingest_source

    gate = threading.Barrier(2, timeout=5)

    def fetch(p, s, e, i):
        """
        Purpose: KOFIA 조회 대역 (2개가 동시에 들어와야 통과, B 는 실패)
        Parameter:
            - p / s / e / i: 상품 · 시작 · 끝 · 간격
        Input: 없음
        Output: 기준가 행
        """
        gate.wait()
        if p["product_code"] == "B":
            raise ConnectionError("down")
        return [{"standard_date": "20261016", "standard_price": "1692.99"}]

    inserted = []
    monkeypatch.setattr(price_plan, "_fetcher", lambda source, deps: fetch)
    monkeypatch.setattr(db, "insert_missing_fund_prices", lambda engine, recs: inserted.extend(recs) or len(recs))
    products = [{"product_code": c, "product_name": c} for c in ("A", "B")]
    windows = [Window(c, "2026-10-12", "2026-10-17", False) for c in ("A", "B")]
    res = ingest_source("FUND_NAV", products, windows, SimpleNamespace(engine=None), 0)
    assert res["rows"] == 1 and list(res["failed"]) == ["B"] and inserted[0]["product_code"] == "A"
