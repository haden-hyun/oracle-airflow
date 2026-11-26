import datetime as dt

from asset_flow.pipeline.fund_nav import (collect_fund_prices, date_runs, expected_nav_date, fetch_all, gap_embed,
                                         gap_records, missing_dates, nav_for_date)

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
    recs, src = collect_fund_prices(["A"], "2026-10-11", {"A": "펀드A"}, HOL, kofia)
    assert recs == [{"standard_date": "2026-10-11", "product_code": "A", "product_name": "펀드A",
                     "standard_price": 2028.06}]
    assert src == {"A": "KOFIA 2026-10-08"} and kofia.calls == [("A", "20260921", "20261011")]


def test_collect_drops_missing_codes():
    # 지연(B)·조회 실패(C) 코드는 레코드에서 빠짐 → check_fund_coverage 가 실패 알림
    kofia = FakeKofia({"A": ROWS, "B": ROWS[:-1]}, fail={"C"})
    recs, src = collect_fund_prices(["A", "B", "C"], "2026-10-13", {}, HOL, kofia)
    assert [r["product_code"] for r in recs] == ["A"] and recs[0]["product_name"] == "A"     # 이름 없으면 코드
    assert src["A"] == "KOFIA 2026-10-13" and src["B"].startswith("missing") and src["C"].startswith("missing")


def test_fund_dag_structure():
    from test_finalize import _load_dag

    t = _load_dag("market_ingest_reference_daily").task_dict
    g = "fund_price."
    assert g + "crawl_fund_prices" not in t and g + "compare_with_fundguide" not in t
    assert t[g + "upload_fund_prices"].upstream_task_ids == {g + "get_standard_date", g + "collect_fund_prices"}
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


def _rows(*pairs):
    """
    Purpose: KOFIA 응답 행 생성
    Parameter:
        - pairs: (YYYYMMDD, 기준가)
    Input: 없음
    Output: get_fund_nav_history 형식 행 리스트
    """
    return [{"standard_date": d, "standard_price": str(p)} for d, p in pairs]


def test_missing_dates_counts_weekends_and_holidays():
    loaded = {D(2026, 11, 20), D(2026, 11, 21)}
    assert missing_dates(loaded, D(2026, 11, 20), D(2026, 11, 23)) == [D(2026, 11, 22), D(2026, 11, 23)]
    assert missing_dates({D(2026, 11, 20)}, D(2026, 11, 20), D(2026, 11, 20)) == []


def test_gap_records_carries_friday_into_weekend():
    # 2026-11-23·24 장애 재현: 11-22(일) = 11-20(금) 값 이월, 11-23(월) = 당일 값
    rows = _rows(("20261119", 1590.0), ("20261120", 1600.0), ("20261123", 1610.0), ("20261124", 1620.0))
    recs, miss = gap_records(rows, [D(2026, 11, 22), D(2026, 11, 23)], "K5X", "펀드", set())
    assert [(r["standard_date"], r["standard_price"]) for r in recs] == [("2026-11-22", 1600.0), ("2026-11-23", 1610.0)]
    assert recs[0] == {"standard_date": "2026-11-22", "product_code": "K5X", "product_name": "펀드",
                       "standard_price": 1600.0}
    assert miss == []


def test_gap_records_matches_daily_path_rule():
    # 휴일·12/31(KRX 휴장, 펀드 기준가는 있음) 포함 — 매일 경로(nav_for_date)와 같은 값
    gaps = [D(2025, 12, 31), D(2026, 10, 5), D(2026, 10, 9), D(2026, 10, 11)]
    recs, miss = gap_records(ROWS, gaps, "K5X", "펀드", HOL)
    assert {r["standard_date"]: r["standard_price"] for r in recs} == \
           {d.isoformat(): nav_for_date(ROWS, d, HOL)[0] for d in gaps}
    assert miss == []


def test_gap_records_skips_before_inception():
    rows = _rows(("20260105", 1000.0), ("20260106", 1001.0))           # 설정일 2026-01-05
    recs, miss = gap_records(rows, [D(2026, 1, 3), D(2026, 1, 4), D(2026, 1, 6)], "K5X", "펀드", set())
    assert [r["standard_date"] for r in recs] == ["2026-01-06"] and miss == []
    assert gap_records([], [D(2026, 1, 6)], "K5X", "펀드", set()) == ([], [])


def test_gap_records_reports_business_day_missing_in_kofia():
    rows = _rows(("20261120", 1600.0), ("20261124", 1620.0))           # 11-23(월) 공시 누락
    recs, miss = gap_records(rows, [D(2026, 11, 22), D(2026, 11, 23)], "K5X", "펀드", set())
    assert [r["standard_date"] for r in recs] == ["2026-11-22"] and miss == [D(2026, 11, 23)]


def test_gap_embed_only_when_filled():
    assert gap_embed({}, {}) is None
    assert gap_embed({}, {"A": ["2026-11-23"]}) is None                # 못 채움만 → 로그만 (매일 반복 경고 방지)
    e = gap_embed({"A": ["2026-11-22", "2026-11-23"], "B": ["2026-11-23"]}, {"B": ["2026-11-20"]})
    values = [f["value"] for f in e["fields"]]
    assert "2026-11-22 ~ 2026-11-23 · 2일 보충" in values and "2026-11-20" in values
    assert values[-1] == "start_date=2026-11-22 end_date=2026-11-23 (2일)"


def test_date_runs_groups_consecutive_days():
    assert date_runs([]) == []
    assert date_runs(["2026-11-23", "2026-03-01", "2026-11-22", "2026-11-22", "2026-02-28"]) == \
           [("2026-02-28", "2026-03-01", 2), ("2026-11-22", "2026-11-23", 2)]


def test_gap_embed_lists_recent_runs_and_rest():
    # 흩어진 빈 날 7구간 → 최근 5구간 + "외 2구간" (1년 전체 범위 안내 방지)
    days = ["2026-01-05", "2026-02-10", "2026-03-15", "2026-03-16", "2026-05-01", "2026-07-07", "2026-09-09",
            "2026-11-22"]
    e = gap_embed({"A": days[:4], "B": days[3:]}, {})
    lines = e["fields"][-1]["value"].split("\n")
    assert lines[0] == "start_date=2026-03-15 end_date=2026-03-16 (2일)"
    assert lines[4] == "start_date=2026-11-22 end_date=2026-11-22 (1일)"
    assert lines[5] == "외 2구간 (2026-01-05 ~ 2026-02-10)" and len(lines) == 6


def test_fill_nav_gaps_runs_after_upload_even_on_failure():
    from test_finalize import _load_dag

    t = _load_dag("market_ingest_reference_daily").task_dict
    g = "fund_price."
    fill = t[g + "fill_nav_gaps"]
    assert fill.upstream_task_ids == {g + "resolve_fund_codes", g + "upload_fund_prices"}
    assert fill.trigger_rule == "all_done" and not fill.downstream_task_ids
    assert g + "get_standard_date" not in fill.upstream_task_ids       # 늦은 run skip 이 보충을 막지 않게
