import datetime as dt
from datetime import date

import pytest

from asset_flow.clients.kis_client import KISApiClient
from asset_flow.config.kis import KIS
from asset_flow.pipeline import reference_data as rd
from asset_flow.transformers.kis_transformer import to_index_frame, transform_exchange_rate_history

KR_HOL = {date(2025, 10, 3), date(2025, 10, 6), date(2025, 10, 7), date(2025, 10, 8), date(2025, 10, 9)}
HOL = {"KR": KR_HOL, "US": set()}


def _weekdays(a: date, b: date, off: set) -> list[date]:
    """
    Purpose: a~b 평일 중 off 제외 (오름차순)
    Parameter:
        - a: 시작
        - b: 끝
        - off: 휴장일
    Input: 없음
    Output: date 리스트
    """
    out, d = [], a
    while d <= b:
        if d.weekday() < 5 and d not in off:
            out.append(d)
        d += dt.timedelta(days=1)
    return out


def _capped_client(monkeypatch, series: list[date], calls: list, cap: int = 100):
    """
    Purpose: FHKST03030100 흉내 — 요청 구간 끝부터 min(cap, 구간의 한국 영업일 수) 행만 돌려줌
    Parameter:
        - monkeypatch: pytest monkeypatch
        - series: 소스가 가진 거래일 (US 등)
        - calls: (DATE_1, DATE_2) 기록
        - cap: 1회 상한 행
    Input: 없음
    Output: KISApiClient (_request 대체)
    """
    def fake(self, name, params, tr_cont=""):
        """
        Purpose: _request 대체
        Parameter:
            - self: 클라이언트
            - name: KIS 키
            - params: 쿼리
            - tr_cont: 무시
        Input: 없음
        Output: (본문, "")
        """
        a = dt.datetime.strptime(params["FID_INPUT_DATE_1"], "%Y%m%d").date()
        b = dt.datetime.strptime(params["FID_INPUT_DATE_2"], "%Y%m%d").date()
        calls.append((a, b))
        n = min(cap, len(_weekdays(a, b, KR_HOL)))
        rows = sorted((d for d in series if a <= d <= b), reverse=True)[:n]
        return {"output2": [{"stck_bsop_date": d.strftime("%Y%m%d"), "ovrs_nmix_prpr": "100"} for d in rows]
                + [{}]}, ""      # 실제 응답처럼 빈 행 섞임

    monkeypatch.setattr(KISApiClient, "_request", fake)
    return KISApiClient("tok", {"appkey": "k" * 8, "secret": "s" * 8})


def test_chart_history_walks_back_past_kr_holiday_caps(monkeypatch):
    # 고정 구간 분할이면 추석 연휴 구간에서 미국 거래일이 빠짐 → 끝 날짜를 당겨 반복 + 시작 앞 패딩으로 전부 받음
    us = _weekdays(date(2025, 9, 1), date(2026, 1, 31), set())
    calls = []
    kis = _capped_client(monkeypatch, us, calls)
    rows = kis.get_chart_history("overseas_index_daily", "SPX", "20251001", "20260130")
    got = sorted(dt.datetime.strptime(r["stck_bsop_date"], "%Y%m%d").date() for r in rows)
    assert got == [d for d in us if date(2025, 10, 1) <= d <= date(2026, 1, 30)]
    assert all(a == date(2025, 9, 17) for a, _ in calls)          # 시작 14일 전부터 요청
    assert len(calls) <= 3


def test_chart_history_start_inside_kr_holiday(monkeypatch):
    # 시작일이 한국 연휴(10/3~10/9) 한가운데 — 패딩이 없으면 남은 구간 한국 영업일 0 → 미국 10/3·10/6~ 유실
    us = _weekdays(date(2025, 9, 1), date(2025, 10, 31), set())
    kis = _capped_client(monkeypatch, us, [])
    rows = kis.get_chart_history("overseas_index_daily", "SPX", "20251003", "20251031")
    assert min(r["stck_bsop_date"] for r in rows) == "20251003" and len(rows) == 21


def test_chart_history_mid_pagination_gap_raises(monkeypatch):
    # 받은 뒤 빈 응답(남은 구간 + 패딩 14일에 행 없음) = 이력이 시작일 전에 끊김 → 잘린 이력을 적재하지 않게 실패
    us = _weekdays(date(2026, 3, 2), date(2026, 10, 30), set())
    kis = _capped_client(monkeypatch, us, [])
    with pytest.raises(RuntimeError, match="끊김"):
        kis.get_chart_history("overseas_index_daily", "SPX", "20251001", "20261030")


def test_chart_history_unknown_code_is_empty(monkeypatch):
    kis = _capped_client(monkeypatch, [], [])
    assert kis.get_chart_history("overseas_index_daily", ".SPX", "20251001", "20251031") == []


def test_chart_history_page_limit_raises(monkeypatch):
    us = _weekdays(date(2020, 1, 1), date(2026, 1, 31), set())
    kis = _capped_client(monkeypatch, us, [], cap=50)
    with pytest.raises(RuntimeError, match="시작일에 닿지 못함"):
        kis.get_chart_history("domestic_index_daily", "0001", "20200101", "20260130")


@pytest.mark.parametrize("utc,expect", [
    ("2026-10-16T21:55:00", date(2026, 10, 16)),   # 06:55 KST = 17:55 EDT → 그날 마감 끝
    ("2026-10-16T15:02:00", date(2026, 10, 15)),   # 00:02 KST = 11:02 EDT 장중 → 전날
    ("2026-12-15T21:20:00", date(2026, 12, 14)),   # 06:20 KST = 16:20 EST, 종가 확정 여유 전 → 전날
])
def test_us_closed_date(utc, expect):
    now = dt.datetime.fromisoformat(utc).replace(tzinfo=dt.timezone.utc)
    assert rd.us_closed_date(now) == expect


def _plan(last=None, params=None, base=date(2026, 10, 17), now="2026-10-16T21:55:00"):
    """
    Purpose: plan_index_windows 호출 도우미
    Parameter:
        - last: {index_code: date}
        - params: DAG Param
        - base: 실행일 T
        - now: UTC ISO 시각
    Input: 없음
    Output: {index_code: (start, end)}
    """
    now_dt = dt.datetime.fromisoformat(now).replace(tzinfo=dt.timezone.utc)
    ws = rd.plan_index_windows(last or {}, base, HOL, date(2025, 10, 13), 5, params or {}, now_dt)
    return {w["index_code"]: (w["start"], w["end"]) for w in ws}


def test_new_indexes_start_at_from_date():
    assert _plan() == {c: ("2025-10-13", "2026-10-16") for c in KIS.MARKET_INDEXES}


def test_daily_refetches_last_five_trading_days():
    p = _plan({c: date(2026, 10, 16) for c in KIS.MARKET_INDEXES})
    assert p["KOSPI"] == ("2026-10-12", "2026-10-16") and p["SPX"] == ("2026-10-12", "2026-10-16")


def test_us_end_capped_when_run_during_us_session():
    p = _plan({c: date(2026, 10, 15) for c in KIS.MARKET_INDEXES}, now="2026-10-16T15:02:00")
    assert p["KOSPI"][1] == "2026-10-16" and p["COMP"][1] == "2026-10-15"


def test_manual_params_override_and_cap_end():
    p = _plan({c: date(2026, 10, 16) for c in KIS.MARKET_INDEXES},
              {"start_date": "2025-01-02", "end_date": "2030-01-01"})
    assert p["KOSDAQ"] == ("2025-01-02", "2026-10-16")


def test_collect_requires_last_trading_day_row():
    class Kis:
        def get_chart_history(self, name, code, start, end):
            """
            Purpose: 가짜 이력 — SPX 만 마지막 거래일(10/16) 행이 없음
            Parameter:
                - name: KIS 키
                - code: 코드
                - start: 시작 YYYYMMDD
                - end: 끝 YYYYMMDD
            Input: 없음
            Output: 원시 행
            """
            last = "20261015" if code == "SPX" else "20261016"
            return [{"stck_bsop_date": last, "bstp_nmix_prpr": "10", "ovrs_nmix_prpr": "10"}]

    ws = [{"index_code": c, "market": m, "start": "2026-10-12", "end": "2026-10-16"}
          for c, (m, _) in KIS.MARKET_INDEXES.items()]
    with pytest.raises(RuntimeError, match=r"SPX 2026-10-16"):
        rd.collect_index_prices(Kis(), ws, HOL)
    # 창 안에 거래일이 없으면(주말만) 검증하지 않음
    assert rd.expected_last_day({"market": "KR", "start": "2026-10-17", "end": "2026-10-18"}, HOL) is None


def test_index_frame_drops_intraday_and_zero_ohlc():
    rows = [{"stck_bsop_date": "20261016", "ovrs_nmix_prpr": "7738.85", "ovrs_nmix_oprc": "0",
             "ovrs_nmix_hgpr": "7756.84", "ovrs_nmix_lwpr": "7730.31"},
            {"stck_bsop_date": "20261017", "ovrs_nmix_prpr": "7800", "ovrs_nmix_oprc": "7790",
             "ovrs_nmix_hgpr": "7801", "ovrs_nmix_lwpr": "7780"}]
    df = to_index_frame("overseas_index_daily", rows, "SPX", "US", "2026-10-16")
    r = df.to_dict("records")
    assert len(r) == 1 and r[0]["closing_price"] == 7738.85 and r[0]["opening_price"] != r[0]["opening_price"]
    assert r[0]["source"] == "KIS_US_INDEX" and r[0]["standard_date"] == "2026-10-16"


def _fx(d, close, o=None):
    """
    Purpose: 환율 이력 원시 행
    Parameter:
        - d: YYYYMMDD
        - close: 종가(매매기준율)
        - o: 시가 (없으면 종가)
    Input: 없음
    Output: dict
    """
    o = o or close
    return {"stck_bsop_date": d, "ovrs_nmix_prpr": str(close), "ovrs_nmix_oprc": str(o),
            "ovrs_nmix_hgpr": str(close), "ovrs_nmix_lwpr": str(o)}


def test_fx_history_carries_calendar_days_and_crosses_by_date():
    raw = {"FX@KRWKFTC": [_fx("20251009", 1400), _fx("20251010", 1410)],          # 금요일까지
           "FX@KRWJS": [_fx("20251010", 9.4), _fx("20251011", 9.5)],              # 토요일 행 있음
           "FX@GBP": [_fx("20251010", 1.3), _fx("20251011", 1.4)],
           "FX@EUR": [_fx("20251010", 1.1)]}
    df = transform_exchange_rate_history(raw, "2025-10-10", "2025-10-12")
    get = {(r["currency_code"], r["standard_date"]): r for r in df.to_dict("records")}
    assert len(df) == 12 and set(df["source"]) == {"KIS_FX_DAILY"}
    assert get[("USD", "2025-10-12")]["base_rate"] == 1410 and get[("USD", "2025-10-12")]["previous_day_closing_price"] == 1400
    assert get[("JPY", "2025-10-12")]["base_rate"] == 9.5 and get[("JPY", "2025-10-12")]["previous_day_closing_price"] == 9.4
    assert get[("GBP", "2025-10-11")]["base_rate"] == pytest.approx(1.4 * 1410)      # 토요일 GBP × 이월 USD
    assert get[("EUR", "2025-10-11")]["currency_pair_name"] == "원/유로"
    assert get[("USD", "2025-10-10")]["source_symbol"] == "FX@KRWKFTC"


def test_fx_history_without_seed_is_short(monkeypatch):
    class Kis:
        def get_chart_history(self, name, code, start, end):
            """
            Purpose: 가짜 환율 이력 — 10/10 부터만 있음 (10/09 이월 시작값 없음)
            Parameter:
                - name: KIS 키
                - code: 심볼
                - start: 시작 YYYYMMDD
                - end: 끝 YYYYMMDD
            Input: 없음
            Output: 원시 행
            """
            return [_fx("20251010", 1.2)]

    with pytest.raises(RuntimeError, match="빠진 날 수"):
        rd.collect_exchange_rate_history(Kis(), "2025-10-09", "2025-10-12")


def test_reference_dag_groups_are_independent():
    from test_finalize import _load_dag

    dag = _load_dag("market_ingest_reference_daily")
    t = dag.task_dict
    group = {tid: tid.split(".")[0] for tid in t if "." in tid}
    for tid, task in t.items():
        for up in task.upstream_task_ids:
            if tid in group and up in group:
                assert group[tid] == group[up], f"{up} → {tid} 그룹 간 의존"
    assert t["wait_for_token"].downstream_task_ids == {
        "exchange_rate.fetch_exchange_rates", "exchange_rate.backfill_exchange_rates",
        "market_index.collect_index_prices"}
    assert not any(tid.startswith("fund_price.") for tid in t["wait_for_token"].downstream_task_ids)
    assert dag.params["groups"] == ["exchange_rate", "fund_price", "market_index"]
    assert t["fund_price.upload_fund_prices"].upstream_task_ids == {"fund_price.get_standard_date",
                                                                   "fund_price.collect_fund_prices"}
    assert t["market_index.refresh_dashboard_views"].upstream_task_ids == {"market_index.upload_index_prices"}


def test_reference_config(app):
    m = app.reference_ingest.market_index
    assert m.from_date == date(2025, 10, 13) and m.refetch_trading_days == 5 and not m.refresh_dashboard_views
    assert app.tables.index_daily.table_name == "index_daily"
