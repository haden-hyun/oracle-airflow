import logging
from datetime import datetime, timezone
from types import SimpleNamespace

from asset_flow.clients.kofia_client import KofiaClient
from asset_flow.config.schemas import PRICE_COLUMNS
from asset_flow.transformers.price_transformer import kst_day_ohlc, to_price_frame

KR_ROWS = [{"stck_bsop_date": "20261013", "stck_clpr": "4185", "stck_oprc": "4205", "stck_hgpr": "4205",
            "stck_lwpr": "4150", "acml_vol": "1040128"},
           {"stck_bsop_date": "20261012", "stck_clpr": "4205", "stck_oprc": "4210", "stck_hgpr": "4210",
            "stck_lwpr": "4190", "acml_vol": "644176"}]
US_ROWS = [{"xymd": "20261013", "clos": "346.8401", "open": "344.6650", "high": "348.1499", "low": "341.1200",
            "tvol": "9248243"},
           {"xymd": "20261012", "clos": "340.9700", "open": "338.2150", "high": "342.6300", "low": "337.6000",
            "tvol": "25900599"}]


def test_kr_drops_intraday_row():
    df = to_price_frame("KIS_KR_DAILY", KR_ROWS, "476800", "KRW", "2026-10-12")
    assert df.to_dict("records") == [{
        "standard_date": "2026-10-12", "product_code": "476800", "opening_price": 4210.0, "highest_price": 4210.0,
        "lowest_price": 4190.0, "closing_price": 4205.0, "trading_volume": 644176.0, "currency_code": "KRW",
        "source": "KIS_KR_DAILY"}]


def test_us_drops_intraday_row():
    df = to_price_frame("KIS_US_DAILY", US_ROWS, "AAPL", "USD", "2026-10-12")
    assert list(df["standard_date"]) == ["2026-10-12"] and df.loc[0, "closing_price"] == 340.97


def _hour(kst: str, o: float, h: float, lo: float, c: float, v: float) -> dict:
    """
    Purpose: 업비트 60분봉 1개 생성
    Parameter:
        - kst: 캔들 시작 KST (YYYY-MM-DDTHH:MM:SS)
        - o: 시가
        - h: 고가
        - lo: 저가
        - c: 종가(trade_price)
        - v: 거래량
    Input: 없음
    Output: 원시 캔들 dict
    """
    return {"market": "KRW-BTC", "candle_date_time_kst": kst, "opening_price": o, "high_price": h, "low_price": lo,
            "trade_price": c, "candle_acc_trade_volume": v}


AT_0710 = datetime(2026, 10, 12, 22, 10, tzinfo=timezone.utc)          # 2026-10-13 07:10 KST


def test_kst_day_ohlc_close_is_2300_candle_and_skips_open_day():
    rows = [_hour("2026-10-13T00:00:00", 9, 9, 9, 9, 1),                # 미마감 (07:10 현재)
            _hour("2026-10-12T23:00:00", 5, 7, 4, 6, 0.1),
            _hour("2026-10-12T00:00:00", 3, 5, 1, 4, 0.2),
            _hour("2026-10-12T12:00:00", 4, 8, 2, 5, 0.3)]
    assert kst_day_ohlc(rows, AT_0710) == [{"standard_date": "2026-10-12", "opening_price": 3, "highest_price": 8,
                                            "lowest_price": 1, "closing_price": 6, "trading_volume": 0.6}]


def test_kst_day_ohlc_includes_day_at_exact_midnight():
    rows = [_hour("2026-10-12T23:00:00", 1, 1, 1, 2, 1)]
    midnight = datetime(2026, 10, 12, 15, 0, tzinfo=timezone.utc)      # 2026-10-13 00:00 KST
    assert [d["standard_date"] for d in kst_day_ohlc(rows, midnight)] == ["2026-10-12"]
    assert kst_day_ohlc(rows, datetime(2026, 10, 12, 14, 59, tzinfo=timezone.utc)) == []


def test_kst_day_ohlc_groups_days_and_dedups():
    rows = [_hour("2026-10-11T23:00:00", 1, 1, 1, 2, 0.5), _hour("2026-10-11T23:00:00", 1, 1, 1, 2, 0.5),
            _hour("2026-10-12T23:00:00", 3, 3, 3, 4, 1.0), _hour("2026-10-10T23:00:00", 5, 5, 5, 6, 2.0)]
    out = kst_day_ohlc(rows, AT_0710)
    assert [(d["standard_date"], d["closing_price"], d["trading_volume"]) for d in out] == [
        ("2026-10-10", 6, 2.0), ("2026-10-11", 2, 0.5), ("2026-10-12", 4, 1.0)]


def test_kst_day_ohlc_missing_hours_warns_before_22(caplog):
    rows = [_hour("2026-10-12T21:00:00", 2, 2, 2, 3, 1), _hour("2026-10-12T01:00:00", 1, 1, 1, 1, 1),
            _hour("2026-10-11T22:00:00", 1, 1, 1, 7, 1)]
    with caplog.at_level(logging.WARNING):
        out = kst_day_ohlc(rows, AT_0710)
    assert [(d["standard_date"], d["opening_price"], d["closing_price"]) for d in out] == [
        ("2026-10-11", 1, 7), ("2026-10-12", 1, 3)]
    assert "2026-10-12 종가 캔들 21:00" in caplog.text and "2026-10-11" not in caplog.text


def test_upbit_day_frame():
    rows = kst_day_ohlc([_hour("2026-10-11T23:00:00", 1.0, 2.0, 0.5, 1.5, 10.12345678)], AT_0710)
    df = to_price_frame("UPBIT_DAY", rows, "BTC", "KRW", "2026-10-12")
    assert df.to_dict("records") == [{
        "standard_date": "2026-10-11", "product_code": "BTC", "opening_price": 1.0, "highest_price": 2.0,
        "lowest_price": 0.5, "closing_price": 1.5, "trading_volume": 10.12345678, "currency_code": "KRW",
        "source": "UPBIT_DAY"}]


def test_empty_inputs_keep_columns():
    assert list(to_price_frame("KIS_KR_DAILY", [], "X", "KRW", "2026-10-12").columns) == PRICE_COLUMNS


def test_kofia_parses_only_requested_code(monkeypatch):
    xml = ("<root><message><DISCondFuncListDTO>"
           "<selectMeta><tmpV1>20261012</tmpV1><tmpV2>1755.96</tmpV2><tmpV12>K55105D43299</tmpV12></selectMeta>"
           "<selectMeta><tmpV1>20261012</tmpV1><tmpV2>1.00</tmpV2><tmpV12>OTHER</tmpV12></selectMeta>"
           "</DISCondFuncListDTO></message></root>").encode()
    monkeypatch.setattr(KofiaClient, "safe_request", lambda self, *a, **k: SimpleNamespace(content=xml))
    monkeypatch.setattr("asset_flow.clients.kofia_client.time.sleep", lambda s: None)
    assert KofiaClient().get_fund_nav_history("K55105D43299", "20261001", "20261012") == [
        {"standard_date": "20261012", "standard_price": "1755.96"}]


def test_price_dag_structure():
    from test_finalize import _load_dag

    t = _load_dag("market_ingest_product_price_daily").task_dict
    assert t["fail_if_upstream_failed"].upstream_task_ids == {"ingest_prices", "check_tracked_coverage"}
    assert t["check_tracked_coverage"].trigger_rule == "all_done"
    assert t["upsert_products"].upstream_task_ids == {"get_dates", "wait_for_token", "wait_for_asset_daily"}
    assert t["wait_for_asset_daily"].timeout == 1800
