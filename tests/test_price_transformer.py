from datetime import datetime, timezone
from types import SimpleNamespace

from asset_flow.clients.kofia_client import KofiaClient
from asset_flow.config.schemas import FUND_PRICE_COLUMNS, PRICE_COLUMNS
from asset_flow.transformers.price_transformer import closed_upbit_candles, to_fund_frame, to_price_frame

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


def test_upbit_open_candle_is_dropped_at_0710_kst():
    now = datetime(2026, 10, 12, 22, 10, tzinfo=timezone.utc)          # 2026-10-13 07:10 KST
    rows = [{"candle_date_time_utc": "2026-10-12T00:00:00"}, {"candle_date_time_utc": "2026-10-11T00:00:00"}]
    assert closed_upbit_candles(rows, now) == rows[1:]


def test_upbit_closed_candle_frame():
    rows = [{"candle_date_time_utc": "2026-10-11T00:00:00", "candle_date_time_kst": "2026-10-11T09:00:00",
             "opening_price": 1.0, "high_price": 2.0, "low_price": 0.5, "trade_price": 1.5,
             "candle_acc_trade_volume": 10.12345678}]
    df = to_price_frame("UPBIT_DAY", rows, "BTC", "KRW", "2026-10-12")
    assert (df.loc[0, "standard_date"], df.loc[0, "closing_price"], df.loc[0, "source"]) == ("2026-10-11", 1.5, "UPBIT_DAY")


def test_fund_frame_drops_today_and_nonpositive():
    rows = [{"standard_date": "20261013", "standard_price": "1755.96"},
            {"standard_date": "20261012", "standard_price": "1750.00"},
            {"standard_date": "20261008", "standard_price": "0"}]
    df = to_fund_frame(rows, "K55105D43299", "TDF2055", "2026-10-12")
    assert df.to_dict("records") == [{"standard_date": "2026-10-12", "product_code": "K55105D43299",
                                      "product_name": "TDF2055", "standard_price": 1750.0}]


def test_empty_inputs_keep_columns():
    assert list(to_price_frame("KIS_KR_DAILY", [], "X", "KRW", "2026-10-12").columns) == PRICE_COLUMNS
    assert list(to_fund_frame([], "X", "Y", "2026-10-12").columns) == FUND_PRICE_COLUMNS


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
    assert t["fail_if_upstream_failed"].upstream_task_ids == {"ingest_prices", "ingest_fund_nav",
                                                              "check_tracked_coverage"}
    assert t["check_tracked_coverage"].trigger_rule == "all_done"
    assert t["upsert_products"].upstream_task_ids == {"get_dates", "wait_for_token", "wait_for_asset_daily"}
    assert t["wait_for_asset_daily"].timeout == 1800
