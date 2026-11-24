import logging

import pytest

from asset_flow.clients import upbit_client
from asset_flow.clients.upbit_client import UpbitApiClient


def _client(monkeypatch, pages):
    """
    Purpose: 응답을 차례로 돌려주는 공개 API 클라이언트 (요청 파라미터 기록)
    Parameter:
        - monkeypatch: pytest monkeypatch
        - pages: 호출마다 돌려줄 응답 리스트
    Input: 없음
    Output: (UpbitApiClient, 호출 기록 [(path_key, params)])
    """
    calls = []

    def fake_get(self, path_key, auth, params=None):
        """
        Purpose: _get 대체 — 기록 후 다음 응답 반환
        Parameter:
            - path_key: UPBIT.PATHS 키
            - auth: 인증 여부
            - params: 쿼리 파라미터
        Input: 없음
        Output: 응답 리스트
        """
        calls.append((path_key, dict(params)))
        return pages[len(calls) - 1]

    monkeypatch.setattr(UpbitApiClient, "_get", fake_get)
    monkeypatch.setattr(upbit_client.time, "sleep", lambda s: None)
    return UpbitApiClient("", ""), calls


def _hours(last_kst_day: str, first_hour: int, n: int) -> list[dict]:
    """
    Purpose: 최근→과거 60분봉 n 개 (KST, 하루 24개 기준으로 날짜를 넘김)
    Parameter:
        - last_kst_day: 첫 캔들 날짜 YYYY-MM-DD
        - first_hour: 첫 캔들 시각
        - n: 개수
    Input: 없음
    Output: 원시 캔들 리스트
    """
    from datetime import datetime, timedelta

    t = datetime.fromisoformat(f"{last_kst_day}T{first_hour:02d}:00:00")
    return [{"candle_date_time_kst": (t - timedelta(hours=i)).isoformat()} for i in range(n)]


def _close(kst: str) -> list[dict]:
    """
    Purpose: get_kst_closes 응답 1개 (60분봉 count 1)
    Parameter:
        - kst: 캔들 시작 KST
    Input: 없음
    Output: 응답 리스트
    """
    return [{"market": "KRW-BTC", "candle_date_time_kst": kst, "trade_price": 84091000.0}]


def test_kst_closes_uses_kst_midnight(monkeypatch):
    client, calls = _client(monkeypatch, [_close("2026-11-20T23:00:00")])
    assert [c["trade_price"] for c in client.get_kst_closes(["KRW-BTC"], "2026-11-21")] == [84091000.0]
    assert calls[0] == ("hour_candle", {"market": "KRW-BTC", "to": "2026-11-21T00:00:00+09:00", "count": 1})


def test_kst_closes_older_candle_warns_and_uses_last_trade(monkeypatch, caplog):
    client, _ = _client(monkeypatch, [_close("2026-11-13T15:00:00")])            # T-1 보다 7일 전 = 허용 한도
    with caplog.at_level(logging.WARNING):
        out = client.get_kst_closes(["KRW-BTC"], "2026-11-21")
    assert [c["candle_date_time_kst"] for c in out] == ["2026-11-13T15:00:00"]
    assert "2026-11-20 종가 캔들 2026-11-13T15:00:00" in caplog.text


def test_kst_closes_rejects_stale_or_empty(monkeypatch):
    client, _ = _client(monkeypatch, [_close("2026-11-12T23:00:00")])            # 8일 전 → 상폐·거래정지 의심
    with pytest.raises(ValueError, match="8일 전"):
        client.get_kst_closes(["KRW-BTC"], "2026-11-21")
    client, _ = _client(monkeypatch, [[]])
    with pytest.raises(ValueError, match="캔들 없음"):
        client.get_kst_closes(["KRW-BTC"], "2026-11-21")


def test_hour_candles_paginates_to_start(monkeypatch):
    p1 = _hours("2026-11-20", 23, 200)                                # 11-20 23:00 → 11-12 16:00
    p2 = _hours("2026-11-12", 15, 200)                                # 11-12 15:00 → 11-04 08:00
    client, calls = _client(monkeypatch, [p1, p2])
    rows = client.get_hour_candles("KRW-BTC", "2026-11-10", "2026-11-20")
    assert len(rows) == 11 * 24 and rows[-1]["candle_date_time_kst"] == "2026-11-10T00:00:00"
    assert [c[1]["to"] for c in calls] == ["2026-11-21T00:00:00+09:00", "2026-11-12T16:00:00+09:00"]


def test_hour_candles_stops_at_start_midnight(monkeypatch):
    client, calls = _client(monkeypatch, [_hours("2026-11-20", 7, 200), []])     # 11-20 07:00 → 11-12 00:00
    rows = client.get_hour_candles("KRW-BTC", "2026-11-12", "2026-11-20")
    assert len(rows) == 200 and len(calls) == 1


def test_hour_candles_drops_after_end(monkeypatch):
    client, _ = _client(monkeypatch, [_hours("2026-11-21", 6, 32)])              # 11-21 06:00 → 11-19 23:00
    rows = client.get_hour_candles("KRW-BTC", "2026-11-19", "2026-11-20")
    assert len(rows) == 24 + 1 and rows[0]["candle_date_time_kst"] == "2026-11-20T23:00:00"


def test_hour_candles_stops_on_short_page(monkeypatch):
    client, calls = _client(monkeypatch, [_hours("2026-11-20", 23, 30)])         # 상장 초기: 200 미만
    rows = client.get_hour_candles("KRW-BTC", "2026-11-01", "2026-11-20")
    assert len(rows) == 30 and len(calls) == 1
