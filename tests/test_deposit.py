import pandas as pd
import pytest

from asset_flow.clients.kis_client import KISApiClient, KisApiError
from asset_flow.config.schemas import BALANCE_COLUMNS
from asset_flow.pipeline import dispatch as dp
from asset_flow.transformers import kis_transformer as kt
from asset_flow.transformers.upbit_transformer import transform_upbit_deposit

D = "2026-10-13"
CONF = {"account": "00000000", "product_code": "01", "type": "위탁", "appkey": "k" * 8, "secret": "s" * 8}


def test_krw_deposit_row():
    df = kt.transform_deposit({"output2": [{"prvs_rcdl_excc_amt": "3,032", "dnca_tot_amt": "9999"}]}, D, CONF)
    assert list(df.columns) == BALANCE_COLUMNS and len(df) == 1
    r = df.iloc[0]
    assert (r["product_code"], r["asset_type"], r["currency_code"], r["exchange_code"]) == ("DEPOSIT", "CASH", "KRW", "KRX")
    assert r["holding_quantity"] == 1 and r["total_purchase_amount"] == r["total_evaluation_amount"] == 3032.0


def test_zero_deposit_writes_no_row():
    assert kt.transform_deposit({"output2": [{"prvs_rcdl_excc_amt": "0"}]}, D, CONF).empty
    assert kt.transform_foreign_deposit({"output2": [{"crcy_cd": "USD", "frcr_dncl_amt_2": "0.00"}]}, D, CONF).empty
    assert kt.transform_foreign_deposit({"output2": []}, D, CONF).empty
    assert transform_upbit_deposit([{"currency": "KRW", "balance": "0", "locked": "0"}], D, "UPBIT", "가상자산").empty


def test_missing_deposit_field_raises():
    with pytest.raises(ValueError, match="예수금 필드 없음"):
        kt.transform_deposit({"output1": []}, D, CONF)


def test_foreign_deposit_per_currency():
    df = kt.transform_foreign_deposit({"output2": [{"crcy_cd": "USD", "frcr_dncl_amt_2": "1.65"},
                                                   {"crcy_cd": "JPY", "frcr_dncl_amt_2": "0"}]}, D, CONF)
    assert list(df["product_code"]) == ["DEPOSIT_USD"] and df.iloc[0]["currency_code"] == "USD"
    assert df.iloc[0]["exchange_code"] is None or pd.isna(df.iloc[0]["exchange_code"])


def test_upbit_deposit_includes_locked():
    df = transform_upbit_deposit([{"currency": "KRW", "balance": "1000", "locked": "500"},
                                  {"currency": "BTC", "balance": "0.1"}], D, "UPBIT", "가상자산")
    assert (df.iloc[0]["total_evaluation_amount"], df.iloc[0]["exchange_code"]) == (1500.0, "UPBIT")


def test_account_total_gap():
    rows = pd.concat([kt.cash_row(D, {"account_code": "A", "account_name": "a"}, "DEPOSIT", "예수금", "KRW", "KRX", 1000),
                      kt.cash_row(D, {"account_code": "A", "account_name": "a"}, "DEPOSIT_USD", "외화", "USD", None, 1)])
    assert kt.account_total_gap(rows, {"output2": {"tot_asst_amt": "2400"}}, 1400.0) is None          # 1000 + 1400
    assert "계좌 총자산" in kt.account_total_gap(rows, {"output2": [{"tot_asst_amt": "3000"}]}, 1400.0)
    assert kt.account_total_gap(rows, {"output2": {"tot_asst_amt": "3000"}}, None) is None           # USD 환율 없음


class _Seq(KISApiClient):
    def __init__(self, responses):
        """
        Purpose: 응답 순서를 돌려주는 KIS 클라이언트 대역
        Parameter:
            - responses: _get_account 가 차례로 돌려줄 dict
        Input: 없음
        Output: 없음
        """
        super().__init__("tok", CONF)
        self.responses = list(responses)

    def _get_account(self, name):
        """
        Purpose: 계좌 조회 흉내
        Parameter:
            - name: API 키
        Input: 없음
        Output: dict
        """
        return self.responses.pop(0)


def test_balance_retry_then_ok(monkeypatch):
    monkeypatch.setattr("asset_flow.clients.kis_client.time.sleep", lambda s: None)
    ok = {"rt_cd": "0", "output1": []}
    assert _Seq([{"rt_cd": "7", "msg_cd": "X"}, ok]).get_domestic_balance() == ok
    with pytest.raises(KisApiError, match="TTTS3012R"):
        _Seq([{"rt_cd": "7"}, {"rt_cd": "7"}]).get_overseas_balance()


@pytest.fixture
def cash_only(monkeypatch):
    """
    Purpose: 종목 없이 예수금만 돌려주는 ISA 핸들러 + 적재 캡처
    Parameter:
        - monkeypatch: pytest monkeypatch
    Input: 없음
    Output: 적재 호출 리스트
    """
    writes = []
    deposit = kt.cash_row(D, {"account_code": "X", "account_name": "x"}, "DEPOSIT", "예수금", "KRW", "KRX", 5000)
    monkeypatch.setitem(dp.HANDLERS, "kis_domestic_stock", lambda acct, dates, deps: deposit)
    monkeypatch.setattr(dp.db, "ensure_writable", lambda e: None)
    monkeypatch.setattr(dp.db, "delete_and_insert_account_assets", lambda *a: writes.append(a) or 1)
    return writes


def _deps(cfg):
    """
    Purpose: collect_account 용 최소 Deps
    Parameter:
        - cfg: 템플릿 secrets
    Input: 없음
    Output: Deps (engine 없음)
    """
    from types import SimpleNamespace

    app = SimpleNamespace(tables=SimpleNamespace(asset_daily=SimpleNamespace(db_schema="account", table_name="asset_daily")))
    return dp.Deps(cfg, app, engine=None, tokens=None)


def test_cash_only_after_holdings_fails_before_write(cfg, cash_only, monkeypatch):
    monkeypatch.setattr(dp.db, "had_holdings_before", lambda *a: True)
    with pytest.raises(dp.EmptyResultError, match="예수금만"):
        dp.collect_account("KIS_ISA", {"standard_date": D}, _deps(cfg))
    assert cash_only == []
    assert dp.collect_account("KIS_ISA", {"standard_date": D}, _deps(cfg), allow_cash_only=True) == 1


def test_cash_only_account_without_prior_holdings_ok(cfg, cash_only, monkeypatch):
    monkeypatch.setattr(dp.db, "had_holdings_before", lambda *a: False)
    assert dp.collect_account("KIS_ISA", {"standard_date": D}, _deps(cfg)) == 1
