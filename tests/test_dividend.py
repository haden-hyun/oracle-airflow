import datetime as dt
import json
from types import SimpleNamespace

import pytest

from asset_flow.config.schemas import DIVIDEND_EVENT_COLUMNS, DIVIDEND_RECEIPT_COLUMNS
from asset_flow.transformers import dividend_transformer as t

D = dt.date
HOL = {D(2026, 10, 9)}                                               # 한글날 (금)

KSD = [  # HHKDB669102C0 output1 (2026-10-13 실측 발췌)
    {"record_date": "20260915", "sht_cd": "476800", "isin_name": "삼성KODEX 한국부동산리츠인프라부동산상장",
     "divi_kind": "", "face_val": "0", "per_sto_divi_amt": "60", "divi_rate": "1.18", "stk_divi_rate": "0.00",
     "divi_pay_dt": "2026/09/17", "stk_div_pay_dt": "", "odd_pay_dt": "", "stk_kind": "보통", "high_divi_gb": ""},
    {"record_date": "20260930", "sht_cd": "005930", "isin_name": "삼성전자", "divi_kind": "분기", "face_val": "100",
     "per_sto_divi_amt": "0", "divi_rate": "0.00", "stk_divi_rate": "0.00", "divi_pay_dt": ""},
    {"record_date": "20261012", "sht_cd": "999999", "isin_name": "가상 주식배당", "divi_kind": "결산",
     "per_sto_divi_amt": "0", "divi_rate": "0", "stk_divi_rate": "5.00", "divi_pay_dt": ""},
]
RIGHTS = [  # CTRGA011R output (계좌번호 마스킹)
    {"acno10": "********01", "rght_type_cd": "32", "bass_dt": "20260915", "shtn_pdno": "476800",
     "prdt_name": "KODEX 한국부동산리츠인프라", "cblc_qty": "675", "last_alct_amt": "40500", "tax_amt": "6230",
     "cash_dfrm_dt": "20260917"},
    {"acno10": "********01", "rght_type_cd": "04", "bass_dt": "20241025", "shtn_pdno": "035720",
     "prdt_name": "카카오", "cblc_qty": "10", "last_alct_amt": "0", "tax_amt": "0", "cash_dfrm_dt": ""},
]
OPR = [  # CTRGT011R output
    {"rght_type_cd": "03", "pdno": "ORCL", "prdt_name": "ORACLE CORP", "acpl_bass_dt": "20260710",
     "crcy_cd": "USD", "alct_frcr_unpr": "0.50000", "dfnt_yn": "Y"},
    {"rght_type_cd": "03", "pdno": "NVDA", "prdt_name": "NVIDIA CORP", "acpl_bass_dt": "20260206",
     "crcy_cd": "USD", "alct_frcr_unpr": "11.00000", "dfnt_yn": "Y"},             # 권리종합에 없는 행 (실측)
]
ICE = [  # HHDFS78330900 output1 (+SYMB)
    {"SYMB": "ORCL", "anno_dt": "20260610", "ca_title": "현금배당", "div_lock_dt": "20260710", "record_dt": "20260710",
     "pay_dt": "20260724"},
    {"SYMB": "ORCL", "anno_dt": "20260910", "ca_title": "현금배당", "div_lock_dt": "20261009", "record_dt": "20261009",
     "pay_dt": "20261023"},                                                         # 금액 아직 없음
    {"SYMB": "ORCL", "anno_dt": "20260330", "ca_title": "공개매수", "div_lock_dt": "", "record_dt": "", "pay_dt": ""},
]


def test_parse_helpers():
    assert t.parse_date("2026/09/17") == D(2026, 9, 17) and t.parse_date("20260915") == D(2026, 9, 15)
    assert t.parse_date("") is None and t.parse_date("2026/13/40") is None
    assert t.parse_num("1,000") == 1000.0 and t.parse_num("") is None


def test_prev_business_day_skips_weekend_and_holiday():
    assert t.prev_business_day(D(2026, 10, 12), HOL) == D(2026, 10, 8)    # 월 → (일·토·금 휴장) → 목
    assert t.prev_business_day(D(2026, 9, 15), set()) == D(2026, 9, 14)


def test_kr_ex_date_when_record_date_is_holiday():
    # 2024-12-31(화) 연말휴장 기준일 → 마지막 영업일 12/30 → 락일 12/27 (T+2 규칙)
    assert t.kr_ex_date(D(2024, 12, 31), {D(2024, 12, 31), D(2024, 12, 25)}) == D(2024, 12, 27)
    assert t.kr_ex_date(D(2026, 9, 30), set()) == D(2026, 9, 29)


def test_ksd_events_cash_unconfirmed_and_stock_dividend():
    recs = {r["product_code"]: r for r in t.transform_ksd_events(KSD, HOL)}
    assert set(recs) == {"476800", "005930"}                              # 주식배당만 있는 행 제외
    r = recs["476800"]
    assert list(r) == DIVIDEND_EVENT_COLUMNS
    assert (r["ex_date"], r["pay_date"], r["dps_amount"], r["is_confirmed"]) == (D(2026, 9, 14), D(2026, 9, 17), 60.0, True)
    assert recs["005930"]["dps_amount"] is None and recs["005930"]["is_confirmed"] is False
    assert recs["005930"]["dividend_label"] == "분기"


def test_ksd_events_dedupe_same_pk():
    assert len(t.transform_ksd_events([KSD[0], dict(KSD[0], per_sto_divi_amt="61")], HOL)) == 1


def test_receipts_drop_noncash_and_account_number():
    recs = t.transform_receipts(RIGHTS, "ACCT-CODE", set())
    assert len(recs) == 1 and list(recs[0]) == DIVIDEND_RECEIPT_COLUMNS
    r = recs[0]
    assert (r["account_code"], r["gross_amount"], r["tax_amount"], r["holding_quantity"]) == ("ACCT-CODE", 40500.0, 6230.0, 675.0)
    assert r["ex_date"] == D(2026, 9, 14) and r["is_estimated"] is False
    assert "acno10" not in json.dumps(recs, default=str) and "********" not in json.dumps(recs, default=str)
    # account_code(= cano-상품코드)는 FK 키라 저장됨 — Discord 노출은 alerts.redact 가 막음 (test_masking)


def test_receipts_zero_rows_is_empty_not_error():
    assert t.transform_receipts([], "X", set()) == []


def test_us_events_outer_join():
    recs = {(r["product_code"], r["record_date"]): r for r in t.transform_us_events(OPR, ICE)}
    assert len(recs) == 3                                                  # 공개매수 제외
    paid = recs[("ORCL", D(2026, 7, 10))]
    assert (paid["dps_amount"], paid["is_confirmed"], paid["pay_date"]) == (0.5, True, D(2026, 7, 24))
    assert paid["source"] == "KIS_ICE+KIS_OVRS_RIGHTS" and paid["announce_date"] == D(2026, 6, 10)
    pending = recs[("ORCL", D(2026, 10, 9))]
    assert pending["dps_amount"] is None and pending["is_confirmed"] is False and pending["ex_date"] == D(2026, 10, 9)
    orphan = recs[("NVDA", D(2026, 2, 6))]
    assert orphan["pay_date"] is None and orphan["ex_date"] == D(2026, 2, 6)   # T+1 이후 → 락일 = 기준일


def test_estimate_us_receipts():
    events = [
        {"product_code": "ORCL", "record_date": D(2026, 7, 10), "dividend_kind": "CASH", "product_name": "ORACLE",
         "ex_date": D(2026, 7, 10), "pay_date": D(2026, 7, 24), "dps_amount": 0.5, "currency_code": "USD"},
        {"product_code": "ORCL", "record_date": D(2026, 10, 9), "dividend_kind": "CASH", "product_name": "ORACLE",
         "ex_date": D(2026, 10, 9), "pay_date": D(2026, 10, 23), "dps_amount": None, "currency_code": "USD"},
        {"product_code": "AAPL", "record_date": D(2026, 8, 10), "dividend_kind": "CASH", "product_name": "APPLE",
         "ex_date": D(2026, 8, 10), "pay_date": D(2026, 8, 13), "dps_amount": 0.27, "currency_code": "USD"},
    ]
    holdings = {("ACCT", "ORCL"): [(D(2026, 7, 9), 2.0), (D(2026, 10, 8), 2.0)],
                ("ACCT", "AAPL"): [(D(2026, 7, 1), 1.0)]}                    # 락일 7일 전보다 오래됨 → 생략
    rates = [(D(2026, 7, 23), 1380.0), (D(2026, 7, 24), 1390.0), (D(2026, 10, 12), 1400.0)]
    recs = {r["record_date"]: r for r in t.estimate_us_receipts(events, holdings, rates, 0.15)}
    assert set(recs) == {D(2026, 7, 10), D(2026, 10, 9)}
    july = recs[D(2026, 7, 10)]
    assert (july["gross_amount"], july["tax_amount"], july["fx_rate"], july["right_type_code"]) == (1.0, 0.15, 1390.0, "EST03")
    octo = recs[D(2026, 10, 9)]
    assert octo["gross_amount"] is None and octo["fx_rate"] is None and octo["is_estimated"] is True


def test_dag_structure():
    from test_finalize import _load_dag

    dag = _load_dag("account_ingest_dividend")
    t = dag.task_dict
    assert dag.schedule == "20 7 * * *" or str(dag.timetable.summary) == "20 7 * * *"
    assert t["estimate_us_receipts"].upstream_task_ids == {"get_windows", "ingest_us_events", "wait_for_asset_daily"}
    assert t["fail_if_upstream_failed"].upstream_task_ids == {"ingest_kr_events", "ingest_receipts", "estimate_us_receipts"}
    assert t["refresh_dashboard_views"].trigger_rule == "all_done"
    assert "wait_for_token" in t["list_targets"].upstream_task_ids
    assert "wait_for_token" in t["list_receipt_accounts"].upstream_task_ids
    assert set(dag.params) == {"start_date", "end_date"}


def test_run_windows_regular_and_backfill(app):
    from asset_flow.pipeline.dividend import run_windows

    deps = SimpleNamespace(app=app)
    w = run_windows("2026-10-14", {}, deps)
    assert w["standard_date"] == "2026-10-13" and w["receipt"] == ["2026-07-06", "2026-10-14"]
    assert w["kr_event"] == ["2025-09-09", "2026-12-13"] and w["estimate"][1] == "2026-10-13"
    b = run_windows("2026-10-14", {"start_date": "2015-01-01", "end_date": None}, deps)
    assert b["receipt"][0] == "2015-01-01" and b["us_ice"][0] == "2014-11-02" and b["held_since"] == "2015-01-01"
    with pytest.raises(ValueError):
        run_windows("2026-10-14", {"start_date": "1999-12-31"}, deps)
