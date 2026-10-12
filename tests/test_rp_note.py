"""위탁계좌 RP/발행어음 변환 — 행 순서 방어(행 수·합계)와 원금·이자 보존."""

import pytest

from asset_flow.transformers.kis_transformer import transform_rp_note_balance

CONF = {"account": "00000000", "product_code": "01", "type": "위탁"}   # 더미 계좌
D = "2026-10-12"


def balance(rp=("500000", "500858", "858"), n_rows=20, total=None):
    """
    Purpose: 위탁계좌 투자계좌자산현황(CTRP6548R) 응답 픽스처
    Parameter:
        - rp: RP/발행어음 행 (매입, 평가, 손익)
        - n_rows: output1 행 수
        - total: 합계 행 평가금액 (None 이면 각 행 합)
    Input: 없음
    Output: 원시 응답 dict
    """
    rows = [{"pchs_amt": "0", "evlu_amt": "0", "evlu_pfls_amt": "0"} for _ in range(n_rows)]
    rows[0] = {"pchs_amt": "2714915", "evlu_amt": "2547270", "evlu_pfls_amt": "0"}      # 주식
    rows[7] = {"pchs_amt": rp[0], "evlu_amt": rp[1], "evlu_pfls_amt": rp[2]}           # RP/발행어음
    rows[-1]["evlu_amt"] = str(total if total is not None else sum(float(r["evlu_amt"]) for r in rows[:-1]))
    return {"rt_cd": "0", "output1": rows}


def test_rp_row_keeps_principal_and_interest():
    row = transform_rp_note_balance(balance(), D, CONF).iloc[0]
    assert (row.product_code, row.product_name, row.asset_type) == ("RP_NOTE", "RP/발행어음", "CASH")
    assert (row.total_purchase_amount, row.total_evaluation_amount, row.total_profit_amount) == (500000, 500858, 858)
    assert row.holding_quantity == 1 and row.valuation_profit_rate == pytest.approx(0.17)
    assert row.account_code == "00000000-01" and row.account_name == "위탁"


def test_zero_rp_gives_empty_frame():
    assert transform_rp_note_balance(balance(rp=("0", "0", "0")), D, CONF).empty


def test_row_count_change_fails():
    with pytest.raises(ValueError, match="행 수 17"):
        transform_rp_note_balance(balance(n_rows=17), D, CONF)


def test_total_mismatch_fails():
    with pytest.raises(ValueError, match="합계"):
        transform_rp_note_balance(balance(total=1), D, CONF)
