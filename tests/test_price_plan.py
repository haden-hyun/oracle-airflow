from datetime import date

import pytest

from asset_flow.pipeline.price_plan import (Window, plan_product, plan_window, recent_start, stale_products,
                                            validate_coverage)

HOL = {"KR": {date(2026, 10, 5), date(2026, 10, 9)}, "US": set()}
BASE = date(2026, 10, 13)


def _p(**kw) -> dict:
    """
    Purpose: market.product 행 대역
    Parameter:
        - kw: 덮어쓸 열
    Input: 없음
    Output: dict
    """
    return {"product_code": "476800", "market": "KR", "price_source": "KIS_KR_DAILY", "price_from_date": None,
            "first_held_date": date(2026, 2, 9), "last_held_date": date(2026, 10, 12), "is_tracked": True,
            "first_loaded": None, "last_loaded": None, **kw}


def test_new_product_gets_one_year():
    w = plan_window(_p(), None, BASE, HOL, 365, 5)
    assert (w.start, w.end, w.is_new) == ("2025-10-13", "2026-10-12", True)


def test_daily_refetches_last_five_trading_days_skipping_kr_holidays():
    assert recent_start(date(2026, 10, 12), "KR", HOL, 5) == date(2026, 10, 2)
    w = plan_window(_p(), date(2026, 10, 12), BASE, HOL, 365, 5)
    assert (w.start, w.is_new) == ("2026-10-02", False)


def test_crypto_recent_start_is_calendar_days():
    assert recent_start(date(2026, 10, 12), "CRYPTO", HOL, 5) == date(2026, 10, 8)


def test_rebuy_gap_is_capped_at_one_year():
    assert plan_window(_p(), date(2024, 1, 2), BASE, HOL, 365, 5).start == "2025-10-13"


def test_listing_within_year_starts_at_price_from_date():
    w = plan_window(_p(product_code="0228G0", price_from_date=date(2026, 8, 11)), None, BASE, HOL, 365, 5)
    assert w.start == "2026-08-11"


def test_zero_row_marker_skips_until_new_dates():
    # 신규 0행 → price_from_date = end+1 기록 → 다음 날은 새 날짜만
    assert plan_window(_p(price_from_date=date(2026, 10, 13)), None, BASE, HOL, 365, 5) is None
    w = plan_window(_p(price_from_date=date(2026, 10, 13)), None, date(2026, 10, 14), HOL, 365, 5)
    assert (w.start, w.end) == ("2026-10-13", "2026-10-13")


def test_fund_head_gap_requests_full_window():
    fund = _p(product_code="K553W5E17401", market="FUND", price_source="FUND_NAV",
              first_loaded=date(2026, 1, 28), last_loaded=date(2026, 10, 12))
    w = plan_product(fund, BASE, HOL, 365, 5, {})
    assert (w.start, w.is_new) == ("2025-10-13", True)
    filled = dict(fund, first_loaded=date(2025, 10, 13))
    assert plan_product(filled, BASE, HOL, 365, 5, {}).start == "2026-10-02"     # 머리 채워진 뒤 = 증분


def test_manual_params_override_window():
    p = _p(last_loaded=date(2026, 10, 12))
    w = plan_product(p, BASE, HOL, 365, 5, {"start_date": "2026-03-01", "end_date": "2026-03-31"})
    assert (w.start, w.end, w.is_new) == ("2026-03-01", "2026-03-31", False)
    full = plan_product(p, BASE, HOL, 365, 5, {"full_refresh": True})
    assert (full.start, full.is_new) == ("2025-10-13", True)
    assert plan_product(p, BASE, HOL, 365, 5, {"start_date": "2026-11-01"}) is None


def test_zero_rows_while_held_is_error():
    w = Window("ORCL", "2025-10-13", "2026-10-12", True)
    with pytest.raises(ValueError, match="시세 0행"):
        validate_coverage(_p(product_code="ORCL", first_held_date=date(2026, 8, 13)), w, [])


def test_zero_rows_before_listing_is_ok():
    w = Window("0228G0", "2025-10-13", "2025-12-31", True)
    validate_coverage(_p(product_code="0228G0", first_held_date=date(2026, 8, 13)), w, [])


def test_stale_products():
    end = date(2026, 10, 12)
    targets = [
        _p(last_loaded=date(2026, 10, 12)),                                       # 최신
        _p(product_code="161510", last_loaded=date(2026, 10, 2)),                # KR 3거래일 초과 지연
        _p(product_code="BTC", market="CRYPTO", price_source="UPBIT_DAY", last_loaded=date(2026, 10, 11)),
        _p(product_code="NEW", price_from_date=date(2026, 10, 13)),              # 0행 기록 → 제외
        _p(product_code="K553W5E17401", market="FUND", price_source="FUND_NAV"),  # 펀드 제외
        _p(product_code="OLD", is_tracked=False),                                 # 추적 해제
    ]
    assert stale_products(targets, end, HOL) == ["161510(2026-10-02)"]
