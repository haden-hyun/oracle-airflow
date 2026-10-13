"""
KIS 배당 원시 응답 → dividend_event / dividend_receipt 레코드 (순수 함수, DB·API 접근 없음)

- transform_ksd_events: HHKDB669102C0 → 국내 일정 (ex_date = kr_ex_date 계산, API 에 락일 없음)
- transform_us_events: CTRGT011R(금액) ⟗ HHDFS78330900(일정) → 미국 일정 (금액 미정 = dps NULL)
- transform_receipts: CTRGA011R → 국내 계좌 수령 원장 (acno10·ctx 는 버리고 secrets account_code 만)
- estimate_us_receipts: 미국 일정 × 락일 직전 asset_daily 수량 → 추정 원장 (세전·세금·환율 각각)
- 레코드 = dict (컬럼 = config.schemas DIVIDEND_*_COLUMNS), 날짜는 datetime.date
"""

from __future__ import annotations

import bisect
import datetime as dt

from asset_flow.config.kis import KIS
from asset_flow.config.schemas import DIVIDEND_EVENT_COLUMNS, DIVIDEND_EVENT_PK, DIVIDEND_RECEIPT_COLUMNS

US_T1_SINCE = dt.date(2024, 5, 28)          # 미국 T+1 결제 시행일 — 이후는 락일 = 기준일
HOLDING_LOOKBACK_DAYS = 7                   # 락일 직전 보유 행을 찾을 최대 거리 (주말·적재 누락)


def parse_date(value: str | None) -> dt.date | None:
    """
    Purpose: KIS 날짜 문자열 → date
    Parameter:
        - value: 'YYYYMMDD' / 'YYYY/MM/DD' / '' / None
    Input: 없음
    Output: date 또는 None (빈 값·형식 오류)
    """
    s = (value or "").strip().replace("/", "").replace("-", "")
    try:
        return dt.datetime.strptime(s, "%Y%m%d").date() if len(s) == 8 else None
    except ValueError:
        return None


def parse_num(value: str | None) -> float | None:
    """
    Purpose: KIS 숫자 문자열 → float
    Parameter:
        - value: '60', '0.27000', '1,000', '' / None
    Input: 없음
    Output: float 또는 None (빈 값·형식 오류)
    """
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def prev_business_day(day: dt.date, holidays: set[dt.date]) -> dt.date:
    """
    Purpose: 직전 영업일 (국내 배당락일 = 기준일 직전 영업일)
    Parameter:
        - day: 기준일
        - holidays: 평일 휴장일 집합 (주말은 따로 판정)
    Input: 없음
    Output: day 보다 앞선 첫 평일·비휴장일
    """
    d = day - dt.timedelta(days=1)
    while d.weekday() >= 5 or d in holidays:
        d -= dt.timedelta(days=1)
    return d


def kr_ex_date(record: dt.date, holidays: set[dt.date]) -> dt.date:
    """
    Purpose: 국내 배당락일 (T+2 결제: 기준일 이하 마지막 영업일의 직전 영업일)
    Parameter:
        - record: 배당 기준일
        - holidays: 평일 휴장일 집합
    Input: 없음
    Output: 락일. 기준일이 영업일이면 직전 영업일, 휴장일(예: 12/31 연말휴장)이면 그 앞 영업일의 직전 영업일
    """
    last = record
    while last.weekday() >= 5 or last in holidays:
        last -= dt.timedelta(days=1)
    return prev_business_day(last, holidays)


def _dedupe(records: list[dict], pk: list[str]) -> list[dict]:
    """
    Purpose: 같은 PK 가 한 응답에 두 번 오면 마지막 행만 남김 (ON CONFLICT 한 문장 내 중복 오류 방지)
    Parameter:
        - records: 레코드 리스트
        - pk: 키 컬럼
    Input: 없음
    Output: PK 유일 레코드 리스트 (입력 순서 유지)
    """
    return list({tuple(r[k] for k in pk): r for r in records}.values())


def transform_ksd_events(rows: list[dict], holidays: set[dt.date]) -> list[dict]:
    """
    Purpose: HHKDB669102C0 output1 → market.dividend_event (KR)
    Parameter:
        - rows: get_ksd_dividend 결과 (여러 종목 합친 것 가능)
        - holidays: 국내 평일 휴장일 집합 (ex_date 계산)
    Input: 없음
    Output: DIVIDEND_EVENT_COLUMNS 레코드. 주식배당만 있는 행(현금 0·stk_divi_rate>0)은 제외,
            현금 0원·지급일 없음 = 미확정 → dps None·is_confirmed False
    """
    out = []
    for r in rows:
        record = parse_date(r.get("record_date"))
        dps, stock_rate = parse_num(r.get("per_sto_divi_amt")) or 0.0, parse_num(r.get("stk_divi_rate")) or 0.0
        if record is None or (dps == 0 and stock_rate > 0):
            continue
        out.append({
            "product_code": (r.get("sht_cd") or "").strip(),
            "record_date": record,
            "dividend_type": "CASH",
            "market": "KR",
            "product_name": (r.get("isin_name") or "").strip() or None,      # 20자 잘림 — 표시는 asset_daily 이름
            "dividend_label": (r.get("divi_kind") or "").strip() or None,
            "ex_date": kr_ex_date(record, holidays),
            "pay_date": parse_date(r.get("divi_pay_dt")),
            "per_share_dividend_amount": dps if dps > 0 else None,
            "currency_code": "KRW",
            "dividend_rate": parse_num(r.get("divi_rate")),
            "is_confirmed": dps > 0,
            "announce_date": None,
            "source": "KIS_KSDINFO",
        })
    return _dedupe(out, DIVIDEND_EVENT_PK)


def transform_us_events(rights: list[dict], ice: list[dict]) -> list[dict]:
    """
    Purpose: CTRGT011R(금액) ⟗ HHDFS78330900(일정) → market.dividend_event (US)
    Parameter:
        - rights: get_overseas_period_rights 결과 (03·75 합친 것)
        - ice: get_overseas_rights_ice 결과 (행마다 SYMB)
    Input: 없음
    Output: DIVIDEND_EVENT_COLUMNS 레코드. 조인 키 (티커, 현지기준일, 종류).
            일정만 있음 = 금액 미정(dps None, is_confirmed False) / 금액만 있음 = 락·지급일 없음(T+1 이후면 락일=기준일)
    """
    merged: dict[tuple, dict] = {}
    for r in ice:
        kind, record = KIS.ICE_DIVIDEND_TITLES.get((r.get("ca_title") or "").strip()), parse_date(r.get("record_dt"))
        if kind is None or record is None:                 # 공개매수·주식분할·상장폐지 등
            continue
        merged[(r["SYMB"], record, kind)] = {
            "label": r["ca_title"].strip(), "ex": parse_date(r.get("div_lock_dt")),
            "pay": parse_date(r.get("pay_dt")), "anno": parse_date(r.get("anno_dt")), "src": {"KIS_ICE"},
        }
    for r in rights:
        kind, record = KIS.US_DIVIDEND_RIGHTS.get(r.get("rght_type_cd")), parse_date(r.get("acpl_bass_dt"))
        if kind is None or record is None:
            continue
        m = merged.setdefault((r["pdno"].strip(), record, kind), {"label": None, "ex": None, "pay": None,
                                                                 "anno": None, "src": set()})
        m |= {"dps": parse_num(r.get("alct_frcr_unpr")), "dfnt": r.get("dfnt_yn") == "Y",
              "cur": (r.get("crcy_cd") or "USD").strip(), "name": (r.get("prdt_name") or "").strip() or None}
        m["src"].add("KIS_OVRS_RIGHTS")
    out = []
    for (symbol, record, kind), m in merged.items():
        dps = m.get("dps") or None                         # 0·빈 값 = 미정
        ex = m["ex"] or (record if record >= US_T1_SINCE else None)
        out.append({
            "product_code": symbol, "record_date": record, "dividend_type": kind, "market": "US",
            "product_name": m.get("name"), "dividend_label": m["label"], "ex_date": ex, "pay_date": m["pay"],
            "per_share_dividend_amount": dps, "currency_code": m.get("cur", "USD"), "dividend_rate": None,
            "is_confirmed": bool(m.get("dfnt")) and dps is not None, "announce_date": m["anno"],
            "source": "+".join(sorted(m["src"])),
        })
    return [{c: r[c] for c in DIVIDEND_EVENT_COLUMNS} for r in out]


def transform_receipts(rows: list[dict], account_code: str, holidays: set[dt.date]) -> list[dict]:
    """
    Purpose: CTRGA011R output → account.dividend_receipt (국내 실수령, 세전·세금 원천값)
    Parameter:
        - rows: get_period_rights 결과
        - account_code: secrets Account.account_code (account_master 키)
        - holidays: 국내 평일 휴장일 집합 (ex_date 계산)
    Input: 없음
    Output: DIVIDEND_RECEIPT_COLUMNS 레코드 — 현금 권리(32·03)·금액>0 만. acno10 등 계좌번호 필드는 버림
    """
    out = []
    for r in rows:
        gross, record = parse_num(r.get("last_alct_amt")) or 0.0, parse_date(r.get("bass_dt"))
        if r.get("rght_type_cd") not in KIS.DIVIDEND_RIGHT_TYPES or gross <= 0 or record is None:
            continue
        out.append({
            "account_code": account_code,
            "product_code": (r.get("shtn_pdno") or "").strip(),
            "record_date": record,
            "right_type_code": r["rght_type_cd"],
            "product_name": (r.get("prdt_name") or "").strip() or None,
            "ex_date": kr_ex_date(record, holidays),
            "pay_date": parse_date(r.get("cash_dfrm_dt")),
            "holding_quantity": parse_num(r.get("cblc_qty")) or 0.0,     # 결제 기준 권리잔고
            "gross_amount": gross,                                       # 세전 = 권리잔고 × 주당
            "tax_amount": parse_num(r.get("tax_amt")) or 0.0,            # 원천값 그대로 (ISA = 미징수 참고값)
            "currency_code": "KRW",
            "exchange_rate": None,
            "is_estimated": False,
            "source": "KIS_PERIOD_RIGHTS",
        })
    return _dedupe(out, ["account_code", "product_code", "record_date", "right_type_code"])


def _last_before(series: list[tuple[dt.date, float]], day: dt.date, inclusive: bool) -> tuple[dt.date, float] | None:
    """
    Purpose: 날짜 오름차순 시계열에서 day 이전(또는 이하) 마지막 점
    Parameter:
        - series: [(date, value)] 오름차순
        - day: 기준일
        - inclusive: True 면 day 포함
    Input: 없음
    Output: (date, value) 또는 None
    """
    dates = [d for d, _ in series]
    i = bisect.bisect_right(dates, day) if inclusive else bisect.bisect_left(dates, day)
    return series[i - 1] if i else None


def estimate_us_receipts(events: list[dict], holdings: dict[tuple[str, str], list[tuple[dt.date, float]]],
                         usd_rates: list[tuple[dt.date, float]], withholding_rate: float) -> list[dict]:
    """
    Purpose: 미국 일정 × 락일 직전 보유수량 → 추정 수령 원장 (미국은 계좌 원장 API 없음)
    Parameter:
        - events: market.dividend_event US 행 (ex_date·pay_date 있는 것)
        - holdings: {(account_code, product_code): [(standard_date, 수량)] 오름차순} — asset_daily
        - usd_rates: [(standard_date, USD base_rate)] 오름차순 — exchange_rate_daily
        - withholding_rate: 원천징수율 (config)
    Input: 없음
    Output: DIVIDEND_RECEIPT_COLUMNS 레코드 (right_type_code EST03 현금 / EST75 특별, is_estimated True).
            락일 직전 7일 안에 보유 행이 없거나 수량 0 이면 생략. 금액 미정이면 gross·tax None.
            지급일이 최신 환율일보다 뒤면 exchange_rate None (다음 run 이 채움)
    """
    out, fx_max = [], usd_rates[-1][0] if usd_rates else None
    for e in events:
        if e["ex_date"] is None or e["pay_date"] is None:
            continue
        for (account_code, code), series in holdings.items():
            if code != e["product_code"]:
                continue
            last = _last_before(series, e["ex_date"], inclusive=False)
            if last is None or (e["ex_date"] - last[0]).days > HOLDING_LOOKBACK_DAYS or last[1] <= 0:
                continue
            gross = round(last[1] * e["per_share_dividend_amount"], 6) if e["per_share_dividend_amount"] is not None else None
            rate = _last_before(usd_rates, e["pay_date"], inclusive=True) if fx_max and e["pay_date"] <= fx_max else None
            out.append({
                "account_code": account_code, "product_code": code, "record_date": e["record_date"],
                "right_type_code": "EST75" if e["dividend_type"] == "SPECIAL" else "EST03",
                "product_name": e.get("product_name"), "ex_date": e["ex_date"], "pay_date": e["pay_date"],
                "holding_quantity": last[1], "gross_amount": gross,
                "tax_amount": round(gross * withholding_rate, 6) if gross is not None else None,
                "currency_code": e["currency_code"], "exchange_rate": rate[1] if rate else None,
                "is_estimated": True, "source": "EST_US_DPS_X_QTY",
            })
    return [{c: r[c] for c in DIVIDEND_RECEIPT_COLUMNS} for r in out]
