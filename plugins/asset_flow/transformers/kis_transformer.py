"""
KIS API 원시 응답 → 도메인 DataFrame 변환 (순수 함수)

- transform_domestic_balance / transform_overseas_balance: 주식 잔고
- transform_pension_fund_balance: 연금저축 펀드 (평가금액 → 좌수 역산, 미보유 시 빈 DF)
- transform_cma_cash_balance: CMA 현금 (CTRP6548R output1[14])
- transform_rp_note_balance: 위탁 RP/발행어음 (CTRP6548R "RP/발행어음" 행, 행 수·합계 검증)
- transform_deposit / transform_foreign_deposit: 예수금 (원화 D+2 정산 · 통화별 외화, 0원이면 행 없음)
- account_total_gap: 적재 행 합계 ↔ CTRP6548R 계좌 총자산 대조 (경고 문구만)
- transform_exchange_rate: 환율 + GBP·EUR 원화 교차 환산 (매일 run, output1 현재값)
- transform_exchange_rate_history: 환율 이력(output2) → 달력일 이월 + 날짜별 교차 환산 (백필)
- to_index_frame: 지수 이력(output2) → index_daily 레코드
"""

from __future__ import annotations

import pandas as pd

from asset_flow.config.kis import KIS
from asset_flow.config.schemas import BALANCE_COLUMNS, CHART_RENAME, EXCHANGE_RATE_COLUMNS, INDEX_COLUMNS, KIS_RENAME
from asset_flow.transformers.base_transformer import normalize_numeric

_CURRENCY_CODE_MAP = {"원/달러": "USD", "원/엔": "JPY", "원/파운드": "GBP", "원/유로": "EUR"}
EXCHANGE_CURRENCIES = sorted(_CURRENCY_CODE_MAP.values())     # 환율 DAG 가 적재하는 통화
_PRICE_COLS = ["base_rate", "previous_day_closing_price", "opening_price", "highest_price", "lowest_price"]
FX_SOURCE = "KIS_FX_DAILY"                                    # exchange_rate_daily.source
_INDEX_NUM = ["opening_price", "highest_price", "lowest_price", "closing_price"]
_AMOUNT_COLS = ["total_purchase_amount", "total_evaluation_amount", "total_profit_amount"]


def _account(config: dict) -> dict:
    """
    Purpose: 계좌 식별 컬럼 생성
    Parameter:
        - config: {account, product_code, type}
    Input: 없음
    Output: {"account_code", "account_name"}
    """
    return {"account_code": f"{config['account']}-{config['product_code']}", "account_name": config["type"]}


def transform_domestic_balance(raw: dict, standard_date: str, config: dict) -> pd.DataFrame:
    """
    Purpose: 국내주식 잔고 변환 (TTTC8434R output1)
    Parameter:
        - raw: get_domestic_balance() 원시 JSON
        - standard_date: 기준일
        - config: 계좌 설정 dict
    Input: 없음
    Output: BALANCE_COLUMNS DataFrame (보유 종목 없으면 빈 DataFrame)
    """
    df = pd.json_normalize(raw["output1"])
    if df.empty:
        return pd.DataFrame()
    return normalize_numeric(
        df.rename(columns=KIS_RENAME["domestic_balance"])
        .assign(standard_date=standard_date, **_account(config), asset_type="STOCK",
                currency_code="KRW", exchange_code="KRX", multiplier=1.0)[BALANCE_COLUMNS]
    )


def transform_overseas_balance(raw: dict, standard_date: str, config: dict) -> pd.DataFrame:
    """
    Purpose: 해외주식 잔고 변환 (TTTS3012R output1, 통화·거래소는 응답값)
    Parameter:
        - raw: get_overseas_balance() 원시 JSON
        - standard_date: 기준일
        - config: 계좌 설정 dict
    Input: 없음
    Output: BALANCE_COLUMNS DataFrame (보유 종목 없으면 빈 DataFrame)
    """
    df = pd.json_normalize(raw["output1"])
    if df.empty:
        return pd.DataFrame()
    return normalize_numeric(
        df.rename(columns=KIS_RENAME["overseas_balance"])
        .assign(standard_date=standard_date, **_account(config), asset_type="STOCK", multiplier=1.0)[BALANCE_COLUMNS]
    )


def transform_pension_fund_balance(
    raw: dict, standard_date: str, config: dict, product_code: str, fund_price: float | None, product_name: str | None
) -> pd.DataFrame:
    """
    Purpose: 연금저축 펀드 잔고 변환 — 평가금액/NAV/0.001 로 좌수, 매입금액으로 매입단가 역산
    Parameter:
        - raw: get_account_balance() 원시 JSON
        - standard_date: 기준일
        - config: 계좌 설정 dict
        - product_code: 보유 펀드 표준코드
        - fund_price: 기준일 NAV (없으면 None)
        - product_name: 펀드명
    Input: 없음
    Output: BALANCE_COLUMNS 1행 DataFrame. 펀드 미보유(행 없음·평가금액 0·좌수 0)면 빈 DataFrame.
            보유했는데 NAV 없으면 ValueError (보유 여부를 NAV 검증보다 먼저 판정)
    """
    rows = raw.get("output1") or []
    if len(rows) < 2:                            # index 1 = "펀드/MMW" 행
        return pd.DataFrame(columns=BALANCE_COLUMNS)
    df = pd.json_normalize(rows).loc[[1]].rename(columns=KIS_RENAME["account_balance"])
    df[_AMOUNT_COLS] = df[_AMOUNT_COLS].astype(float)
    if not (df["total_evaluation_amount"] > 0).all():
        return pd.DataFrame(columns=BALANCE_COLUMNS)
    if not fund_price:
        raise ValueError(f"fund_price가 없다 — {standard_date} 연금저축 펀드 평가 불가 (NAV 결측)")
    df = df.assign(
        standard_date=standard_date, **_account(config), asset_type="FUND", currency_code="KRW",
        exchange_code="KRX", product_code=product_code, product_name=product_name,
        unit_market_price=fund_price, multiplier=0.001,
    )
    df["holding_quantity"] = (df["total_evaluation_amount"] / fund_price / 0.001).round(0)
    if not (df["holding_quantity"] > 0).all():   # 잔량 소수점 → 반올림 좌수 0
        return pd.DataFrame(columns=BALANCE_COLUMNS)
    df["unit_purchase_price"] = (df["total_purchase_amount"] / df["holding_quantity"] / 0.001).round(0)
    purchase = df["total_purchase_amount"]
    df["valuation_profit_rate"] = (df["total_profit_amount"] / purchase * 100).round(2) if (purchase > 0).all() else 0.0
    return normalize_numeric(df)[BALANCE_COLUMNS]


def transform_cma_cash_balance(raw: dict, standard_date: str, config: dict) -> pd.DataFrame:
    """
    Purpose: CMA 현금 잔고 변환 (output1[14] 외화단기사채 행, 수량 1·단가=평가금액)
    Parameter:
        - raw: get_account_balance() 원시 JSON
        - standard_date: 기준일
        - config: 계좌 설정 dict
    Input: 없음
    Output: BALANCE_COLUMNS 1행 DataFrame (응답 비면 빈 DataFrame)
    """
    df = pd.json_normalize(raw["output1"])
    if df.empty:
        return pd.DataFrame()
    df = df.loc[[14]].rename(columns=KIS_RENAME["account_balance"])
    df[_AMOUNT_COLS] = df[_AMOUNT_COLS].astype(float)
    amount = df["total_evaluation_amount"]
    df = df.assign(
        standard_date=standard_date, **_account(config), total_purchase_amount=amount, asset_type="CASH",
        currency_code="KRW", exchange_code="KRX", product_code="CMA", product_name="CMA",
        unit_market_price=amount, multiplier=1.0, holding_quantity=1.0, unit_purchase_price=amount,
        valuation_profit_rate=0.0,
    )
    return normalize_numeric(df)[BALANCE_COLUMNS]


def transform_rp_note_balance(raw: dict, standard_date: str, config: dict) -> pd.DataFrame:
    """
    Purpose: 위탁계좌 RP/발행어음 잔고 변환 (CTRP6548R output1 의 "RP/발행어음" 행, 둘을 구분하지 않고 1행)
    Parameter:
        - raw: get_account_balance() 원시 JSON
        - standard_date: 기준일
        - config: 계좌 설정 dict
    Input: 없음
    Output: BALANCE_COLUMNS 1행 (asset_type=CASH, product_code=RP_NOTE). 잔고 0이면 빈 DataFrame.
            행 수·합계 불일치(행 순서 변경 의심)면 ValueError
    """
    rows, names = raw.get("output1") or [], KIS.ACCOUNT_BALANCE_ROW_NAMES
    # 행 이름 없음(순서로 구분) → 행 수 다르면 실패
    if len(rows) != len(names):
        raise ValueError(f"자산현황 행 수 {len(rows)} ≠ {len(names)} — 행 순서 변경 의심, RP 적재 중단")
    evals = [float(r.get("evlu_amt") or 0) for r in rows]
    # 행 합 = 합계 행 (1원 오차 허용)
    if abs(sum(evals[:-1]) - evals[-1]) > 1:
        raise ValueError(f"자산현황 행 합 {sum(evals[:-1]):,.0f} ≠ 합계 {evals[-1]:,.0f} — 행 순서 변경 의심")
    row = rows[names.index("RP/발행어음")]
    evaluation = float(row.get("evlu_amt") or 0)
    if evaluation == 0:
        return pd.DataFrame(columns=BALANCE_COLUMNS)
    purchase, profit = float(row.get("pchs_amt") or 0), float(row.get("evlu_pfls_amt") or 0)
    # 원금·경과이자 그대로 유지 (CMA 와 달리 매입≠평가)
    df = pd.DataFrame([{
        "standard_date": standard_date, **_account(config), "product_code": "RP_NOTE",
        "product_name": "RP/발행어음", "asset_type": "CASH", "currency_code": "KRW", "exchange_code": "KRX",
        "multiplier": 1.0, "holding_quantity": 1.0, "unit_purchase_price": purchase, "unit_market_price": evaluation,
        "total_purchase_amount": purchase, "total_evaluation_amount": evaluation, "total_profit_amount": profit,
        "valuation_profit_rate": round(profit / purchase * 100, 2) if purchase else 0.0,
    }])
    return normalize_numeric(df)[BALANCE_COLUMNS]


def transform_exchange_rate(raw_list: list, standard_date: str) -> pd.DataFrame:
    """
    Purpose: 환율 응답 변환 + 달러 기준 GBP·EUR 를 원/달러로 교차 환산
    Parameter:
        - raw_list: get_exchange_rate() 응답 리스트
        - standard_date: 기준일
    Input: 없음
    Output: EXCHANGE_RATE_COLUMNS DataFrame (USD, JPY, GBP, EUR)
    """
    df = pd.concat([pd.json_normalize(r["output1"]) for r in raw_list], ignore_index=True)
    df = df.rename(columns=KIS_RENAME["exchange_rate"])
    df.insert(0, "standard_date", standard_date)
    df[_PRICE_COLS] = df[_PRICE_COLS].astype(float)
    usd_krw = df.loc[df["currency_pair_name"] == "원/달러", _PRICE_COLS].to_numpy()[0]
    cross_mask = df["currency_pair_name"].isin(["달러/파운드", "달러/유로"])
    cross = df[cross_mask].copy()
    cross[_PRICE_COLS] = cross[_PRICE_COLS].multiply(usd_krw, axis=1)
    cross["currency_pair_name"] = cross["currency_pair_name"].str.replace("달러/", "원/")
    result = pd.concat([df[~cross_mask], cross], ignore_index=True)
    result["currency_code"] = result["currency_pair_name"].map(_CURRENCY_CODE_MAP)
    result["source"] = FX_SOURCE
    return result[EXCHANGE_RATE_COLUMNS]


def transform_exchange_rate_history(raw: dict[str, list[dict]], start: str, end: str) -> pd.DataFrame:
    """
    Purpose: 환율 이력 → 달력일 행 (주말·휴일 = 직전 고시 이월) + 날짜별 GBP·EUR 원화 교차 환산
    Parameter:
        - raw: {심볼: get_chart_history("exchange_rate", …) 행} — start 보다 앞선 이월 시작값 포함
        - start: 적재 시작일 YYYY-MM-DD
        - end: 적재 끝일 YYYY-MM-DD
    Input: 없음
    Output: EXCHANGE_RATE_COLUMNS DataFrame (start~end 달력일 × 통화, 이월 시작값이 없는 날은 제외).
            매일 run(06:55 현재값을 T-1 로 기록)과 같은 의미: 그날 이하 마지막 고시값, 전일 종가 = 그 직전 고시값
    """
    days = pd.date_range(start, end, freq="D").strftime("%Y-%m-%d")
    frames = {}
    for symbol, rows in raw.items():
        df = pd.DataFrame(rows).rename(columns=CHART_RENAME["exchange_rate"])
        if df.empty:
            continue
        df["standard_date"] = pd.to_datetime(df["standard_date"], format="%Y%m%d").dt.strftime("%Y-%m-%d")
        cols = ["base_rate", "opening_price", "highest_price", "lowest_price"]
        df[cols] = df[cols].apply(pd.to_numeric, errors="coerce")
        df = df[df["base_rate"] > 0].drop_duplicates("standard_date").sort_values("standard_date")
        df["previous_day_closing_price"] = df["base_rate"].shift(1)
        df = df.set_index("standard_date")[_PRICE_COLS]
        frames[symbol] = df.reindex(df.index.union(days)).sort_index().ffill().loc[days]
    usd = frames.get("FX@KRWKFTC")
    out = []
    for symbol, df in frames.items():
        pair = KIS.EXCHANGE_RATE_PAIRS[symbol]
        if pair.startswith("달러/"):
            if usd is None:
                raise ValueError("교차 환산에 원/달러 이력이 필요함 (FX@KRWKFTC 0행)")
            df, pair = df * usd, pair.replace("달러/", "원/")
        df = df.dropna(subset=["base_rate"]).reset_index(names="standard_date")
        out.append(df.assign(source_symbol=symbol, currency_pair_name=pair,
                             currency_code=_CURRENCY_CODE_MAP[pair], source=FX_SOURCE))
    if not out:
        return pd.DataFrame(columns=EXCHANGE_RATE_COLUMNS)
    return pd.concat(out, ignore_index=True)[EXCHANGE_RATE_COLUMNS]


def to_index_frame(name: str, rows: list[dict], index_code: str, market: str, end: str) -> pd.DataFrame:
    """
    Purpose: 지수 이력 원시 행 → index_daily 레코드
    Parameter:
        - name: CHART_RENAME 키 (domestic_index_daily | overseas_index_daily)
        - rows: get_chart_history 결과
        - index_code: 적재 키 (KOSPI 등)
        - market: KR | US
        - end: 적재 상한일 YYYY-MM-DD (이후 = 장중 행 제거)
    Input: 없음
    Output: INDEX_COLUMNS DataFrame (standard_date 오름차순, 종가 0 이하 제거). 행 없으면 빈 DataFrame
    """
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=INDEX_COLUMNS)
    df = df.rename(columns=CHART_RENAME[name])
    df["standard_date"] = pd.to_datetime(df["standard_date"], format="%Y%m%d").dt.strftime("%Y-%m-%d")
    for col in _INDEX_NUM:
        df[col] = pd.to_numeric(df[col], errors="coerce") if col in df else float("nan")
    df[_INDEX_NUM[:3]] = df[_INDEX_NUM[:3]].where(df[_INDEX_NUM[:3]] > 0)      # 시가 등 0 = 값 없음
    df = df.assign(index_code=index_code, market=market, source=KIS.INDEX_SOURCES[market])
    df = df[(df["standard_date"] <= end) & (df["closing_price"] > 0)]
    return df.drop_duplicates("standard_date").sort_values("standard_date")[INDEX_COLUMNS].reset_index(drop=True)


# ── 예수금 (asset_type=CASH, product_code=DEPOSIT[_통화]) ──
def cash_row(standard_date: str, account: dict, code: str, name: str, currency: str, exchange: str | None,
             amount: float) -> pd.DataFrame:
    """
    Purpose: 현금성 1행 공통 (수량 1, 단가 = 금액, 매입 = 평가)
    Parameter:
        - standard_date: 기준일
        - account: {"account_code", "account_name"}
        - code: 상품 코드 (DEPOSIT 등)
        - name: 표시명
        - currency: 통화
        - exchange: 거래소 코드 (없으면 None)
        - amount: 금액 (통화 단위)
    Input: 없음
    Output: BALANCE_COLUMNS 1행
    """
    df = pd.DataFrame([{
        "standard_date": standard_date, **account, "product_code": code, "product_name": name,
        "asset_type": "CASH", "currency_code": currency, "exchange_code": exchange, "multiplier": 1.0,
        "holding_quantity": 1.0, "unit_purchase_price": amount, "unit_market_price": amount,
        "total_purchase_amount": amount, "total_evaluation_amount": amount, "total_profit_amount": 0.0,
        "valuation_profit_rate": 0.0,
    }])
    return normalize_numeric(df)[BALANCE_COLUMNS]


def transform_deposit(raw_domestic: dict, standard_date: str, config: dict) -> pd.DataFrame:
    """
    Purpose: 원화 예수금 (TTTC8434R output2[0].prvs_rcdl_excc_amt = D+2 정산 후 — 미결제 매수와 이중 계산 방지)
    Parameter:
        - raw_domestic: get_domestic_balance() 원시 JSON
        - standard_date: 기준일
        - config: 계좌 설정 dict
    Input: 없음
    Output: DEPOSIT 1행, 0원이면 빈 DataFrame. output2 예수금 필드가 없으면 ValueError
    """
    o2 = (raw_domestic.get("output2") or [None])[0]
    if not o2 or "prvs_rcdl_excc_amt" not in o2:
        raise ValueError("TTTC8434R output2 예수금 필드 없음 — 응답 형식 변경 의심")
    amount = float(str(o2["prvs_rcdl_excc_amt"] or 0).replace(",", ""))
    if amount == 0:                                       # 0원은 행 없음 (대시보드가 0원으로 처리)
        return pd.DataFrame(columns=BALANCE_COLUMNS)
    return cash_row(standard_date, _account(config), "DEPOSIT", "예수금", "KRW", "KRX", amount)


def transform_foreign_deposit(raw: dict, standard_date: str, config: dict) -> pd.DataFrame:
    """
    Purpose: 외화 예수금 통화별 (CTRP6504R output2[].frcr_dncl_amt_2)
    Parameter:
        - raw: get_overseas_present_balance() 원시 JSON
        - standard_date: 기준일
        - config: 계좌 설정 dict
    Input: 없음
    Output: DEPOSIT_<통화> 행 (0인 통화는 제외, 모두 0이면 빈 DataFrame)
    """
    amounts = {r["crcy_cd"].strip(): float(str(r.get("frcr_dncl_amt_2") or 0).replace(",", ""))
               for r in raw.get("output2") or [] if (r.get("crcy_cd") or "").strip()}
    rows = [cash_row(standard_date, _account(config), f"DEPOSIT_{c}", f"외화예수금({c})", c, None, a)
            for c, a in sorted(amounts.items()) if a != 0]
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=BALANCE_COLUMNS)


def account_total_gap(rows: pd.DataFrame, raw_account_balance: dict, usd_rate: float | None,
                      tolerance: float = 0.003) -> str | None:
    """
    Purpose: 적재 행 합계(원화 환산)와 CTRP6548R output2.tot_asst_amt 대조 (적재는 막지 않음)
    Parameter:
        - rows: 이 계좌의 BALANCE_COLUMNS 행
        - raw_account_balance: get_account_balance() 원시 JSON
        - usd_rate: 기준일 USD 기준율 (USD 행 환산, 없으면 비교 생략)
        - tolerance: 허용 비율 (기본 0.3%)
    Input: 없음
    Output: 차이가 허용 비율을 넘으면 경고 문구, 아니면 None (총자산 없음·USD 환율 없음도 None)
    """
    o2 = raw_account_balance.get("output2")
    o2 = o2[0] if isinstance(o2, list) and o2 else o2
    total = float(str((o2 or {}).get("tot_asst_amt") or 0).replace(",", ""))
    if total <= 0 or rows.empty:
        return None
    usd = rows["currency_code"] == "USD"
    if usd.any() and not usd_rate:
        return None
    loaded = float(rows.loc[~usd, "total_evaluation_amount"].sum() + rows.loc[usd, "total_evaluation_amount"].sum() * (usd_rate or 0))
    gap = loaded / total - 1
    return f"적재 합계 {loaded:,.0f} vs 계좌 총자산 {total:,.0f} ({gap:+.2%})" if abs(gap) > tolerance else None
