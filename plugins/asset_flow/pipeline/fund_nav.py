"""
펀드 기준가 수집 (market_ingest_fund_price_daily 본체) — KOFIA 주 경로 + fundguide 크롤러 대체 경로

- nav_for_date: KOFIA 기준일 이력 → 기준일 값 (T-1 이하 최신 기준가 = 주말·휴일 이월, 지연이면 None)
- collect_fund_prices: 코드별 KOFIA 조회 → 빠진 코드만 크롤러 (config funds.crawler_fallback)
- compare_sources: KOFIA 적재값 ↔ 크롤러 값 대조 (config funds.shadow_compare, 전환 검증 기간용)
"""

from __future__ import annotations

import datetime as dt
import logging

log = logging.getLogger(__name__)

LOOKBACK_DAYS = 20          # 조회 창 — 추석·설 연휴(최장 10일)에도 직전 기준가가 창 안에 있게
PRICE_TOLERANCE = 0.005     # 대조 허용 오차 (원, 소수 둘째 자리 반올림)


def expected_nav_date(standard_date: dt.date, holidays: set[dt.date]) -> dt.date:
    """
    Purpose: 기준일에 쓸 기준가가 최소한 이 날짜 것이어야 함 (기준일 이하 마지막 영업일)
    Parameter:
        - standard_date: 기준일 T-1
        - holidays: KR 평일 휴장일
    Input: 없음
    Output: 기준일 이하 마지막 평일·비휴장일 (KRX 휴장인 12/31 은 펀드 기준가가 있어도 그 전 영업일)
    """
    d = standard_date
    while d.weekday() >= 5 or d in holidays:
        d -= dt.timedelta(days=1)
    return d


def nav_for_date(rows: list[dict], standard_date: dt.date, holidays: set[dt.date]) -> tuple[float, dt.date] | None:
    """
    Purpose: 기준일 값 = 기준일 이하 최신 KOFIA 기준가 (주말·휴일은 직전 기준가 이월)
    Parameter:
        - rows: get_fund_nav_history 결과 [{"standard_date": YYYYMMDD, "standard_price": str}]
        - standard_date: 기준일 T-1
        - holidays: KR 평일 휴장일 (지연 판정용)
    Input: 없음
    Output: (기준가, 기준가 날짜) 또는 None (기준일 이하 행 없음 · 마지막 영업일보다 오래됨 = 공시 지연)
    """
    navs = sorted((dt.datetime.strptime(r["standard_date"], "%Y%m%d").date(), float(r["standard_price"]))
                  for r in rows if r.get("standard_date") and r.get("standard_price"))
    past = [(d, p) for d, p in navs if d <= standard_date and p > 0]
    if not past:
        return None
    nav_date, price = past[-1]
    if nav_date < expected_nav_date(standard_date, holidays):
        return None
    return price, nav_date


def collect_fund_prices(codes: list[str], standard_date: str, names: dict[str, str], holidays: set[dt.date],
                        kofia, crawl=None) -> tuple[list[dict], dict[str, str]]:
    """
    Purpose: 기준일 펀드 기준가 레코드 수집 (KOFIA 먼저, 빠진 코드만 크롤러)
    Parameter:
        - codes: 대상 펀드 표준코드
        - standard_date: 기준일 YYYY-MM-DD
        - names: {코드: 펀드명} (DB 기존 이름, 없으면 코드)
        - holidays: KR 평일 휴장일
        - kofia: get_fund_nav_history 를 가진 클라이언트
        - crawl: 대체 크롤러 crawl(codes, standard_date) → 레코드 (None 이면 대체 없음)
    Input: KOFIA, (대체) fundguide.net
    Output: (fund_price_daily 레코드, {코드: 출처 "KOFIA <기준가 날짜>" | "fundguide" | "missing: 사유"})
    """
    std = dt.date.fromisoformat(standard_date)
    start = (std - dt.timedelta(days=LOOKBACK_DAYS)).strftime("%Y%m%d")
    records, source, missing = [], {}, []
    for code in codes:
        try:
            hit = nav_for_date(kofia.get_fund_nav_history(code, start, std.strftime("%Y%m%d")), std, holidays)
        except Exception as e:                    # noqa: BLE001 — 코드 단위 격리, 대체 경로로 넘김
            log.warning("[KOFIA] %s 조회 실패: %s", code, type(e).__name__)
            hit = None
        if hit is None:
            missing.append(code)
            source[code] = "missing: KOFIA 값 없음·지연"
            continue
        price, nav_date = hit
        records.append({"standard_date": standard_date, "product_code": code,
                        "product_name": names.get(code) or code, "standard_price": price})
        source[code] = f"KOFIA {nav_date}"
    if missing and crawl is not None:
        log.warning("[fallback] KOFIA 누락 %s → fundguide 크롤링", missing)
        for r in crawl(missing, standard_date):
            records.append(r)
            source[r["product_code"]] = "fundguide"
    return records, source


def compare_sources(kofia_records: list[dict], crawled: list[dict]) -> list[str]:
    """
    Purpose: KOFIA 값과 크롤러 값 대조 (전환 검증 기간)
    Parameter:
        - kofia_records: 적재한 레코드 (KOFIA 출처만)
        - crawled: 같은 기준일 크롤러 레코드
    Input: 없음
    Output: 불일치 설명 리스트 (크롤러 실패 코드는 비교 생략)
    """
    got = {r["product_code"]: r["standard_price"] for r in crawled}
    return [f"{r['product_code']} KOFIA {r['standard_price']} ≠ fundguide {got[r['product_code']]}"
            for r in kofia_records
            if r["product_code"] in got and abs(r["standard_price"] - got[r["product_code"]]) > PRICE_TOLERANCE]
