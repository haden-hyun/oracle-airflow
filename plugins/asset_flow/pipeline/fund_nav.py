"""
펀드 기준가 수집 (market_ingest_reference_daily fund_price 그룹 본체) — KOFIA 주 경로 + fundguide 크롤러 대체 경로

- nav_for_date: KOFIA 기준일 이력 → 기준일 값 (T-1 이하 최신 기준가 = 주말·휴일 이월, 지연이면 None)
- fetch_all: KOFIA 조회 병렬 실행 (응답 지연이 생겨도 펀드 수만큼 늘지 않게)
- collect_fund_prices: 코드별 KOFIA 조회(병렬) → 빠진 코드만 크롤러 (config funds.crawler_fallback)
- compare_sources: KOFIA 적재값 ↔ 크롤러 값 대조 (config funds.shadow_compare, 전환 검증 기간용)
- missing_dates / gap_records: 최근 1년 빈 날짜 보충 (fill_nav_gaps 본체, 매일 경로와 같은 nav_for_date 규칙)
"""

from __future__ import annotations

import datetime as dt
import logging
from concurrent.futures import ThreadPoolExecutor

from asset_flow.config.kofia import KOFIA

log = logging.getLogger(__name__)

LOOKBACK_DAYS = 20          # 조회 창 — 추석·설 연휴(최장 10일)에도 직전 기준가가 창 안에 있게
PRICE_TOLERANCE = 0.005     # 대조 허용 오차 (원, 소수 둘째 자리 반올림)
GAP_WINDOW_DAYS = 365       # 빈 날짜 보충 탐색 범위 (T-1 기준 과거 1년)


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


def fetch_all(fetch, keys: list, workers: int = KOFIA.MAX_WORKERS) -> dict:
    """
    Purpose: 조회 함수를 키마다 병렬 실행 (KOFIA 동시 요청은 각각 처리됨 — 실측)
    Parameter:
        - fetch: key → 결과 함수
        - keys: 조회 키 리스트 (hashable, 중복 없음)
        - workers: 동시 요청 상한
    Input: 외부 API (fetch 가 호출)
    Output: {key: 결과 또는 발생한 Exception} — 예외는 호출자가 키 단위로 처리
    """
    def run(key):
        """
        Purpose: 키 1개 조회 (예외를 값으로 돌려 다른 키에 영향 없게)
        Parameter:
            - key: 조회 키
        Input: fetch
        Output: 결과 또는 Exception
        """
        try:
            return fetch(key)
        except Exception as e:                    # noqa: BLE001 — 키 단위 격리, 호출자에게 그대로 넘김
            return e

    if not keys:
        return {}
    pool = ThreadPoolExecutor(max_workers=max(1, min(workers, len(keys))))
    try:
        futures = [pool.submit(run, k) for k in keys]
        return {k: f.result() for k, f in zip(keys, futures)}
    finally:                                      # 태스크 중단(BaseException) 시 대기 중 요청은 버림
        pool.shutdown(wait=False, cancel_futures=True)


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
    fetched = fetch_all(lambda c: kofia.get_fund_nav_history(c, start, std.strftime("%Y%m%d")), codes)
    for code in codes:
        try:
            if isinstance(fetched[code], Exception):
                raise fetched[code]
            hit = nav_for_date(fetched[code], std, holidays)
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


def missing_dates(loaded: set[dt.date], start: dt.date, end: dt.date) -> list[dt.date]:
    """
    Purpose: 적재 안 된 달력일 (주말·휴일 포함 — 매일 경로가 이월 행을 만들므로 모든 날에 행이 있어야 함)
    Parameter:
        - loaded: 이 펀드의 적재일
        - start: 탐색 시작일 (포함)
        - end: 탐색 종료일 (포함)
    Input: 없음
    Output: 빈 날짜 오름차순
    """
    days = (start + dt.timedelta(days=i) for i in range((end - start).days + 1))
    return [d for d in days if d not in loaded]


def gap_records(rows: list[dict], gaps: list[dt.date], code: str, name: str,
                holidays: set[dt.date]) -> tuple[list[dict], list[dt.date]]:
    """
    Purpose: 빈 날짜별 기준가 레코드 (매일 경로와 같은 nav_for_date 규칙 — 주말·휴일은 직전 기준가 이월)
    Parameter:
        - rows: KOFIA 이력 (가장 이른 빈 날짜 - LOOKBACK_DAYS 부터 — 이월할 직전 기준가 포함)
        - gaps: missing_dates 결과
        - code: 펀드 표준코드
        - name: 표시명
        - holidays: KR 평일 휴장일
    Input: 없음
    Output: (fund_price_daily 레코드, 못 채운 날짜). KOFIA 첫 행(설정일 또는 조회 시작) 이전 날짜는 둘 다에서 제외
    """
    dates = [r["standard_date"] for r in rows if r.get("standard_date")]
    if not dates:
        return [], []
    first = dt.datetime.strptime(min(dates), "%Y%m%d").date()
    records, unfilled = [], []
    for d in gaps:
        if d < first:
            continue
        hit = nav_for_date(rows, d, holidays)
        if hit is None:
            unfilled.append(d)
            continue
        records.append({"standard_date": d.isoformat(), "product_code": code,
                        "product_name": name, "standard_price": hit[0]})
    return records, unfilled


def gap_embed(filled: dict[str, list[str]], unfilled: dict[str, list[str]]) -> dict | None:
    """
    Purpose: 빈 날짜 보충 알림 embed (보충이 있을 때만 — 못 채운 날만 있으면 로그만, 매일 같은 경고 반복 방지)
    Parameter:
        - filled: {코드: 보충한 날짜 YYYY-MM-DD 오름차순}
        - unfilled: {코드: KOFIA 에 값이 없어 못 채운 날짜}
    Input: 없음
    Output: Discord embed 또는 None (보충 0건)
    """
    if not filled:
        return None
    fields = [{"name": code, "value": f"{d[0]} ~ {d[-1]} · {len(d)}일 보충", "inline": False}
              for code, d in filled.items()]
    fields += [{"name": f"⚠ {code} 못 채움", "value": ", ".join(d[:10]) + (" …" if len(d) > 10 else ""),
                "inline": False} for code, d in unfilled.items()]
    lo, hi = min(d[0] for d in filled.values()), max(d[-1] for d in filled.values())
    fields.append({"name": "수기 계좌(IRP·DC) 재계산",
                   "value": f"account_rebuild_ledger_assets start_date={lo} end_date={hi}", "inline": False})
    return {"title": "🩹 펀드 기준가 빈 날짜 보충", "color": 0x1E88E5,
            "description": "market.fund_price_daily — KOFIA 이력으로 빈 날짜만 INSERT (기존 행 불변)",
            "fields": fields}
