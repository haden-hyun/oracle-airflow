"""
시세 빈 구간 채우기 — 수집 창 계산(순수) + 소스별 수집·적재 루프 (market_ingest_product_price_daily 본체)

- plan_window / recent_start / plan_product: 상품 1개의 [start, end] (순수 함수)
- build_plan: 대상 상품 × Param → {source: [Window dict]} (펀드 FUND_NAV 는 06:55 fund_price 담당이라 제외)
- run_source → ingest_source: 소스 하나의 상품을 차례로 수집·적재 (상품 단위 격리)
- stale_tracked: 추적 종목 최신성 점검 대상
- KIS 토큰은 발급하지 않음 (TokenManager.existing_token)
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from datetime import date, timedelta

log = logging.getLogger(__name__)

KIS_SOURCES = ("KIS_KR_DAILY", "KIS_US_DAILY")
PRICE_SOURCES = ("KIS_KR_DAILY", "KIS_US_DAILY", "UPBIT_DAY")
HOLIDAY_MARKET = {"KR": "KR", "FUND": "KR", "US": "US"}      # CRYPTO 는 휴장 없음
STALE_TRADING_DAYS = 3                                      # 추적 종목이 이 거래일 수 넘게 뒤처지면 지연


@dataclass(frozen=True)
class Window:
    product_code: str
    start: str          # YYYY-MM-DD
    end: str            # YYYY-MM-DD
    is_new: bool        # 적재분 없음 → 결과로 price_from_date 기록


def recent_start(end: date, market: str, holidays: dict[str, set[date]], n: int) -> date:
    """
    Purpose: end 포함 거꾸로 n 번째 거래일
    Parameter:
        - end: 상한일
        - market: KR | US | FUND | CRYPTO
        - holidays: {"KR": {date}, "US": {date}} 평일 휴장일
        - n: 거래일 수
    Input: 없음
    Output: date (CRYPTO 는 end − (n−1)일)
    """
    if market == "CRYPTO":
        return end - timedelta(days=n - 1)
    off = holidays.get(HOLIDAY_MARKET[market], set())
    d, count = end, 0
    while True:
        if d.weekday() < 5 and d not in off:
            count += 1
            if count == n:
                return d
        d -= timedelta(days=1)


def plan_window(product: dict, last: date | None, base: date, holidays: dict[str, set[date]],
                lookback_days: int, refetch_n: int) -> Window | None:
    """
    Purpose: 상품 1개의 수집 창 계산 (신규 1년 · 증분 · 최근 n 거래일 재수집 · 재매수 최대 1년 갭)
    Parameter:
        - product: market.product 행 dict (product_code, market, price_from_date)
        - last: 이미 적재된 마지막 날짜 (없으면 None)
        - base: 실행일 T (KST)
        - holidays: 평일 휴장일
        - lookback_days: 최대 소급 일수 (365)
        - refetch_n: 재수집 거래일 수 (5)
    Input: 없음
    Output: Window 또는 None (받을 구간 없음)
    """
    end = base - timedelta(days=1)
    floor = base - timedelta(days=lookback_days)
    if product.get("price_from_date"):
        floor = max(floor, product["price_from_date"])
    if last is None:
        start = floor
    else:
        start = max(floor, min(last + timedelta(days=1), recent_start(end, product["market"], holidays, refetch_n)))
    if start > end:
        return None
    return Window(product["product_code"], start.isoformat(), end.isoformat(), last is None)


def plan_product(product: dict, base: date, holidays: dict[str, set[date]], lookback_days: int, refetch_n: int,
                 params: dict) -> Window | None:
    """
    Purpose: 상품 1개 창 = 자동 규칙(plan_window) + 수동 Param 덮어쓰기
    Parameter:
        - product: get_price_targets 행 (last_loaded 포함)
        - base: 실행일 T
        - holidays: 평일 휴장일
        - lookback_days: 최대 소급 일수
        - refetch_n: 재수집 거래일 수
        - params: DAG Param {start_date, end_date, full_refresh}
    Input: 없음
    Output: Window 또는 None
    """
    last = None if params.get("full_refresh") else product.get("last_loaded")
    w = plan_window(product, last, base, holidays, lookback_days, refetch_n)
    start, end = params.get("start_date") or None, params.get("end_date") or None
    if not (start or end):
        return w
    s = start or (w.start if w else (base - timedelta(days=lookback_days)).isoformat())
    e = end or (base - timedelta(days=1)).isoformat()
    return Window(product["product_code"], s, e, last is None) if s <= e else None


def build_plan(dates: dict, params: dict) -> dict:
    """
    Purpose: 수집 대상 × 창 계산 (plan_fetch 태스크 본체)
    Parameter:
        - dates: {"base_date": T, "end": T-1}
        - params: DAG Param {product_codes, start_date, end_date, full_refresh}
    Input: secrets·config, market.product·시세 테이블, market.trading_holiday
    Output: {price_source: [{"product_code","start","end","is_new"}]} (PRICE_SOURCES 만)
    """
    from asset_flow import db_manager as db
    from asset_flow.pipeline.dispatch import Deps

    deps = Deps.load()
    cfg, base = deps.app.price_ingest, date.fromisoformat(dates["base_date"])
    targets = db.get_price_targets(deps.engine, list(params.get("product_codes") or []) or None)
    holidays = db.get_holiday_sets(deps.engine, (base - timedelta(days=cfg.lookback_days + 40)).isoformat(),
                                   base.isoformat())
    plan: dict[str, list[dict]] = {}
    for p in targets:
        if p["price_source"] not in PRICE_SOURCES:
            continue
        w = plan_product(p, base, holidays, cfg.lookback_days, cfg.refetch_trading_days, params)
        if w:
            plan.setdefault(p["price_source"], []).append(asdict(w))
    print({src: len(ws) for src, ws in plan.items()})
    return plan


def run_source(source: str, windows: list[dict]) -> dict:
    """
    Purpose: 소스 1개 수집·적재 (ingest_prices 태스크 본체)
    Parameter:
        - source: KIS_KR_DAILY | KIS_US_DAILY | UPBIT_DAY
        - windows: build_plan 결과 중 이 소스 (Window dict)
    Input: secrets·config, 토큰, 소스 API, 업무 DB
    Output: {"rows", "products", "failed"} (+ DB 쓰기). 창이 없으면 API·DB 호출 없음
    """
    from asset_flow import db_manager as db
    from asset_flow.pipeline.dispatch import Deps

    ws = [Window(**w) for w in windows]
    if not ws:
        return {"rows": 0, "products": 0, "failed": {}}
    deps = Deps.load()
    products = db.get_price_targets(deps.engine, [w.product_code for w in ws])
    return ingest_source(source, products, ws, deps, deps.app.price_ingest.request_interval_sec)


def ingest_source(source: str, products: list[dict], windows: list[Window], deps, interval: float) -> dict:
    """
    Purpose: price_source 하나의 상품을 차례로 수집·적재 (상품 단위 커밋 — 실패는 그 상품만)
    Parameter:
        - source: KIS_KR_DAILY | KIS_US_DAILY | UPBIT_DAY
        - products: 해당 소스의 market.product 행
        - windows: plan 결과 (같은 소스)
        - deps: Deps (engine, tokens, secrets, app)
        - interval: 업비트 호출 간격 초 (KIS 는 클라이언트가 유지)
    Input: 소스 API, 업무 DB
    Output: {"rows": upsert 요청 행, "products": 처리 수, "failed": {code: 사유}} (+ DB 쓰기)
    """
    from asset_flow import db_manager as db
    from asset_flow.transformers.price_transformer import kst_day_ohlc, to_price_frame

    fetch = _fetcher(source, deps)
    by_code = {p["product_code"]: p for p in products}
    rows, failed = 0, {}
    for w in windows:
        p = by_code[w.product_code]
        try:
            raw = fetch(p, w.start, w.end, interval)
            if source == "UPBIT_DAY":
                raw = kst_day_ohlc(raw)
            df = to_price_frame(source, raw, p["product_code"], p["currency_code"], w.end)
            validate_coverage(p, w, df)
            n = db.upsert_product_prices(deps.engine, df.to_dict("records"))
            if w.is_new:
                first = df["standard_date"].min() if len(df) else (date.fromisoformat(w.end) + timedelta(days=1)).isoformat()
                db.set_price_from_date(deps.engine, p["product_code"], first)
            rows += n
            print(f"[{source}] {p['product_code']} {w.start}~{w.end}: 수신 {len(df)} · 적재 {n}")
        except Exception as e:                      # noqa: BLE001 — 상품 단위 격리, 다른 상품은 계속
            failed[p["product_code"]] = f"{type(e).__name__}: {e}"[:200]
            log.error("[%s] %s 실패: %s", source, p["product_code"], failed[p["product_code"]])
    return {"rows": rows, "products": len(windows), "failed": failed}


def validate_coverage(product: dict, window: Window, df) -> None:
    """
    Purpose: '보유했는데 시세 0행' 검출 (KIS 는 잘못된 코드·거래소에도 rt_cd=0, 0행을 준다)
    Parameter:
        - product: market.product 행
        - window: 수집 창
        - df: 변환 결과 (DataFrame 또는 리스트)
    Input: 없음
    Output: 없음. 창이 보유 구간과 5일 넘게 겹치는데 0행이면 ValueError
    """
    held_from, held_to = product.get("first_held_date"), product.get("last_held_date")
    if len(df) or held_from is None:
        return
    lo, hi = max(date.fromisoformat(window.start), held_from), min(date.fromisoformat(window.end), held_to)
    if (hi - lo).days > 5:
        raise ValueError(f"보유 구간 {lo}~{hi} 시세 0행 — source_symbol·exchange_code 확인")


def stale_products(targets: list[dict], end: date, holidays: dict[str, set[date]],
                   n: int = STALE_TRADING_DAYS) -> list[str]:
    """
    Purpose: 추적 종목 중 최신 시세가 n 거래일 넘게 뒤처진 상품 (순수 함수)
    Parameter:
        - targets: get_price_targets 행 (is_tracked, last_loaded, price_from_date)
        - end: 기대 상한 T-1
        - holidays: 평일 휴장일
        - n: 허용 거래일 수
    Input: 없음
    Output: "코드(마지막 적재일)" 리스트. 펀드는 06:55 check_fund_coverage 담당이라 제외
    """
    out = []
    for p in targets:
        if p["price_source"] not in PRICE_SOURCES or not p.get("is_tracked"):
            continue
        expected = recent_start(end, p["market"], holidays, n)
        if p.get("price_from_date") and p["price_from_date"] > expected:      # 상장 직후·0행 기록 상품
            continue
        if p.get("last_loaded") is None or p["last_loaded"] < expected:
            out.append(f"{p['product_code']}({p.get('last_loaded')})")
    return out


def stale_tracked(dates: dict) -> list[str]:
    """
    Purpose: 추적 종목 최신성 점검 (check_tracked_coverage 태스크 본체)
    Parameter:
        - dates: {"base_date": T, "end": T-1}
    Input: market.product·시세 테이블, market.trading_holiday
    Output: 지연 상품 리스트 (없으면 [])
    """
    from asset_flow import db_manager as db
    from asset_flow.pipeline.dispatch import Deps

    deps, end = Deps.load(), date.fromisoformat(dates["end"])
    holidays = db.get_holiday_sets(deps.engine, (end - timedelta(days=30)).isoformat(), end.isoformat())
    return stale_products(db.get_price_targets(deps.engine), end, holidays)


def _fetcher(source: str, deps):
    """
    Purpose: 소스별 원시 조회 함수 생성 (클라이언트는 태스크당 1개)
    Parameter:
        - source: price_source
        - deps: Deps
    Input: secrets(KIS 키), 토큰 저장소 (발급 안 함)
    Output: fetch(product, start, end, interval) → 원시 행 리스트
    """
    ymd = lambda s: s.replace("-", "")                          # noqa: E731
    if source in KIS_SOURCES:
        from asset_flow.clients.kis_client import KISApiClient
        from asset_flow.config.kis import KIS
        from asset_flow.secrets_config import kis_client_config

        cred = deps.secrets.credential(deps.secrets.services.exchange_rate_credential)
        kis = KISApiClient(deps.tokens.existing_token(cred.id), kis_client_config(cred))
        if source == "KIS_KR_DAILY":
            return lambda p, s, e, i: kis.get_domestic_daily_price(p["source_symbol"], ymd(s), ymd(e))
        return lambda p, s, e, i: kis.get_overseas_daily_price(
            KIS.OVERSEAS_PRICE_EXCD[p["exchange_code"]], p["source_symbol"], ymd(s), ymd(e))
    if source == "UPBIT_DAY":
        from asset_flow.clients.upbit_client import UpbitApiClient

        upbit = UpbitApiClient("", "")                         # 공개 엔드포인트만 사용
        return lambda p, s, e, i: upbit.get_hour_candles(p["source_symbol"], s, e, i)
    raise ValueError(f"알 수 없는 price_source: {source}")
