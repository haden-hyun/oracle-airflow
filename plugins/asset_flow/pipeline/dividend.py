"""
배당 수집·적재 (account_ingest_dividend 의 태스크 본체)

- run_windows: run 날짜(T)·Param 으로 조회 창 계산 (늦은 run skip 없음 — 기간 이력 API 라 덮어쓰기 위험 없음)
- ingest_kr_events / ingest_us_events: 종목별 조회 → 변환 → market.dividend_event UPSERT (종목 단위 실패 격리)
- ingest_receipts: 계좌 1개 원장 조회 → account.dividend_receipt UPSERT (0건 = 정상)
- estimate_us_receipts: 미국 일정 × asset_daily 수량 → 추정 원장 UPSERT
- 원시 응답(계좌번호 포함)은 이 모듈 밖으로 내보내지 않음 — 반환값은 건수·id·코드만
- 토큰은 발급하지 않음 (발급 주체 = 06:50 auth_issue_api_tokens). 유효 토큰이 없으면 즉시 실패
"""

from __future__ import annotations

import datetime as dt
import logging

import holidays as holiday_lib

from asset_flow import db_manager as db
from asset_flow.clients.kis_client import KISApiClient
from asset_flow.config.kis import KIS
from asset_flow.config.schemas import DIVIDEND_EVENT_PK, DIVIDEND_RECEIPT_PK
from asset_flow.pipeline.dispatch import Deps
from asset_flow.secrets_config import kis_client_config
from asset_flow.transformers import dividend_transformer as dt_

log = logging.getLogger(__name__)
EARLIEST = "2000-01-01"                          # Param start_date 하한 (KSD 는 기간 제한 없음, 2013~ 실측)
EVENT_KEEP = ("ex_date", "pay_date", "dps_amount", "announce_date", "product_name")
RECEIPT_KEEP = ("gross_amount", "tax_amount", "fx_rate", "pay_date")


class PartialFailureError(RuntimeError):
    """일부 종목·계좌 실패 — 성공분은 이미 적재됨 (태스크는 실패로 알림)."""


def _ymd(d: dt.date) -> str:
    """
    Purpose: date → KIS 날짜 문자열
    Parameter:
        - d: 날짜
    Input: 없음
    Output: YYYYMMDD
    """
    return d.strftime("%Y%m%d")


def run_windows(base_date: str, params: dict, deps: Deps) -> dict:
    """
    Purpose: 조회 창 계산 (정기 = config 창, 수동 Param start_date/end_date = 백필)
    Parameter:
        - base_date: run 의 KST 날짜 T (YYYY-MM-DD)
        - params: DAG Param {start_date, end_date} (None = 정기 창)
        - deps: 의존성 (config dividend.windows)
    Input: 없음
    Output: {base_date, standard_date, kr_event, us_rights, us_ice, receipt, estimate, held_since} —
            각 창은 [시작, 끝] YYYY-MM-DD. start_date 가 있으면 모든 창의 시작을 그 날로 (권리종합은 −60일)
    """
    w, t = deps.app.dividend.windows, dt.date.fromisoformat(base_date)
    start = dt.date.fromisoformat(params["start_date"]) if params.get("start_date") else None
    end = dt.date.fromisoformat(params["end_date"]) if params.get("end_date") else None
    if start and start < dt.date.fromisoformat(EARLIEST):
        raise ValueError(f"start_date {start} < {EARLIEST}")
    if start and end and start > end:
        raise ValueError(f"start_date {start} > end_date {end}")

    def win(back: int, forward: int, lead: int = 0) -> list[str]:
        """
        Purpose: [T−back, T+forward] 창 (Param 이 있으면 덮어씀)
        Parameter:
            - back: 과거 일수
            - forward: 미래 일수
            - lead: Param start_date 를 더 앞당길 일수 (공시일 기준 API)
        Input: 없음
        Output: [시작, 끝] YYYY-MM-DD
        """
        s = (start - dt.timedelta(days=lead)) if start else t - dt.timedelta(days=back)
        e = end or t + dt.timedelta(days=forward)
        return [s.isoformat(), e.isoformat()]

    return {
        "base_date": base_date, "standard_date": (t - dt.timedelta(days=1)).isoformat(),
        "kr_event": win(w.kr_event_back, w.kr_event_forward),
        "us_rights": win(w.us_event_back, 0),
        "us_ice": win(w.us_event_back, w.us_event_forward, lead=60),
        "receipt": win(w.receipt_back, 0),
        "estimate": [win(w.estimate_back, 0)[0], (end or t - dt.timedelta(days=1)).isoformat()],
        "held_since": (start or t - dt.timedelta(days=w.held_back)).isoformat(),
    }


def kr_holidays(deps: Deps, start: str, end: str) -> set[dt.date]:
    """
    Purpose: 국내 휴장일 = holidays XKRX(전 기간) ∪ market.trading_holiday KR(수동 포함, 2025~2027)
    Parameter:
        - deps: 의존성
        - start: 시작 YYYY-MM-DD
        - end: 끝 YYYY-MM-DD
    Input: holidays 패키지, market.trading_holiday
    Output: date 집합 (주말 포함 가능 — prev_business_day 가 주말을 따로 거름)
    """
    years = range(int(start[:4]) - 1, int(end[:4]) + 1)
    lib = set(holiday_lib.financial_holidays("XKRX", years=years))
    return lib | db.get_holiday_sets(deps.engine, start, end)["KR"]


def market_client(deps: Deps) -> KISApiClient:
    """
    Purpose: 계좌 불필요 조회(일정·해외 권리)용 KIS 클라이언트
    Parameter:
        - deps: 의존성
    Input: secrets services.exchange_rate_credential, 토큰 저장소
    Output: KISApiClient (계좌 정보 없음)
    """
    cred_id = deps.secrets.services.exchange_rate_credential
    return KISApiClient(deps.tokens.existing_token(cred_id), kis_client_config(deps.secrets.credential(cred_id)))


def _upsert(deps: Deps, table_key: str, records: list[dict], pk: list[str], seen: str, keep: tuple) -> int:
    """
    Purpose: config 테이블로 UPSERT
    Parameter:
        - deps: 의존성
        - table_key: config tables 키 (dividend_event / dividend_receipt)
        - records: 레코드
        - pk: 충돌 키
        - seen: last_seen_date (run 날짜 T)
        - keep: NULL 이면 기존 값 유지할 컬럼
    Input: 없음
    Output: 처리 행 수 (+ DB 쓰기)
    """
    t = getattr(deps.app.tables, table_key)
    return db.upsert_rows(deps.engine, t.db_schema, t.table_name, records, pk, seen, keep)


def ingest_kr_events(codes: list[str], win: dict, deps: Deps) -> dict:
    """
    Purpose: 국내 종목별 배당 일정 조회·적재 (종목 1개 실패가 나머지를 막지 않음)
    Parameter:
        - codes: 국내 종목 코드
        - win: run_windows 결과
        - deps: 의존성
    Input: KIS HHKDB669102C0, 휴장일
    Output: {"codes", "rows", "failed"} (+ dividend_event UPSERT). 실패 종목이 있으면 성공분 적재 후 PartialFailureError
    """
    db.ensure_writable(deps.engine)                       # 외부 호출 전 쓰기 가드
    kis, (s, e) = market_client(deps), win["kr_event"]
    hol, raw, failed = kr_holidays(deps, s, e), [], {}
    for code in codes:
        try:
            raw += kis.get_ksd_dividend(code, _ymd(dt.date.fromisoformat(s)), _ymd(dt.date.fromisoformat(e)))
        except Exception as ex:                           # noqa: BLE001 — 종목 단위 격리
            failed[code] = str(ex)
            log.error("[KR %s] 일정 조회 실패: %s", code, ex)
    rows = _upsert(deps, "dividend_event", dt_.transform_ksd_events(raw, hol), DIVIDEND_EVENT_PK,
                   win["base_date"], EVENT_KEEP)
    log.info("국내 일정 %s종목 → %s행 (실패 %s)", len(codes), rows, sorted(failed))
    if failed:
        raise PartialFailureError(f"국내 일정 조회 실패 {sorted(failed)} (성공분 {rows}행 적재됨)")
    return {"codes": len(codes), "rows": rows, "failed": []}


def ingest_us_events(symbols: list[list[str]], win: dict, deps: Deps) -> dict:
    """
    Purpose: 미국 종목별 금액(03·75) + 권리종합 조회 → 조인 → 적재 (종목 3회 호출 중 하나라도 실패면 그 종목 제외)
    Parameter:
        - symbols: [[티커, 거래소코드 NASD|NYSE|AMEX]]
        - win: run_windows 결과
        - deps: 의존성
    Input: KIS CTRGT011R, HHDFS78330900
    Output: {"symbols", "rows", "failed"} (+ dividend_event UPSERT). 실패 종목이 있으면 성공분 적재 후 PartialFailureError
    """
    db.ensure_writable(deps.engine)
    kis, failed, records = market_client(deps), {}, []
    rs, re_ = (_ymd(dt.date.fromisoformat(d)) for d in win["us_rights"])
    is_, ie = (_ymd(dt.date.fromisoformat(d)) for d in win["us_ice"])
    for symbol, exchange in symbols:
        prdt = KIS.OVERSEAS_PRDT_TYPE.get(exchange or "")
        if prdt is None:
            failed[symbol] = f"거래소 코드 {exchange!r} 미지원"
            continue
        try:
            rights = [r for kind in KIS.US_DIVIDEND_RIGHTS
                      for r in kis.get_overseas_period_rights(symbol, prdt, rs, re_, kind)]
            records += dt_.transform_us_events(rights, kis.get_overseas_rights_ice(symbol, is_, ie))
        except Exception as ex:                           # noqa: BLE001 — 종목 단위 격리
            failed[symbol] = str(ex)
            log.error("[US %s] 권리 조회 실패: %s", symbol, ex)
    rows = _upsert(deps, "dividend_event", records, DIVIDEND_EVENT_PK, win["base_date"], EVENT_KEEP)
    log.info("미국 일정 %s종목 → %s행 (금액 미정 %s행, 실패 %s)", len(symbols), rows,
             sum(r["dps_amount"] is None for r in records), sorted(failed))
    if failed:
        raise PartialFailureError(f"미국 권리 조회 실패 {sorted(failed)} (성공분 {rows}행 적재됨)")
    return {"symbols": len(symbols), "rows": rows, "failed": []}


def ingest_receipts(account_id: str, win: dict, deps: Deps) -> int:
    """
    Purpose: 계좌 1개 국내 수령 원장 조회·적재 (계좌 자기 키, 0건 = 정상)
    Parameter:
        - account_id: secrets 계좌 id
        - win: run_windows 결과
        - deps: 의존성
    Input: KIS CTRGA011R, 휴장일
    Output: 적재 행 수 (+ dividend_receipt UPSERT). 비활성·비 KIS 계좌면 API 호출 전 예외
    """
    acct = deps.secrets.account(account_id)
    if not acct.enabled or not acct.kind.startswith("kis_"):
        raise RuntimeError(f"[{account_id}] 원장 조회 대상 아님 (enabled={acct.enabled}, kind={acct.kind})")
    db.ensure_writable(deps.engine)
    conf = kis_client_config(deps.secrets.credential(acct.credential), acct)
    kis, (s, e) = KISApiClient(deps.tokens.existing_token(acct.credential), conf), win["receipt"]
    raw = kis.get_period_rights(_ymd(dt.date.fromisoformat(s)), _ymd(dt.date.fromisoformat(e)))
    records = dt_.transform_receipts(raw, acct.account_code, kr_holidays(deps, s, e))
    rows = _upsert(deps, "dividend_receipt", records, DIVIDEND_RECEIPT_PK, win["base_date"], RECEIPT_KEEP)
    log.info("[%s] 원장 %s~%s 원시 %s행 → 적재 %s행", account_id, s, e, len(raw), rows)
    return rows


def us_stock_accounts(deps: Deps) -> list:
    """
    Purpose: 미국 주식 보유 가능 계좌 (추정 원장·asset_daily 센서 대상)
    Parameter:
        - deps: 의존성
    Input: secrets
    Output: kind=kis_stock 활성 계좌 리스트
    """
    return deps.secrets.active_accounts("kis_stock")


def estimate_us_receipts(win: dict, deps: Deps) -> int:
    """
    Purpose: 미국 추정 수령 원장 생성·적재 (락일 직전 asset_daily 수량 × 주당 × (1−원천징수) × 지급일 환율)
    Parameter:
        - win: run_windows 결과
        - deps: 의존성
    Input: market.dividend_event(US), account.asset_daily, market.exchange_rate_daily
    Output: 적재 행 수 (+ dividend_receipt UPSERT, is_estimated=true)
    """
    db.ensure_writable(deps.engine)
    s, e = win["estimate"]
    events = db.get_us_dividend_events(deps.engine, s, e)
    codes = [a.account_code for a in us_stock_accounts(deps)]
    hold_from = (dt.date.fromisoformat(s) - dt.timedelta(days=dt_.HOLDING_LOOKBACK_DAYS + 1)).isoformat()
    df = db.get_us_holdings(deps.engine, codes, hold_from, e)
    holdings: dict[tuple[str, str], list] = {}
    for r in df.itertuples(index=False):
        holdings.setdefault((r.account_code, r.product_code), []).append((r.standard_date, r.holding_quantity))
    rates = db.get_usd_rates(deps.engine, s)
    records = dt_.estimate_us_receipts(events, holdings, rates, deps.app.dividend.us_withholding_rate)
    rows = _upsert(deps, "dividend_receipt", records, DIVIDEND_RECEIPT_PK, win["base_date"], RECEIPT_KEEP)
    log.info("미국 추정 원장: 일정 %s건 × 계좌 %s → %s행 (금액 미정 %s, 환율 대기 %s)", len(events), len(codes), rows,
             sum(r["gross_amount"] is None for r in records), sum(r["fx_rate"] is None for r in records))
    return rows
