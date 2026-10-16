"""
시장 기준 데이터 — 지수 일봉 수집 창·검증, 환율 이력 백필 (market_ingest_reference_daily 본체)

- us_closed_date: 정규장이 끝난 마지막 미국 날짜 (장중 행 차단)
- plan_index_windows: 지수별 [start, end] (순수 함수 — 신규 = from_date, 이후 = 마지막+1 과 최근 n 거래일 중 이른 날)
- collect_index_prices: 창별 KIS 조회 → 변환 → '창 끝 거래일 행' 검증 (하나라도 없으면 전체 실패 → 태스크 재시도)
- collect_exchange_rate_history: 환율 이력 → 달력일 이월 레코드 + 날짜×통화 빠짐 검증
- KIS 토큰은 발급하지 않음 (호출부가 기존 토큰 주입)
"""

from __future__ import annotations

import datetime as dt
import logging
from datetime import date, timedelta

import pendulum

from asset_flow.config.kis import KIS
from asset_flow.pipeline.price_plan import recent_start

log = logging.getLogger(__name__)

NEW_YORK = pendulum.timezone("America/New_York")
US_CLOSE_SETTLED = dt.time(16, 30)            # 16:00 정규장 마감 + 종가 확정 여유
CHART_NAMES = {"KR": "domestic_index_daily", "US": "overseas_index_daily"}


def us_closed_date(now: dt.datetime | None = None) -> date:
    """
    Purpose: 정규장이 끝난 마지막 미국 날짜 (주말·휴장 판정은 하지 않음 — 그 날짜 이하 최신 거래일이 기준)
    Parameter:
        - now: 기준 시각 (테스트용, 기본 현재)
    Input: 시스템 시계
    Output: 뉴욕 시각 16:30 이후면 뉴욕 오늘, 아니면 뉴욕 어제
    """
    ny = pendulum.instance(now).in_timezone(NEW_YORK) if now else pendulum.now(NEW_YORK)
    return ny.date() if ny.time() >= US_CLOSE_SETTLED else ny.date() - timedelta(days=1)


def plan_index_windows(last_loaded: dict[str, date], base: date, holidays: dict[str, set[date]], from_date: date,
                       refetch_n: int, params: dict, now: dt.datetime | None = None) -> list[dict]:
    """
    Purpose: 지수별 수집 창 계산
    Parameter:
        - last_loaded: {index_code: 적재된 마지막 날짜} (없는 지수 = 신규)
        - base: 실행일 T (KST)
        - holidays: {"KR": {date}, "US": {date}} 평일 휴장일
        - from_date: 신규 지수 시작일 (config)
        - refetch_n: 매일 재수집 거래일 수 (정정 흡수)
        - params: DAG Param {start_date, end_date} (있으면 자동 규칙 대신 사용, end 는 상한과 비교해 작은 값)
        - now: 기준 시각 (테스트용)
    Input: 없음
    Output: [{"index_code", "market", "start", "end"}] (YYYY-MM-DD, start > end 인 지수 제외)
    """
    caps = {"KR": base - timedelta(days=1), "US": min(base - timedelta(days=1), us_closed_date(now))}
    p_start, p_end = params.get("start_date") or None, params.get("end_date") or None
    out = []
    for code, (market, _) in KIS.MARKET_INDEXES.items():
        end = min(caps[market], date.fromisoformat(p_end)) if p_end else caps[market]
        last = last_loaded.get(code)
        if p_start:
            start = date.fromisoformat(p_start)
        elif last is None:
            start = from_date
        else:
            start = max(from_date, min(last + timedelta(days=1), recent_start(end, market, holidays, refetch_n)))
        if start <= end:
            out.append({"index_code": code, "market": market, "start": start.isoformat(), "end": end.isoformat()})
    return out


def expected_last_day(window: dict, holidays: dict[str, set[date]]) -> date | None:
    """
    Purpose: 창 안의 마지막 거래일 (이 날 행이 없으면 아직 미공시·코드 오류)
    Parameter:
        - window: plan_index_windows 원소
        - holidays: 평일 휴장일
    Input: 없음
    Output: date, 창 안에 거래일이 없으면 None
    """
    d = recent_start(date.fromisoformat(window["end"]), window["market"], holidays, 1)
    return d if d >= date.fromisoformat(window["start"]) else None


def collect_index_prices(kis, windows: list[dict], holidays: dict[str, set[date]]) -> list[dict]:
    """
    Purpose: 창별 지수 이력 조회 → index_daily 레코드 (모든 창 끝 거래일 행 검증)
    Parameter:
        - kis: KISApiClient (get_chart_history)
        - windows: plan_index_windows 결과
        - holidays: 평일 휴장일
    Input: KIS API
    Output: INDEX_COLUMNS 레코드 리스트. 창 끝 거래일 행이 없는 지수가 있으면 RuntimeError (부분 적재 안 함 —
            재시도·다음 날 재수집으로 채움)
    """
    from asset_flow.transformers.kis_transformer import to_index_frame

    records, missing = [], []
    for w in windows:
        name, (_, kis_code) = CHART_NAMES[w["market"]], KIS.MARKET_INDEXES[w["index_code"]]
        rows = kis.get_chart_history(name, kis_code, w["start"].replace("-", ""), w["end"].replace("-", ""))
        df = to_index_frame(name, rows, w["index_code"], w["market"], w["end"])
        want = expected_last_day(w, holidays)
        if want and want.isoformat() not in set(df["standard_date"]):
            missing.append(f"{w['index_code']} {want}")
        log.info("[%s] %s~%s → %s행", w["index_code"], w["start"], w["end"], len(df))
        records += df.to_dict("records")
    if missing:
        raise RuntimeError(f"지수 창 끝 거래일 행 없음: {missing} — KIS 미공시 또는 코드 오류 (재시도·수동 재실행)")
    return records


def collect_exchange_rate_history(kis, start: str, end: str) -> list[dict]:
    """
    Purpose: 환율 이력 백필 레코드 (4종 × start~end 달력일)
    Parameter:
        - kis: KISApiClient
        - start: 적재 시작일 YYYY-MM-DD
        - end: 적재 끝일 YYYY-MM-DD
    Input: KIS API
    Output: EXCHANGE_RATE_COLUMNS 레코드. 날짜×통화 하나라도 비면 RuntimeError (이월 시작값 부족·심볼 오류)
    """
    from asset_flow.transformers.kis_transformer import EXCHANGE_CURRENCIES, transform_exchange_rate_history

    seed = (date.fromisoformat(start) - timedelta(days=KIS.CHART_QUERY_PAD_DAYS)).strftime("%Y%m%d")
    raw = {s: kis.get_chart_history("exchange_rate", s, seed, end.replace("-", ""))
           for s in KIS.EXCHANGE_RATE_PRODUCT_CODES}
    df = transform_exchange_rate_history(raw, start, end)
    days = (date.fromisoformat(end) - date.fromisoformat(start)).days + 1
    short = {c: days - n for c in EXCHANGE_CURRENCIES
             if (n := int((df["currency_code"] == c).sum())) != days}
    if short:
        raise RuntimeError(f"환율 이력 {start}~{end} 빠진 날 수 {short} — 이월 시작값·심볼 확인")
    return df.to_dict("records")
