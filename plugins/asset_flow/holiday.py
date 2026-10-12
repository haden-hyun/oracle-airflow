"""
증시 휴장일 계산 (market_ingest_trading_holiday 용, DB 접근 없음)

- library_records(year): holidays 패키지 XKRX·XNYS 3년치 평일 휴장일 (US 이름 한국어 번역)
- fetch_kasi_records(service_key, year): 공공데이터포털 특일정보 올해·내년 평일 공휴일
- desired_rows / diff_rows: 목표 상태 계산·현재와 비교 (변경 없으면 DB 쓰기 생략)
- 레코드 형태: [market, 'YYYY-MM-DD', name, source] (XCom 직렬화 위해 날짜는 문자열)
"""

from __future__ import annotations

import datetime as dt

import holidays
import requests

KASI_URL = "https://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService/getRestDeInfo"
US_NAME_KO = {
    "New Year's Day": "신정",
    "Martin Luther King Jr. Day": "마틴 루서 킹 데이",
    "Washington's Birthday": "대통령의 날",
    "Good Friday": "성금요일",
    "Memorial Day": "현충일(메모리얼 데이)",
    "Juneteenth National Independence Day": "준틴스",
    "Independence Day": "독립기념일",
    "Labor Day": "노동절",
    "Thanksgiving Day": "추수감사절",
    "Christmas Day": "크리스마스",
}
OBSERVED = " (observed)"


def year_range(year: int) -> tuple[str, str]:
    """
    Purpose: 다시 쓰는 기간 (작년 1/1 ~ 내년 12/31)
    Parameter:
        - year: 기준 연도
    Input: 없음
    Output: (시작일, 종료일) YYYY-MM-DD
    """
    return f"{year - 1}-01-01", f"{year + 1}-12-31"


def translate_us(name: str) -> str:
    """
    Purpose: XNYS 영어 휴일명 → 한국어 ("A; B" 분리, observed → 대체 휴장)
    Parameter:
        - name: holidays 패키지 이름
    Input: 없음
    Output: 한국어 이름 (표에 없으면 원문 유지)
    """
    parts = []
    for part in name.split("; "):
        base = part.removesuffix(OBSERVED)
        ko = US_NAME_KO.get(base, base)
        parts.append(f"{ko} 대체 휴장" if part.endswith(OBSERVED) else ko)
    return "; ".join(parts)


def library_records(year: int) -> list[list]:
    """
    Purpose: holidays 패키지로 3년치 평일 휴장일 계산
    Parameter:
        - year: 기준 연도
    Input: holidays 패키지 (설치 버전에 고정)
    Output: KR → US 순 레코드 리스트 (주말 제외, source='holidays <버전>')
    """
    years = [year - 1, year, year + 1]
    source = f"holidays {holidays.__version__}"
    kr = holidays.financial_holidays("XKRX", years=years, language="ko")
    us = holidays.financial_holidays("XNYS", years=years)
    records = [["KR", d.isoformat(), name, source] for d, name in sorted(kr.items()) if d.weekday() < 5]
    records += [["US", d.isoformat(), translate_us(name), source] for d, name in sorted(us.items()) if d.weekday() < 5]
    return records


def kasi_items(payload: dict) -> list[dict]:
    """
    Purpose: KASI 응답의 item 정규화 (1건 dict / 여러 건 list / 0건 빈 문자열)
    Parameter:
        - payload: 응답 JSON
    Input: 없음
    Output: item dict 리스트
    """
    items = (payload.get("response", {}).get("body", {}).get("items") or {})
    item = items.get("item", []) if isinstance(items, dict) else []
    return [item] if isinstance(item, dict) else list(item)


def kasi_records(items: list[dict]) -> list[list]:
    """
    Purpose: KASI item → 평일 공휴일 KR 레코드
    Parameter:
        - items: kasi_items() 결과
    Input: 없음
    Output: [["KR", 'YYYY-MM-DD', dateName, 'kasi'], ...] (isHoliday=Y·평일만)
    """
    out = []
    for it in items:
        day = dt.datetime.strptime(str(it["locdate"]), "%Y%m%d").date()
        if it.get("isHoliday") == "Y" and day.weekday() < 5:
            out.append(["KR", day.isoformat(), it["dateName"], "kasi"])
    return out


def fetch_kasi_records(service_key: str, year: int) -> list[list]:
    """
    Purpose: 올해·내년 24개월 특일정보 조회 (임시공휴일 조기 반영)
    Parameter:
        - service_key: 공공데이터포털 serviceKey (Decoding)
        - year: 기준 연도
    Input: data.go.kr 특일정보 API
    Output: KR 레코드 리스트 (오류는 RuntimeError → 태스크 재시도. 메시지에 URL·키 미포함)
    """
    records = []
    for y in (year, year + 1):
        for m in range(1, 13):
            params = {"serviceKey": service_key, "solYear": y, "solMonth": f"{m:02d}", "_type": "json", "numOfRows": 50}
            try:
                resp = requests.get(KASI_URL, params=params, timeout=(5, 20))
                resp.raise_for_status()
                payload = resp.json()
            except (requests.RequestException, ValueError) as e:
                # 예외 문자열엔 serviceKey 가 든 URL 이 포함될 수 있음 → 이름만
                raise RuntimeError(f"KASI 조회 실패 {y}-{m:02d} ({type(e).__name__})") from None
            records += kasi_records(kasi_items(payload))
    return records


def desired_rows(library: list[list], kasi: list[list], manual_keys: set[tuple]) -> dict[tuple, tuple]:
    """
    Purpose: 자동 행의 목표 상태 (라이브러리 우선, KASI 는 빈 날만, 수동 행 날짜는 제외)
    Parameter:
        - library: 라이브러리 레코드
        - kasi: KASI 레코드
        - manual_keys: 수동 행 {(market, date)} — DAG 가 건드리지 않음
    Input: 없음
    Output: {(market, date): (name, source)}
    """
    rows: dict[tuple, tuple] = {}
    for market, day, name, source in library + kasi:
        key = (market, day)
        if key not in manual_keys:
            rows.setdefault(key, (name, source))
    return rows


def diff_rows(current: dict[tuple, tuple], desired: dict[tuple, tuple]) -> dict[str, list[str]]:
    """
    Purpose: 현재 자동 행과 목표 상태 비교
    Parameter:
        - current: DB 자동 행 {(market, date): (name, source)}
        - desired: desired_rows() 결과
    Input: 없음
    Output: {"added", "removed", "renamed"} — 항목은 "KR 2026-10-09 한글날" 형식 (모두 비면 변경 없음)
    """
    fmt = lambda k, rows: f"{k[0]} {k[1]} {rows[k][0]}"   # noqa: E731
    return {
        "added": [fmt(k, desired) for k in sorted(desired.keys() - current.keys())],
        "removed": [fmt(k, current) for k in sorted(current.keys() - desired.keys())],
        "renamed": [f"{fmt(k, current)} → {desired[k][0]}" for k in sorted(desired.keys() & current.keys())
                    if desired[k][0] != current[k][0]],
    }
