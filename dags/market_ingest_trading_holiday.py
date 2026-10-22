"""
증시 휴장일 달력 갱신 DAG (매일 06:00 KST, 변경 없으면 skip)

- 3년치(작년·올해·내년) 평일 휴장일 = holidays 패키지 + 공공데이터포털 특일정보(임시공휴일 조기 반영)
- 목표 상태를 현재 DB 와 비교 → 변경 있을 때만 단일 트랜잭션 재작성 + Discord 알림 (is_manual 행 보호)
- 변경 없음도 Asset 이벤트(rows=0)를 남긴다 — 대시보드가 '갱신 안 됨'(KASI 실패 skip)과 '변경 없음'을 구분
- 특일정보 API 실패한 날은 쓰지 않고 skip (KASI 행이 삭제→재추가로 깜빡이는 것 방지)
- 키: secrets services.holiday_api_credential (미설정이면 KASI 단계 [] — 라이브러리만으로 진행)
"""

import time
from datetime import datetime, timedelta

from airflow.sdk import dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException

from alerts import discord_failure_callback, discord_recovery_callback
from asset_flow.assets import SOURCE_KASI, TRADING_HOLIDAY, mark
from asset_flow.runtime import KST, kst_run_end

KASI_ATTEMPTS = 3

DAG_DOC = """
### 목적
대시보드의 "전일 대비" 판정용 평일 휴장일 달력(`market.trading_holiday`)을 매일 확인한다.
변경이 없으면 DB 를 쓰지 않고 `rows=0` 이벤트만 남긴 뒤 알림 skip(회색), 변경이 있으면 재작성 후 Discord 알림.

| Task | 내용 |
|---|---|
| `build_from_library` | holidays XKRX·XNYS 3년치 평일 휴장일 (US 이름 한국어) |
| `fetch_kasi_holidays` | 특일정보 API 올해·내년 (키 없으면 `[]`, 3회 실패면 `None`) |
| `upsert_holidays` | 목표 vs 현재 비교 → KASI 실패면 skip, 변경 없으면 `rows=0` 이벤트만, 있으면 재작성 |
| `report_changes` | 추가·삭제·이름변경 Discord 알림 (변경 없으면 skip) |
"""


@dag(
    dag_id="market_ingest_trading_holiday",
    description="매일 06:00 증시 휴장일 달력 확인 (KR·US) — 변경 있을 때만 갱신·알림",
    doc_md=DAG_DOC,
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=KST),
    catchup=False,
    default_args={"owner": "haejun", "retries": 1, "retry_delay": timedelta(minutes=5),
                  "on_failure_callback": discord_failure_callback},
    on_success_callback=discord_recovery_callback,
    tags=["market", "calendar", "holiday", "daily"],
)
def market_ingest_trading_holiday():
    """
    Purpose: DAG 구성 (라이브러리·KASI → 비교·적재 → 변경 보고)
    Parameter: 없음
    Input: 없음
    Output: 없음
    """

    def _year() -> int:
        """
        Purpose: 기준 연도 (run 시각 KST)
        Parameter: 없음
        Input: 태스크 컨텍스트
        Output: 연도
        """
        return kst_run_end(get_current_context()).year

    @task(task_id="build_from_library")
    def build_from_library() -> list:
        """
        Purpose: holidays 패키지 휴장일 계산
        Parameter: 없음
        Input: holidays 패키지
        Output: 레코드 리스트
        """
        from asset_flow.holiday import library_records

        return library_records(_year())

    @task(task_id="fetch_kasi_holidays", inlets=[SOURCE_KASI], retries=0)
    def fetch_kasi_holidays() -> list | None:
        """
        Purpose: 특일정보 API 공휴일 조회 (태스크 안에서 3회 재시도)
        Parameter: 없음
        Input: secrets 파일, data.go.kr API
        Output: 레코드 리스트 (키 없으면 [], 3회 모두 실패면 None → 오늘 갱신 생략). secrets 로드 실패는 예외
        """
        from asset_flow.holiday import fetch_kasi_records
        from asset_flow.secrets_config import load_secrets

        cfg = load_secrets()
        cred_id = cfg.services.holiday_api_credential
        if cred_id is None:
            print("services.holiday_api_credential 미설정 — KASI 단계 생략")
            return []
        for attempt in range(1, KASI_ATTEMPTS + 1):
            try:
                return fetch_kasi_records(cfg.credential(cred_id).app_key, _year())
            except RuntimeError as e:
                print(f"[WARN] KASI 조회 실패 {attempt}/{KASI_ATTEMPTS}: {e}")
                time.sleep(30 * attempt)
        return None

    @task(task_id="upsert_holidays", multiple_outputs=False, outlets=[TRADING_HOLIDAY])
    def upsert_holidays(library: list, kasi: list | None) -> dict:
        """
        Purpose: 목표 상태와 현재 DB 비교 → 변경 있을 때만 재작성
        Parameter:
            - library: 라이브러리 레코드 (우선)
            - kasi: KASI 레코드 (라이브러리에 없는 날만 채움). None 이면 조회 실패
        Input: market.trading_holiday
        Output: {"added", "removed", "renamed"} (+ 변경 있으면 DB 쓰기). KASI 실패면 AirflowSkipException
        """
        from asset_flow import db_manager as db
        from asset_flow.holiday import desired_rows, diff_rows, year_range

        if kasi is None:
            raise AirflowSkipException("특일정보 API 실패 — 오늘은 갱신 생략 (내일 재시도)")
        engine, period = db.asset_engine(), year_range(_year())
        current, manual = db.get_trading_holidays(engine, *period)
        diff = diff_rows(current, desired_rows(library, kasi, manual))
        counts = {k: len(v) for k, v in diff.items()}
        data_date = kst_run_end(get_current_context()).to_date_string()
        if not any(diff.values()):
            mark(TRADING_HOLIDAY, data_date=data_date, rows=0, **counts)
            print(f"변경 없음 (자동 {len(current)}건, 수동 {len(manual)}건)")
            return diff
        db.replace_trading_holidays(engine, *period, library + kasi)
        mark(TRADING_HOLIDAY, data_date=data_date, rows=len(library + kasi), **counts)   # 3년치 자동 행 재작성
        print(f"휴장일 갱신: {diff}")
        return diff

    @task(task_id="report_changes")
    def report_changes(diff: dict) -> None:
        """
        Purpose: 변경 내역 Discord 알림
        Parameter:
            - diff: upsert_holidays 결과
        Input: Connection discord_alert[_test]
        Output: 없음 (+ Discord 전송, 전송 실패는 로그만). 변경 없으면 AirflowSkipException
        """
        from alerts import field, make_payload, post_discord

        if not any(diff.values()):
            raise AirflowSkipException("변경 없음 — 알림 생략")

        labels = {"added": "추가", "removed": "삭제", "renamed": "이름 변경"}
        post_discord(make_payload({
            "title": "📅 휴장일 달력 변경",
            "color": 0x1E88E5,
            "description": "market.trading_holiday — 대시보드 휴장 판정에 반영됨",
            "fields": [field(labels[k], "\n".join(v), inline=False) for k, v in diff.items() if v],
        }))

    report_changes(upsert_holidays(build_from_library(), fetch_kasi_holidays()))


market_ingest_trading_holiday()
