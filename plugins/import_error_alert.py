"""
DAG import error → Discord 리스너 플러그인 (dag-processor 에서 실행)

- 훅: on_new_dag_import_error / on_existing_dag_import_error
- 같은 (파일, stacktrace) 는 6시간에 1회만 알림 (상태: env IMPORT_ERROR_ALERT_STATE, 기본 logs/ 아래)
- 웹후크는 env Connection JSON 직접 파싱 (alerts.webhook_url_from_env), 1회 시도·예외 미전파
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from pathlib import Path

from airflow.listeners import hookimpl
from airflow.plugins_manager import AirflowPlugin

from alerts import make_payload, post_discord, webhook_url_from_env

log = logging.getLogger(__name__)

STATE_PATH = Path(os.getenv("IMPORT_ERROR_ALERT_STATE", "/opt/airflow/logs/.import_error_alert.json"))
REMIND_SEC = 6 * 3600
COLOR = 0xFB8C00


def should_send(filename: str, stacktrace: str, now: float | None = None, state_path: Path = STATE_PATH) -> bool:
    """
    Purpose: 6시간 중복 억제 판정 + 발송 기록
    Parameter:
        - filename: 오류 DAG 파일
        - stacktrace: 오류 내용
        - now: 현재 epoch 초 (기본 time.time())
        - state_path: 상태 파일 경로
    Input: 상태 파일
    Output: 보낼 차례면 True (+ 상태 파일 갱신, 6시간 지난 항목 정리)
    """
    now = time.time() if now is None else now
    key = hashlib.sha256(f"{filename}\0{stacktrace}".encode()).hexdigest()
    try:
        state = json.loads(state_path.read_text())
    except (OSError, ValueError):
        state = {}
    if key in state and now - state[key] < REMIND_SEC:
        return False
    state = {k: t for k, t in state.items() if now - t < REMIND_SEC} | {key: now}
    state_path.write_text(json.dumps(state))
    return True


def notify(filename: str, stacktrace: str, new: bool) -> None:
    """
    Purpose: import error 1건 알림 (리스너는 절대 예외를 내지 않음)
    Parameter:
        - filename: 오류 DAG 파일
        - stacktrace: 오류 내용 (뒤 1500자만)
        - new: 신규 오류 여부
    Input: 상태 파일, env 웹후크
    Output: 없음 (+ Discord 전송)
    """
    try:
        if not should_send(filename, stacktrace):
            return
        body = stacktrace[-1500:].replace("```", "'''")
        post_discord(
            make_payload({
                "title": f"🟠 DAG import error{' (신규)' if new else ''}: {Path(filename).name}",
                "description": f"```\n{body}\n```",
                "color": COLOR,
            }),
            url=webhook_url_from_env(),
            attempts=1,
        )
    except Exception:
        log.exception("[import_error_alert] 알림 실패 (무시)")


class _Listener:
    """dag-processor import error 훅 구현."""

    @hookimpl
    def on_new_dag_import_error(self, filename: str, stacktrace: str) -> None:
        """
        Purpose: 신규 import error 훅
        Parameter:
            - filename: 오류 파일
            - stacktrace: 오류 내용
        Input: 없음
        Output: 없음 (+ notify)
        """
        notify(filename, stacktrace, new=True)

    @hookimpl
    def on_existing_dag_import_error(self, filename: str, stacktrace: str) -> None:
        """
        Purpose: 기존 import error 재파싱 훅 (30초마다 호출 → 중복 억제 필수)
        Parameter:
            - filename: 오류 파일
            - stacktrace: 오류 내용
        Input: 없음
        Output: 없음 (+ notify)
        """
        notify(filename, stacktrace, new=False)


class ImportErrorAlertPlugin(AirflowPlugin):
    name = "import_error_alert"
    listeners = [_Listener()]
