"""
Discord 알림 콜백 (Airflow 3.x 전용)

- 태스크 실패: default_args={"on_failure_callback": discord_failure_callback}
- DAG 복구: @dag(on_success_callback=discord_recovery_callback) — 직전 같은 유형 run 이 실패였을 때만
- 공개 전송: post_discord(payload) / make_payload(embed) — 만료·휴장일·import error 알림이 재사용
- 웹후크: env ASSET_FLOW_ENV=prod → Connection discord_alert, 그 외 → discord_alert_test (password = 전체 URL)
"""

from __future__ import annotations

import json
import logging
import os
import socket
import time
import traceback
from datetime import datetime, timezone
from urllib.parse import quote

import pendulum
import requests
from airflow.sdk import BaseHook, conf

log = logging.getLogger(__name__)

KST = pendulum.timezone("Asia/Seoul")
COLOR_FAIL, COLOR_OK = 0xE53935, 0x43A047
# Discord embed 한도 (Embed Limits)
LIM_TITLE, LIM_DESC, LIM_FIELDS, LIM_NAME, LIM_VALUE, LIM_FOOTER, LIM_TOTAL = 256, 4096, 25, 256, 1024, 2048, 6000
ERROR_BUDGET = 1500                              # 에러 본문 상한 (모바일 가독성 + 6000자 보호)
HTTP_TIMEOUT = (3, 5)
MAX_ATTEMPTS = 3
MAX_TOTAL_WAIT = 10.0                            # 429 대기 총합 상한 (워커 슬롯 점유 방지)


# ── 웹후크 선택·전송 ──
def conn_id() -> str:
    """
    Purpose: 환경별 Discord Connection id 선택
    Parameter: 없음
    Input: env ASSET_FLOW_ENV
    Output: prod 면 "discord_alert", 그 외 "discord_alert_test"
    """
    return "discord_alert" if os.getenv("ASSET_FLOW_ENV") == "prod" else "discord_alert_test"


def webhook_url_from_env() -> str | None:
    """
    Purpose: Airflow API 없이 env Connection JSON 에서 웹후크 URL 직접 파싱 (import error 리스너용)
    Parameter: 없음
    Input: env AIRFLOW_CONN_DISCORD_ALERT[_TEST] ({"conn_type": "http", "password": "<URL>"})
    Output: URL 또는 None (미설정·형식 오류)
    """
    try:
        return json.loads(os.environ[f"AIRFLOW_CONN_{conn_id().upper()}"]).get("password") or None
    except (KeyError, ValueError, AttributeError):
        return None


def _webhook_url() -> str | None:
    """
    Purpose: Connection 에서 웹후크 URL 조회
    Parameter: 없음
    Input: Airflow Connection discord_alert[_test]
    Output: URL 또는 None (Connection 없음 등)
    """
    try:
        return BaseHook.get_connection(conn_id()).password or None
    except Exception:
        return None


def post_discord(payload: dict, url: str | None = None, attempts: int = MAX_ATTEMPTS) -> bool:
    """
    Purpose: Discord 웹후크 전송 (예외를 밖으로 내지 않음, URL 은 로그에 남기지 않음)
    Parameter:
        - payload: 웹후크 JSON (make_payload 결과)
        - url: 웹후크 URL (None 이면 Connection 조회)
        - attempts: 최대 시도 횟수 (기본 3)
    Input: Discord 웹후크, Connection discord_alert[_test]
    Output: 전송 성공 여부. 429 는 retry_after 대기(총 10초), 5xx·네트워크는 재시도, 401/403/404 는 재시도 금지
    """
    target = "지정 URL" if url else conn_id()
    url = url or _webhook_url()
    if not url:
        log.warning("[alerts] Connection '%s' 미설정 → Discord 알림 생략", conn_id())
        return False
    waited = 0.0
    for attempt in range(1, attempts + 1):
        try:
            resp = requests.post(url, params={"wait": "true"}, json=payload, timeout=HTTP_TIMEOUT)
        except requests.RequestException as e:
            log.warning("[alerts] Discord 전송 예외(%s/%s): %s", attempt, attempts, type(e).__name__)
            time.sleep(min(2**attempt, 4))
            continue
        status = resp.status_code
        if status in (200, 204):
            log.info("[alerts] Discord 전송 완료 (%s)", target)
            return True
        if status in (401, 403, 404):
            log.error("[alerts] Discord 웹후크 무효(%s) — Connection '%s' 갱신 필요", status, conn_id())
            return False
        if status == 429:
            try:
                retry_after = float(resp.json().get("retry_after", 1.0))
            except ValueError:
                retry_after = float(resp.headers.get("Retry-After", 1.0))
            if waited + retry_after > MAX_TOTAL_WAIT:
                break
            waited += retry_after
            time.sleep(retry_after)
            continue
        if status >= 500:
            time.sleep(min(2**attempt, 4))
            continue
        log.error("[alerts] Discord 전송 실패 %s: %s", status, resp.text[:300])   # 400: 한도·형식 오류
        return False
    log.error("[alerts] Discord 전송 포기 (재시도 소진)")
    return False


# ── embed 조립 ──
def _trunc(s: object, n: int) -> str:
    """
    Purpose: 문자열 길이 제한
    Parameter:
        - s: 값
        - n: 최대 길이
    Input: 없음
    Output: n 자 이하 문자열 (잘리면 끝에 …)
    """
    s = "" if s is None else str(s)
    return s if len(s) <= n else s[: n - 1] + "…"


def field(name: object, value: object, inline: bool = True) -> dict:
    """
    Purpose: embed 필드 1개 생성
    Parameter:
        - name: 필드 이름
        - value: 필드 값 (빈 값은 "-")
        - inline: 가로 배치 여부 (기본 True)
    Input: 없음
    Output: {"name", "value", "inline"}
    """
    return {"name": _trunc(name, LIM_NAME), "value": _trunc(value, LIM_VALUE) or "-", "inline": inline}


def _embed_size(e: dict) -> int:
    """
    Purpose: Discord 총합 한도 계산 대상 글자 수
    Parameter:
        - e: embed
    Input: 없음
    Output: title+description+footer+fields 글자 수
    """
    return (len(e["title"]) + len(e["description"]) + len(e.get("footer", {}).get("text", ""))
            + sum(len(f["name"]) + len(f["value"]) for f in e["fields"]))


def _finalize(embed: dict) -> dict:
    """
    Purpose: embed 한도 강제 (항목별 길이, 필드 25개, 총 6000자)
    Parameter:
        - embed: 원본 embed (변경됨)
    Input: 없음
    Output: 한도 안의 embed — 초과분은 description 부터 줄이고, 그래도 넘치면 뒤쪽 필드부터 버림
    """
    embed["title"] = _trunc(embed.get("title"), LIM_TITLE)
    embed["description"] = _trunc(embed.get("description"), LIM_DESC)
    fields = embed.get("fields", [])[:LIM_FIELDS]
    embed["fields"] = [field(f["name"], f["value"], f.get("inline", True)) for f in fields]
    if "footer" in embed:
        embed["footer"] = {"text": _trunc(embed["footer"].get("text"), LIM_FOOTER)}
    over = _embed_size(embed) - LIM_TOTAL
    if over > 0:
        embed["description"] = _trunc(embed["description"], max(len(embed["description"]) - over, 50))
    while _embed_size(embed) > LIM_TOTAL and embed["fields"]:
        embed["fields"].pop()
    return embed


def make_payload(embed: dict) -> dict:
    """
    Purpose: 웹후크 payload 생성 (username 은 웹후크 설정 이름 사용, 멘션 차단)
    Parameter:
        - embed: embed dict
    Input: 호스트 이름 (footer)
    Output: {"embeds": [한도 적용 embed], "allowed_mentions": {"parse": []}}
    """
    embed.setdefault("footer", {"text": f"asset-flow · {socket.gethostname()}"})
    embed.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
    return {"embeds": [_finalize(embed)], "allowed_mentions": {"parse": []}}


# ── 컨텍스트 → 표시값 (None 안전) ──
def _run_dt(context: dict):
    """
    Purpose: 표시용 run 기준 시각
    Parameter:
        - context: 콜백 컨텍스트
    Input: 없음
    Output: logical_date → dag_run.run_after → start_date 순 첫 값, 없으면 None
    """
    dr = context.get("dag_run")
    for value in (context.get("logical_date"), getattr(dr, "logical_date", None),
                  getattr(dr, "run_after", None), getattr(dr, "start_date", None)):
        if value is not None:
            return value
    return None


def _kst_label(value) -> str:
    """
    Purpose: 시각을 KST 문자열로
    Parameter:
        - value: datetime 또는 None
    Input: 없음
    Output: "YYYY-MM-DD HH:MM KST" 또는 "-"
    """
    return "-" if value is None else pendulum.instance(value).in_timezone(KST).strftime("%Y-%m-%d %H:%M KST")


def _run_url(dag_id: str, run_id: str, task_id: str | None = None) -> str:
    """
    Purpose: 새 UI run/task 링크 생성
    Parameter:
        - dag_id: DAG id
        - run_id: run id
        - task_id: task id (None 이면 run 화면)
    Input: [api] base_url
    Output: {base_url}/dags/{dag_id}/runs/{run_id}[/tasks/{task_id}]
    """
    base = conf.get("api", "base_url", fallback="http://localhost:8080").rstrip("/")
    url = f"{base}/dags/{quote(dag_id, safe='')}/runs/{quote(run_id, safe='')}"
    return f"{url}/tasks/{quote(task_id, safe='')}" if task_id else url


def _error_text(context: dict) -> str:
    """
    Purpose: 실패 원인 요약 (traceback 마지막 8줄)
    Parameter:
        - context: 콜백 컨텍스트
    Input: 없음
    Output: 1500자 이하 문자열 (코드블록 깨짐 방지 치환)
    """
    exc = context.get("exception")
    if exc is None:
        return str(context.get("reason") or "에러 정보 없음")
    if isinstance(exc, BaseException):
        text = "\n".join("".join(traceback.format_exception(exc)).strip().splitlines()[-8:])
    else:
        text = str(exc)
    return _trunc(text.replace("```", "'''"), ERROR_BUDGET)


def _was_previous_run_failed(context: dict) -> bool:
    """
    Purpose: 직전 run 이 같은 run_type 이고 실패였는지 (Task SDK API, ORM 미사용)
    Parameter:
        - context: 콜백 컨텍스트 (ti, dag_run)
    Input: Airflow API (ti.get_previous_dagrun)
    Output: True 면 복구 알림 대상. logical_date 없음·조회 실패는 False
    """
    ti = context.get("ti") or context.get("task_instance")
    dr = context.get("dag_run")
    if dr is None or getattr(dr, "logical_date", None) is None:
        log.info("[alerts] logical_date 없는 run → 복구 판정 생략")
        return False
    if ti is None:
        log.info("[alerts] 콜백 컨텍스트에 ti 없음 → 복구 판정 생략")
        return False
    try:
        prev = ti.get_previous_dagrun()
    except Exception:
        log.exception("[alerts] 이전 DagRun 조회 실패")
        return False
    # enum·문자열 혼재 → 값으로 정규화
    prev_type, cur_type = _enum_value(getattr(prev, "run_type", None)), _enum_value(getattr(dr, "run_type", None))
    failed = prev is not None and prev_type == cur_type and _enum_value(getattr(prev, "state", None)) == "failed"
    log.info("[alerts] 복구 판정: 직전 run=%s (%s, %s) / 현재 %s → %s", getattr(prev, "run_id", None), prev_type,
             _enum_value(getattr(prev, "state", None)), cur_type, "알림" if failed else "생략")
    return failed


def _enum_value(v) -> str:
    """
    Purpose: enum·문자열을 비교 가능한 소문자 값으로 정규화
    Parameter:
        - v: DagRunType/DagRunState enum 또는 문자열 (None 가능)
    Input: 없음
    Output: 소문자 문자열 (예: "manual", "failed"), None 이면 ""
    """
    return str(getattr(v, "value", v) or "").lower()


# ── 콜백 ──
def discord_failure_callback(context: dict) -> None:
    """
    Purpose: 태스크 최종 실패 알림 (매핑 태스크면 map 이름 표시)
    Parameter:
        - context: 태스크 콜백 컨텍스트
    Input: 컨텍스트, Connection discord_alert[_test]
    Output: 없음 (+ Discord 전송). 내부 오류는 로그만 — 예외를 밖으로 내지 않음
    """
    try:
        ti = context.get("ti") or context.get("task_instance")
        dr = context.get("dag_run")
        dag_id = getattr(context.get("dag"), "dag_id", None) or getattr(ti, "dag_id", "?")
        task_id = getattr(ti, "task_id", "-")
        run_id = context.get("run_id") or getattr(ti, "run_id", "-")
        fields = [
            field("Task", f"`{task_id}`"),
            field("시도", f"{getattr(ti, 'try_number', '?')} / {(getattr(ti, 'max_tries', 0) or 0) + 1}"),
            field("실행 기준", _kst_label(_run_dt(context))),
            field("Run 유형", getattr(dr, "run_type", "-")),
        ]
        if map_name := getattr(ti, "rendered_map_index", None):
            fields.insert(1, field("계좌/Map", f"`{map_name}`"))
        fields.append(field("Run ID", f"`{run_id}`", inline=False))
        post_discord(make_payload({
            "title": f"🔴 DAG 실패: {dag_id}",
            "url": _run_url(dag_id, run_id, task_id),
            "color": COLOR_FAIL,
            "description": f"**에러 메시지**\n```\n{_error_text(context)}\n```",
            "fields": fields,
        }))
    except Exception:
        log.exception("[alerts] discord_failure_callback 내부 오류 (알림 생략)")


def discord_recovery_callback(context: dict) -> None:
    """
    Purpose: DAG 성공 시 직전 같은 유형 run 이 실패였으면 복구 알림
    Parameter:
        - context: DAG 콜백 컨텍스트
    Input: 컨텍스트, Airflow API(이전 run), Connection discord_alert[_test]
    Output: 없음 (+ Discord 전송). 내부 오류는 로그만 — 예외를 밖으로 내지 않음
    """
    try:
        if not _was_previous_run_failed(context):
            return
        dr = context.get("dag_run")
        dag_id = getattr(context.get("dag"), "dag_id", None) or getattr(dr, "dag_id", "?")
        run_id = context.get("run_id") or getattr(dr, "run_id", "-")
        post_discord(make_payload({
            "title": f"🟢 DAG 복구: {dag_id}",
            "url": _run_url(dag_id, run_id),
            "color": COLOR_OK,
            "description": "이전 실패 → 현재 성공 ✅",
            "fields": [
                field("실행 기준", _kst_label(_run_dt(context))),
                field("Run 유형", getattr(dr, "run_type", "-")),
                field("Run ID", f"`{run_id}`", inline=False),
            ],
        }))
    except Exception:
        log.exception("[alerts] discord_recovery_callback 내부 오류 (알림 생략)")
