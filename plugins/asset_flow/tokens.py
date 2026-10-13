"""
KIS 액세스 토큰 관리 (credential 단위 발급·저장·재사용)

- 진입점: token_for(발급 가능) / ensure_all / existing_token·has_valid_token(발급 안 함 — 시세·배당·환율·펀드 DAG)
- 저장: TOKEN_DIR/tokens.json {"format": 2, "credentials": {id: {token, fp, issued_at, expires_at}}} — 원자적 교체
- 유효: 키 지문(fp) 일치 + 만료까지 1시간 이상. 발급은 flock 잠금 후 재확인 → credential 별 격리
- TOKEN_ISSUE_ENABLED(기본 false): 금지면 HTTP·잠금 0회 (테스트·점검용 안전장치)
- Upbit 은 저장하지 않음 (UpbitApiClient 가 요청마다 서명)
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import logging
import os
import tempfile
import time
from collections.abc import Iterable, Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from asset_flow.config.kis import KIS
from asset_flow.runtime import kst_today
from asset_flow.secrets_config import Credential, SecretsConfig, credential_fingerprint

log = logging.getLogger(__name__)

DEFAULT_TOKEN_DIR = "/opt/airflow/data/tokens"
STORE_NAME = "tokens.json"
MIN_REMAINING = timedelta(hours=1)
RATE_LIMIT_CODE = "EGW00133"                     # KIS 1분 1회 발급 제한
RATE_LIMIT_WAIT = 61


class TokenIssueDisabledError(RuntimeError):
    """TOKEN_ISSUE_ENABLED=false 인데 발급이 필요해짐 (점검·테스트 환경 정상 차단)."""


def issue_enabled() -> bool:
    """
    Purpose: KIS 토큰 발급 허용 여부 (기본 금지 — 운영 .env 에서만 true)
    Parameter: 없음
    Input: env TOKEN_ISSUE_ENABLED
    Output: true/1/yes 일 때만 True
    """
    return os.getenv("TOKEN_ISSUE_ENABLED", "false").strip().lower() in ("1", "true", "yes")


def _issue_kis(cred: Credential) -> tuple[str, datetime]:
    """
    Purpose: KIS 토큰 발급 HTTP 호출 (최하위 함수 — 첫 줄에서 발급 플래그 재검사)
    Parameter:
        - cred: KIS credential
    Input: env TOKEN_ISSUE_ENABLED, KIS /oauth2/tokenP
    Output: (토큰, 만료 시각 UTC). 금지 모드면 HTTP 없이 TokenIssueDisabledError,
            EGW00133 이면 61초 대기 후 1회 재시도, 실패 시 RuntimeError (secrets 만료일 힌트 포함)
    """
    if not issue_enabled():
        raise TokenIssueDisabledError(f"[{cred.id}] TOKEN_ISSUE_ENABLED=false — KIS 토큰 발급 금지")
    body = {"grant_type": "client_credentials", "appkey": cred.app_key, "appsecret": cred.app_secret}
    for attempt in (1, 2):
        resp = requests.post(f"{KIS.BASE_URL}{KIS.PATHS['token']}", json=body, timeout=(5, 10))
        data = _json_or_empty(resp)
        if resp.ok and data.get("access_token"):
            expires_in = int(data.get("expires_in") or 86400)
            return data["access_token"], datetime.now(timezone.utc) + timedelta(seconds=expires_in)
        if attempt == 1 and data.get("error_code") == RATE_LIMIT_CODE:
            log.warning("[%s] %s(1분 제한) → %s초 대기 후 재시도", cred.id, RATE_LIMIT_CODE, RATE_LIMIT_WAIT)
            time.sleep(RATE_LIMIT_WAIT)
            continue
        break
    code = data.get("error_code") or "-"
    raise RuntimeError(f"[{cred.id}] KIS 토큰 발급 실패 (HTTP {resp.status_code}, {code}){_expiry_hint(cred)}")


def _json_or_empty(resp: requests.Response) -> dict:
    """
    Purpose: 응답 JSON 안전 파싱
    Parameter:
        - resp: HTTP 응답
    Input: 없음
    Output: dict (파싱 실패·비 dict 면 {})
    """
    try:
        data = resp.json()
    except ValueError:
        return {}
    return data if isinstance(data, dict) else {}


def _expiry_hint(cred: Credential) -> str:
    """
    Purpose: 발급 실패 메시지용 만료일 힌트 (키 만료가 원인인지 바로 보이게)
    Parameter:
        - cred: credential
    Input: 없음
    Output: " — secrets 기준 만료일 YYYY-MM-DD (D-n | 오늘 만료 | 만료 n일 경과)" 또는 "" (null)
    """
    if cred.expires_on is None:
        return ""
    d = (cred.expires_on - kst_today()).days
    label = f"D-{d}" if d > 0 else ("오늘 만료" if d == 0 else f"만료 {-d}일 경과")
    return f" — secrets 기준 만료일 {cred.expires_on} ({label})"


class TokenManager:
    """credential 단위 KIS 토큰 캐시."""

    def __init__(self, cfg: SecretsConfig, token_dir: str | os.PathLike | None = None):
        """
        Purpose: 설정·저장 경로 보관
        Parameter:
            - cfg: 검증된 secrets 설정
            - token_dir: 토큰 디렉터리 (None 이면 env TOKEN_DIR → /opt/airflow/data/tokens)
        Input: env TOKEN_DIR
        Output: 없음
        """
        self.cfg = cfg
        self.dir = Path(token_dir or os.getenv("TOKEN_DIR", DEFAULT_TOKEN_DIR))
        self.store = self.dir / STORE_NAME

    def token_for(self, cred_id: str) -> str:
        """
        Purpose: KIS credential 의 유효 토큰 반환 (필요 시 발급)
        Parameter:
            - cred_id: KIS credential id
        Input: tokens.json, (허용 모드) KIS 발급 API
        Output: 토큰 문자열 (+ 발급 시 tokens.json 갱신). 확보 불가면 RuntimeError
        """
        cred = self.cfg.credential(cred_id)
        if cred.provider != "kis":
            raise ValueError(f"[{cred_id}] KIS credential 만 토큰 관리 대상")
        fp = credential_fingerprint(cred)
        if token := self._valid_token(cred_id, fp):
            return token
        if not issue_enabled():
            raise TokenIssueDisabledError(f"[{cred_id}] 유효 토큰 없음, TOKEN_ISSUE_ENABLED=false")
        with self._lock():
            if token := self._valid_token(cred_id, fp):     # 잠금 후 재확인 (동시 발급 방지)
                return token
            token, expires_at = _issue_kis(cred)
            self._save(cred_id, token, fp, expires_at)
            log.info("[%s] KIS 토큰 발급·저장 (만료 %s)", cred_id, expires_at.isoformat())
            return token

    def has_valid_token(self, cred_id: str) -> bool:
        """
        Purpose: 유효 토큰 보유 여부 (발급하지 않음, 센서용)
        Parameter:
            - cred_id: KIS credential id
        Input: tokens.json
        Output: 키 지문 일치 + 만료 1시간 이상 남은 토큰이 있으면 True
        """
        return self._valid_token(cred_id, credential_fingerprint(self.cfg.credential(cred_id))) is not None

    def existing_token(self, cred_id: str) -> str:
        """
        Purpose: 이미 발급된 유효 토큰만 반환 (발급하지 않음 — 시세·배당 DAG 용, 발급 주체는 06:50 토큰 DAG)
        Parameter:
            - cred_id: KIS credential id
        Input: tokens.json
        Output: 토큰 문자열. 없으면 RuntimeError (토큰 DAG 복구 후 재실행)
        """
        if token := self._valid_token(cred_id, credential_fingerprint(self.cfg.credential(cred_id))):
            return token
        raise RuntimeError(f"[{cred_id}] 유효 토큰 없음 — auth_issue_api_tokens 확인 후 재실행 (이 DAG 는 발급 안 함)")

    def ensure_all(self, cred_ids: Iterable[str]) -> list[str]:
        """
        Purpose: 여러 credential 토큰 확보 — 실패는 그 id 에만 격리
        Parameter:
            - cred_ids: KIS credential id 목록
        Input: token_for 와 동일
        Output: 확보된 id 리스트 (성공분은 저장된 뒤, 실패 id 가 있으면 RuntimeError)
        """
        ok, failed = [], {}
        for cred_id in cred_ids:
            try:
                self.token_for(cred_id)
                ok.append(cred_id)
            except Exception as e:
                failed[cred_id] = str(e)
                log.error("[%s] 토큰 확보 실패: %s", cred_id, e)
        if failed:
            raise RuntimeError(f"토큰 확보 실패 {sorted(failed)}: " + " / ".join(failed.values()))
        return ok

    # ── 내부 ──
    def _read_store(self) -> dict:
        """
        Purpose: tokens.json 의 credentials 맵 읽기
        Parameter: 없음
        Input: TOKEN_DIR/tokens.json
        Output: {id: entry} (파일 없음·형식 불일치·손상이면 {})
        """
        try:
            data = json.loads(self.store.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {}
        except (OSError, ValueError):
            log.warning("tokens.json 손상 — 빈 저장소로 간주")
            return {}
        return data.get("credentials", {}) if data.get("format") == 2 else {}

    def _valid_token(self, cred_id: str, fp: str) -> str | None:
        """
        Purpose: 저장된 토큰이 지금 쓸 수 있는지 판정
        Parameter:
            - cred_id: credential id
            - fp: 현재 키 지문
        Input: tokens.json
        Output: 지문 일치 + 만료 1시간 이상 남았으면 토큰, 아니면 None
        """
        entry = self._read_store().get(cred_id)
        if not entry or entry.get("fp") != fp:
            return None
        if datetime.fromisoformat(entry["expires_at"]) - datetime.now(timezone.utc) < MIN_REMAINING:
            return None
        return entry["token"]

    @contextlib.contextmanager
    def _lock(self) -> Iterator[None]:
        """
        Purpose: 발급 구간 프로세스 간 배타 잠금
        Parameter: 없음
        Input: TOKEN_DIR/.lock
        Output: 컨텍스트 (종료 시 잠금 해제)
        """
        self.dir.mkdir(parents=True, exist_ok=True)
        with open(self.dir / ".lock", "a") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

    def _save(self, cred_id: str, token: str, fp: str, expires_at: datetime) -> None:
        """
        Purpose: tokens.json 에 한 credential 항목을 원자적으로 기록
        Parameter:
            - cred_id: credential id
            - token: 토큰
            - fp: 키 지문
            - expires_at: 만료 시각
        Input: tokens.json (기존 항목 유지)
        Output: 없음 (+ 임시 파일(0600) → fsync → os.replace, 실패 시 임시 파일 삭제)
        """
        creds = self._read_store()
        creds[cred_id] = {
            "token": token, "fp": fp,
            "issued_at": datetime.now(timezone.utc).isoformat(), "expires_at": expires_at.isoformat(),
        }
        fd, tmp = tempfile.mkstemp(dir=self.dir, prefix=".tokens.", suffix=".tmp")   # 0600
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump({"format": 2, "credentials": creds}, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.store)
        except BaseException:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(tmp)
            raise
