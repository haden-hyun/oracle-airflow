import json
import multiprocessing
import time
from datetime import datetime, timedelta, timezone

import jwt
import pytest

from asset_flow import tokens as tk
from asset_flow.clients import base_client
from asset_flow.clients.upbit_client import UpbitApiClient
from asset_flow.secrets_config import credential_fingerprint


class _Resp:
    def __init__(self, status=200, body=None):
        """
        Purpose: 가짜 HTTP 응답 생성
        Parameter:
            - status: HTTP 상태코드
            - body: 응답 본문
        Input: 없음
        Output: 없음
        """
        self.status_code, self.ok = status, status == 200
        self._body = body or {"access_token": "NEW", "expires_in": 86400}
        self.text = json.dumps(self._body)

    def json(self):
        """
        Purpose: 응답 본문 반환
        Parameter: 없음
        Input: 없음
        Output: dict
        """
        return self._body


@pytest.fixture
def calls(monkeypatch):
    """
    Purpose: KIS 발급 호출 캡처 (requests.post 대체)
    Parameter:
        - monkeypatch: pytest monkeypatch
    Input: 없음
    Output: 호출된 appkey 리스트
    """
    log = []

    def fake_post(url, json=None, timeout=None):
        """
        Purpose: KIS 발급 응답 흉내
        Parameter:
            - url: 요청 URL
            - json: 요청 본문
            - timeout: 타임아웃
        Input: 없음
        Output: 가짜 응답
        """
        log.append(json["appkey"])
        return _Resp(body={"access_token": f"TOK-{json['appkey']}", "expires_in": 86400})

    monkeypatch.setattr(tk.requests, "post", fake_post)
    return log


def _store(dirpath, cfg, **entries):
    """
    Purpose: tokens.json 작성
    Parameter:
        - dirpath: 토큰 디렉터리
        - cfg: secrets 설정
        - entries: id=(토큰, 남은시간h, 지문일치)
    Input: 없음
    Output: 없음 (파일 쓰기)
    """
    creds = {}
    for cid, (token, hours, fp_ok) in entries.items():
        fp = credential_fingerprint(cfg.credential(cid)) if fp_ok else "stale"
        exp = datetime.now(timezone.utc) + timedelta(hours=hours)
        creds[cid] = {"token": token, "fp": fp, "issued_at": "x", "expires_at": exp.isoformat()}
    (dirpath / "tokens.json").write_text(json.dumps({"format": 2, "credentials": creds}))


def test_2_nothing_issue_disabled(tmp_path, cfg, calls, monkeypatch):
    monkeypatch.setenv("TOKEN_ISSUE_ENABLED", "false")
    with pytest.raises(RuntimeError, match="TOKEN_ISSUE_ENABLED=false"):
        tk.TokenManager(cfg, tmp_path).token_for("KIS_ISA")
    with pytest.raises(RuntimeError, match="발급 금지"):
        tk._issue_kis(cfg.credential("KIS_ISA"))                  # 최하위 함수도 차단
    assert calls == [] and list(tmp_path.iterdir()) == []         # 잠금 파일도 없음


def test_3_reissue_only_mismatched(tmp_path, cfg, calls, monkeypatch):
    monkeypatch.setenv("TOKEN_ISSUE_ENABLED", "true")
    _store(tmp_path, cfg, KIS_STOCK=("KEEP", 20, True), KIS_ISA=("STALE", 20, False))
    tm = tk.TokenManager(cfg, tmp_path)
    assert tm.token_for("KIS_STOCK") == "KEEP"
    assert tm.token_for("KIS_ISA") == "TOK-<KIS_APPKEY_ISA>"
    assert tm.token_for("KIS_ISA") == "TOK-<KIS_APPKEY_ISA>"
    assert calls == ["<KIS_APPKEY_ISA>"]
    saved = json.loads((tmp_path / "tokens.json").read_text())["credentials"]
    assert saved["KIS_STOCK"]["token"] == "KEEP"
    assert saved["KIS_ISA"]["fp"] == credential_fingerprint(cfg.credential("KIS_ISA"))


def test_3b_near_expiry_reissued(tmp_path, cfg, calls, monkeypatch):
    monkeypatch.setenv("TOKEN_ISSUE_ENABLED", "true")
    _store(tmp_path, cfg, KIS_STOCK=("OLD", 0.5, True))                # 만료 1시간 미만 → 재발급
    assert tk.TokenManager(cfg, tmp_path).token_for("KIS_STOCK") == "TOK-<KIS_APPKEY_STOCK>"


def _worker(cfg, path):
    """
    Purpose: 다른 프로세스에서 token_for 호출 (flock 검증)
    Parameter:
        - cfg: secrets 설정
        - path: 파일 경로
    Input: 없음
    Output: 없음 (결과를 큐에 넣음)
    """
    tk.TokenManager(cfg, path).token_for("KIS_STOCK")


def test_4_concurrent_processes_issue_once(tmp_path, cfg, monkeypatch):
    monkeypatch.setenv("TOKEN_ISSUE_ENABLED", "true")
    counter = tmp_path / "count"

    def slow_post(url, json=None, timeout=None):
        """
        Purpose: 느린 KIS 발급 응답 (동시성 검증)
        Parameter:
            - url: 요청 URL
            - json: 요청 본문
            - timeout: 타임아웃
        Input: 없음
        Output: 가짜 응답
        """
        with open(counter, "a") as f:
            f.write("x")
        time.sleep(0.5)
        return _Resp()

    monkeypatch.setattr(tk.requests, "post", slow_post)
    store = tmp_path / "tokens"
    ctx = multiprocessing.get_context("fork")
    procs = [ctx.Process(target=_worker, args=(cfg, store)) for _ in range(2)]
    for p in procs:
        p.start()
    for p in procs:
        p.join(10)
    assert [p.exitcode for p in procs] == [0, 0]
    assert counter.read_text() == "x"


def test_5_failure_isolated(tmp_path, cfg, monkeypatch):
    monkeypatch.setenv("TOKEN_ISSUE_ENABLED", "true")

    def post(url, json=None, timeout=None):
        """
        Purpose: KIS 응답 시퀀스 재생
        Parameter:
            - url: 요청 URL
            - json: 요청 본문
            - timeout: 타임아웃
        Input: 없음
        Output: 가짜 응답
        """
        return _Resp(403, {"error_code": "EGW00002"}) if json["appkey"] == "<KIS_APPKEY_ISA>" else _Resp()

    monkeypatch.setattr(tk.requests, "post", post)
    with pytest.raises(RuntimeError) as ei:
        tk.TokenManager(cfg, tmp_path).ensure_all(["KIS_STOCK", "KIS_ISA", "KIS_PENSION_DEDUCTIBLE"])
    assert "['KIS_ISA']" in str(ei.value) and "만료일 2027-03-31" in str(ei.value)
    saved = json.loads((tmp_path / "tokens.json").read_text())["credentials"]
    assert set(saved) == {"KIS_STOCK", "KIS_PENSION_DEDUCTIBLE"}


def test_5b_rate_limit_waits_once(tmp_path, cfg, monkeypatch):
    monkeypatch.setenv("TOKEN_ISSUE_ENABLED", "true")
    responses = [_Resp(403, {"error_code": "EGW00133"}), _Resp()]
    monkeypatch.setattr(tk.requests, "post", lambda *a, **k: responses.pop(0))
    slept = []
    monkeypatch.setattr(tk.time, "sleep", slept.append)
    assert tk.TokenManager(cfg, tmp_path).token_for("KIS_STOCK") == "NEW"
    assert slept == [61]


def test_6_disabled_reuses_valid_store_without_writes(tmp_path, cfg, calls, monkeypatch):
    # 발급 금지 모드: 유효 토큰은 재사용, 파일·잠금 쓰기 없음
    monkeypatch.delenv("TOKEN_ISSUE_ENABLED", raising=False)
    _store(tmp_path, cfg, KIS_ISA=("ISA", 20, True))
    before = {p.name: p.read_text() for p in tmp_path.iterdir()}
    assert tk.TokenManager(cfg, tmp_path).ensure_all(["KIS_ISA"]) == ["KIS_ISA"]
    assert {p.name: p.read_text() for p in tmp_path.iterdir()} == before and calls == []


def test_7_upbit_signs_each_request(monkeypatch):
    seen = []

    class R:
        def raise_for_status(self):
            """
            Purpose: 정상 응답 흉내
            Parameter: 없음
            Input: 없음
            Output: 없음
            """
            pass

        def json(self):
            """
            Purpose: 응답 본문 반환
            Parameter: 없음
            Input: 없음
            Output: dict
            """
            return []

    def fake_request(method, url, headers=None, **kw):
        """
        Purpose: Upbit 요청 캡처
        Parameter:
            - method: HTTP 메서드
            - url: 요청 URL
            - headers: 요청 헤더
            - kw: 키워드 인자
        Input: 없음
        Output: 가짜 응답
        """
        seen.append((url, headers))
        return R()

    monkeypatch.setattr(base_client.requests, "request", fake_request)
    client = UpbitApiClient("ACCESS-KEY", "SECRET-KEY-0123456789-0123456789-XY")
    client.get_balance()
    client.get_balance()
    client.get_market_codes()
    secret = "SECRET-KEY-0123456789-0123456789-XY"
    nonces = [jwt.decode(h["Authorization"].split()[1], secret, algorithms=["HS256"])["nonce"] for _, h in seen[:2]]
    assert nonces[0] != nonces[1]
    assert "Authorization" not in seen[2][1]                       # 공개 엔드포인트


def test_upbit_not_managed(tmp_path, cfg):
    with pytest.raises(ValueError):
        tk.TokenManager(cfg, tmp_path).token_for("UPBIT")


def test_issue_failure_message_has_expiry_hint(tmp_path, cfg, monkeypatch):
    monkeypatch.setenv("TOKEN_ISSUE_ENABLED", "true")
    monkeypatch.setattr(tk.requests, "post", lambda *a, **k: _Resp(403, {"error_code": "EGW00121"}))
    with pytest.raises(RuntimeError, match=r"HTTP 403, EGW00121\) — secrets 기준 만료일"):
        tk.TokenManager(cfg, tmp_path).token_for("KIS_ISA")
    assert not list(tmp_path.glob(".tokens.*.tmp"))                # 임시 파일 잔존 없음


def test_has_valid_token_never_issues(tmp_path, cfg, calls, monkeypatch):
    # 환율 DAG 센서용: 유효 토큰만 True, 발급 시도 없음
    monkeypatch.setenv("TOKEN_ISSUE_ENABLED", "true")
    _store(tmp_path, cfg, KIS_STOCK=("OK", 20, True), KIS_ISA=("NEAR", 0.5, True))
    tm = tk.TokenManager(cfg, tmp_path)
    assert tm.has_valid_token("KIS_STOCK") is True
    assert tm.has_valid_token("KIS_ISA") is False
    assert calls == []


def test_existing_token_never_issues(tmp_path, cfg, calls, monkeypatch):
    # 시세·배당 DAG 용: 발급 허용 환경이어도 발급하지 않고 실패
    monkeypatch.setenv("TOKEN_ISSUE_ENABLED", "true")
    _store(tmp_path, cfg, KIS_STOCK=("OK", 20, True), KIS_ISA=("NEAR", 0.5, True))
    tm = tk.TokenManager(cfg, tmp_path)
    assert tm.existing_token("KIS_STOCK") == "OK"
    with pytest.raises(RuntimeError, match="유효 토큰 없음"):
        tm.existing_token("KIS_ISA")
    assert calls == []
