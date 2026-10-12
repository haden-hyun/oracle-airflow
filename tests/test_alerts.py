import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

import alerts
import import_error_alert as iea


class _Resp:
    def __init__(self, status, body=None):
        """
        Purpose: 가짜 HTTP 응답 생성
        Parameter:
            - status: HTTP 상태코드
            - body: 응답 본문
        Input: 없음
        Output: 없음
        """
        self.status_code, self._body, self.headers, self.text = status, body or {}, {}, ""

    def json(self):
        """
        Purpose: 응답 본문 반환
        Parameter: 없음
        Input: 없음
        Output: dict
        """
        return self._body


@pytest.fixture
def sent(monkeypatch):
    """
    Purpose: Discord 전송 캡처 (requests.post 대체)
    Parameter:
        - monkeypatch: pytest monkeypatch
    Input: 없음
    Output: 캡처 리스트
    """
    out = []
    monkeypatch.setattr(alerts, "post_discord", lambda payload, **kw: out.append(payload) or True)
    monkeypatch.setattr(alerts, "_run_url", lambda *a: "http://x")
    return out


def test_channel_selection(monkeypatch):
    monkeypatch.delenv("ASSET_FLOW_ENV", raising=False)
    assert alerts.conn_id() == "discord_alert_test"
    monkeypatch.setenv("ASSET_FLOW_ENV", "test")
    assert alerts.conn_id() == "discord_alert_test"
    monkeypatch.setenv("ASSET_FLOW_ENV", "prod")
    assert alerts.conn_id() == "discord_alert"


def test_webhook_url_from_env(monkeypatch):
    monkeypatch.delenv("ASSET_FLOW_ENV", raising=False)
    monkeypatch.setenv("AIRFLOW_CONN_DISCORD_ALERT_TEST", json.dumps({"conn_type": "http", "password": "https://hook"}))
    monkeypatch.setenv("AIRFLOW_CONN_DISCORD_ALERT", json.dumps({"conn_type": "http", "password": "https://prod"}))
    assert alerts.webhook_url_from_env() == "https://hook"
    monkeypatch.setenv("AIRFLOW_CONN_DISCORD_ALERT_TEST", "not-json")
    assert alerts.webhook_url_from_env() is None


def test_finalize_limits():
    embed = {"title": "t" * 300, "description": "d" * 5000,
             "fields": [{"name": f"n{i}", "value": "v" * 1100} for i in range(40)]}
    out = alerts.make_payload(embed)
    e = out["embeds"][0]
    assert len(e["title"]) <= 256 and len(e["fields"]) <= 25
    assert all(len(f["value"]) <= 1024 for f in e["fields"])
    assert alerts._embed_size(e) <= 6000
    assert out["allowed_mentions"] == {"parse": []} and "username" not in out


def test_run_dt_none_safe():
    assert alerts._run_dt({}) is None
    ra = datetime(2026, 10, 11, 21, 50, tzinfo=timezone.utc)
    assert alerts._run_dt({"dag_run": SimpleNamespace(logical_date=None, run_after=ra)}) == ra
    assert alerts._kst_label(ra) == "2026-10-12 06:50 KST"
    assert alerts._kst_label(None) == "-"


def _ctx(prev, run_type="scheduled", logical=True):
    """
    Purpose: 콜백용 가짜 컨텍스트
    Parameter:
        - prev: 직전 run
        - run_type: run 유형
        - logical: logical_date 존재 여부
    Input: 없음
    Output: context dict
    """
    ti = SimpleNamespace(get_previous_dagrun=lambda: prev)
    dr = SimpleNamespace(run_type=run_type, logical_date=datetime(2026, 1, 1) if logical else None)
    return {"ti": ti, "dag_run": dr}


def test_was_previous_run_failed():
    failed = SimpleNamespace(run_type="scheduled", state="failed")
    assert alerts._was_previous_run_failed(_ctx(failed)) is True
    assert alerts._was_previous_run_failed(_ctx(SimpleNamespace(run_type="manual", state="failed"))) is False
    assert alerts._was_previous_run_failed(_ctx(SimpleNamespace(run_type="scheduled", state="success"))) is False
    assert alerts._was_previous_run_failed(_ctx(failed, logical=False)) is False
    assert alerts._was_previous_run_failed(_ctx(None)) is False


def test_failure_callback_shows_map_name(sent):
    ti = SimpleNamespace(dag_id="d", task_id="collect_account", run_id="r", try_number=4, max_tries=3,
                         rendered_map_index="KIS_ISA")
    alerts.discord_failure_callback({"ti": ti, "exception": ValueError("boom")})
    fields = {f["name"]: f["value"] for f in sent[0]["embeds"][0]["fields"]}
    assert fields["계좌/Map"] == "`KIS_ISA`" and fields["시도"] == "4 / 4"


def test_callbacks_never_raise(monkeypatch):
    def boom(*a, **k):
        """
        Purpose: 항상 예외를 내는 대체 함수
        Parameter:
            - a: 무시되는 위치 인자
            - k: 키워드 인자
        Input: 없음
        Output: 없음 (RuntimeError)
        """
        raise RuntimeError("network down")

    monkeypatch.setattr(alerts, "post_discord", boom)
    monkeypatch.setattr(alerts, "_run_url", lambda *a: "http://x")
    alerts.discord_failure_callback({"ti": SimpleNamespace(task_id="t"), "exception": ValueError("x")})
    alerts.discord_failure_callback({})                                   # 빈 컨텍스트
    alerts.discord_recovery_callback(_ctx(SimpleNamespace(run_type="scheduled", state="failed")))
    broken_ti = SimpleNamespace(get_previous_dagrun=boom)
    alerts.discord_recovery_callback({"ti": broken_ti, "dag_run": _ctx(None)["dag_run"]})


def test_recovery_sent_only_after_failure(sent):
    alerts.discord_recovery_callback(_ctx(SimpleNamespace(run_type="scheduled", state="success")))
    assert sent == []
    alerts.discord_recovery_callback(_ctx(SimpleNamespace(run_type="scheduled", state="failed")))
    assert sent and sent[0]["embeds"][0]["title"].startswith("🟢")


def test_post_discord_retry_policy(monkeypatch):
    monkeypatch.setattr(alerts.time, "sleep", lambda s: None)
    seq = [_Resp(429, {"retry_after": 0.1}), _Resp(500), _Resp(204)]
    calls = []
    monkeypatch.setattr(alerts.requests, "post", lambda *a, **k: calls.append(1) or seq.pop(0))
    assert alerts.post_discord({"embeds": []}, url="https://hook") is True and len(calls) == 3
    calls.clear()
    monkeypatch.setattr(alerts.requests, "post", lambda *a, **k: calls.append(1) or _Resp(404))
    assert alerts.post_discord({"embeds": []}, url="https://hook") is False and len(calls) == 1


def test_post_discord_without_connection(monkeypatch):
    monkeypatch.setattr(alerts, "_webhook_url", lambda: None)
    assert alerts.post_discord({"embeds": []}) is False


def test_import_error_dedup(tmp_path):
    state = tmp_path / "s.json"
    assert iea.should_send("a.py", "err", now=1000, state_path=state) is True
    assert iea.should_send("a.py", "err", now=1030, state_path=state) is False
    assert iea.should_send("a.py", "other", now=1030, state_path=state) is True
    assert iea.should_send("a.py", "err", now=1000 + 6 * 3600, state_path=state) is True


def test_import_error_notify_never_raises(monkeypatch):
    monkeypatch.setattr(iea, "should_send", lambda *a: (_ for _ in ()).throw(OSError("ro")))
    iea.notify("a.py", "err", new=True)


def test_was_previous_run_failed_with_enums():
    # dag-processor 는 enum, API 조회값은 문자열 → 섞여도 같은 값으로 판정
    from airflow.utils.state import DagRunState
    from airflow.utils.types import DagRunType

    prev = SimpleNamespace(run_id="r1", run_type="manual", state=DagRunState.FAILED)
    assert alerts._was_previous_run_failed(_ctx(prev, run_type=DagRunType.MANUAL)) is True
    prev_ok = SimpleNamespace(run_id="r1", run_type=DagRunType.MANUAL, state="success")
    assert alerts._was_previous_run_failed(_ctx(prev_ok, run_type="manual")) is False
