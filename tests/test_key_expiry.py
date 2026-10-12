from datetime import date, timedelta

import pytest
import requests

from alerts import LIM_FIELDS, LIM_TOTAL, _embed_size, make_payload
from asset_flow.ops import key_expiry as ke

MON = date(2026, 10, 12)
TUE = date(2026, 10, 13)


@pytest.fixture
def rule(app):
    """
    Purpose: 기본 만료 알림 규칙
    Parameter:
        - app: 비밀 아닌 설정
    Input: 없음
    Output: Rule
    """
    return app.credential_check


def _item(d, as_of, rule):
    """
    Purpose: 판정용 ExpiryItem 생성
    Parameter:
        - d: 기준일
        - as_of: 기준일
        - rule: 알림 규칙
    Input: 없음
    Output: ExpiryItem
    """
    return ke.ExpiryItem("KIS_ISA", "kis", as_of + timedelta(days=d), days_left=d, level=ke.level_of(d, rule))


@pytest.mark.parametrize("d,as_of,expected", [
    (15, TUE, False), (14, TUE, True), (13, TUE, False), (10, MON, True), (7, TUE, True), (5, TUE, False),
    (4, TUE, False), (3, TUE, True), (1, TUE, True), (0, TUE, True), (-1, TUE, True), (-30, TUE, True),
    (20, MON, False), (15, MON, False),
])
def test_should_notify_boundaries(d, as_of, expected, rule):
    assert ke.should_notify(_item(d, as_of, rule), as_of, rule) is expected


def test_level_boundaries(rule):
    L = ke.Level
    assert [ke.level_of(x, rule) for x in (15, 14, 8, 7, 4, 3, 0, -1, None)] == [
        L.OK, L.INFO, L.INFO, L.WARN, L.WARN, L.CRIT, L.CRIT, L.EXPIRED, L.OK]


def test_evaluate_excludes_null_and_unused(cfg, rule):
    cfg2 = cfg.model_copy(update={"credentials": [
        c.model_copy(update={"expires_on": None}) if c.id == "UPBIT" else c for c in cfg.credentials]})
    names = {i.name for i in ke.evaluate(cfg2, MON, rule)}
    assert "UPBIT" not in names and "KIS_ISA" in names
    isa = next(i for i in ke.evaluate(cfg2, MON, rule) if i.name == "KIS_ISA")
    assert isa.used_by == ("KIS_ISA", "KIS_CMA")


def test_select_bundles_and_sorts(rule):
    items = [_item(20, TUE, rule), _item(10, TUE, rule), _item(3, TUE, rule)]
    assert ke.select(items[:2], TUE, rule) == []                       # 트리거 없음
    picked = ke.select(items, TUE, rule)
    assert [i.days_left for i in picked] == [3, 10]                    # OK 제외, 심각도순


def test_embed_within_discord_limits():
    items = [ke.ExpiryItem(f"K{i}", "kis", MON, days_left=0, level=ke.Level.CRIT, notes=["x" * 900])
             for i in range(30)]
    emb = make_payload(ke.build_embed(items, MON))["embeds"][0]
    assert len(emb["fields"]) <= LIM_FIELDS and _embed_size(emb) <= LIM_TOTAL


def test_config_issues_three_kinds(cfg, app):
    master = {a.account_code: {"account_name": a.display_name, "account_type": a.account_type}
              for a in cfg.active_accounts()}
    assert ke.config_issues(cfg, app, master) == []
    master.pop(cfg.account("KIS_ISA").account_code)
    master[cfg.account("UPBIT").account_code]["account_name"] = "업비트(구)"
    types = {k: v for k, v in app.manual_asset_types.items() if k != "DC"}
    bad_app = app.model_copy(update={"manual_asset_types": types})
    issues = ke.config_issues(cfg, bad_app, master)
    assert any(i.startswith("KIS_ISA: account_master") for i in issues)
    assert any(i.startswith("UPBIT: display_name") for i in issues)
    assert any(i.startswith("DC: account_type") for i in issues)
    master[cfg.account("IRP").account_code]["account_type"] = "DC"
    assert any(i.startswith("IRP: account_type") for i in ke.config_issues(cfg, app, master))
    assert all("0000" not in i for i in issues)                        # 계좌번호 미노출
    item = ke.config_item(issues)
    assert item.force and ke.select([item], TUE, rule=app.credential_check) == [item]


class _FakeUpbit:
    def __init__(self, keys=None, exc=None):
        """
        Purpose: 가짜 Upbit 클라이언트 생성
        Parameter:
            - keys: 반환할 키 목록
            - exc: 예외 객체
        Input: 없음
        Output: 없음
        """
        self.keys, self.exc = keys, exc

    def get_api_keys(self):
        """
        Purpose: Upbit API 키 목록 응답
        Parameter: 없음
        Input: 없음
        Output: list
        """
        if self.exc:
            raise self.exc
        return self.keys


def test_upbit_crosscheck_mismatch_forces(rule):
    client = _FakeUpbit([{"access_key": "AK", "expire_at": "2026-12-01T15:30:00+09:00"}])
    api_date, err = ke.upbit_expire_on(client, "AK")
    assert (api_date, err) == (date(2026, 12, 1), None)
    item = ke.ExpiryItem("UPBIT", "upbit", date(2026, 12, 30), days_left=79, level=ke.level_of(79, rule))
    ke.apply_upbit_crosscheck(item, api_date, err, TUE, rule)
    assert item.force and item.level == ke.Level.CONFIG and ke.should_notify(item, TUE, rule)


def test_upbit_expired_key(rule):
    resp = requests.Response()
    resp.status_code, resp._content = 401, b'{"error": {"name": "expired_access_key"}}'
    _, err = ke.upbit_expire_on(_FakeUpbit(exc=requests.HTTPError(response=resp)), "AK")
    item = ke.ExpiryItem("UPBIT", "upbit", date(2027, 1, 1), days_left=80)
    ke.apply_upbit_crosscheck(item, None, err, TUE, rule)
    assert err == "expired_access_key" and item.level == ke.Level.EXPIRED and item.force
    assert ke.upbit_expire_on(_FakeUpbit([]), "AK") == (None, "access_key_not_listed")
