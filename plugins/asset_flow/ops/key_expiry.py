"""
API 키 만료·설정 정합성 점검 — 판정(순수 함수)과 Discord embed 생성

- 진입점: evaluate() → select() → build_embed() (auth_issue_api_tokens.check_key_expiry 가 호출)
- 규칙: D-14·D-7 단계, D-3 부터 매일(만료 후 포함), 구간 내 월요일 따라잡기. expires_on=null 제외
- 보조: config_issues()(secrets ↔ account_master ↔ config), upbit_expire_on()·apply_upbit_crosscheck()
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import IntEnum

import requests

from asset_flow.app_config import AppConfig, CredentialCheck
from asset_flow.secrets_config import MANUAL_KINDS, SecretsConfig

MONDAY = 0
RENEW_GUIDE = {
    "kis": "KIS Developers > 신청현황에서 갱신 → app_key/app_secret/expires_on 교체",
    "upbit": "업비트 Open API 관리에서 신규 발급(서버 IP 등록) → app_key/app_secret/expires_on 교체",
    "data_go_kr": "공공데이터포털 활용기간 연장 → expires_on 갱신",
}


class Level(IntEnum):
    OK = 0
    CONFIG = 1      # 설정 불일치 (즉시성은 force)
    INFO = 2
    WARN = 3
    CRIT = 4
    EXPIRED = 5


COLOR = {Level.CONFIG: 0x757575, Level.INFO: 0xFDD835, Level.WARN: 0xFB8C00,
         Level.CRIT: 0xE53935, Level.EXPIRED: 0x6A1B9A}
ICON = {Level.CONFIG: "⚙️", Level.INFO: "🟡", Level.WARN: "🟠", Level.CRIT: "🔴", Level.EXPIRED: "⛔"}


@dataclass
class ExpiryItem:
    name: str
    provider: str
    expires_on: date | None
    used_by: tuple[str, ...] = ()
    days_left: int | None = None
    level: Level = Level.OK
    notes: list[str] = field(default_factory=list)
    force: bool = False     # 날짜 규칙과 무관하게 즉시 알림


def level_of(days_left: int | None, rule: CredentialCheck) -> Level:
    """
    Purpose: 남은 일수 → 심각도
    Parameter:
        - days_left: 만료까지 남은 일수 (None = 날짜 없음)
        - rule: 알림 규칙 (stage_days, daily_from, window)
    Input: 없음
    Output: EXPIRED(<0) / CRIT(≤daily_from) / WARN(≤최소 단계일) / INFO(≤window) / OK, None 이면 OK
    """
    if days_left is None:
        return Level.OK
    if days_left < 0:
        return Level.EXPIRED
    if days_left <= rule.daily_from:
        return Level.CRIT
    if days_left <= min(rule.stage_days):
        return Level.WARN
    return Level.INFO if days_left <= rule.window else Level.OK


def should_notify(item: ExpiryItem, as_of: date, rule: CredentialCheck) -> bool:
    """
    Purpose: 오늘 알림을 보낼 항목인지 (상태 저장 없는 멱등 규칙)
    Parameter:
        - item: 판정 항목
        - as_of: 기준일 (KST)
        - rule: 알림 규칙
    Input: 없음
    Output: force / 단계일 / D-daily_from 이하 / 구간 내 월요일이면 True
    """
    d = item.days_left
    if item.force:
        return True
    if d is None:
        return False
    return d in rule.stage_days or d <= rule.daily_from or (0 <= d <= rule.window and as_of.weekday() == MONDAY)


def d_label(d: int | None) -> str:
    """
    Purpose: 남은 일수 표시 문구
    Parameter:
        - d: 남은 일수
    Input: 없음
    Output: "D-n" / "오늘 만료(D-day)" / "만료 n일 경과" / "만료일 없음"
    """
    if d is None:
        return "만료일 없음"
    return f"D-{d}" if d > 0 else ("오늘 만료(D-day)" if d == 0 else f"만료 {-d}일 경과")


def evaluate(cfg: SecretsConfig, as_of: date, rule: CredentialCheck) -> list[ExpiryItem]:
    """
    Purpose: 사용 중 credential 의 만료 판정 (expires_on=null 은 제외)
    Parameter:
        - cfg: secrets 설정
        - as_of: 기준일 (KST)
        - rule: 알림 규칙
    Input: 없음
    Output: ExpiryItem 리스트 (영향 범위 = used_by)
    """
    items = []
    for c in cfg.credentials_in_use():
        if c.expires_on is None:
            continue
        d = (c.expires_on - as_of).days
        items.append(ExpiryItem(c.id, c.provider, c.expires_on, cfg.used_by(c.id), d, level_of(d, rule)))
    return items


def config_issues(cfg: SecretsConfig, app: AppConfig, master: dict[str, dict]) -> list[str]:
    """
    Purpose: secrets ↔ 업무 DB account_master ↔ config 정합성 점검
    Parameter:
        - cfg: secrets 설정
        - app: 비밀 아닌 설정
        - master: get_account_master() 결과 {account_code: {account_name, account_type}}
    Input: 없음
    Output: 불일치 설명 리스트 (계좌 id 로만 표기, 계좌번호 미포함). master 에만 있는 계좌는 대상 아님
    """
    issues = []
    for a in cfg.active_accounts():
        row = master.get(a.account_code)
        if row is None:
            issues.append(f"{a.id}: account_master 에 활성 행 없음")
            continue
        if row["account_name"] != a.display_name:
            issues.append(f"{a.id}: display_name ≠ account_master.account_name")
        if a.kind in MANUAL_KINDS and (a.account_type not in app.manual_asset_types
                                       or a.account_type != row["account_type"]):
            issues.append(f"{a.id}: account_type '{a.account_type}' 이 config·account_master 와 불일치")
    return issues


def config_item(issues: list[str]) -> ExpiryItem:
    """
    Purpose: 정합성 불일치를 즉시 알림 항목으로 변환
    Parameter:
        - issues: config_issues() 결과
    Input: 없음
    Output: level=CONFIG, force=True 인 ExpiryItem
    """
    return ExpiryItem("설정 정합성", "config", None, (), None, Level.CONFIG, list(issues), force=True)


def upbit_expire_on(client, access_key: str) -> tuple[date | None, str | None]:
    """
    Purpose: Upbit /v1/api_keys 에서 해당 키 만료일 조회
    Parameter:
        - client: UpbitApiClient (get_api_keys 제공)
        - access_key: 대조할 access key
    Input: Upbit API
    Output: (만료일, None) 또는 (None, 오류명 — expired_access_key / access_key_not_listed / 예외 이름)
    """
    try:
        keys = client.get_api_keys()
    except requests.HTTPError as e:
        try:
            return None, e.response.json()["error"]["name"]
        except Exception:
            return None, f"HTTP {getattr(e.response, 'status_code', '?')}"
    except Exception as e:
        return None, type(e).__name__
    for k in keys:
        if k.get("access_key") == access_key:
            return datetime.fromisoformat(k["expire_at"]).date(), None
    return None, "access_key_not_listed"


def apply_upbit_crosscheck(
    item: ExpiryItem, api_date: date | None, err: str | None, as_of: date, rule: CredentialCheck
) -> None:
    """
    Purpose: Upbit API 만료일로 판정 보정 (API 값 우선)
    Parameter:
        - item: Upbit 항목 (변경됨)
        - api_date: API 만료일
        - err: 조회 오류명
        - as_of: 기준일
        - rule: 알림 규칙
    Input: 없음
    Output: 없음 (expired_access_key → EXPIRED·force, 날짜 불일치 → 더 임박한 등급·force, 기타 오류 → 메모만)
    """
    if err == "expired_access_key":
        item.level, item.force = Level.EXPIRED, True
        item.notes.append("Upbit API 응답: expired_access_key")
    elif err:
        item.notes.append(f"Upbit 만료일 조회 실패({err}) — YAML 값으로만 판정")
    elif api_date and api_date != item.expires_on:
        api_days = (api_date - as_of).days
        item.force = True
        item.level = max(item.level, level_of(api_days, rule), Level.CONFIG)
        item.notes.append(f"YAML({item.expires_on}) ≠ API({api_date}, {d_label(api_days)}) — YAML 갱신 필요")


def select(items: list[ExpiryItem], as_of: date, rule: CredentialCheck) -> list[ExpiryItem]:
    """
    Purpose: 알림 트리거가 하나라도 있으면 구간 내 전 항목을 한 메시지로 묶음
    Parameter:
        - items: 판정 항목
        - as_of: 기준일
        - rule: 알림 규칙
    Input: 없음
    Output: 심각도 내림차순·남은 일수 오름차순 리스트 (트리거 없으면 [])
    """
    if not any(should_notify(i, as_of, rule) for i in items):
        return []
    shown = [i for i in items if i.force or i.level != Level.OK]
    return sorted(shown, key=lambda i: (-int(i.level), 9999 if i.days_left is None else i.days_left))


def build_embed(items: list[ExpiryItem], as_of: date) -> dict:
    """
    Purpose: 만료 점검 embed 생성 (키 값 미포함 — id·날짜·영향 범위만)
    Parameter:
        - items: select() 결과 (비어 있지 않음)
        - as_of: 기준일
    Input: 없음
    Output: embed dict (한도 적용은 alerts.make_payload)
    """
    fields = []
    for i in items:
        lines = [f"만료일 **{i.expires_on or '-'}** ({d_label(i.days_left)})"] if i.provider != "config" else []
        if i.used_by:
            lines.append(f"영향 범위: {', '.join(i.used_by)}")
        lines += [f"• {n}" for n in i.notes]
        if i.provider in RENEW_GUIDE:
            lines.append(f"조치: {RENEW_GUIDE[i.provider]}")
        name = f"{ICON.get(i.level, '')} {i.name} ({i.provider})"
        fields.append({"name": name, "value": "\n".join(lines), "inline": False})
    return {
        "title": f"🔑 API 키·설정 점검 — {len(items)}건 확인 필요",
        "color": COLOR[max(i.level for i in items)],
        "description": f"기준일 {as_of} (KST) · secrets 의 expires_on 기준",
        "fields": fields,
    }
