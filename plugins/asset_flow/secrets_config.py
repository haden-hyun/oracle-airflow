"""
자격증명·계좌의 단일 진실 공급원 로더 (secrets/asset_flow.secrets.yaml)

- 진입점: load_secrets() → SecretsConfig (pydantic strict, frozen)
- 경로: env ASSET_FLOW_SECRETS (기본 /opt/airflow/secrets/asset_flow.secrets.yaml)
- 실패 시 SecretsConfigError — 메시지엔 경로·행·필드만, 값은 넣지 않음 (fail-closed)
- CLI: python -m asset_flow.secrets_config [path] [--previous path] → 종료코드 0 통과 / 1 실패 / 2 경고
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import os
import re
import sys
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Annotated, Literal

import yaml
from airflow.sdk.log import mask_secret
from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, StringConstraints, ValidationError, model_validator

DEFAULT_PATH = "/opt/airflow/secrets/asset_flow.secrets.yaml"


class SecretsConfigError(RuntimeError):
    """secrets 파일 읽기·검증 실패. 메시지에는 위치만 담고 값은 담지 않는다."""


# ── YAML: 중복 키 거부 + timestamp 암묵 해석 제거 ──
class _StrictLoader(yaml.SafeLoader):
    """중복 키 거부·날짜 자동 변환 없는 SafeLoader."""


# 날짜는 문자열로 받아 _iso_date 에서만 검증
_StrictLoader.yaml_implicit_resolvers = {
    ch: [(tag, rx) for tag, rx in rs if tag != "tag:yaml.org,2002:timestamp"]
    for ch, rs in yaml.SafeLoader.yaml_implicit_resolvers.items()
}


def _mapping_without_duplicates(loader: yaml.SafeLoader, node: yaml.MappingNode, deep: bool = False) -> dict:
    """
    Purpose: YAML 매핑 생성 시 같은 키가 두 번 나오면 거부
    Parameter:
        - loader: PyYAML 로더
        - node: 매핑 노드
        - deep: 하위 객체까지 즉시 생성할지 (기본 False)
    Input: 없음
    Output: 생성된 dict (중복 키면 SecretsConfigError — 키 이름·행 번호만 표시)
    """
    seen: dict = {}
    for key_node, _ in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in seen:
            raise SecretsConfigError(f"중복 키 {key!r}: {key_node.start_mark.line + 1}행 (처음 {seen[key] + 1}행)")
        seen[key] = key_node.start_mark.line
    return loader.construct_mapping(node, deep=deep)


_StrictLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _mapping_without_duplicates)


# ── 스키마 ──
def _iso_date(v: object) -> dt.date:
    """
    Purpose: YYYY-MM-DD 문자열만 날짜로 변환
    Parameter:
        - v: YAML 원시 값
    Input: 없음
    Output: date (형식·실재성 오류는 ValueError → pydantic 검증 오류)
    """
    if isinstance(v, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", v):
        return dt.date.fromisoformat(v)
    raise ValueError("YYYY-MM-DD 형식의 날짜여야 함")


ISODate = Annotated[dt.date, BeforeValidator(_iso_date)]
Ident = Annotated[str, StringConstraints(pattern=r"^[A-Z][A-Z0-9_]{1,63}$")]
KeyStr = Annotated[str, StringConstraints(min_length=8)]
Cano = Annotated[str, StringConstraints(pattern=r"^\d{8}$")]
ProductCode = Annotated[str, StringConstraints(pattern=r"^\d{2}$")]
ManualAccountNo = Annotated[str, StringConstraints(pattern=r"^\d[\d-]{5,24}\d$")]
FundCode = Annotated[str, StringConstraints(pattern=r"^[A-Z0-9]{12}$")]
Provider = Literal["kis", "upbit", "data_go_kr"]
Kind = Literal[
    "kis_stock", "kis_domestic_stock", "kis_pension_fund", "kis_cma_cash", "upbit", "manual_fund", "manual_cash"
]
KIND_PROVIDER: dict[str, str | None] = {
    "kis_stock": "kis", "kis_domestic_stock": "kis", "kis_pension_fund": "kis", "kis_cma_cash": "kis",
    "upbit": "upbit", "manual_fund": None, "manual_cash": None,
}
MANUAL_KINDS = ("manual_fund", "manual_cash")
FUND_CODE_KINDS = ("kis_pension_fund", "manual_fund")


class _Strict(BaseModel):
    # strict: 따옴표 없는 숫자 거부 / forbid: 오타 필드 거부 / hide_input: 오류에 값 미포함
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, hide_input_in_errors=True)


class Credential(_Strict):
    id: Ident                                    # = 토큰 dict 키 (KIS_STOCK 등)
    provider: Provider
    app_key: KeyStr                              # KIS appkey / Upbit access key / data.go.kr serviceKey
    app_secret: KeyStr | None = None             # data_go_kr 만 생략 가능
    expires_on: ISODate | None                   # 키 생략 불가. null = 만료 없음 확인 → 알림 제외
    issued_on: ISODate | None = None
    note: str | None = None

    @model_validator(mode="after")
    def _secret_required(self) -> Credential:
        """
        Purpose: data_go_kr 외 provider 는 app_secret 필수
        Parameter: 없음
        Input: 없음
        Output: self (누락 시 ValueError)
        """
        if self.app_secret is None and self.provider != "data_go_kr":
            raise ValueError(f"provider={self.provider}: app_secret 필수")
        return self


class Account(_Strict):
    id: Ident                                    # 비밀 아닌 별칭 (XCom·Param·로그·map_index 에 사용)
    kind: Kind
    display_name: str = Field(min_length=1)      # = account_master.account_name (예: 위탁)
    enabled: bool = True
    credential: Ident | None = None              # API 계좌만
    cano: Cano | None = None                     # KIS 종합계좌번호 8자리
    product_code: ProductCode | None = None      # KIS 계좌상품코드 2자리
    account_no: ManualAccountNo | None = None    # 수기 원장 계좌번호 원문
    account_type: Ident | None = None            # 수기 원장 계좌: account_master.account_type
    fund_code: FundCode | None = None            # 보유 펀드 override (kis_pension_fund·manual_fund 만)

    @model_validator(mode="after")
    def _shape(self) -> Account:
        """
        Purpose: kind 별 필수·금지 필드 조합 검증
        Parameter: 없음
        Input: 없음
        Output: self (조합 위반 시 ValueError — 필드 이름만 표시)
        """
        provider = KIND_PROVIDER[self.kind]
        if provider == "kis":
            need, forbid = ("credential", "cano", "product_code"), ("account_no", "account_type")
        elif provider == "upbit":
            need, forbid = ("credential",), ("cano", "product_code", "account_no", "account_type")
        else:
            need, forbid = ("account_no", "account_type"), ("credential", "cano", "product_code")
        if self.kind not in FUND_CODE_KINDS:
            forbid += ("fund_code",)
        missing = [f for f in need if getattr(self, f) is None]
        extra = [f for f in forbid if getattr(self, f) is not None]
        if missing or extra:
            raise ValueError(f"kind={self.kind}: 필수 누락 {missing}, 허용 안 됨 {extra}")
        return self

    @property
    def account_code(self) -> str:
        """
        Purpose: 업무 DB(asset_daily·account_master) account_code 규약 값
        Parameter: 없음
        Input: 없음
        Output: KIS "cano-상품코드" / 수기 account_no 원문 / Upbit id ('UPBIT')
        """
        if self.cano:
            return f"{self.cano}-{self.product_code}"
        return self.account_no or self.id


class Services(_Strict):
    exchange_rate_credential: Ident              # 환율 조회용 KIS 키
    holiday_api_credential: Ident | None = None  # 휴장일 KASI API 키 (data_go_kr, 선택)


class SecretsConfig(_Strict):
    version: Literal[1]
    credentials: list[Credential] = Field(min_length=1)
    accounts: list[Account] = Field(min_length=1)
    services: Services

    @model_validator(mode="after")
    def _integrity(self) -> SecretsConfig:
        """
        Purpose: id·account_code 유일성과 credential 참조 무결성 검증
        Parameter: 없음
        Input: 없음
        Output: self (위반 시 ValueError — id 만 표시, 계좌번호 미표시)
        """
        for name, items in (("credentials", self.credentials), ("accounts", self.accounts)):
            ids = [i.id for i in items]
            dup = sorted({i for i in ids if ids.count(i) > 1})
            if dup:
                raise ValueError(f"{name}.id 중복: {dup}")
        codes = [a.account_code for a in self.accounts]
        if len(codes) != len(set(codes)):
            raise ValueError("accounts: 같은 계좌번호(account_code)가 두 번 이상 등장")
        creds = {c.id: c for c in self.credentials}
        for a in self.accounts:
            if a.credential is None:
                continue
            c = creds.get(a.credential)
            if c is None:
                raise ValueError(f"accounts[{a.id}].credential: 정의되지 않은 {a.credential!r}")
            if c.provider != KIND_PROVIDER[a.kind]:
                raise ValueError(f"accounts[{a.id}]: kind와 credential provider 불일치")
        ex = creds.get(self.services.exchange_rate_credential)
        if ex is None or ex.provider != "kis":
            raise ValueError("services.exchange_rate_credential: KIS credential id여야 함")
        hol = self.services.holiday_api_credential
        if hol is not None and getattr(creds.get(hol), "provider", None) != "data_go_kr":
            raise ValueError("services.holiday_api_credential: data_go_kr credential id여야 함")
        return self

    def credential(self, cred_id: str) -> Credential:
        """
        Purpose: id 로 credential 조회
        Parameter:
            - cred_id: credential id
        Input: 없음
        Output: Credential (없으면 SecretsConfigError)
        """
        for c in self.credentials:
            if c.id == cred_id:
                return c
        raise SecretsConfigError(f"credentials에 없는 id: {cred_id!r}")

    def account(self, account_id: str) -> Account:
        """
        Purpose: id 로 계좌 조회
        Parameter:
            - account_id: 계좌 id
        Input: 없음
        Output: Account (없으면 SecretsConfigError)
        """
        for a in self.accounts:
            if a.id == account_id:
                return a
        raise SecretsConfigError(f"accounts에 없는 id: {account_id!r}")

    def active_accounts(self, *kinds: str) -> list[Account]:
        """
        Purpose: 활성 계좌 목록 (kind 필터 선택)
        Parameter:
            - kinds: 남길 kind 목록. 비우면 전체
        Input: 없음
        Output: enabled 계좌 리스트 (파일 순서)
        """
        return [a for a in self.accounts if a.enabled and (not kinds or a.kind in kinds)]

    def used_by(self, cred_id: str) -> tuple[str, ...]:
        """
        Purpose: credential 을 쓰는 활성 계좌·서비스 (만료 시 영향 범위)
        Parameter:
            - cred_id: credential id
        Input: 없음
        Output: 계좌 id 들 + 서비스 이름("exchange_rate", "holiday_api") 튜플
        """
        users = [a.id for a in self.active_accounts() if a.credential == cred_id]
        if self.services.exchange_rate_credential == cred_id:
            users.append("exchange_rate")
        if self.services.holiday_api_credential == cred_id:
            users.append("holiday_api")
        return tuple(users)

    def credentials_in_use(self) -> list[Credential]:
        """
        Purpose: 활성 계좌·services 가 참조하는 credential (발급·만료 점검 대상)
        Parameter: 없음
        Input: 없음
        Output: Credential 리스트 (미참조 키 제외)
        """
        return [c for c in self.credentials if self.used_by(c.id)]


def credential_fingerprint(cred: Credential) -> str:
    """
    Purpose: 키 교체 감지용 지문 (원문 복원 불가)
    Parameter:
        - cred: credential
    Input: 없음
    Output: sha256(app_key + "\\0" + app_secret) 앞 16자 hex
    """
    raw = f"{cred.app_key}\0{cred.app_secret or ''}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def kis_client_config(cred: Credential, acct: Account | None = None) -> dict:
    """
    Purpose: KISApiClient·kis_transformer 용 config dict 생성
    Parameter:
        - cred: KIS credential
        - acct: 계좌 (환율처럼 계좌 불필요하면 None)
    Input: 없음
    Output: {appkey, secret[, account, product_code, type]}
    """
    conf = {"appkey": cred.app_key, "secret": cred.app_secret}
    if acct is not None:
        conf |= {"account": acct.cano, "product_code": acct.product_code, "type": acct.display_name}
    return conf


def secret_values(cfg: SecretsConfig) -> list[str]:
    """
    Purpose: 마스킹 대상 비밀값 (태스크 로그·Discord 알림)
    Parameter:
        - cfg: 검증된 설정
    Input: 없음
    Output: app_key·app_secret·cano·account_no 값 리스트
    """
    vals = [v for c in cfg.credentials for v in (c.app_key, c.app_secret) if v]
    vals += [a.cano for a in cfg.accounts if a.cano]
    vals += [a.account_no for a in cfg.accounts if a.account_no]
    return vals


def load_secrets(path: str | os.PathLike | None = None, *, mask: bool = True) -> SecretsConfig:
    """
    Purpose: secrets YAML 을 읽어 검증하고 비밀 값을 마스킹 등록
    Parameter:
        - path: 파일 경로 (None 이면 env ASSET_FLOW_SECRETS → 기본 경로)
        - mask: True 면 mask_secret 등록 (CLI 는 False)
    Input: secrets 파일, env ASSET_FLOW_SECRETS
    Output: SecretsConfig (읽기·검증 실패 시 SecretsConfigError, 원본 예외 체인 차단)
    """
    p = Path(path or os.getenv("ASSET_FLOW_SECRETS", DEFAULT_PATH))
    try:
        with p.open(encoding="utf-8") as f:      # 스트림 파싱 → 오류에 원문 줄 미포함
            raw = yaml.load(f, Loader=_StrictLoader)  # noqa: S506 — SafeLoader 파생
    except FileNotFoundError:
        raise SecretsConfigError(f"secrets 파일 없음: {p}") from None
    except SecretsConfigError as e:
        raise SecretsConfigError(f"{p}: {e}") from None
    except yaml.MarkedYAMLError as e:
        line = e.problem_mark.line + 1 if e.problem_mark else "?"
        raise SecretsConfigError(f"{p}: YAML 문법 오류 {line}행 ({e.problem})") from None
    try:
        cfg = SecretsConfig.model_validate(raw)
    except ValidationError as e:
        locs = "; ".join(f"{'.'.join(map(str, er['loc'])) or '<root>'}: {er['msg']}" for er in e.errors())
        raise SecretsConfigError(f"{p}: 스키마 검증 실패 {e.error_count()}건 — {locs}") from None
    if mask:
        for v in secret_values(cfg):
            mask_secret(v)
    return cfg


def main(argv: list[str] | None = None) -> int:
    """
    Purpose: 검증 CLI — 사람이 파일을 고친 직후 실행
    Parameter:
        - argv: 명령행 인자 (None 이면 sys.argv)
    Input: 검증 대상 파일, --previous 파일(선택)
    Output: 종료코드 0 통과 / 1 검증 실패 / 2 경고만 있음 (값은 출력하지 않음)
    """
    ap = argparse.ArgumentParser(prog="python -m asset_flow.secrets_config")
    ap.add_argument("path", nargs="?")
    ap.add_argument("--previous", help="변경 전 파일 — account_code·지문 변화 비교")
    args = ap.parse_args(argv)
    try:
        cfg = load_secrets(args.path, mask=False)
        prev = load_secrets(args.previous, mask=False) if args.previous else None
    except SecretsConfigError as e:
        print(f"FAIL: {e}")
        return 1
    print(f"OK: credentials={len(cfg.credentials)} accounts={len(cfg.accounts)}")
    warn = False
    today = dt.datetime.now(ZoneInfo("Asia/Seoul")).date()
    for c in cfg.credentials_in_use():
        if c.expires_on is None:
            print(f"  INFO {c.id}: expires_on=null (만료 없음으로 확인됨 — 맞는지 재확인)")
        elif (left := (c.expires_on - today).days) <= 14:
            print(f"  WARN {c.id}: 만료일 {c.expires_on} (남은 {left}일)")
            warn = True
    if prev:
        old_codes = {a.id: a.account_code for a in prev.accounts}
        old_fps = {c.id: credential_fingerprint(c) for c in prev.credentials}
        for a in cfg.accounts:
            if a.id in old_codes and old_codes[a.id] != a.account_code:
                print(f"  WARN {a.id}: account_code 변경 — 업무 DB 의 account_code 마이그레이션 먼저 확인")
                warn = True
        for c in cfg.credentials:
            if c.id in old_fps and old_fps[c.id] != credential_fingerprint(c):
                print(f"  INFO {c.id}: 키 지문 변경 — 다음 사용 시 토큰 재발급")
    return 2 if warn else 0


if __name__ == "__main__":
    sys.exit(main())
