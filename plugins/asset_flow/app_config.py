"""
비밀이 아닌 운영 설정 로더 (dags/config/asset_flow.yaml, 커밋 대상)

- 진입점: load_app_config() → AppConfig (pydantic strict, frozen)
- 경로: env ASSET_FLOW_CONFIG (기본 /opt/airflow/dags/config/asset_flow.yaml)
- 내용: 적재 테이블, 펀드코드, 수기 계좌 유형별 product_code·multiplier, 만료 알림 규칙, 대시보드 갱신 프로시저,
        시세 수집(price_ingest), 배당 수집(dividend)
- yaml 과 이 모델은 같은 커밋으로 배포 (extra=forbid — 한쪽만 바뀌면 모든 DAG 의 설정 로드 실패)
"""

from __future__ import annotations

import os
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError

DEFAULT_PATH = "/opt/airflow/dags/config/asset_flow.yaml"


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, populate_by_name=True)


class Table(_Strict):
    db_schema: str = Field(alias="schema")
    table_name: str


class Tables(_Strict):
    asset_daily: Table
    exchange_rate_daily: Table
    fund_price_daily: Table
    product: Table
    product_price_daily: Table
    dividend_event: Table
    dividend_receipt: Table


class Funds(_Strict):
    pension_fund_code: str                      # kind=kis_pension_fund 기본 펀드
    crawl_codes: list[str]                      # 항상 수집할 펀드
    crawler_fallback: bool = True               # KOFIA 누락 코드만 fundguide 크롤링
    shadow_compare: bool = False                # KOFIA 값 ↔ 크롤러 값 매일 대조 (전환 검증)


class ManualAssetType(_Strict):
    product_code: str | None                    # 원장 product_code (현금성은 null)
    multiplier: float = Field(gt=0)             # 좌수 × NAV × multiplier


class CredentialCheck(_Strict):
    stage_days: list[int]                       # 단계 알림일 (D-14, D-7)
    daily_from: int                             # D-n 부터 매일
    window: int                                 # 알림 구간 (월요일 따라잡기 범위)


class Dashboard(_Strict):
    # 적재 직후 CALL 할 MV 갱신 프로시저 (스키마.이름, 대시보드 저장소 소유)
    refresh_procedure: str = Field(pattern=r"^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$")


class PriceIngest(_Strict):
    lookback_days: int = Field(ge=30, le=3650)          # 신규·재매수 백필 한도
    refetch_trading_days: int = Field(ge=1, le=30)      # 매일 재수집 거래일 수
    tracked_after_sale_days: int = Field(ge=0, le=365)  # 매도 후 추적 일수
    request_interval_sec: float = Field(ge=0.1, le=5)   # 업비트·KOFIA 호출 간격
    fund_nav_enabled: bool = False                      # KOFIA 펀드 기준가 보충 스위치


class DividendWindows(_Strict):
    # 기준 = run 의 KST 날짜(T), 단위 일 (값의 근거는 yaml 주석)
    kr_event_back: int = Field(ge=1)            # 국내 일정 기준일 T−n
    kr_event_forward: int = Field(ge=0)         # 국내 일정 기준일 T+n
    us_event_back: int = Field(ge=60)           # 미국 일정 T−n (권리종합은 공시일 기준 → 기준일보다 앞서 시작)
    us_event_forward: int = Field(ge=0)         # 미국 일정 T+n
    receipt_back: int = Field(ge=1)             # 국내 원장 기준일 T−n
    estimate_back: int = Field(ge=1)            # 미국 추정 락일 T−n
    held_back: int = Field(ge=1)                # 조회 대상 = 이 기간 보유 이력 종목


class Dividend(_Strict):
    windows: DividendWindows
    receipt_kinds: list[str]                    # 원장(CTRGA011R) 조회 계좌 kind (secrets accounts[].kind)
    us_withholding_rate: float = Field(ge=0, lt=1)   # 미국 추정 원장 원천징수율
    extra_kr_codes: list[str] = []              # 보유 이력 없이도 조회할 국내 코드
    refresh_dashboard_views: bool = False       # true 면 마지막에 대시보드 MV 갱신


class AppConfig(_Strict):
    tables: Tables
    funds: Funds
    manual_asset_types: dict[str, ManualAssetType]   # 키 = account_master.account_type
    credential_check: CredentialCheck
    dashboard: Dashboard
    price_ingest: PriceIngest
    dividend: Dividend


def load_app_config(path: str | os.PathLike | None = None) -> AppConfig:
    """
    Purpose: 비밀 아닌 설정 YAML 을 읽어 검증
    Parameter:
        - path: 파일 경로 (None 이면 env ASSET_FLOW_CONFIG → 기본 경로)
    Input: config 파일, env ASSET_FLOW_CONFIG
    Output: AppConfig (파일 없음·검증 실패 시 ValueError)
    """
    p = Path(path or os.getenv("ASSET_FLOW_CONFIG", DEFAULT_PATH))
    try:
        return AppConfig.model_validate(yaml.safe_load(p.read_text(encoding="utf-8")))
    except (OSError, yaml.YAMLError, ValidationError) as e:
        raise ValueError(f"{p}: 설정 로드 실패 — {e}") from None
