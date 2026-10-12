"""
비밀이 아닌 운영 설정 로더 (dags/config/asset_flow.yaml, 커밋 대상)

- 진입점: load_app_config() → AppConfig (pydantic strict, frozen)
- 경로: env ASSET_FLOW_CONFIG (기본 /opt/airflow/dags/config/asset_flow.yaml)
- 내용: 적재 테이블, 펀드코드, 수기 계좌 유형별 product_code·multiplier, 만료 알림 규칙, 대시보드 갱신 프로시저
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


class Funds(_Strict):
    pension_fund_code: str                      # kind=kis_pension_fund 기본 펀드
    crawl_codes: list[str]                      # 항상 크롤링할 펀드


class ManualAssetType(_Strict):
    product_code: str | None                    # 원장 product_code (현금성은 null)
    multiplier: float = Field(gt=0)             # 좌수 × NAV × multiplier


class CredentialCheck(_Strict):
    stage_days: list[int]                       # 단계 알림일 (D-14, D-7)
    daily_from: int                             # D-n 부터 매일
    window: int                                 # 알림 구간 (월요일 따라잡기 범위)


class Dashboard(_Strict):
    # 적재 직후 CALL 할 MV 갱신 프로시저 (스키마.이름, 대시보드 마이그레이션 009)
    refresh_procedure: str = Field(pattern=r"^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$")


class AppConfig(_Strict):
    tables: Tables
    funds: Funds
    manual_asset_types: dict[str, ManualAssetType]   # 키 = account_master.account_type
    credential_check: CredentialCheck
    dashboard: Dashboard


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
