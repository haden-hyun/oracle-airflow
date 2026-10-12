"""
계좌 kind 별 수집 디스패치 (account_ingest_asset_daily·account_rebuild_ledger_assets 공용)

- 진입점: collect_account(account_id, dates, deps) — 수집 → 0건이면 예외 → (standard_date, account_code) 적재
- HANDLERS: secrets kind 7종 → 계좌 1개의 BALANCE_COLUMNS DataFrame
- 보조: fund_code_for / manual_product / required_fund_codes / fund_codes_to_crawl (secrets + config 결합 규칙)
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass

import pandas as pd
from sqlalchemy.engine import Engine

from asset_flow import db_manager as db
from asset_flow.app_config import AppConfig, load_app_config
from asset_flow.clients.kis_client import KISApiClient
from asset_flow.clients.upbit_client import UpbitApiClient
from asset_flow.config.schemas import BALANCE_COLUMNS
from asset_flow.secrets_config import Account, SecretsConfig, kis_client_config, load_secrets
from asset_flow.tokens import TokenManager
from asset_flow.transformers import kis_transformer as kt
from asset_flow.transformers.ledger_transformer import transform_cash_position, transform_fund_position
from asset_flow.transformers.upbit_transformer import transform_upbit_balance

log = logging.getLogger(__name__)


class EmptyResultError(RuntimeError):
    """수집 0건 — DELETE 전에 실패시켜 기존 적재분 보존."""


@dataclass(frozen=True)
class Deps:
    secrets: SecretsConfig
    app: AppConfig
    engine: Engine
    tokens: TokenManager

    @classmethod
    def load(cls) -> Deps:
        """
        Purpose: 태스크 실행 시점 의존성 일괄 생성
        Parameter: 없음
        Input: secrets 파일, config 파일, Connection postgres_asset, TOKEN_DIR
        Output: Deps (secrets·config 오류 시 예외 → 태스크 실패)
        """
        secrets = load_secrets()
        return cls(secrets, load_app_config(), db.asset_engine(), TokenManager(secrets))


# ── secrets + config 결합 규칙 ──
def fund_code_for(acct: Account, app: AppConfig) -> str | None:
    """
    Purpose: 계좌가 보유한 펀드코드 (계좌 override → 유형 기본값)
    Parameter:
        - acct: 계좌
        - app: 비밀 아닌 설정
    Input: 없음
    Output: 펀드 표준코드 (펀드 계좌가 아니면 None)
    """
    if acct.fund_code:
        return acct.fund_code
    if acct.kind == "kis_pension_fund":
        return app.funds.pension_fund_code
    if acct.kind == "manual_fund":
        return app.manual_asset_types[acct.account_type].product_code
    return None


def manual_product(acct: Account, app: AppConfig) -> tuple[str | None, float]:
    """
    Purpose: 수기 계좌의 원장 product_code 와 multiplier
    Parameter:
        - acct: 수기 계좌
        - app: 비밀 아닌 설정
    Input: 없음
    Output: (product_code, multiplier) (유형이 config 에 없으면 KeyError)
    """
    mtype = app.manual_asset_types[acct.account_type]
    return (acct.fund_code or mtype.product_code), mtype.multiplier


def required_fund_codes(secrets: SecretsConfig, app: AppConfig) -> list[str]:
    """
    Purpose: 자산 수집이 기준가를 읽는 펀드코드 (연금저축·수기 펀드 계좌)
    Parameter:
        - secrets: secrets 설정
        - app: 비밀 아닌 설정
    Input: 없음
    Output: 정렬된 코드 리스트
    """
    accts = secrets.active_accounts("kis_pension_fund", "manual_fund")
    return sorted({c for a in accts if (c := fund_code_for(a, app))})


def fund_codes_to_crawl(secrets: SecretsConfig, app: AppConfig, extra: list[str] = ()) -> list[str]:
    """
    Purpose: 크롤링 대상 펀드코드 = config ∪ 활성 계좌 펀드 ∪ 수기 유형 펀드 ∪ 추가 Param
    Parameter:
        - secrets: secrets 설정
        - app: 비밀 아닌 설정
        - extra: 수동 추가 코드 (기본 없음)
    Input: 없음
    Output: 정렬된 중복 없는 코드 리스트
    """
    codes = set(app.funds.crawl_codes) | set(extra)
    codes |= {c for a in secrets.active_accounts() if (c := fund_code_for(a, app))}
    codes |= {t.product_code for t in app.manual_asset_types.values() if t.product_code}
    return sorted(codes)


# ── kind 별 핸들러: (계좌, dates, deps) → DataFrame ──
def _concat(*frames: pd.DataFrame) -> pd.DataFrame:
    """
    Purpose: 빈 프레임을 뺀 concat (pandas 3 빈 프레임 dtype 경고 회피)
    Parameter:
        - frames: DataFrame 들
    Input: 없음
    Output: 합친 DataFrame (모두 비면 BALANCE_COLUMNS 빈 DataFrame)
    """
    frames = [f for f in frames if not f.empty]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=BALANCE_COLUMNS)


def _kis(acct: Account, deps: Deps) -> tuple[KISApiClient, dict]:
    """
    Purpose: 계좌용 KIS 클라이언트와 설정 dict 생성
    Parameter:
        - acct: KIS 계좌
        - deps: 의존성
    Input: 토큰 저장소 (TokenManager)
    Output: (KISApiClient, config dict)
    """
    conf = kis_client_config(deps.secrets.credential(acct.credential), acct)
    return KISApiClient(deps.tokens.token_for(acct.credential), conf), conf


def _kis_stock(acct: Account, dates: dict, deps: Deps) -> pd.DataFrame:
    """
    Purpose: 위탁계좌 해외·국내 주식 + RP/발행어음 (같은 계좌라 한 writer)
    Parameter:
        - acct: 계좌
        - dates: {standard_date, base_date}
        - deps: 의존성
    Input: KIS API (해외잔고·국내잔고·투자계좌자산현황)
    Output: BALANCE_COLUMNS DataFrame (셋 다 0건일 때만 빈 DF → 수집 실패)
    """
    kis, conf = _kis(acct, deps)
    d = dates["standard_date"]
    # 예수금·외화 미적재
    return _concat(kt.transform_overseas_balance(kis.get_overseas_balance(), d, conf),
                   kt.transform_domestic_balance(kis.get_domestic_balance(), d, conf),
                   kt.transform_rp_note_balance(kis.get_account_balance(), d, conf))


def _kis_domestic_stock(acct: Account, dates: dict, deps: Deps) -> pd.DataFrame:
    """
    Purpose: 국내 주식 잔고 (ISA 등)
    Parameter:
        - acct: 계좌
        - dates: {standard_date, base_date}
        - deps: 의존성
    Input: KIS API
    Output: BALANCE_COLUMNS DataFrame
    """
    kis, conf = _kis(acct, deps)
    return kt.transform_domestic_balance(kis.get_domestic_balance(), dates["standard_date"], conf)


def _kis_pension_fund(acct: Account, dates: dict, deps: Deps) -> pd.DataFrame:
    """
    Purpose: 연금저축 펀드(좌수 역산) + 국내 주식
    Parameter:
        - acct: 계좌
        - dates: {standard_date, base_date}
        - deps: 의존성
    Input: KIS API, market.fund_price_daily
    Output: BALANCE_COLUMNS DataFrame (펀드 보유인데 NAV 없으면 ValueError)
    """
    kis, conf = _kis(acct, deps)
    d, code = dates["standard_date"], fund_code_for(acct, deps.app)
    price, name = db.get_fund_price(deps.engine, code, d)
    fund = kt.transform_pension_fund_balance(kis.get_account_balance(), d, conf, code, price, name)
    return _concat(fund, kt.transform_domestic_balance(kis.get_domestic_balance(), d, conf))


def _kis_cma_cash(acct: Account, dates: dict, deps: Deps) -> pd.DataFrame:
    """
    Purpose: CMA 현금 잔고 (credential 공유는 secrets 에 명시)
    Parameter:
        - acct: 계좌
        - dates: {standard_date, base_date}
        - deps: 의존성
    Input: KIS API
    Output: BALANCE_COLUMNS DataFrame
    """
    kis, conf = _kis(acct, deps)
    return kt.transform_cma_cash_balance(kis.get_account_balance(), dates["standard_date"], conf)


def _upbit(acct: Account, dates: dict, deps: Deps) -> pd.DataFrame:
    """
    Purpose: Upbit 코인 잔고 × T-1 종가
    Parameter:
        - acct: 계좌
        - dates: {standard_date, base_date}
        - deps: 의존성
    Input: Upbit API
    Output: BALANCE_COLUMNS DataFrame
    """
    cred = deps.secrets.credential(acct.credential)
    upbit = UpbitApiClient(cred.app_key, cred.app_secret)
    balance = upbit.get_balance()
    markets = [f"KRW-{b['currency']}" for b in balance if b["currency"] != "KRW"]
    prices = upbit.get_daily_candles(markets, to_date=dates["base_date"]) if markets else []
    return transform_upbit_balance(balance, upbit.get_market_codes(), prices, dates["standard_date"],
                                   acct.account_code, acct.display_name)


def _ledger_row(acct: Account, standard_date: str, deps: Deps) -> dict | None:
    """
    Purpose: 기준일에 유효한 이 계좌의 원장 행 (account_code 로 선택)
    Parameter:
        - acct: 수기 계좌
        - standard_date: 기준일
        - deps: 의존성
    Input: account.manual_position_ledger, account.account_master
    Output: 행 dict 또는 None (개설 전·비활성)
    """
    positions = db.get_manual_positions(deps.engine, standard_date)
    rows = positions[positions["account_code"] == acct.account_code]
    return None if rows.empty else rows.iloc[0].to_dict()


def _manual_fund(acct: Account, dates: dict, deps: Deps) -> pd.DataFrame:
    """
    Purpose: IRP·DC — 원장 좌수 × NAV × multiplier
    Parameter:
        - acct: 수기 계좌
        - dates: {standard_date[, base_date]}
        - deps: 의존성
    Input: 원장, market.fund_price_daily
    Output: BALANCE_COLUMNS DataFrame (원장 없으면 빈 DF, NAV 없으면 ValueError)
    """
    d = dates["standard_date"]
    row = _ledger_row(acct, d, deps)
    if row is None:
        return pd.DataFrame()
    price, name = db.get_fund_price(deps.engine, row["product_code"], d)
    return transform_fund_position(row, d, price, name, manual_product(acct, deps.app)[1])


def _manual_cash(acct: Account, dates: dict, deps: Deps) -> pd.DataFrame:
    """
    Purpose: 적금·청약 — 원장 원금 캐리
    Parameter:
        - acct: 수기 계좌
        - dates: {standard_date[, base_date]}
        - deps: 의존성
    Input: 원장
    Output: BALANCE_COLUMNS DataFrame (원장 없으면 빈 DF)
    """
    row = _ledger_row(acct, dates["standard_date"], deps)
    return pd.DataFrame() if row is None else transform_cash_position(row, dates["standard_date"])


HANDLERS: dict[str, Callable[[Account, dict, Deps], pd.DataFrame]] = {
    "kis_stock": _kis_stock,
    "kis_domestic_stock": _kis_domestic_stock,
    "kis_pension_fund": _kis_pension_fund,
    "kis_cma_cash": _kis_cma_cash,
    "upbit": _upbit,
    "manual_fund": _manual_fund,
    "manual_cash": _manual_cash,
}


def write_account(acct: Account, standard_date: str, df: pd.DataFrame, deps: Deps) -> int:
    """
    Purpose: 계좌 1개·하루치 적재 (DELETE 스코프 = standard_date + account_code)
    Parameter:
        - acct: 계좌
        - standard_date: 기준일
        - df: 적재할 DataFrame
        - deps: 의존성
    Input: config tables.asset_daily
    Output: 삽입 행 수 (+ DB 쓰기, 쓰기 가드 적용)
    """
    table = deps.app.tables.asset_daily
    return db.delete_and_insert_account_assets(
        deps.engine, table.db_schema, table.table_name, standard_date, acct.account_code, df.to_dict("records")
    )


def collect_account(account_id: str, dates: dict, deps: Deps) -> int:
    """
    Purpose: 계좌 1개 수집·적재 (get+upload 통합 — 빈 결과로 기존 적재분을 지우지 않음)
    Parameter:
        - account_id: secrets 계좌 id
        - dates: {standard_date, base_date}
        - deps: 의존성
    Input: kind 별 외부 API·업무 DB
    Output: 적재 행 수. 비활성 계좌·쓰기 가드 위반이면 API 호출 전에, 0건이면 DELETE 전에 예외
    """
    acct = deps.secrets.account(account_id)
    if not acct.enabled:
        raise RuntimeError(f"[{account_id}] enabled=false 계좌 — 수집 대상 아님")
    db.ensure_writable(deps.engine)                      # 외부 호출 전 쓰기 가드
    df = HANDLERS[acct.kind](acct, dates, deps)
    if df.empty:
        raise EmptyResultError(f"[{account_id}] 데이터 없음 — 빈 응답 (기존 적재분 보존)")
    rows = write_account(acct, dates["standard_date"], df, deps)
    log.info("[%s] %s 적재 %s건", account_id, dates["standard_date"], rows)
    return rows
