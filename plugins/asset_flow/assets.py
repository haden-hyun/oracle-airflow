"""
Airflow Asset 정의 (적재 테이블·외부 출처·대시보드 MV = Asset, 계보·갱신 이력 표시 전용)

- 적재 태스크가 outlets 로 선언 → 성공 시 메타DB 에 이벤트 1건 (스케줄에는 쓰지 않음)
- inlets: 외부 API·수기 입력을 쓰는 태스크 → source.*, 다른 DAG 가 쓴 테이블을 읽는 태스크 → 그 테이블
  (같은 DAG 가 쓰는 테이블은 inlets 에서 뺀다 — DAG 단위 순환 방지)
- source.* 는 이름만 (URL·키·계좌 정보 금지)
- mark: 이벤트에 공통 키 data_date(YYYY-MM-DD, 구간이면 끝)·rows(int) + 계좌 id 첨부 (계좌번호 금지)
"""

from __future__ import annotations

from airflow.sdk import Asset

TRADING_HOLIDAY = Asset("market.trading_holiday", uri="asset://market/trading_holiday", group="market")
EXCHANGE_RATE = Asset("market.exchange_rate_daily", uri="asset://market/exchange_rate_daily", group="market")
FUND_PRICE = Asset("market.fund_price_daily", uri="asset://market/fund_price_daily", group="market")
ASSET_DAILY = Asset("account.asset_daily", uri="asset://account/asset_daily", group="account")
PRODUCT = Asset("market.product", uri="asset://market/product", group="market")
PRODUCT_PRICE = Asset("market.product_price_daily", uri="asset://market/product_price_daily", group="market")
DIVIDEND_EVENT = Asset("market.dividend_event", uri="asset://market/dividend_event", group="dividend")
DIVIDEND_RECEIPT = Asset("account.dividend_receipt", uri="asset://account/dividend_receipt", group="dividend")
MARKET_INDEX = Asset("market.index_daily", uri="asset://market/index_daily", group="market")
MANUAL_LEDGER = Asset("account.manual_position_ledger", uri="asset://account/manual_position_ledger", group="account")

MV_POSITION_DAILY = Asset("dashboard.mv_position_daily", uri="asset://dashboard/mv_position_daily", group="dashboard")
MV_COST_BASIS_KRW = Asset("dashboard.mv_cost_basis_krw", uri="asset://dashboard/mv_cost_basis_krw", group="dashboard")

SOURCE_KIS = Asset("source.kis", uri="asset://source/kis", group="source")
SOURCE_UPBIT = Asset("source.upbit", uri="asset://source/upbit", group="source")
SOURCE_KOFIA = Asset("source.kofia", uri="asset://source/kofia", group="source")
SOURCE_FUNDGUIDE = Asset("source.fundguide", uri="asset://source/fundguide", group="source")
SOURCE_KASI = Asset("source.kasi", uri="asset://source/kasi", group="source")
SOURCE_MANUAL = Asset("source.manual", uri="asset://source/manual", group="source")


def mark(asset: Asset, **extra) -> None:
    """
    Purpose: 실행 중 태스크의 Asset 이벤트에 메모 첨부
    Parameter:
        - asset: outlets 에 선언한 Asset
        - extra: 메모 (JSON 직렬화 가능 값) — 공통 키 data_date·rows 필수, 구간 적재는 data_from·data_to
    Input: 태스크 컨텍스트, env ASSET_FLOW_ENV
    Output: 없음 (태스크 성공 시 이벤트 extra 로 저장, env 키 자동 추가). 공통 키가 없거나 형식이 틀리면 ValueError
    """
    import os
    import re

    from airflow.sdk import get_current_context

    if missing := {"data_date", "rows"} - extra.keys():
        raise ValueError(f"{asset.name} 이벤트 메모에 {sorted(missing)} 없음")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(extra["data_date"])) or type(extra["rows"]) is not int:
        raise ValueError(f"{asset.name} data_date={extra['data_date']!r}(YYYY-MM-DD)·rows={extra['rows']!r}(int) 형식 오류")
    get_current_context()["outlet_events"][asset].extra = {**extra, "env": os.environ.get("ASSET_FLOW_ENV", "test")}
