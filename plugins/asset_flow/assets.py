"""
Airflow Asset 정의 (적재 테이블 = Asset, 계보·갱신 이력 표시 전용)

- 적재 태스크가 outlets 로 선언 → 성공 시 메타DB 에 이벤트 1건 (스케줄에는 쓰지 않음)
- mark: 이벤트에 기준일·건수 메모 첨부 (계좌번호 금지, 계좌 id 만)
"""

from __future__ import annotations

from airflow.sdk import Asset

TRADING_HOLIDAY = Asset("market.trading_holiday", uri="asset://market/trading_holiday", group="market")
EXCHANGE_RATE = Asset("market.exchange_rate_daily", uri="asset://market/exchange_rate_daily", group="market")
FUND_PRICE = Asset("market.fund_price_daily", uri="asset://market/fund_price_daily", group="market")
ASSET_DAILY = Asset("account.asset_daily", uri="asset://account/asset_daily", group="account")
MANUAL_LEDGER = Asset("account.manual_position_ledger", uri="asset://account/manual_position_ledger", group="account")


def mark(asset: Asset, **extra) -> None:
    """
    Purpose: 실행 중 태스크의 Asset 이벤트에 메모 첨부
    Parameter:
        - asset: outlets 에 선언한 Asset
        - extra: 메모 (JSON 직렬화 가능 값)
    Input: 태스크 컨텍스트
    Output: 없음 (태스크 성공 시 이벤트 extra 로 저장)
    """
    from airflow.sdk import get_current_context

    get_current_context()["outlet_events"][asset].extra = extra
