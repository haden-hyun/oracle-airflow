import re
from pathlib import Path

import pytest

from asset_flow import assets as a

DAGS = Path(__file__).resolve().parents[1] / "dags"


def test_asset_names_match_config_tables(app):
    # Asset 이름 = 실제 적재 테이블 (config 와 어긋나면 계보가 거짓이 됨)
    t = app.tables
    assert a.ASSET_DAILY.name == f"{t.asset_daily.db_schema}.{t.asset_daily.table_name}"
    assert a.EXCHANGE_RATE.name == f"{t.exchange_rate_daily.db_schema}.{t.exchange_rate_daily.table_name}"
    assert a.FUND_PRICE.name == f"{t.fund_price_daily.db_schema}.{t.fund_price_daily.table_name}"
    for asset, table in [(a.PRODUCT, t.product), (a.PRODUCT_PRICE, t.product_price_daily), (a.MARKET_INDEX, t.index_daily),
                         (a.DIVIDEND_EVENT, t.dividend_event), (a.DIVIDEND_RECEIPT, t.dividend_receipt)]:
        assert asset.name == f"{table.db_schema}.{table.table_name}"


@pytest.mark.parametrize("dag_file,asset,task_id", [
    ("market_ingest_reference_daily", "EXCHANGE_RATE", "upload_exchange_rates"),
    ("market_ingest_reference_daily", "EXCHANGE_RATE", "backfill_exchange_rates"),
    ("market_ingest_reference_daily", "FUND_PRICE", "upload_fund_prices"),
    ("market_ingest_reference_daily", "MARKET_INDEX", "upload_index_prices"),
    ("account_ingest_asset_daily", "ASSET_DAILY", "collect_account"),
    ("account_rebuild_ledger_assets", "ASSET_DAILY", "rebuild_account"),
    ("account_upsert_manual_position_ledger", "MANUAL_LEDGER", "upsert"),
    ("account_upsert_manual_position_ledger", "ASSET_DAILY", "rebuild_from_ledger"),
    ("market_ingest_trading_holiday", "TRADING_HOLIDAY", "upsert_holidays"),
    ("account_ingest_dividend", "DIVIDEND_EVENT", "ingest_kr_events"),
    ("account_ingest_dividend", "DIVIDEND_EVENT", "ingest_us_events"),
    ("account_ingest_dividend", "DIVIDEND_RECEIPT", "ingest_receipts"),
    ("account_ingest_dividend", "DIVIDEND_RECEIPT", "estimate_us_receipts"),
    ("market_ingest_product_price_daily", "PRODUCT", "upsert_products"),
    ("market_ingest_product_price_daily", "PRODUCT_PRICE", "ingest_prices"),
    ("market_ingest_product_price_daily", "FUND_PRICE", "ingest_fund_nav"),
])
def test_writer_task_declares_outlet_and_marks(dag_file, asset, task_id):
    # 쓰기 태스크마다 outlets 선언 + 이벤트 메모 첨부
    src = (DAGS / f"{dag_file}.py").read_text(encoding="utf-8")
    block = re.search(rf'task_id="{task_id}".*?(?=task_id=|\Z)', src, re.S).group(0)
    assert f"outlets=[{asset}]" in block and f"mark({asset}," in block


def test_no_dag_schedules_on_assets():
    # 표시 전용: Asset 으로 스케줄하지 않음 (날짜 없는 이벤트 → 과거 기준일 덮어쓰기 위험)
    for f in DAGS.glob("*.py"):
        assert not re.search(r"schedule=\s*[\[(]?\s*(Asset|[A-Z_]{4,}\s*[&|\]])", f.read_text(encoding="utf-8")), f.name
