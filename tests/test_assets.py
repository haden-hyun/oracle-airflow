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


@pytest.mark.parametrize("dag_file,asset,task_id", [
    ("market_ingest_exchange_rate_daily", "EXCHANGE_RATE", "upload_exchange_rates"),
    ("market_ingest_fund_price_daily", "FUND_PRICE", "upload_fund_prices"),
    ("account_ingest_asset_daily", "ASSET_DAILY", "collect_account"),
    ("account_rebuild_ledger_assets", "ASSET_DAILY", "rebuild_account"),
    ("account_upsert_manual_position_ledger", "MANUAL_LEDGER", "upsert"),
    ("account_upsert_manual_position_ledger", "ASSET_DAILY", "rebuild_from_ledger"),
    ("market_ingest_trading_holiday", "TRADING_HOLIDAY", "upsert_holidays"),
])
def test_writer_task_declares_outlet_and_marks(dag_file, asset, task_id):
    # 쓰기 태스크마다 outlets 선언 + 이벤트 메모 첨부
    src = (DAGS / f"{dag_file}.py").read_text(encoding="utf-8")
    block = re.search(rf'task_id="{task_id}".*?(?=\n    @task|\n    [a-z_]+ = |\Z)', src, re.S).group(0)
    assert f"outlets=[{asset}]" in block and f"mark({asset}," in block


def test_no_dag_schedules_on_assets():
    # 표시 전용: Asset 으로 스케줄하지 않음 (날짜 없는 이벤트 → 과거 기준일 덮어쓰기 위험)
    for f in DAGS.glob("*.py"):
        assert not re.search(r"schedule=\s*[\[(]?\s*(Asset|[A-Z_]{4,}\s*[&|\]])", f.read_text(encoding="utf-8")), f.name
