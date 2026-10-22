import ast
import importlib.util
import re
from pathlib import Path

import pytest
from airflow.sdk import Asset

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


def _load_dag(name):
    """
    Purpose: DAG 파일을 import 해 DAG 객체 반환
    Parameter:
        - name: DAG 파일명(확장자 제외) = dag_id
    Input: dags/<name>.py
    Output: DAG (@dag 팩토리를 다시 호출해 생성)
    """
    spec = importlib.util.spec_from_file_location(f"_a_{name}", DAGS / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return getattr(mod, name)()


DAG_IDS = sorted(f.stem for f in DAGS.glob("*.py"))
KNOWN = {v.name for v in vars(a).values() if isinstance(v, Asset)}


def test_groups_sources_and_mvs():
    # 대시보드는 group 으로 시장·계좌·배당·출처를 나누고, 이름 = 스키마.테이블 로 DB 객체와 합친다
    assert a.DIVIDEND_EVENT.group == a.DIVIDEND_RECEIPT.group == "dividend"
    assert (a.DIVIDEND_EVENT.name, a.DIVIDEND_RECEIPT.name) == ("market.dividend_event", "account.dividend_receipt")
    sources = {v.name: v for v in vars(a).values() if isinstance(v, Asset) and v.group == "source"}
    assert set(sources) == {f"source.{s}" for s in ("kis", "upbit", "kofia", "fundguide", "kasi", "manual")}
    for name, s in sources.items():                     # 이름만 — URL·키·계좌 정보 금지
        assert s.uri == f"asset://source/{name.split('.')[1]}"
    assert {a.MV_POSITION_DAILY.name, a.MV_COST_BASIS_KRW.name} == {"dashboard.mv_position_daily",
                                                                    "dashboard.mv_cost_basis_krw"}


@pytest.mark.parametrize("dag_id,task_id,expected", [
    ("auth_issue_api_tokens", "generate_tokens", {"source.kis"}),
    ("auth_issue_api_tokens", "check_key_expiry", {"source.upbit"}),
    ("market_ingest_trading_holiday", "fetch_kasi_holidays", {"source.kasi"}),
    ("market_ingest_reference_daily", "exchange_rate.fetch_exchange_rates", {"source.kis"}),
    ("market_ingest_reference_daily", "exchange_rate.backfill_exchange_rates", {"source.kis"}),
    ("market_ingest_reference_daily", "fund_price.collect_fund_prices",
     {"source.kofia", "source.fundguide", "market.trading_holiday"}),
    ("market_ingest_reference_daily", "fund_price.compare_with_fundguide", {"source.fundguide"}),
    ("market_ingest_reference_daily", "market_index.collect_index_prices", {"source.kis", "market.trading_holiday"}),
    ("account_ingest_asset_daily", "wait_for_exchange_rate", {"market.exchange_rate_daily"}),
    ("account_ingest_asset_daily", "wait_for_fund_price_daily", {"market.fund_price_daily"}),
    ("account_ingest_asset_daily", "collect_account", {"source.kis", "source.upbit", "market.exchange_rate_daily",
                                                       "market.fund_price_daily", "account.manual_position_ledger"}),
    ("market_ingest_product_price_daily", "wait_for_asset_daily", {"account.asset_daily"}),
    ("market_ingest_product_price_daily", "upsert_products", {"account.asset_daily"}),
    ("market_ingest_product_price_daily", "ingest_prices", {"source.kis", "source.upbit"}),
    ("market_ingest_product_price_daily", "ingest_fund_nav", {"source.kofia"}),
    ("account_ingest_dividend", "list_targets", {"market.product", "account.asset_daily"}),
    ("account_ingest_dividend", "ingest_kr_events", {"source.kis", "market.trading_holiday"}),
    ("account_ingest_dividend", "ingest_us_events", {"source.kis"}),
    ("account_ingest_dividend", "ingest_receipts", {"source.kis", "market.trading_holiday"}),
    ("account_ingest_dividend", "wait_for_asset_daily", {"account.asset_daily"}),
    ("account_ingest_dividend", "estimate_us_receipts", {"account.asset_daily", "market.exchange_rate_daily"}),
    ("account_rebuild_ledger_assets", "rebuild_account", {"account.manual_position_ledger", "market.fund_price_daily"}),
    ("account_upsert_manual_position_ledger", "validate", {"source.manual", "market.fund_price_daily"}),
    ("account_upsert_manual_position_ledger", "upsert", {"source.manual"}),
])
def test_task_inlets(dag_id, task_id, expected):
    # 외부 API·수기 입력 → source.*, 다른 DAG 가 쓴 테이블 읽기 → 그 테이블
    assert {i.name for i in _load_dag(dag_id).get_task(task_id).inlets} == expected


@pytest.mark.parametrize("dag_id", DAG_IDS)
def test_lineage_rules(dag_id):
    # inlets·outlets 는 assets.py 의 Asset 만 / 같은 DAG 가 쓰는 Asset 은 읽기로 선언하지 않음 (DAG 단위 순환 방지)
    tasks = _load_dag(dag_id).tasks
    written = {o.name for t in tasks for o in t.outlets}
    for t in tasks:
        names = {x.name for x in [*t.inlets, *t.outlets]}
        assert names <= KNOWN, (t.task_id, names - KNOWN)
        assert not {i.name for i in t.inlets} & written, t.task_id


def test_refresh_dashboard_views_declares_mv_outlets():
    # MV 갱신 태스크 (finalize 공용 + 배당·지수 스위치) → MV Asset 이벤트
    for dag_id, task_id in [("account_ingest_asset_daily", "refresh_dashboard_views"),
                            ("account_rebuild_ledger_assets", "refresh_dashboard_views"),
                            ("account_upsert_manual_position_ledger", "refresh_dashboard_views"),
                            ("account_ingest_dividend", "refresh_dashboard_views"),
                            ("market_ingest_reference_daily", "market_index.refresh_dashboard_views")]:
        task = _load_dag(dag_id).get_task(task_id)
        assert {o.name for o in task.outlets} == {"dashboard.mv_position_daily", "dashboard.mv_cost_basis_krw"}


def test_every_mark_has_common_keys():
    # 대시보드가 기준일·갱신량을 키 하나로 읽는다 (data_date·rows), 계좌번호 키 금지
    calls = [n for f in DAGS.glob("*.py") for n in ast.walk(ast.parse(f.read_text(encoding="utf-8")))
             if isinstance(n, ast.Call) and getattr(n.func, "id", None) == "mark"]
    assert calls
    for c in calls:
        keys = {k.arg for k in c.keywords}
        assert {"data_date", "rows"} <= keys and "account_code" not in keys, ast.unparse(c)


@pytest.mark.parametrize("extra", [{"rows": 1}, {"data_date": None, "rows": 0},
                                   {"data_date": "2026-10-22", "rows": "3"}])
def test_mark_rejects_bad_common_keys(extra):
    with pytest.raises(ValueError, match="data_date"):
        a.mark(a.ASSET_DAILY, **extra)
