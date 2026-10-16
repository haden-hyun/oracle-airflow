from pathlib import Path
from types import SimpleNamespace

import pytest

from asset_flow import db_manager as db
from asset_flow.pipeline import dispatch as dp
from asset_flow.transformers.kis_transformer import EXCHANGE_CURRENCIES


class _Conn:
    def __init__(self, rows: dict, log: list):
        """
        Purpose: SQL 을 기록하는 가짜 커넥션
        Parameter:
            - rows: 테이블명 → 반환할 값 리스트
            - log: (sql, params) 기록 리스트
        Input: 없음
        Output: 없음
        """
        self.rows, self.log = rows, log

    def __enter__(self):
        """
        Purpose: with 진입
        Parameter: 없음
        Input: 없음
        Output: 자기 자신
        """
        return self

    def __exit__(self, *exc):
        """
        Purpose: with 종료
        Parameter:
            - exc: 예외 정보
        Input: 없음
        Output: False (예외 전파)
        """
        return False

    def execute(self, sql, params=None):
        """
        Purpose: SQL 기록 + 테이블별 값 반환
        Parameter:
            - sql: text 객체
            - params: 바인드 값
        Input: 없음
        Output: scalars() 를 가진 결과 대역
        """
        q = str(sql)
        self.log.append((q, params))
        vals = next((v for t, v in self.rows.items() if t in q), [])
        return SimpleNamespace(scalars=lambda: iter(vals))


def _engine(rows=None, log=None, name="asset_staging"):
    """
    Purpose: 가짜 엔진
    Parameter:
        - rows: 테이블명 → 반환 값
        - log: SQL 기록 리스트
        - name: DB 이름 (쓰기 가드 통과용)
    Input: 없음
    Output: 가짜 Engine
    """
    conn = _Conn(rows or {}, log if log is not None else [])
    return SimpleNamespace(url=SimpleNamespace(database=name), connect=lambda: conn, begin=lambda: conn)


def test_gaps_reports_missing_and_nonpositive():
    eng = _engine({"fund_price_daily": ["F1"], "exchange_rate_daily": ["USD", "JPY"]})
    gaps = db.get_market_data_gaps(eng, "2026-10-12", ["F1", "F2"], EXCHANGE_CURRENCIES)
    assert gaps == {"fund": ["F2"], "fx": ["EUR", "GBP"]}


def test_gaps_empty_when_complete():
    eng = _engine({"fund_price_daily": ["F1"], "exchange_rate_daily": EXCHANGE_CURRENCIES})
    assert db.get_market_data_gaps(eng, "2026-10-12", ["F1"], EXCHANGE_CURRENCIES) == {"fund": [], "fx": []}


def test_currencies_match_fx_dag_output():
    assert EXCHANGE_CURRENCIES == ["EUR", "GBP", "JPY", "USD"]


def test_keyed_replace_deletes_only_loaded_codes(monkeypatch):
    # 부분 수집(F1 만)이 같은 날 이미 적재된 F2 를 지우지 않음
    monkeypatch.setenv("ASSET_FLOW_ENV", "test")
    monkeypatch.setattr(db, "_append", lambda conn, s, t, r: None)
    log = []
    n = db.replace_daily_rows(_engine(log=log), "market", "fund_price_daily", "2026-10-12",
                              [{"product_code": "F1", "standard_price": 1.0}], key_column="product_code")
    sql, params = log[0]
    assert n == 1 and "product_code = ANY(:keys)" in sql and params == {"d": "2026-10-12", "keys": ["F1"]}


def test_unkeyed_replace_keeps_date_scope(monkeypatch):
    monkeypatch.setenv("ASSET_FLOW_ENV", "test")
    monkeypatch.setattr(db, "_append", lambda conn, s, t, r: None)
    log = []
    db.replace_daily_rows(_engine(log=log), "market", "exchange_rate_daily", "2026-10-12", [{"x": 1}])
    assert "ANY" not in log[0][0] and log[0][1] == {"d": "2026-10-12"}


def test_required_fund_codes_only_nav_accounts(cfg, app):
    # 연금저축·수기 펀드 계좌 코드만 (config 의 추가 크롤링 코드는 제외)
    need = dp.required_fund_codes(cfg, app)
    expect = {dp.fund_code_for(a, app) for a in cfg.active_accounts("kis_pension_fund", "manual_fund")}
    assert need == sorted(expect) and None not in need
    assert set(need) <= set(dp.fund_codes_to_crawl(cfg, app))


@pytest.mark.parametrize("dag_file", ["account_ingest_asset_daily", "market_ingest_reference_daily"])
def test_no_time_coupled_external_sensor(dag_file):
    # 상류 run 시각(execution_delta) 결합 금지 → 수동 재실행에서도 센서가 동작해야 함
    src = (Path(__file__).resolve().parents[1] / "dags" / f"{dag_file}.py").read_text(encoding="utf-8")
    assert "ExternalTaskSensor" not in src and "execution_delta" not in src
