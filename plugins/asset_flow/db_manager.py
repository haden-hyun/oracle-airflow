"""
업무 DB(postgres_asset) 접근 헬퍼

- asset_engine(): Connection postgres_asset 엔진 (태스크 안에서만 호출)
- 조회: get_fund_price / get_latest_fund_price / get_market_data_gaps / get_asset_date_bounds / get_ledger_dates / get_manual_positions / get_account_master / get_previous_ledger_amount / get_trading_holidays
- 쓰기: delete_and_insert_account_assets / replace_daily_rows / upsert_ledger / replace_trading_holidays / call_procedure
- 모든 쓰기 함수는 첫 줄에서 ensure_writable() — prod 가 아니면 *_staging DB 에만 쓴다
"""

from __future__ import annotations

import logging
import os
from datetime import date

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)
CONN_ID = "postgres_asset"
DATE_COLUMNS = ("standard_date", "holiday_date")    # DB date 타입 컬럼


def asset_engine() -> Engine:
    """
    Purpose: 업무 DB SQLAlchemy 엔진 생성
    Parameter: 없음
    Input: Airflow Connection postgres_asset
    Output: Engine (hide_parameters — DB 오류 메시지·알림에 계좌번호 등 바인드 값 미노출)
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=CONN_ID).get_sqlalchemy_engine(engine_kwargs={"hide_parameters": True})


def to_frame(records: list) -> pd.DataFrame:
    """
    Purpose: 적재용 DataFrame 생성 (날짜 문자열 → date)
    Parameter:
        - records: 적재 레코드
    Input: 없음
    Output: DATE_COLUMNS 를 datetime.date 로 바꾼 DataFrame
    """
    # pandas 3 + psycopg3: 문자열 날짜는 date 컬럼 INSERT 실패 → 명시 변환
    df = pd.DataFrame(records)
    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col]).dt.date
    return df


def _append(conn, schema: str, table_name: str, records: list) -> None:
    """
    Purpose: 레코드 append (to_frame 변환 후 to_sql)
    Parameter:
        - conn: 트랜잭션 커넥션
        - schema: 스키마명
        - table_name: 테이블명
        - records: 적재 레코드
    Input: 없음
    Output: 없음 (+ DB INSERT)
    """
    df = to_frame(records)
    df.to_sql(table_name, conn, schema=schema, if_exists="append", index=False)


def ensure_writable(engine: Engine) -> None:
    """
    Purpose: 쓰기 가드 — 비 prod 환경이 운영 DB 에 쓰는 사고 방지
    Parameter:
        - engine: 쓰기 대상 엔진
    Input: env ASSET_FLOW_ENV
    Output: 없음 (prod 가 아닌데 DB 명이 _staging 으로 끝나지 않으면 RuntimeError)
    """
    db = engine.url.database or ""
    if os.getenv("ASSET_FLOW_ENV") != "prod" and not db.endswith("_staging"):
        raise RuntimeError(f"쓰기 차단: ASSET_FLOW_ENV!=prod 인데 대상 DB '{db}' 가 *_staging 이 아님")


# ── 조회 ──
def get_fund_price(engine: Engine, product_code: str, standard_date: str) -> tuple[float | None, str | None]:
    """
    Purpose: 펀드 기준가·이름 조회
    Parameter:
        - engine: 업무 DB 엔진
        - product_code: 펀드 표준코드
        - standard_date: 기준일 (YYYY-MM-DD)
    Input: market.fund_price_daily
    Output: (기준가, 펀드명). 없거나 0 이면 (None, None)
    """
    query = text("""
        SELECT standard_price, product_name FROM market.fund_price_daily
         WHERE product_code = :product_code AND standard_date = :standard_date
         LIMIT 1
    """)
    with engine.connect() as conn:
        row = conn.execute(query, {"product_code": product_code, "standard_date": standard_date}).fetchone()
    if row is None or not row.standard_price:
        return None, None
    return float(row.standard_price), row.product_name


def get_latest_fund_price(engine: Engine, product_code: str):
    """
    Purpose: 가장 최근 양수 기준가 조회 (당일 입력 자릿수 검증용)
    Parameter:
        - engine: 업무 DB 엔진
        - product_code: 펀드 표준코드
    Input: market.fund_price_daily
    Output: Row(standard_price, standard_date) 또는 None
    """
    query = text("""
        SELECT standard_price, standard_date FROM market.fund_price_daily
         WHERE product_code = :product_code AND standard_price > 0
         ORDER BY standard_date DESC LIMIT 1
    """)
    with engine.connect() as conn:
        return conn.execute(query, {"product_code": product_code}).fetchone()


def get_market_data_gaps(engine: Engine, standard_date: str, fund_codes: list[str],
                         currencies: list[str]) -> dict[str, list[str]]:
    """
    Purpose: 기준일 시장 데이터 결측 확인 (자산 수집 선행 조건)
    Parameter:
        - engine: 업무 DB 엔진
        - standard_date: 기준일 (YYYY-MM-DD)
        - fund_codes: 있어야 할 펀드코드
        - currencies: 있어야 할 통화코드
    Input: market.fund_price_daily, market.exchange_rate_daily
    Output: {"fund": 결측 펀드코드, "fx": 결측 통화} (값 0 이하도 결측)
    """
    with engine.connect() as conn:
        funds = set(conn.execute(text(
            "SELECT product_code FROM market.fund_price_daily WHERE standard_date = :d AND standard_price > 0"),
            {"d": standard_date}).scalars())
        fx = set(conn.execute(text(
            "SELECT currency_code FROM market.exchange_rate_daily WHERE standard_date = :d AND base_rate > 0"),
            {"d": standard_date}).scalars())
    return {"fund": sorted(set(fund_codes) - funds), "fx": sorted(set(currencies) - fx)}


def get_asset_date_bounds(engine: Engine, schema: str, table_name: str) -> tuple[date | None, date | None]:
    """
    Purpose: asset_daily 기준일 범위 (원장 날짜 검증·재계산 종료일용)
    Parameter:
        - engine: 업무 DB 엔진
        - schema: 스키마명
        - table_name: 테이블명
    Input: asset_daily
    Output: (최소 기준일, 최대 기준일). 비어 있으면 (None, None)
    """
    with engine.connect() as conn:
        row = conn.execute(text(f"SELECT min(standard_date), max(standard_date) FROM {schema}.{table_name}")).one()
    return row[0], row[1]


def get_ledger_dates(engine: Engine, account_code: str) -> list[date]:
    """
    Purpose: 계좌의 기존 원장 기준일 목록
    Parameter:
        - engine: 업무 DB 엔진
        - account_code: 계좌번호
    Input: account.manual_position_ledger
    Output: 기준일 리스트 (오름차순)
    """
    with engine.connect() as conn:
        return list(conn.execute(text(
            "SELECT standard_date FROM account.manual_position_ledger WHERE account_code = :a ORDER BY 1"),
            {"a": account_code}).scalars())


def get_manual_positions(engine: Engine, standard_date: str) -> pd.DataFrame:
    """
    Purpose: 기준일에 유효한 수기 자산 포지션 (계좌별 최신 원장 1행)
    Parameter:
        - engine: 업무 DB 엔진
        - standard_date: 기준일 (YYYY-MM-DD)
    Input: account.manual_position_ledger, account.account_master (활성 계좌만)
    Output: DataFrame (없으면 빈 DataFrame)
    """
    query = text("""
        SELECT DISTINCT ON (l.account_code)
               l.standard_date, l.account_code, l.product_code,
               l.holding_quantity, l.total_purchase_amount,
               m.account_name, m.account_type
          FROM account.manual_position_ledger l
          JOIN account.account_master m USING (account_code)
         WHERE l.standard_date <= :standard_date AND m.is_active
         ORDER BY l.account_code, l.standard_date DESC
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params={"standard_date": standard_date})


def get_account_master(engine: Engine) -> dict[str, dict]:
    """
    Purpose: 활성 account_master 조회 (secrets 정합성 점검·upsert 검증용)
    Parameter:
        - engine: 업무 DB 엔진
    Input: account.account_master
    Output: {account_code: {"account_name", "account_type"}} (is_active 만)
    """
    query = text("SELECT account_code, account_name, account_type FROM account.account_master WHERE is_active")
    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()
    return {r.account_code: {"account_name": r.account_name, "account_type": r.account_type} for r in rows}


def get_previous_ledger_amount(engine: Engine, account_code: str, standard_date: str) -> float | None:
    """
    Purpose: 기준일 직전 원장의 누적 납입원금 조회
    Parameter:
        - engine: 업무 DB 엔진
        - account_code: 계좌 코드
        - standard_date: 기준일 (YYYY-MM-DD, 미포함)
    Input: account.manual_position_ledger
    Output: 직전 total_purchase_amount 또는 None
    """
    query = text("""
        SELECT total_purchase_amount FROM account.manual_position_ledger
         WHERE account_code = :account_code AND standard_date < :standard_date
         ORDER BY standard_date DESC LIMIT 1
    """)
    with engine.connect() as conn:
        row = conn.execute(query, {"account_code": account_code, "standard_date": standard_date}).fetchone()
    return None if row is None else float(row.total_purchase_amount)


# ── 쓰기 (모두 ensure_writable 선행) ──
def delete_and_insert_account_assets(
    engine: Engine, schema: str, table_name: str, standard_date: str, account_code: str, records: list
) -> int:
    """
    Purpose: (standard_date, account_code) 스코프 멱등 적재 — 다른 계좌·날짜는 건드리지 않음
    Parameter:
        - engine: 업무 DB 엔진
        - schema: 스키마명
        - table_name: 테이블명
        - standard_date: 기준일
        - account_code: 계좌 코드 (DELETE 스코프)
        - records: 적재 레코드
    Input: 없음
    Output: 삽입 행 수 (+ DB DELETE/INSERT, 단일 트랜잭션)
    """
    ensure_writable(engine)
    with engine.begin() as conn:
        deleted = conn.execute(
            text(f"DELETE FROM {schema}.{table_name} WHERE standard_date = :d AND account_code = :c"),
            {"d": standard_date, "c": account_code},
        ).rowcount
        if deleted:
            log.info("[재적재] %s: 기존 %s건 삭제 후 재적재", standard_date, deleted)
        if records:
            _append(conn, schema, table_name, records)
    return len(records)


def replace_daily_rows(engine: Engine, schema: str, table_name: str, standard_date: str, records: list,
                       key_column: str | None = None) -> int:
    """
    Purpose: standard_date 단위 멱등 적재 (환율·펀드 기준가)
    Parameter:
        - engine: 업무 DB 엔진
        - schema: 스키마명
        - table_name: 테이블명
        - standard_date: 기준일 (DELETE 스코프)
        - records: 적재 레코드 (비어 있으면 아무것도 하지 않음)
        - key_column: 지정 시 records 에 있는 키만 삭제 (부분 적재가 기존 행을 지우지 않게)
    Input: 없음
    Output: 삽입 행 수 (+ DB DELETE/INSERT, 단일 트랜잭션)
    """
    if not records:
        return 0
    ensure_writable(engine)
    where, params = "standard_date = :d", {"d": standard_date}
    if key_column:
        where += f" AND {key_column} = ANY(:keys)"
        params["keys"] = sorted({r[key_column] for r in records})
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {schema}.{table_name} WHERE {where}"), params)
        _append(conn, schema, table_name, records)
    return len(records)


def call_procedure(engine: Engine, name: str) -> None:
    """
    Purpose: 인자 없는 DB 프로시저 호출 (대시보드 MV 갱신 등)
    Parameter:
        - engine: 업무 DB 엔진
        - name: 스키마.프로시저 (config 에서 패턴 검증된 값)
    Input: 없음
    Output: 없음 (+ 프로시저가 하는 쓰기, 쓰기 가드 적용). 실패 시 DB 예외
    """
    ensure_writable(engine)
    with engine.begin() as conn:
        conn.execute(text(f"CALL {name}()"))


def upsert_ledger(engine: Engine, row: dict) -> None:
    """
    Purpose: 수기 원장 UPSERT — 같은 (standard_date, account_code) 재입력은 오타 정정으로 덮어씀
    Parameter:
        - engine: 업무 DB 엔진
        - row: {standard_date, account_code, product_code, holding_quantity, total_purchase_amount}
    Input: 없음
    Output: 없음 (+ account.manual_position_ledger 쓰기)
    """
    ensure_writable(engine)
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO account.manual_position_ledger
                    (standard_date, account_code, product_code, holding_quantity, total_purchase_amount)
                VALUES (:standard_date, :account_code, :product_code, :holding_quantity, :total_purchase_amount)
                ON CONFLICT (standard_date, account_code) DO UPDATE SET
                    product_code = EXCLUDED.product_code,
                    holding_quantity = EXCLUDED.holding_quantity,
                    total_purchase_amount = EXCLUDED.total_purchase_amount
            """),
            row,
        )


def get_trading_holidays(engine: Engine, start: str, end: str) -> tuple[dict, set]:
    """
    Purpose: 기간 내 휴장일 현재 상태 조회
    Parameter:
        - engine: 업무 DB 엔진
        - start: 기간 시작 (YYYY-MM-DD)
        - end: 기간 끝 (YYYY-MM-DD)
    Input: market.trading_holiday
    Output: (자동 행 {(market, date): (name, source)}, 수동 행 {(market, date)})
    """
    sql = text("SELECT market, holiday_date::text AS d, name, source, is_manual FROM market.trading_holiday "
               "WHERE holiday_date BETWEEN :s AND :e")
    auto, manual = {}, set()
    with engine.connect() as conn:
        for r in conn.execute(sql, {"s": start, "e": end}):
            if r.is_manual:
                manual.add((r.market, r.d))
            else:
                auto[(r.market, r.d)] = (r.name, r.source)
    return auto, manual


def replace_trading_holidays(engine: Engine, start: str, end: str, records: list) -> tuple[set, set]:
    """
    Purpose: 기간 내 자동 휴장일 행을 통째로 다시 씀 (수동 행 보호)
    Parameter:
        - engine: 업무 DB 엔진
        - start: 기간 시작 (YYYY-MM-DD)
        - end: 기간 끝 (YYYY-MM-DD)
        - records: [market, 'YYYY-MM-DD', name, source] — 앞쪽이 우선 (ON CONFLICT DO NOTHING)
    Input: market.trading_holiday
    Output: (변경 전, 변경 후) {(market, date)} 집합 (+ 단일 트랜잭션 DELETE non-manual + INSERT)
    """
    ensure_writable(engine)
    period = "holiday_date BETWEEN :s AND :e"
    select = text(f"SELECT market, holiday_date::text AS d FROM market.trading_holiday WHERE {period}")
    rng = {"s": start, "e": end}
    with engine.begin() as conn:
        before = {(r.market, r.d) for r in conn.execute(select, rng)}
        conn.execute(text(f"DELETE FROM market.trading_holiday WHERE NOT is_manual AND {period}"), rng)
        if records:
            conn.execute(
                text("""
                    INSERT INTO market.trading_holiday (market, holiday_date, name, source)
                    VALUES (:market, CAST(:holiday_date AS date), :name, :source)
                    ON CONFLICT (market, holiday_date) DO NOTHING
                """),
                [dict(zip(("market", "holiday_date", "name", "source"), r, strict=True)) for r in records],
            )
        after = {(r.market, r.d) for r in conn.execute(select, rng)}
    return before, after
