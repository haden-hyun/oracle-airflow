"""
업무 DB(postgres_asset) 접근 헬퍼

- asset_engine(): Connection postgres_asset 엔진 (태스크 안에서만 호출)
- 조회: get_fund_price / get_latest_fund_price / get_fund_names / get_market_data_gaps / get_asset_date_bounds / get_ledger_dates / get_manual_positions / get_account_master / get_previous_ledger_amount / get_trading_holidays
- 쓰기: delete_and_insert_account_assets / replace_daily_rows / upsert_ledger / replace_trading_holidays / call_procedure
- 공통 조회(시세·배당): has_asset_rows / get_holiday_sets / table_has_rows
- 배당: get_dividend_targets / get_us_holdings / get_usd_rates / get_us_dividend_events / upsert_rows(쓰기)
- 시세: get_price_targets (조회), upsert_products / upsert_product_prices /
        insert_missing_fund_prices / set_price_from_date (쓰기)
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


def get_fund_names(engine: Engine, codes: list[str]) -> dict[str, str]:
    """
    Purpose: 펀드 표시명 (fund_price_daily 최신 행의 이름 — KOFIA 응답엔 이름 없음)
    Parameter:
        - engine: 업무 DB 엔진
        - codes: 펀드 표준코드
    Input: market.fund_price_daily
    Output: {코드: 이름} (이력 없는 코드는 빠짐)
    """
    sql = text("SELECT DISTINCT ON (product_code) product_code, product_name FROM market.fund_price_daily "
               "WHERE product_code = ANY(:c) AND product_name IS NOT NULL ORDER BY product_code, standard_date DESC")
    with engine.connect() as conn:
        return {r.product_code: r.product_name for r in conn.execute(sql, {"c": list(codes)})}


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


# ── 시세·배당 공통 조회 ──
def has_asset_rows(engine: Engine, standard_date: str, account_codes: list[str] | None = None) -> bool:
    """
    Purpose: 기준일 asset_daily 적재 여부 (시세·배당 DAG 센서용)
    Parameter:
        - engine: 업무 DB 엔진
        - standard_date: 기준일 (YYYY-MM-DD)
        - account_codes: 확인할 계좌 코드 (모두 있어야 True). None 이면 비현금 행 1개 이상
    Input: account.asset_daily
    Output: bool (account_codes 가 빈 리스트면 True)
    """
    with engine.connect() as conn:
        if account_codes is None:
            return conn.execute(text("SELECT EXISTS (SELECT 1 FROM account.asset_daily "
                                     "WHERE standard_date = :d AND asset_type <> 'CASH')"),
                                {"d": standard_date}).scalar()
        sql = text("SELECT count(DISTINCT account_code) FROM account.asset_daily "
                   "WHERE standard_date = :d AND account_code = ANY(:c)")
        return conn.execute(sql, {"d": standard_date, "c": list(account_codes)}).scalar() == len(set(account_codes))


def get_holiday_sets(engine: Engine, start: str, end: str) -> dict[str, set[date]]:
    """
    Purpose: 평일 휴장일 (수동 행 포함) — 거래일·배당락일 계산용
    Parameter:
        - engine: 업무 DB 엔진
        - start: 기간 시작 (YYYY-MM-DD)
        - end: 기간 끝 (YYYY-MM-DD)
    Input: market.trading_holiday
    Output: {"KR": {date}, "US": {date}}
    """
    out: dict[str, set[date]] = {"KR": set(), "US": set()}
    sql = text("SELECT market, holiday_date FROM market.trading_holiday WHERE holiday_date BETWEEN :s AND :e")
    with engine.connect() as conn:
        for r in conn.execute(sql, {"s": start, "e": end}):
            out.setdefault(r.market, set()).add(r.holiday_date)
    return out


def table_has_rows(engine: Engine, qualified_name: str) -> bool:
    """
    Purpose: 테이블이 있고 행이 1개 이상인지 (다른 DAG 가 채우는 테이블의 fallback 판정)
    Parameter:
        - engine: 업무 DB 엔진
        - qualified_name: 스키마.테이블 (코드 상수만 — 사용자 입력 금지)
    Input: pg_catalog, 대상 테이블
    Output: 없거나 비었으면 False
    """
    with engine.connect() as conn:
        if not conn.execute(text("SELECT to_regclass(:n) IS NOT NULL"), {"n": qualified_name}).scalar():
            return False
        return conn.execute(text(f"SELECT EXISTS (SELECT 1 FROM {qualified_name})")).scalar()


# ── 배당 (account_ingest_dividend) ──
def get_dividend_targets(engine: Engine, since: str) -> dict:
    """
    Purpose: 배당 조회 대상 종목 — 기간 내 보유 이력 ∪ 원장에 나온 국내 종목
    Parameter:
        - engine: 업무 DB 엔진
        - since: 보유 이력 시작일 (YYYY-MM-DD, 보통 T−400)
    Input: market.product (행이 있으면) 또는 account.asset_daily, account.dividend_receipt
    Output: {"kr": [코드], "us": [[티커, 거래소]], "source": "market.product"|"asset_daily"} (정렬)
    """
    if table_has_rows(engine, "market.product"):
        held = text("""
            SELECT product_code, market, exchange_code FROM market.product
             WHERE asset_type = 'STOCK' AND market IN ('KR', 'US') AND last_held_date >= :since
        """)
        source = "market.product"
    else:                                         # 시세 DAG 가 아직 product 를 못 채운 경우
        held = text("""
            SELECT product_code, CASE currency_code WHEN 'KRW' THEN 'KR' ELSE 'US' END AS market,
                   max(exchange_code) AS exchange_code
              FROM account.asset_daily
             WHERE asset_type = 'STOCK' AND currency_code IN ('KRW', 'USD') AND standard_date >= :since
               AND (holding_quantity > 0 OR total_evaluation_amount > 0)
             GROUP BY 1, 2
        """)
        source = "asset_daily"
    receipt = text("SELECT DISTINCT product_code FROM account.dividend_receipt WHERE NOT is_estimated")
    with engine.connect() as conn:
        rows = conn.execute(held, {"since": since}).fetchall()
        kr = {r.product_code for r in rows if r.market == "KR"} | set(conn.execute(receipt).scalars())
    us = sorted({(r.product_code, r.exchange_code) for r in rows if r.market == "US"})
    return {"kr": sorted(kr), "us": [list(u) for u in us], "source": source}


def get_us_holdings(engine: Engine, account_codes: list[str], start: str, end: str) -> pd.DataFrame:
    """
    Purpose: 미국 주식 일별 보유수량 (추정 원장의 락일 직전 수량)
    Parameter:
        - engine: 업무 DB 엔진
        - account_codes: 대상 계좌 코드
        - start: 기간 시작 (YYYY-MM-DD)
        - end: 기간 끝 (YYYY-MM-DD)
    Input: account.asset_daily (currency_code USD, STOCK)
    Output: DataFrame[account_code, product_code, standard_date, holding_quantity] 오름차순
    """
    sql = text("""
        SELECT account_code, product_code, standard_date, holding_quantity::float AS holding_quantity
          FROM account.asset_daily
         WHERE asset_type = 'STOCK' AND currency_code = 'USD' AND account_code = ANY(:c)
           AND standard_date BETWEEN :s AND :e
         ORDER BY 1, 2, 3
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"c": list(account_codes), "s": start, "e": end})


def get_usd_rates(engine: Engine, start: str) -> list[tuple[date, float]]:
    """
    Purpose: USD 기준율 시계열 (추정 원장 fx_rate = 지급일 이하 최신값)
    Parameter:
        - engine: 업무 DB 엔진
        - start: 시작일 (YYYY-MM-DD)
    Input: market.exchange_rate_daily
    Output: [(standard_date, base_rate)] 오름차순 (0 이하 제외)
    """
    sql = text("SELECT standard_date, base_rate::float FROM market.exchange_rate_daily "
               "WHERE currency_code = 'USD' AND base_rate > 0 AND standard_date >= :s ORDER BY 1")
    with engine.connect() as conn:
        return [(r[0], r[1]) for r in conn.execute(sql, {"s": start})]


def get_us_dividend_events(engine: Engine, ex_from: str, ex_to: str) -> list[dict]:
    """
    Purpose: 추정 대상 미국 배당 일정 (락일 범위)
    Parameter:
        - engine: 업무 DB 엔진
        - ex_from: 락일 시작 (YYYY-MM-DD)
        - ex_to: 락일 끝 (YYYY-MM-DD, 보통 기준일 — asset_daily 가 있는 날까지)
    Input: market.dividend_event
    Output: 레코드 dict 리스트 (ex_date·pay_date 있는 행만, dps_amount 는 float|None)
    """
    sql = text("""
        SELECT product_code, record_date, dividend_kind, product_name, ex_date, pay_date,
               dps_amount::float AS dps_amount, currency_code
          FROM market.dividend_event
         WHERE market = 'US' AND ex_date BETWEEN :s AND :e AND pay_date IS NOT NULL
    """)
    with engine.connect() as conn:
        return [dict(r._mapping) for r in conn.execute(sql, {"s": ex_from, "e": ex_to})]


def upsert_rows(engine: Engine, schema: str, table_name: str, records: list, pk: list[str], seen_date: str,
                keep_if_null: tuple[str, ...] = ()) -> int:
    """
    Purpose: PK 기준 UPSERT (정정 흡수, DELETE 없음) + last_seen_date 갱신
    Parameter:
        - engine: 업무 DB 엔진
        - schema: 스키마명 (config 값)
        - table_name: 테이블명 (config 값)
        - records: 컬럼 dict 리스트 (모두 같은 키)
        - pk: 충돌 키 컬럼
        - seen_date: 이번 run 날짜 YYYY-MM-DD (last_seen_date — 기존보다 작으면 유지)
        - keep_if_null: 새 값이 NULL 이면 기존 값을 유지할 컬럼 (일시적 결측이 확정값을 지우지 않게)
    Input: 없음
    Output: 처리 행 수 (+ 단일 트랜잭션 INSERT … ON CONFLICT DO UPDATE). 0건이면 DB 미접속.
            update_datetime 은 값이 바뀐 행만 갱신 (정정 감지용)
    """
    if not records:
        return 0
    ensure_writable(engine)
    cols = list(records[0])
    new = {c: f"COALESCE(EXCLUDED.{c}, t.{c})" if c in keep_if_null else f"EXCLUDED.{c}"
           for c in cols if c not in pk}
    changed = f"({', '.join(f't.{c}' for c in new)}) IS DISTINCT FROM ({', '.join(new.values())})"
    sql = text(f"""
        INSERT INTO {schema}.{table_name} AS t ({", ".join(cols)}, last_seen_date)
        VALUES ({", ".join(":" + c for c in cols)}, CAST(:last_seen_date AS date))
        ON CONFLICT ({", ".join(pk)}) DO UPDATE SET {", ".join(f"{c} = {v}" for c, v in new.items())},
            last_seen_date = GREATEST(t.last_seen_date, EXCLUDED.last_seen_date),
            update_datetime = CASE WHEN {changed} THEN now() ELSE t.update_datetime END
    """)
    with engine.begin() as conn:
        conn.execute(sql, [r | {"last_seen_date": seen_date} for r in records])
    return len(records)


# ── 상품·시세 (market_ingest_product_price_daily) ──
HELD = "(a.holding_quantity > 0 OR a.total_evaluation_amount > 0)"     # GOLD 는 수량 0·평가액만 있음


def upsert_products(engine: Engine, base_date: str, tracked_after_sale_days: int) -> dict:
    """
    Purpose: asset_daily 비현금 상품을 market.product 에 upsert (이름·보유구간·is_tracked 재계산)
    Parameter:
        - engine: 업무 DB 엔진
        - base_date: 실행일 T (YYYY-MM-DD) — 매도 후 추적 기준 T-1
        - tracked_after_sale_days: 매도 후 추적 일수 (30)
    Input: account.asset_daily
    Output: {"total", "tracked", "new"} (+ market.product INSERT/UPDATE, 삭제 없음, 단일 트랜잭션).
            price_source·market·source_symbol 은 INSERT 때만 정함 (사람이 고친 값 보존)
    """
    ensure_writable(engine)
    sql = text(f"""
        WITH latest AS (
            SELECT DISTINCT ON (product_code) product_code, product_name, asset_type, exchange_code, currency_code
              FROM account.asset_daily WHERE asset_type <> 'CASH'
             ORDER BY product_code, standard_date DESC, total_evaluation_amount DESC
        ), held AS (
            SELECT a.product_code, min(a.standard_date) FILTER (WHERE {HELD}) AS f,
                   max(a.standard_date) FILTER (WHERE {HELD}) AS l
              FROM account.asset_daily a WHERE a.asset_type <> 'CASH' GROUP BY 1
        ), src AS (
            SELECT l.*, h.f, h.l,
                   CASE WHEN l.asset_type = 'STOCK' AND l.currency_code = 'KRW' THEN 'KR'
                        WHEN l.asset_type = 'STOCK' THEN 'US'
                        WHEN l.asset_type = 'CRYPTO' THEN 'CRYPTO'
                        WHEN l.asset_type = 'FUND' THEN 'FUND' ELSE 'KR' END AS market,
                   CASE WHEN l.asset_type = 'STOCK' AND l.currency_code = 'KRW' THEN 'KIS_KR_DAILY'
                        WHEN l.asset_type = 'STOCK' AND l.exchange_code IN ('NASD','NYSE','AMEX') THEN 'KIS_US_DAILY'
                        WHEN l.asset_type = 'CRYPTO' THEN 'UPBIT_DAY'
                        WHEN l.asset_type = 'FUND' THEN 'FUND_NAV' ELSE 'NONE' END AS price_source
              FROM latest l JOIN held h USING (product_code)
        )
        INSERT INTO market.product AS p (product_code, product_name, asset_type, market, exchange_code, currency_code,
                                         price_source, source_symbol, first_held_date, last_held_date, is_tracked)
        SELECT product_code, product_name, asset_type, market, exchange_code, currency_code, price_source,
               CASE price_source WHEN 'NONE' THEN NULL WHEN 'UPBIT_DAY' THEN 'KRW-' || product_code
                    ELSE product_code END,
               f, l, l IS NOT NULL AND l >= CAST(:d AS date) - 1 - :keep
          FROM src
        ON CONFLICT (product_code) DO UPDATE SET
               product_name = EXCLUDED.product_name, exchange_code = EXCLUDED.exchange_code,
               first_held_date = EXCLUDED.first_held_date, last_held_date = EXCLUDED.last_held_date,
               is_tracked = EXCLUDED.is_tracked, update_datetime = now()
         WHERE (p.product_name, p.exchange_code, p.first_held_date, p.last_held_date, p.is_tracked)
               IS DISTINCT FROM (EXCLUDED.product_name, EXCLUDED.exchange_code, EXCLUDED.first_held_date,
                                 EXCLUDED.last_held_date, EXCLUDED.is_tracked)
        RETURNING (xmax = 0) AS inserted
    """)
    with engine.begin() as conn:
        new = sum(1 for r in conn.execute(sql, {"d": base_date, "keep": tracked_after_sale_days}) if r.inserted)
        total, tracked = conn.execute(text(
            "SELECT count(*), count(*) FILTER (WHERE is_tracked) FROM market.product")).one()
    return {"total": total, "tracked": tracked, "new": new}


def get_price_targets(engine: Engine, codes: list[str] | None = None) -> list[dict]:
    """
    Purpose: 수집 대상 상품과 적재 범위
    Parameter:
        - engine: 업무 DB 엔진
        - codes: 지정 시 이 코드만 (is_tracked 무시 — 수동 백필용), None 이면 is_tracked 전부
    Input: market.product, market.product_price_daily, market.fund_price_daily
    Output: [{product 열…, "first_loaded": date|None, "last_loaded": date|None}] (price_source NONE 제외)
    """
    sql = text("""
        WITH px AS (SELECT product_code, min(standard_date) f, max(standard_date) l
                      FROM market.product_price_daily GROUP BY 1),
             nav AS (SELECT product_code, min(standard_date) f, max(standard_date) l
                       FROM market.fund_price_daily GROUP BY 1)
        SELECT p.product_code, p.product_name, p.asset_type, p.market, p.exchange_code, p.currency_code,
               p.price_source, p.source_symbol, p.first_held_date, p.last_held_date, p.is_tracked, p.price_from_date,
               CASE WHEN p.price_source = 'FUND_NAV' THEN nav.f ELSE px.f END AS first_loaded,
               CASE WHEN p.price_source = 'FUND_NAV' THEN nav.l ELSE px.l END AS last_loaded
          FROM market.product p
          LEFT JOIN px  ON px.product_code  = p.product_code
          LEFT JOIN nav ON nav.product_code = p.product_code
         WHERE p.price_source <> 'NONE'
           AND ((CAST(:codes AS text[]) IS NULL AND p.is_tracked) OR p.product_code = ANY(CAST(:codes AS text[])))
         ORDER BY p.price_source, p.product_code
    """)
    with engine.connect() as conn:
        return [dict(r._mapping) for r in conn.execute(sql, {"codes": codes})]


def upsert_product_prices(engine: Engine, records: list) -> int:
    """
    Purpose: 일봉 upsert — 값이 다를 때만 갱신 (DELETE 없음)
    Parameter:
        - engine: 업무 DB 엔진
        - records: PRICE_COLUMNS 레코드 (standard_date 는 YYYY-MM-DD)
    Input: 없음
    Output: 넘긴 행 수 (+ market.product_price_daily 쓰기, 단일 트랜잭션)
    """
    if not records:
        return 0
    ensure_writable(engine)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO market.product_price_daily AS t
                   (standard_date, product_code, opening_price, highest_price, lowest_price, closing_price,
                    trading_volume, currency_code, source)
            VALUES (CAST(:standard_date AS date), :product_code, :opening_price, :highest_price, :lowest_price,
                    :closing_price, :trading_volume, :currency_code, :source)
            ON CONFLICT (product_code, standard_date) DO UPDATE SET
                   opening_price = EXCLUDED.opening_price, highest_price = EXCLUDED.highest_price,
                   lowest_price = EXCLUDED.lowest_price, closing_price = EXCLUDED.closing_price,
                   trading_volume = EXCLUDED.trading_volume, update_datetime = now()
             WHERE (t.opening_price, t.highest_price, t.lowest_price, t.closing_price, t.trading_volume)
                   IS DISTINCT FROM (EXCLUDED.opening_price, EXCLUDED.highest_price, EXCLUDED.lowest_price,
                                     EXCLUDED.closing_price, EXCLUDED.trading_volume)
        """), [_nan_to_none(r) for r in records])
    return len(records)


def insert_missing_fund_prices(engine: Engine, records: list) -> int:
    """
    Purpose: 펀드 기준가 빈 날짜만 추가 — 기존 행(크롤링·이월분)은 절대 바꾸지 않음
    Parameter:
        - engine: 업무 DB 엔진
        - records: FUND_PRICE_COLUMNS 레코드
    Input: 없음
    Output: 실제 추가된 행 수 (+ market.fund_price_daily INSERT ... ON CONFLICT DO NOTHING, 건별 rowcount 합)
    """
    if not records:
        return 0
    ensure_writable(engine)
    sql = text("""
        INSERT INTO market.fund_price_daily (standard_date, product_code, product_name, standard_price)
        VALUES (CAST(:standard_date AS date), :product_code, :product_name, :standard_price)
        ON CONFLICT (standard_date, product_code) DO NOTHING
    """)
    with engine.begin() as conn:
        # executemany rowcount 합계는 드라이버마다 달라 건별 실행 (펀드 2종 × 수백 행)
        return sum(conn.execute(sql, r).rowcount for r in records)


def set_price_from_date(engine: Engine, product_code: str, first_date: str) -> None:
    """
    Purpose: 첫 적재 결과로 시세 하한 기록 (상장일 또는 0건이면 종료일+1 → 재요청 방지)
    Parameter:
        - engine: 업무 DB 엔진
        - product_code: 상품코드
        - first_date: YYYY-MM-DD
    Input: 없음
    Output: 없음 (+ market.product UPDATE — 이미 값이 있으면 더 이른 날짜만 반영)
    """
    ensure_writable(engine)
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE market.product SET price_from_date = CAST(:d AS date), update_datetime = now()
             WHERE product_code = :c AND (price_from_date IS NULL OR price_from_date > CAST(:d AS date))
        """), {"c": product_code, "d": first_date})


def _nan_to_none(r: dict) -> dict:
    """
    Purpose: NaN → None (numeric NULL 바인딩)
    Parameter:
        - r: 레코드
    Input: 없음
    Output: 새 dict
    """
    return {k: (None if isinstance(v, float) and v != v else v) for k, v in r.items()}
