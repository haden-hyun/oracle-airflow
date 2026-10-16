-- 004 — market.index_daily (주요 지수 일봉: 코스피·코스닥·S&P 500·나스닥 종합)
--
-- 왜: 대시보드 홈 '시장' 카드와 성과 수익률 차트의 비교 선. DB 에 지수 데이터가 없었다.
-- 채우는 DAG: market_ingest_reference_daily 의 market_index 그룹 (06:55, 첫 run 이 2025-10-13 부터 백필)
-- 규칙
--   - 거래일만 있다(휴장일 행 없음, 이월 없음) — 대시보드가 기간 등락률을 '마지막 거래일 종가'로 계산
--   - standard_date = 그 시장의 현지 거래일 (US 는 미국 날짜)
--   - 매일 최근 5거래일을 다시 받아 PK upsert (정정 흡수). 행 삭제 없음
-- 거래량은 넣지 않는다: 국내는 천 주 단위, S&P 500 은 0 으로 와서 지수 간 의미가 맞지 않는다.
-- index_code 에 CHECK 를 두지 않는다: 지수 추가는 코드 상수(KIS.MARKET_INDEXES)만 바꾸면 되게.
BEGIN;

CREATE TABLE IF NOT EXISTS market.index_daily (
    standard_date    date          NOT NULL,
    index_code       varchar(20)   NOT NULL,
    market           varchar(10)   NOT NULL,
    opening_price    numeric(20,6),
    highest_price    numeric(20,6),
    lowest_price     numeric(20,6),
    closing_price    numeric(20,6) NOT NULL,
    source           varchar(20)   NOT NULL,
    insert_datetime  timestamp     NOT NULL DEFAULT now(),
    update_datetime  timestamp     NOT NULL DEFAULT now(),
    CONSTRAINT index_daily_pkey PRIMARY KEY (index_code, standard_date),
    CONSTRAINT index_daily_market_check CHECK (market IN ('KR', 'US')),
    CONSTRAINT index_daily_source_check CHECK (source IN ('KIS_KR_INDEX', 'KIS_US_INDEX')),
    CONSTRAINT index_daily_close_check  CHECK (closing_price > 0)
);

CREATE INDEX IF NOT EXISTS index_daily_date_idx ON market.index_daily (standard_date);

COMMENT ON TABLE  market.index_daily IS '주요 지수 일봉. 거래일만 있다(휴장일 행 없음, 이월 없음). Airflow market_ingest_reference_daily(market_index) 가 매일 upsert';
COMMENT ON COLUMN market.index_daily.standard_date   IS '그 시장의 현지 거래일 (KR 한국 날짜, US 미국 날짜)';
COMMENT ON COLUMN market.index_daily.index_code      IS 'KOSPI | KOSDAQ | SPX(S&P 500) | COMP(나스닥 종합) — 대시보드가 이 값으로 읽는다';
COMMENT ON COLUMN market.index_daily.market          IS 'KR | US — 날짜 기준과 휴장 판정 단위 (market.trading_holiday.market)';
COMMENT ON COLUMN market.index_daily.closing_price   IS '종가 (지수 포인트)';
COMMENT ON COLUMN market.index_daily.source          IS 'KIS_KR_INDEX(FHKUP03500100 업종 0001·1001) | KIS_US_INDEX(FHKST03030100 FID_COND_MRKT_DIV_CODE=N, SPX·COMP)';
COMMENT ON COLUMN market.index_daily.update_datetime IS '값이 바뀐 upsert 때만 갱신 (정정 감지)';

COMMIT;

-- 검증 (적용 직후)
-- SELECT to_regclass('market.index_daily');
-- SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = 'market.index_daily'::regclass ORDER BY 1;
