-- 005 — 환율 테이블 컬럼명 표준화 + 출처·갱신 시각 (market.exchange_rate_daily), 펀드 기준가 PK 이름 정리
--
-- 왜: exchange_rate_daily 는 001 이전 테이블이라 관례(README '컬럼 이름 규칙')와 어긋난다.
--   - symbol → source_symbol : 값이 KIS 조회 키(FX@KRWKFTC) = market.product.source_symbol 과 같은 의미
--   - source 추가            : 다른 적재 테이블은 모두 출처 컬럼이 있다. 기존 행은 전부 KIS FHKST03030100
--   - update_datetime 추가   : 기존 테이블 관례 (기존 행은 이 마이그레이션 시각 — UPDATE 금지 규칙)
--   - insert_datetime NOT NULL (기존 NULL 0행)
--   - market.fund_price_daily PK 이름 fund_nav_daily_pkey → fund_price_daily_pkey (옛 테이블명 잔재)
-- 주의
--   - 데이터 값 변경 없음. 의존 뷰(dashboard.v_*·mv_*)는 symbol 을 쓰지 않고, 써도 PostgreSQL 이 새 이름을 추적
--   - 대시보드 앱이 symbol 을 직접 읽는다(app/routers/market.py·positions.py, tests) → 대시보드와 동시 배포
--   - Airflow 코드(ExchangeRateRecord.source_symbol)와 같은 시각에 배포 — 06:55 run 이후, 다음 run 전
--   - 재실행 안전: 이미 바뀐 이름·있는 컬럼이면 건너뜀

BEGIN;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns
              WHERE table_schema = 'market' AND table_name = 'exchange_rate_daily' AND column_name = 'symbol') THEN
    ALTER TABLE market.exchange_rate_daily RENAME COLUMN symbol TO source_symbol;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_constraint
              WHERE conname = 'fund_nav_daily_pkey' AND conrelid = 'market.fund_price_daily'::regclass) THEN
    ALTER TABLE market.fund_price_daily RENAME CONSTRAINT fund_nav_daily_pkey TO fund_price_daily_pkey;
  END IF;
END $$;

ALTER TABLE market.exchange_rate_daily
    ADD COLUMN IF NOT EXISTS source          varchar(20) NOT NULL DEFAULT 'KIS_FX_DAILY',
    ADD COLUMN IF NOT EXISTS update_datetime timestamp   NOT NULL DEFAULT now(),
    ALTER COLUMN insert_datetime SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint
                  WHERE conname = 'exchange_rate_daily_source_check' AND conrelid = 'market.exchange_rate_daily'::regclass) THEN
    ALTER TABLE market.exchange_rate_daily
        ADD CONSTRAINT exchange_rate_daily_source_check CHECK (source IN ('KIS_FX_DAILY'));
  END IF;
END $$;

COMMENT ON TABLE  market.exchange_rate_daily IS '원화 기준 환율 (USD·JPY·GBP·EUR). 달력일마다 행이 있다(주말·휴일 = 직전 고시 이월). Airflow market_ingest_reference_daily(exchange_rate)';
COMMENT ON COLUMN market.exchange_rate_daily.source_symbol   IS 'KIS 조회 심볼 (FX@KRWKFTC 원/달러, FX@KRWJS 원/엔(1엔당), FX@GBP·FX@EUR 은 달러 기준 → 원/달러로 교차 환산해 적재)';
COMMENT ON COLUMN market.exchange_rate_daily.base_rate       IS '원화 환율 (JPY 는 1엔당 원). 대시보드 원/100엔 표시는 ×100';
COMMENT ON COLUMN market.exchange_rate_daily.source          IS 'KIS_FX_DAILY (FHKST03030100 FID_COND_MRKT_DIV_CODE=X)';
COMMENT ON COLUMN market.exchange_rate_daily.update_datetime IS '행을 쓴 시각 (005 이전 행은 005 적용 시각)';

COMMIT;

-- 검증 (적용 직후)
-- SELECT column_name, is_nullable, column_default FROM information_schema.columns
--  WHERE table_schema = 'market' AND table_name = 'exchange_rate_daily' ORDER BY ordinal_position;
-- SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint
--  WHERE conrelid IN ('market.exchange_rate_daily'::regclass, 'market.fund_price_daily'::regclass) ORDER BY 1;
