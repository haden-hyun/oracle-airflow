-- 006 — product_price_daily 가상자산 컬럼 설명을 KST 자정 종가 정의로 갱신 (market.product_price_daily)
--
-- 왜: Upbit 가격 기준이 09:00 마감 일봉(/v1/candles/days) → KST 자정 종가(60분봉 KST 일별 집계)로 바뀜
--   - 001 COMMENT 는 구 정의 — 적용 후 수정 금지라 다음 번호로 갱신
-- 주의
--   - COMMENT 만 변경 (스키마·데이터 변경 없음), 재실행 안전

BEGIN;

COMMENT ON COLUMN market.product_price_daily.standard_date IS 'KR·US: 현지 거래일 / 가상자산: KST 날짜 (00:00~24:00)';
COMMENT ON COLUMN market.product_price_daily.closing_price IS '종가 (KR FID_ORG_ADJ_PRC=1, US MODP=0 원주가 — asset_daily.unit_market_price 와 같은 기준). 가상자산은 다음날 00:00 KST 직전 체결가';
COMMENT ON COLUMN market.product_price_daily.source        IS 'KIS_KR_DAILY(FHKST03010100) | KIS_US_DAILY(HHDFS76240000) | UPBIT_DAY(/v1/candles/minutes/60 → KST 일별 집계)';

COMMIT;

-- 검증 (적용 직후)
-- SELECT a.attname, col_description(a.attrelid, a.attnum) FROM pg_attribute a
--  WHERE a.attrelid = 'market.product_price_daily'::regclass AND a.attname IN ('standard_date', 'closing_price', 'source');
