-- 003 — 배당 테이블 컬럼명 표준화 (약어·kind 제거, 기존 테이블 관례에 맞춤)
--
-- 왜: 기존 account·market 테이블은 약어 없이 `_type`·`_amount`·`_rate` 를 쓴다
--     (asset_type · total_purchase_amount · base_rate).
--   - market.dividend_event.dps_amount   → per_share_dividend_amount
--   - market.dividend_event.dividend_kind → dividend_type
--   - account.dividend_receipt.fx_rate    → exchange_rate
-- 주의
--   - 데이터 변경 없음 (RENAME 만). 의존 뷰(dashboard.v_dividend_*)는 PostgreSQL 이 새 이름으로 자동 추적,
--     뷰 출력 컬럼명은 그대로라 대시보드 앱 영향 없음
--   - 재실행 안전: 이미 바뀐 이름이면 건너뜀

BEGIN;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns
              WHERE table_schema = 'market' AND table_name = 'dividend_event' AND column_name = 'dps_amount') THEN
    ALTER TABLE market.dividend_event RENAME COLUMN dps_amount TO per_share_dividend_amount;
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns
              WHERE table_schema = 'market' AND table_name = 'dividend_event' AND column_name = 'dividend_kind') THEN
    ALTER TABLE market.dividend_event RENAME COLUMN dividend_kind TO dividend_type;
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns
              WHERE table_schema = 'account' AND table_name = 'dividend_receipt' AND column_name = 'fx_rate') THEN
    ALTER TABLE account.dividend_receipt RENAME COLUMN fx_rate TO exchange_rate;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dividend_event_kind_check') THEN
    ALTER TABLE market.dividend_event RENAME CONSTRAINT dividend_event_kind_check TO dividend_event_type_check;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dividend_event_dps_check') THEN
    ALTER TABLE market.dividend_event RENAME CONSTRAINT dividend_event_dps_check TO dividend_event_per_share_check;
  END IF;
END $$;

COMMENT ON COLUMN market.dividend_event.dividend_type IS
    'CASH 현금 / SPECIAL 특별(US RGHT_TYPE_CD 75·특별배당). 주식배당은 적재하지 않는다';
COMMENT ON COLUMN market.dividend_event.per_share_dividend_amount IS
    '주당 배당(원통화). NULL = 금액 미정(US 금액 지연, KR 미확정 0원)';
COMMENT ON COLUMN account.dividend_receipt.exchange_rate IS
    '미국 추정만: 지급일 이하 최신 USD 기준율(exchange_rate_daily.base_rate). 지급일이 최신 환율일보다 뒤면 NULL(다음 run 이 채움)';

COMMIT;
