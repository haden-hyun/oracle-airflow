-- 2026-11-25 1회성 — asset_daily CRYPTO 를 Upbit KST 자정 종가(ppd UPBIT_DAY)로 재평가 + Upbit 빈 날(11-18·22·23) 복구
--
-- 선행 (안 지키면 가드가 RAISE → 전체 롤백)
--   1) 새 코드 배포 (get_kst_closes · get_hour_candles · kst_day_ohlc)
--   2) ppd 백업 market.product_price_daily_upbit_bak_20261125 (UPBIT_DAY 전 행) 후 재작성
--   3) 시세 DAG 수동 run 으로 ppd BTC·ETH 2025-10-13 ~ asset_daily CRYPTO 최신일 재작성 완료
-- 내용
--   - 백업: account.asset_daily_crypto_bak_20261125 (CRYPTO 행 전부, 이미 있으면 덮지 않음)
--   - 복구: UPBIT 11-18 = 11-17 행, 11-22·11-23 = 11-21 행 복사 (그날 UPBIT 행이 있으면 건너뜀). CASH 는 그대로
--   - 재평가: CRYPTO 행 unit_market_price = 같은 날 ppd 종가 → 평가·손익·수익률 재계산 (수량·매입단가·매입금액 불변)
--     계산식 = transform_upbit_balance 와 같은 결과 (평가 = 수량 × 가격, 손익 = 평가 − 수량 × 매입단가, 반올림은 컬럼 scale)
--     SQL 은 × multiplier 를 곱하지만 Python 은 미사용 — Upbit multiplier = 1 이라 동일
-- 재실행 안전: 백업 IF NOT EXISTS · 복구 NOT EXISTS · 재평가 멱등
-- 적용 후: CALL dashboard.refresh_views(); · ppd COMMENT 는 db/migrations/006
-- 되돌리기: 복구 3일 UPBIT 행 DELETE + CRYPTO 행을 백업 값으로 UPDATE (백업에 복구일 행은 없음)
-- 백업 2주 보관 → 2026-12-09 이후 DROP TABLE account.asset_daily_crypto_bak_20261125;

BEGIN;

CREATE TEMP TABLE upbit_fill (target_date date PRIMARY KEY, source_date date NOT NULL) ON COMMIT DROP;
INSERT INTO upbit_fill VALUES ('2026-11-18', '2026-11-17'), ('2026-11-22', '2026-11-21'), ('2026-11-23', '2026-11-21');

DO $$
DECLARE
    btc numeric;
    missing int;
    stale int;
    next_date date;
    diff int;
BEGIN
    -- 가드 1: 새 정의로 재작성됐는지 (BTC 2026-11-20 = 1분봉 23:59 실측 84,091,000, 구 일봉 81,134,000)
    SELECT closing_price INTO btc FROM market.product_price_daily
     WHERE product_code = 'BTC' AND standard_date = '2026-11-20' AND source = 'UPBIT_DAY';
    IF btc IS DISTINCT FROM 84091000 THEN
        RAISE EXCEPTION 'ppd BTC 2026-11-20 = % (기대 84091000) — 시세 DAG 이력 재작성 먼저', btc;
    END IF;

    -- 가드 2: 대상 (날짜, 코인) 전부 ppd 에 있고 재작성 이후 값인지
    WITH need AS (
        SELECT standard_date, product_code FROM account.asset_daily WHERE asset_type = 'CRYPTO'
        UNION
        SELECT f.target_date, a.product_code
          FROM upbit_fill f JOIN account.asset_daily a
            ON a.standard_date = f.source_date AND a.account_code = 'UPBIT' AND a.asset_type = 'CRYPTO'
    )
    SELECT count(*) FILTER (WHERE p.product_code IS NULL),
           count(*) FILTER (WHERE p.update_datetime < '2026-11-24 12:00')
      INTO missing, stale
      FROM need n LEFT JOIN market.product_price_daily p
        ON p.product_code = n.product_code AND p.standard_date = n.standard_date AND p.source = 'UPBIT_DAY';
    IF missing > 0 OR stale > 0 THEN
        RAISE EXCEPTION 'ppd 미커버 %행 · 재작성 전 값 %행 — 시세 DAG 재작성 범위 확인', missing, stale;
    END IF;

    -- 가드 3: 빈 날 앞뒤 UPBIT 보유 동일 (코인 수량·매입단가, 예수금 금액)
    SELECT count(*) INTO diff FROM (
        (SELECT product_code, holding_quantity, unit_purchase_price, total_evaluation_amount * (asset_type = 'CASH')::int
           FROM account.asset_daily WHERE account_code = 'UPBIT' AND standard_date = '2026-11-17'
         EXCEPT
         SELECT product_code, holding_quantity, unit_purchase_price, total_evaluation_amount * (asset_type = 'CASH')::int
           FROM account.asset_daily WHERE account_code = 'UPBIT' AND standard_date = '2026-11-19')
        UNION ALL
        (SELECT product_code, holding_quantity, unit_purchase_price, total_evaluation_amount * (asset_type = 'CASH')::int
           FROM account.asset_daily WHERE account_code = 'UPBIT' AND standard_date = '2026-11-19'
         EXCEPT
         SELECT product_code, holding_quantity, unit_purchase_price, total_evaluation_amount * (asset_type = 'CASH')::int
           FROM account.asset_daily WHERE account_code = 'UPBIT' AND standard_date = '2026-11-17')
    ) d;
    IF diff > 0 OR NOT EXISTS (SELECT 1 FROM account.asset_daily WHERE account_code = 'UPBIT' AND standard_date = '2026-11-17') THEN
        RAISE EXCEPTION 'UPBIT 11-17 ↔ 11-19 보유 불일치 %행 (또는 11-17 행 없음) — 11-18 복구 불가', diff;
    END IF;

    SELECT min(standard_date) INTO next_date FROM account.asset_daily
     WHERE account_code = 'UPBIT' AND standard_date > '2026-11-23';
    IF next_date IS NULL THEN
        RAISE EXCEPTION 'UPBIT 11-23 이후 행 없음 — 11-21 ↔ 다음 날 보유 대조 불가';
    END IF;
    SELECT count(*) INTO diff FROM (
        (SELECT product_code, holding_quantity, unit_purchase_price, total_evaluation_amount * (asset_type = 'CASH')::int
           FROM account.asset_daily WHERE account_code = 'UPBIT' AND standard_date = '2026-11-21'
         EXCEPT
         SELECT product_code, holding_quantity, unit_purchase_price, total_evaluation_amount * (asset_type = 'CASH')::int
           FROM account.asset_daily WHERE account_code = 'UPBIT' AND standard_date = next_date)
        UNION ALL
        (SELECT product_code, holding_quantity, unit_purchase_price, total_evaluation_amount * (asset_type = 'CASH')::int
           FROM account.asset_daily WHERE account_code = 'UPBIT' AND standard_date = next_date
         EXCEPT
         SELECT product_code, holding_quantity, unit_purchase_price, total_evaluation_amount * (asset_type = 'CASH')::int
           FROM account.asset_daily WHERE account_code = 'UPBIT' AND standard_date = '2026-11-21')
    ) d;
    IF diff > 0 THEN
        RAISE EXCEPTION 'UPBIT 11-21 ↔ % 보유 불일치 %행 — 11-22·23 복구 불가', next_date, diff;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS account.asset_daily_crypto_bak_20261125 AS
SELECT * FROM account.asset_daily WHERE asset_type = 'CRYPTO';

INSERT INTO account.asset_daily (standard_date, account_code, account_name, product_code, product_name, asset_type,
                                 currency_code, exchange_code, multiplier, holding_quantity, unit_purchase_price,
                                 unit_market_price, total_purchase_amount, total_evaluation_amount,
                                 total_profit_amount, valuation_profit_rate)
SELECT f.target_date, a.account_code, a.account_name, a.product_code, a.product_name, a.asset_type,
       a.currency_code, a.exchange_code, a.multiplier, a.holding_quantity, a.unit_purchase_price,
       a.unit_market_price, a.total_purchase_amount, a.total_evaluation_amount,
       a.total_profit_amount, a.valuation_profit_rate                -- CRYPTO 가격은 아래 재평가가 덮음
  FROM upbit_fill f
  JOIN account.asset_daily a ON a.standard_date = f.source_date AND a.account_code = 'UPBIT'
 WHERE NOT EXISTS (SELECT 1 FROM account.asset_daily x
                    WHERE x.standard_date = f.target_date AND x.account_code = 'UPBIT');

UPDATE account.asset_daily a
   SET unit_market_price       = p.closing_price,
       total_evaluation_amount = round(a.holding_quantity * p.closing_price * a.multiplier, 2),
       total_profit_amount     = round((a.holding_quantity * p.closing_price
                                        - a.holding_quantity * a.unit_purchase_price) * a.multiplier, 2),
       valuation_profit_rate   = CASE WHEN a.holding_quantity * a.unit_purchase_price = 0 THEN 0
                                      ELSE round((p.closing_price - a.unit_purchase_price) / a.unit_purchase_price * 100, 2)
                                 END
  FROM market.product_price_daily p
 WHERE a.asset_type = 'CRYPTO'
   AND p.source = 'UPBIT_DAY' AND p.product_code = a.product_code AND p.standard_date = a.standard_date;

COMMIT;

-- 검증 (적용 후 실행, 기대값)
-- 1) 가격 불일치 0 · ppd 없는 CRYPTO 행 0
--    SELECT count(*) FILTER (WHERE p.closing_price IS NULL) AS no_ppd,
--           count(*) FILTER (WHERE a.unit_market_price <> p.closing_price) AS mismatch
--      FROM account.asset_daily a LEFT JOIN market.product_price_daily p
--        ON p.product_code = a.product_code AND p.standard_date = a.standard_date AND p.source = 'UPBIT_DAY'
--     WHERE a.asset_type = 'CRYPTO';
-- 2) 행 수: CRYPTO = 백업 + 6 (2코인 × 3일), 복구일 UPBIT 각 3행
--    SELECT (SELECT count(*) FROM account.asset_daily WHERE asset_type = 'CRYPTO') AS now,
--           (SELECT count(*) FROM account.asset_daily_crypto_bak_20261125) AS bak;
--    SELECT standard_date, count(*) FROM account.asset_daily
--     WHERE account_code = 'UPBIT' AND standard_date IN ('2026-11-18', '2026-11-22', '2026-11-23') GROUP BY 1 ORDER BY 1;
-- 3) 수량·매입 변화 0
--    SELECT count(*) FROM account.asset_daily a JOIN account.asset_daily_crypto_bak_20261125 b USING (standard_date, account_code, product_code)
--     WHERE (a.holding_quantity, a.unit_purchase_price, a.total_purchase_amount)
--           IS DISTINCT FROM (b.holding_quantity, b.unit_purchase_price, b.total_purchase_amount);
-- 4) 일별 평가액 차이 분포 (보고용)
--    SELECT min(d), percentile_cont(0.5) WITHIN GROUP (ORDER BY d), max(d) FROM (
--      SELECT a.standard_date, sum(a.total_evaluation_amount - b.total_evaluation_amount) AS d
--        FROM account.asset_daily a JOIN account.asset_daily_crypto_bak_20261125 b USING (standard_date, account_code, product_code)
--       GROUP BY 1) t;
