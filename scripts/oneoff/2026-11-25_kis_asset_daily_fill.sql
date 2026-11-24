-- 2026-11-25 1회성 — KIS 계좌 asset_daily 빈 날(11-18·22·23) 복구 (수량 이어쓰기 × 그날 가격)
--
-- 근거
--   - 11-18: 11-17 ↔ 11-19 보유 동일 (아래 가드), 11-22·23: 11-21 = 현재 잔고 (적용 전 확인), 11월 KIS 체결 0건
--   - 가격 규칙 = 정상 적재분과 같음 (11-17·19·20·21 STOCK 전 행 일치 확인)
--     - 국내·해외 주식: product_price_daily 에서 그날 이하 마지막 종가 (KR=KIS_KR_DAILY, US=현지일 KIS_US_DAILY)
--     - 펀드: fund_price_daily 그날 기준가, 평가 = 좌수 × 기준가 × multiplier 원 단위 반올림 (KIS 평가금액과 ±1원)
--     - 현금(예수금·외화예수금·CMA): 원본 행 그대로
-- 제외 (사용자 결정): 중개형ISA 11-18 예수금(11-17 48,291 → 11-19 없음), CMA 11-22·23 (11-21 0 → 현재 330,023, 입금일 불명)
-- 선행
--   1) ppd KIS_KR_DAILY·KIS_US_DAILY 11-23 까지 적재
--   2) 11-23 이후 KIS 행이 없으면: 현재 잔고 = 11-21 행 확인 후 앞에 `SET asset_fix.kis_live_verified = on;`
-- 재실행 안전: (계좌, 날짜) 에 행이 있으면 건너뜀
-- 되돌리기: DELETE FROM account.asset_daily WHERE standard_date IN (복구일) AND account_code IN (KIS 계좌) AND insert_datetime >= 적용 시각
-- 적용 후: CALL dashboard.refresh_views();

BEGIN;

CREATE TEMP TABLE kis_fill (target_date date PRIMARY KEY, source_date date NOT NULL, check_date date) ON COMMIT DROP;
INSERT INTO kis_fill VALUES ('2026-11-18', '2026-11-17', '2026-11-19'),
                            ('2026-11-22', '2026-11-21', NULL),
                            ('2026-11-23', '2026-11-21', NULL);

-- 대상 = 원본일 행 중 복구일에 행이 없는 계좌 (수기 계좌는 복구일 행이 있어 빠짐, UPBIT 은 별도 SQL)
CREATE TEMP VIEW kis_src AS
SELECT f.target_date, f.source_date, a.*
  FROM kis_fill f
  JOIN account.asset_daily a ON a.standard_date = f.source_date
 WHERE a.account_code <> 'UPBIT'
   AND NOT EXISTS (SELECT 1 FROM account.asset_daily x
                    WHERE x.standard_date = f.target_date AND x.account_code = a.account_code)
   AND NOT (a.account_name = '중개형ISA' AND a.product_code = 'DEPOSIT' AND f.target_date = '2026-11-18')
   AND NOT (a.product_code = 'CMA' AND f.target_date IN ('2026-11-22', '2026-11-23'));

CREATE TEMP VIEW kis_priced AS
SELECT s.*,
       CASE s.asset_type
            WHEN 'STOCK' THEN (SELECT p.closing_price FROM market.product_price_daily p
                                WHERE p.product_code = s.product_code AND p.standard_date <= s.target_date
                                  AND p.source = CASE s.currency_code WHEN 'USD' THEN 'KIS_US_DAILY' ELSE 'KIS_KR_DAILY' END
                                ORDER BY p.standard_date DESC LIMIT 1)
            WHEN 'FUND'  THEN (SELECT f.standard_price FROM market.fund_price_daily f
                                WHERE f.product_code = s.product_code AND f.standard_date = s.target_date)
            ELSE s.unit_market_price
       END AS new_price
  FROM kis_src s;

DO $$
DECLARE
    n int;
BEGIN
    -- 가드 0: 대상 계좌 수 (11-18 = KIS 5계좌, 11-22·23 = CMA 제외 4계좌)
    SELECT count(*) INTO n FROM (SELECT target_date, count(DISTINCT account_code) c FROM kis_src GROUP BY 1) t
     WHERE (target_date, c) NOT IN (('2026-11-18'::date, 5), ('2026-11-22'::date, 4), ('2026-11-23'::date, 4));
    IF n > 0 OR (SELECT count(DISTINCT target_date) FROM kis_src) <> 3 THEN
        RAISE EXCEPTION '대상 계좌 수가 기대와 다름 — kis_src 확인';
    END IF;

    -- 가드 1: 가격 있음 + 그날 종가가 실제 거래일 값인지 (KR 11-23 · US 11-23 적재 확인)
    SELECT count(*) INTO n FROM kis_priced WHERE new_price IS NULL;
    IF n > 0 THEN
        RAISE EXCEPTION '가격 없는 행 % — ppd·fund_price_daily 적재 먼저', n;
    END IF;
    SELECT count(*) INTO n FROM (VALUES ('KIS_KR_DAILY'), ('KIS_US_DAILY')) v(src)
     WHERE NOT EXISTS (SELECT 1 FROM market.product_price_daily WHERE source = v.src AND standard_date = '2026-11-23');
    IF n > 0 THEN
        RAISE EXCEPTION 'ppd 11-23 KR·US 일봉 없음 — 시세 DAG 먼저';
    END IF;

    -- 가드 2: 가격 규칙이 원본일 저장 가격을 재현하는지 (STOCK·FUND)
    SELECT count(*) INTO n FROM kis_src s
     WHERE s.asset_type = 'STOCK'
       AND s.unit_market_price IS DISTINCT FROM
           (SELECT p.closing_price FROM market.product_price_daily p
             WHERE p.product_code = s.product_code AND p.standard_date <= s.source_date
               AND p.source = CASE s.currency_code WHEN 'USD' THEN 'KIS_US_DAILY' ELSE 'KIS_KR_DAILY' END
             ORDER BY p.standard_date DESC LIMIT 1)
        OR s.asset_type = 'FUND'
       AND s.unit_market_price IS DISTINCT FROM
           (SELECT f.standard_price FROM market.fund_price_daily f
             WHERE f.product_code = s.product_code AND f.standard_date = s.source_date);
    IF n > 0 THEN
        RAISE EXCEPTION '원본일 가격 재현 실패 %행 — 가격 규칙 확인', n;
    END IF;

    -- 가드 3: 배당락 (다음 KR 거래일 배당락이면 07:00 가격 = 종가 − 분배금 → 이 규칙 불가)
    SELECT count(*) INTO n FROM market.dividend_event d
     WHERE d.ex_date BETWEEN '2026-11-18' AND '2026-11-24'
       AND d.product_code IN (SELECT product_code FROM kis_src);
    IF n > 0 THEN
        RAISE EXCEPTION '복구 기간 배당락 %건 — 수동 확인', n;
    END IF;

    -- 가드 4: 11-18 = 11-17 ↔ 11-19 보유 동일 (ISA 예수금 제외)
    SELECT count(*) INTO n FROM (
        (SELECT account_code, product_code, holding_quantity, unit_purchase_price
           FROM account.asset_daily
          WHERE standard_date = '2026-11-17' AND account_code IN (SELECT account_code FROM kis_src WHERE target_date = '2026-11-18')
            AND NOT (account_name = '중개형ISA' AND product_code = 'DEPOSIT')
         EXCEPT
         SELECT account_code, product_code, holding_quantity, unit_purchase_price
           FROM account.asset_daily
          WHERE standard_date = '2026-11-19' AND account_code IN (SELECT account_code FROM kis_src WHERE target_date = '2026-11-18'))
        UNION ALL
        (SELECT account_code, product_code, holding_quantity, unit_purchase_price
           FROM account.asset_daily
          WHERE standard_date = '2026-11-19' AND account_code IN (SELECT account_code FROM kis_src WHERE target_date = '2026-11-18')
         EXCEPT
         SELECT account_code, product_code, holding_quantity, unit_purchase_price
           FROM account.asset_daily
          WHERE standard_date = '2026-11-17' AND account_code IN (SELECT account_code FROM kis_src WHERE target_date = '2026-11-18')
            AND NOT (account_name = '중개형ISA' AND product_code = 'DEPOSIT'))
    ) d;
    IF n > 0 THEN
        RAISE EXCEPTION 'KIS 11-17 ↔ 11-19 보유 불일치 %행 — 11-18 복구 불가', n;
    END IF;

    -- 가드 5: 11-22·23 = 11-21 ↔ 다음 정상일, 없으면 현재 잔고 확인 플래그
    IF NOT EXISTS (SELECT 1 FROM account.asset_daily a
                    WHERE a.standard_date > '2026-11-23' AND a.account_code IN (SELECT account_code FROM kis_src))
       AND current_setting('asset_fix.kis_live_verified', true) IS DISTINCT FROM 'on' THEN
        RAISE EXCEPTION 'KIS 11-23 이후 행 없음 — 현재 잔고 = 11-21 확인 후 SET asset_fix.kis_live_verified = on';
    END IF;
END $$;

INSERT INTO account.asset_daily (standard_date, account_code, account_name, product_code, product_name, asset_type,
                                 currency_code, exchange_code, multiplier, holding_quantity, unit_purchase_price,
                                 unit_market_price, total_purchase_amount, total_evaluation_amount,
                                 total_profit_amount, valuation_profit_rate)
SELECT target_date, account_code, account_name, product_code, product_name, asset_type,
       currency_code, exchange_code, multiplier, holding_quantity, unit_purchase_price,
       new_price, total_purchase_amount, ev, ev - total_purchase_amount,
       CASE WHEN total_purchase_amount = 0 THEN 0 ELSE round((ev - total_purchase_amount) / total_purchase_amount * 100, 2) END
  FROM (SELECT p.*,
               CASE asset_type
                    WHEN 'STOCK' THEN round(holding_quantity * new_price * multiplier, 2)
                    WHEN 'FUND'  THEN round(holding_quantity * new_price * multiplier, 0)
                    ELSE total_evaluation_amount
               END AS ev
          FROM kis_priced p) q;

COMMIT;

-- 검증 (적용 후)
-- 1) 날짜별 계좌 수: 11-18 = 10, 11-22·23 = 9 (CMA 제외) — UPBIT 포함 기준
--    SELECT standard_date, count(DISTINCT account_code) FROM account.asset_daily
--     WHERE standard_date BETWEEN '2026-11-17' AND '2026-11-23' GROUP BY 1 ORDER BY 1;
-- 2) 복구 행 수량 = 원본일 수량 (불일치 0)
--    SELECT count(*) FROM account.asset_daily t JOIN account.asset_daily s
--        ON s.account_code = t.account_code AND s.product_code = t.product_code
--       AND s.standard_date = CASE WHEN t.standard_date = '2026-11-18' THEN date '2026-11-17' ELSE date '2026-11-21' END
--     WHERE t.standard_date IN ('2026-11-18', '2026-11-22', '2026-11-23') AND t.account_code <> 'UPBIT'
--       AND t.holding_quantity <> s.holding_quantity;
