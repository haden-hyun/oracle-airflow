-- 2026-11-26 1회성 — CMA 계좌 asset_daily 이력 삭제 (수집 제외와 함께)
--
-- 왜: CMA 는 입출금 경유 계좌(RP 아님) → 투자 자산 범위에서 제외. 이력도 지워 과거·현재 정의를 맞춤
-- 선행: secrets KIS_CMA `enabled: false` (다음 정기 run 부터 미수집)
-- 대상: product_code = 'CMA' 행 (CMA 계좌에만 있음 — 가드로 확인, 계좌번호는 파일에 두지 않음)
-- 백업: account.asset_daily_cma_bak_20261126 (이미 있으면 덮지 않음) → 2026-12-10 이후 DROP
-- 재실행 안전: 두 번째 실행은 삭제 0행
-- 되돌리기: INSERT INTO account.asset_daily SELECT * FROM account.asset_daily_cma_bak_20261126;
-- 적용 후: CALL dashboard.refresh_views();

BEGIN;

DO $$
BEGIN
    IF (SELECT count(DISTINCT account_code) FROM account.asset_daily WHERE product_code = 'CMA') > 1 THEN
        RAISE EXCEPTION 'product_code CMA 가 여러 계좌에 있음 — 대상 확인';
    END IF;
    IF EXISTS (SELECT 1 FROM account.asset_daily
                WHERE account_code IN (SELECT account_code FROM account.asset_daily WHERE product_code = 'CMA')
                  AND product_code <> 'CMA') THEN
        RAISE EXCEPTION 'CMA 계좌에 CMA 외 행 있음 — 대상 확인';
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS account.asset_daily_cma_bak_20261126 AS
SELECT * FROM account.asset_daily WHERE product_code = 'CMA';

DELETE FROM account.asset_daily WHERE product_code = 'CMA';

COMMIT;

-- 검증: CMA 0행 · 백업 177행
-- SELECT (SELECT count(*) FROM account.asset_daily WHERE product_code = 'CMA') AS now,
--        (SELECT count(*) FROM account.asset_daily_cma_bak_20261126) AS bak;
