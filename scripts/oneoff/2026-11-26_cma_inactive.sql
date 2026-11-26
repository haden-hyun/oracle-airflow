-- 2026-11-26 1회성 — CMA 계좌 account_master 비활성 (2026-11-26_cma_remove.sql 후속)
--
-- 왜: 수집·이력은 이미 제외 → 대시보드 계좌 화면에 범위 없는 '종료' 행으로 남지 않게
-- 대상: account_type = 'CMA' (계좌번호는 파일에 두지 않음, 1계좌 가드)
-- 재실행 안전 · 되돌리기: UPDATE account.account_master SET is_active = true WHERE account_type = 'CMA';

BEGIN;

DO $$
BEGIN
    IF (SELECT count(*) FROM account.account_master WHERE account_type = 'CMA') <> 1 THEN
        RAISE EXCEPTION 'account_type CMA 계좌가 1개가 아님 — 대상 확인';
    END IF;
END $$;

UPDATE account.account_master SET is_active = false WHERE account_type = 'CMA' AND is_active;

COMMIT;

-- 검증: SELECT account_type, is_active FROM account.account_master WHERE account_type = 'CMA';  → f
