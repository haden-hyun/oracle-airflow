-- 002 — 배당 일정(market.dividend_event) · 계좌 배당 수령 원장(account.dividend_receipt)
--
-- 왜: 증권계좌 예수금이 적재되지 않아 분배금·배당이 성과·자산 어디에도 잡히지 않는다(T9 기획 §0-3).
--     DAG account_ingest_dividend(매일 07:20)가 KIS 4개 API 로 채우고, 대시보드는 dashboard.v_dividend_* 뷰로 읽는다.
-- 소유: Airflow(~/airflow). market·account 스키마 정의는 이 디렉터리가 출처다
--       (예전 market.trading_holiday 는 대시보드 db/migrations/006 에 정의돼 있다 — 이력).
-- 주의
--   - 비파괴: DAG 는 INSERT … ON CONFLICT DO UPDATE 만 한다. DELETE 없음. 정정으로 사라진 옛 PK 행은
--     last_seen_date 가 멈춘 채 남고, 읽는 쪽이 거른다(handoff §1-4).
--   - market.product(001, 시세 handoff 소유)에 FK 를 걸지 않는다 — 배당은 product 없이도 동작해야 한다.
--   - 재실행 안전(IF NOT EXISTS). 컬럼을 바꿀 때는 이 파일을 고치지 말고 다음 번호로.

BEGIN;

CREATE SCHEMA IF NOT EXISTS market;
CREATE SCHEMA IF NOT EXISTS account;

CREATE TABLE IF NOT EXISTS market.dividend_event (
    product_code    varchar(30)   NOT NULL,
    record_date     date          NOT NULL,
    dividend_kind   varchar(10)   NOT NULL,
    market          varchar(4)    NOT NULL,
    product_name    varchar(200),
    dividend_label  varchar(40),
    ex_date         date,
    pay_date        date,
    dps_amount      numeric(20,6),
    currency_code   varchar(5)    NOT NULL,
    dividend_rate   numeric(12,4),
    is_confirmed    boolean       NOT NULL,
    announce_date   date,
    source          varchar(40)   NOT NULL,
    last_seen_date  date          NOT NULL,
    insert_datetime timestamp     NOT NULL DEFAULT now(),
    update_datetime timestamp     NOT NULL DEFAULT now(),
    CONSTRAINT dividend_event_pkey PRIMARY KEY (product_code, record_date, dividend_kind),
    CONSTRAINT dividend_event_market_check CHECK (market IN ('KR', 'US')),
    CONSTRAINT dividend_event_kind_check CHECK (dividend_kind IN ('CASH', 'SPECIAL')),
    CONSTRAINT dividend_event_dps_check CHECK (dps_amount IS NULL OR dps_amount > 0)
);

CREATE INDEX IF NOT EXISTS dividend_event_market_ex_date_idx ON market.dividend_event (market, ex_date);
CREATE INDEX IF NOT EXISTS dividend_event_pay_date_idx ON market.dividend_event (pay_date);

COMMENT ON TABLE market.dividend_event IS
    '종목 배당·분배 일정(주당). KR = KIS 예탁원 배당일정(HHKDB669102C0), US = 해외 기간별권리(CTRGT011R, 금액) ⟗ 권리종합(HHDFS78330900, 일정). Airflow account_ingest_dividend 가 UPSERT, DELETE 없음';
COMMENT ON COLUMN market.dividend_event.record_date IS '배당 기준일. KR = 기준일, US = 현지 기준일(acpl_bass_dt = record_dt)';
COMMENT ON COLUMN market.dividend_event.dividend_kind IS 'CASH 현금 / SPECIAL 특별(US RGHT_TYPE_CD 75·특별배당). 주식배당은 적재하지 않는다';
COMMENT ON COLUMN market.dividend_event.dividend_label IS '원문 구분. KR divi_kind(결산·중간·분기·반기·빈값) / US ca_title(현금배당·특별배당)';
COMMENT ON COLUMN market.dividend_event.ex_date IS '배당락일. KR = 계산(기준일 이하 마지막 영업일의 직전 영업일), US = 권리종합 div_lock_dt(없으면 2024-05-28 이후는 기준일)';
COMMENT ON COLUMN market.dividend_event.dps_amount IS '주당 배당(원통화). NULL = 금액 미정(US 금액 지연, KR 미확정 0원)';
COMMENT ON COLUMN market.dividend_event.dividend_rate IS 'KR divi_rate 원문. ETF 는 액면 0 인데 값이 있어 의미 불명 — 화면에 쓰지 않는다';
COMMENT ON COLUMN market.dividend_event.is_confirmed IS 'KR = 금액>0, US = dfnt_yn=Y 이고 금액 있음';
COMMENT ON COLUMN market.dividend_event.product_name IS 'KR isin_name 은 20자에서 잘림 — 표시는 asset_daily.product_name 우선';
COMMENT ON COLUMN market.dividend_event.last_seen_date IS '이 행을 마지막으로 본 run 날짜(KST). 같은 종목 최신값보다 작고 기준일이 재조회 창 안이면 정정으로 사라진 행';

CREATE TABLE IF NOT EXISTS account.dividend_receipt (
    account_code     varchar(20)   NOT NULL,
    product_code     varchar(30)   NOT NULL,
    record_date      date          NOT NULL,
    right_type_code  varchar(10)   NOT NULL,
    product_name     varchar(200),
    ex_date          date,
    pay_date         date,
    holding_quantity numeric(20,8) NOT NULL,
    gross_amount     numeric(20,6),
    tax_amount       numeric(20,6),
    currency_code    varchar(5)    NOT NULL,
    fx_rate          numeric(15,5),
    is_estimated     boolean       NOT NULL,
    source           varchar(40)   NOT NULL,
    last_seen_date   date          NOT NULL,
    insert_datetime  timestamp     NOT NULL DEFAULT now(),
    update_datetime  timestamp     NOT NULL DEFAULT now(),
    CONSTRAINT dividend_receipt_pkey PRIMARY KEY (account_code, product_code, record_date, right_type_code),
    CONSTRAINT dividend_receipt_account_fkey FOREIGN KEY (account_code) REFERENCES account.account_master (account_code),
    CONSTRAINT dividend_receipt_right_check CHECK (right_type_code IN ('32', '03', 'EST03', 'EST75')),
    CONSTRAINT dividend_receipt_amount_check CHECK (is_estimated OR gross_amount IS NOT NULL),
    CONSTRAINT dividend_receipt_estimate_check CHECK (is_estimated = (right_type_code LIKE 'EST%'))
);

CREATE INDEX IF NOT EXISTS dividend_receipt_pay_date_idx ON account.dividend_receipt (pay_date);
CREATE INDEX IF NOT EXISTS dividend_receipt_ex_date_idx ON account.dividend_receipt (ex_date);

COMMENT ON TABLE account.dividend_receipt IS
    '계좌별 배당·분배 수령 원장. 국내 = KIS 기간별계좌권리현황(CTRGA011R) 실수령, 미국 = 일정 × 락일 직전 asset_daily 수량 추정(is_estimated). Airflow account_ingest_dividend 가 UPSERT, DELETE 없음';
COMMENT ON COLUMN account.dividend_receipt.account_code IS 'secrets account_code(= account_master). 응답의 acno10·ctx_area 는 저장하지 않는다';
COMMENT ON COLUMN account.dividend_receipt.right_type_code IS '32 ETF 분배금 / 03 주식 현금배당 / EST03 미국 추정 현금 / EST75 미국 추정 특별';
COMMENT ON COLUMN account.dividend_receipt.holding_quantity IS '국내 = 결제 기준 권리잔고(cblc_qty, 기준일 당일·전일 매수분 제외) / 미국 = 락일 직전 asset_daily 수량';
COMMENT ON COLUMN account.dividend_receipt.gross_amount IS '원통화 세전. 국내 last_alct_amt(= 권리잔고 × 주당), 미국 추정 = 수량 × dps (금액 미정이면 NULL)';
COMMENT ON COLUMN account.dividend_receipt.tax_amount IS '원천 tax_amt 그대로. 고정 15.4% 아님. ISA 는 지급 시 미징수 — 만기 정산 참고값. 미국 추정 = 세전 × 15%';
COMMENT ON COLUMN account.dividend_receipt.fx_rate IS '미국 추정만: 지급일 이하 최신 USD 기준율(exchange_rate_daily). 지급일이 최신 환율일보다 뒤면 NULL(다음 run 이 채움)';
COMMENT ON COLUMN account.dividend_receipt.last_seen_date IS '이 행을 마지막으로 본 run 날짜(KST). 같은 계좌 최신값보다 작고 기준일이 재조회 창 안이면 정정으로 사라진 행';

COMMIT;
