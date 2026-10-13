-- 001 — market.product(상품 목록 겸 시세 수집 대상) + market.product_price_daily(일봉 OHLCV)
--
-- 왜: asset_daily 는 '보유한 날의 07시 스냅샷 가격 1개'만 있다. 매수 전·매도 후·2026-01-28 이전·OHLC 가 없어
--     종목 시세 드로어(1개월~1년)를 그릴 수 없다. 시세 DAG(market_ingest_product_price_daily)가 매일 빈 구간을 채운다.
-- 소유: Airflow. 배당 수집(002)도 조회 대상 종목을 market.product 에서 읽는다.
-- 펀드 기준가는 여기 넣지 않는다 — 기존 market.fund_price_daily(수기 계좌 평가 입력)에 과거 날짜만 채운다.
-- 주의: 재실행 안전(IF NOT EXISTS). 행 삭제 없음. 1년이 지난 시세도 지우지 않는다.
BEGIN;

CREATE TABLE IF NOT EXISTS market.product (
    product_code     varchar(30)  NOT NULL,
    product_name     varchar(200) NOT NULL,
    asset_type       varchar(10)  NOT NULL,
    market           varchar(10)  NOT NULL,
    exchange_code    varchar(10),
    currency_code    varchar(5)   NOT NULL,
    price_source     varchar(20)  NOT NULL,
    source_symbol    varchar(30),
    first_held_date  date,
    last_held_date   date,
    is_tracked       boolean      NOT NULL DEFAULT true,
    price_from_date  date,
    insert_datetime  timestamp    NOT NULL DEFAULT now(),
    update_datetime  timestamp    NOT NULL DEFAULT now(),
    CONSTRAINT product_pkey PRIMARY KEY (product_code),
    CONSTRAINT product_asset_type_check   CHECK (asset_type   IN ('STOCK', 'FUND', 'CRYPTO', 'GOLD')),
    CONSTRAINT product_market_check       CHECK (market       IN ('KR', 'US', 'CRYPTO', 'FUND')),
    CONSTRAINT product_price_source_check CHECK (price_source IN ('KIS_KR_DAILY', 'KIS_US_DAILY', 'UPBIT_DAY', 'FUND_NAV', 'NONE')),
    CONSTRAINT product_symbol_check       CHECK (price_source = 'NONE' OR source_symbol IS NOT NULL),
    CONSTRAINT product_held_range_check   CHECK (first_held_date IS NULL OR last_held_date >= first_held_date)
);

CREATE INDEX IF NOT EXISTS product_tracked_idx ON market.product (price_source) WHERE is_tracked;

COMMENT ON TABLE  market.product IS '상품 목록 겸 시세 수집 대상. asset_daily 에 한 번이라도 나온 비현금 상품. Airflow market_ingest_product_price_daily 가 매일 upsert';
COMMENT ON COLUMN market.product.product_code    IS 'asset_daily.product_code 와 같은 값 (국내 6자리, 미국 티커, 코인 심볼, 펀드 표준코드, GOLD)';
COMMENT ON COLUMN market.product.product_name    IS '표시명 — asset_daily 최신 행의 product_name';
COMMENT ON COLUMN market.product.asset_type      IS 'STOCK | FUND | CRYPTO | GOLD (asset_daily.asset_type, CASH 제외)';
COMMENT ON COLUMN market.product.market          IS 'KR | US | CRYPTO | FUND — 날짜 기준과 휴장 판정 단위';
COMMENT ON COLUMN market.product.exchange_code   IS 'asset_daily.exchange_code 그대로 (KRX, NASD, NYSE, AMEX, UPBIT). KIS 시세 API 코드(NAS/NYS/AMS)는 코드에서 변환';
COMMENT ON COLUMN market.product.currency_code   IS '시세 통화 (KRW, USD)';
COMMENT ON COLUMN market.product.price_source    IS 'KIS_KR_DAILY | KIS_US_DAILY | UPBIT_DAY | FUND_NAV(KOFIA→fund_price_daily) | NONE(수집 안 함)';
COMMENT ON COLUMN market.product.source_symbol   IS '소스 조회 키 (476800, AAPL, KRW-BTC, 펀드 표준코드). NONE 이면 NULL';
COMMENT ON COLUMN market.product.first_held_date IS 'asset_daily 에서 보유(수량>0 또는 평가액>0)한 첫 기준일';
COMMENT ON COLUMN market.product.last_held_date  IS 'asset_daily 에서 보유한 마지막 기준일';
COMMENT ON COLUMN market.product.is_tracked      IS '매일 수집 여부 = 현재 보유 또는 매도 후 30일 이내 (DAG 가 매일 재계산)';
COMMENT ON COLUMN market.product.price_from_date IS '이 상품 시세의 하한. 첫 적재 때 소스가 준 첫 날짜(상장 1년 미만이면 상장일). 0건이면 조회 종료일+1 — 이 날짜 이전은 다시 요청하지 않는다';

CREATE TABLE IF NOT EXISTS market.product_price_daily (
    standard_date    date          NOT NULL,
    product_code     varchar(30)   NOT NULL,
    opening_price    numeric(20,6),
    highest_price    numeric(20,6),
    lowest_price     numeric(20,6),
    closing_price    numeric(20,6) NOT NULL,
    trading_volume   numeric(28,8),
    currency_code    varchar(5)    NOT NULL,
    source           varchar(20)   NOT NULL,
    insert_datetime  timestamp     NOT NULL DEFAULT now(),
    update_datetime  timestamp     NOT NULL DEFAULT now(),
    CONSTRAINT product_price_daily_pkey PRIMARY KEY (product_code, standard_date),
    CONSTRAINT product_price_daily_product_fkey FOREIGN KEY (product_code) REFERENCES market.product (product_code),
    CONSTRAINT product_price_daily_close_check  CHECK (closing_price > 0),
    CONSTRAINT product_price_daily_source_check CHECK (source IN ('KIS_KR_DAILY', 'KIS_US_DAILY', 'UPBIT_DAY'))
);

CREATE INDEX IF NOT EXISTS product_price_daily_date_idx ON market.product_price_daily (standard_date);

COMMENT ON TABLE  market.product_price_daily IS '주식·ETF·가상자산 일봉 (원주가, 수정주가 아님). 거래일만 있다(휴장일 행 없음). 펀드는 market.fund_price_daily';
COMMENT ON COLUMN market.product_price_daily.standard_date  IS 'KR·US: 현지 거래일 / 가상자산: 업비트 일봉 시작일(KST 09:00 시작, 다음날 09:00 마감)';
COMMENT ON COLUMN market.product_price_daily.closing_price  IS '종가 (KR FID_ORG_ADJ_PRC=1, US MODP=0 원주가 — asset_daily.unit_market_price 와 같은 기준). 가상자산은 KST 09:00 마감가';
COMMENT ON COLUMN market.product_price_daily.trading_volume IS '거래량 (주 / 코인 수량)';
COMMENT ON COLUMN market.product_price_daily.source         IS 'KIS_KR_DAILY(FHKST03010100) | KIS_US_DAILY(HHDFS76240000) | UPBIT_DAY(/v1/candles/days)';

COMMIT;

-- 검증 (적용 직후)
-- SELECT to_regclass('market.product'), to_regclass('market.product_price_daily');
-- SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid IN ('market.product'::regclass, 'market.product_price_daily'::regclass) ORDER BY 1;
