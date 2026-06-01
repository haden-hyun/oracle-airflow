# asset_flow 라이브러리

DAG에서 공통으로 사용하는 비즈니스 로직 모음입니다.
`plugins/` 하위에 위치하므로 별도 설치 없이 DAG에서 직접 import할 수 있습니다.

## 모듈 구조

```
asset_flow/
├── clients/
│   ├── base_client.py          # HTTP 요청 공통 래퍼
│   ├── kis_client.py           # 한국투자증권 API 클라이언트
│   └── upbit_client.py         # 업비트 API 클라이언트
├── config/
│   ├── kis.py                  # KIS API 경로·TR_ID·고정 파라미터 상수
│   ├── upbit.py                # Upbit API 경로 상수
│   └── schemas.py              # 컬럼 리네임 맵 / DB 스키마 컬럼 목록
├── crawler/
│   └── fund_crawler.py         # Playwright 기반 펀드 기준가 크롤러
├── managers/
│   ├── token_manager.py        # API 토큰 발급·파일 저장·조회
│   └── db_manager.py           # DB 조회 헬퍼
└── transformers/
    ├── base_transformer.py     # 숫자 정규화 공통 함수
    ├── kis_transformer.py      # KIS 원시 응답 → DataFrame 변환
    └── upbit_transformer.py    # Upbit 원시 응답 → DataFrame 변환
```

---

## clients

### `KISApiClient`

한국투자증권 REST API 래퍼. 원시 JSON 응답만 반환하며 변환 로직을 포함하지 않습니다.

```python
from asset_flow.clients.kis_client import KISApiClient

kis = KISApiClient(token=tokens['KIS_STOCK'], config=config)
```

| 메서드 | TR_ID | 설명 |
|---|---|---|
| `get_overseas_balance()` | `TTTS3012R` | 해외주식 잔고 조회 |
| `get_domestic_balance()` | `TTTC8434R` | 국내주식 잔고 조회 |
| `get_account_balance()` | `CTRP6548R` | 투자계좌자산현황 조회 (연금저축·CMA 용도) |
| `get_exchange_rate(standard_date, product_codes)` | `FHKST03030100` | 환율 조회 (통화 코드별 순회) |

**계좌 설정 구조 (`config`):**

```python
# Airflow Variable에서 deserialize_json=True로 읽는 형태
{
    "appkey": "...",
    "secret": "...",
    "account": "12345678",   # 계좌번호
    "product_code": "01",    # 상품코드
    "type": "한국투자 주식"    # 계좌명 (transformers에서 account_name으로 사용)
}
```

---

### `UpbitApiClient`

업비트 REST API 래퍼.

```python
from asset_flow.clients.upbit_client import UpbitApiClient

upbit = UpbitApiClient(token=tokens['UPBIT'])
```

| 메서드 | 설명 |
|---|---|
| `get_balance()` | 보유 자산 조회 (KRW 포함 전 종목) |
| `get_market_codes()` | 마켓 코드 조회 (한글명 매핑용) |
| `get_current_prices(markets)` | 현재가 조회 (`['KRW-BTC', ...]` 형태 입력) |

---

## config

### `KIS` (kis.py)

KIS API 호출에 필요한 모든 상수를 클래스 변수로 관리합니다.

| 속성 | 내용 |
|---|---|
| `BASE_URL` | `https://openapi.koreainvestment.com:9443` |
| `PATHS` | 엔드포인트 경로 딕셔너리 (`token`, `domestic_balance`, `overseas_balance`, `account_balance`, `exchange_rate`) |
| `TR_IDS` | 거래 ID 딕셔너리 |
| `PARAMS` | 고정 쿼리 파라미터 딕셔너리 (계좌별 공통 파라미터) |
| `EXCHANGE_RATE_PRODUCT_CODES` | 환율 조회 상품코드 리스트 (`USD, JPY, GBP, EUR`) |
| `ACCOUNT_BALANCE_ROW_NAMES` | 투자계좌자산현황 응답의 행 순서 목록 (API가 인덱스 없이 순서로 반환하므로 인덱스 접근 시 참고용) |

---

## crawler

### `fund_crawler.py`

[fundguide.net](https://www.fundguide.net)에서 펀드 기준가를 Playwright로 크롤링합니다.

**주요 함수:**

| 함수 | 설명 |
|---|---|
| `fetch_fund_quote(page, code)` | 이미 생성된 Playwright `page` 객체로 단일 펀드 기준가 조회. `FundQuote` 반환 |
| `to_fund_price_record(raw, standard_date)` | `FundQuote.to_dict()` 결과를 `fund_price_daily` 스키마 레코드로 변환 |
| `crawl_fund(code)` | 브라우저 생성부터 종료까지 포함한 단일 펀드 독립 크롤링 (로컬 실행용) |

**크롤링 방식:**

1. fundguide.net 상품 검색 URL로 이동 (`networkidle` 대기)
2. "검색결과" 탭 클릭 → 기준가 그리드 렌더링 대기
3. `td.taC[data-tab="tab0"]` 셀에서 기준가(NAV)·등락 파싱

**DAG에서의 사용 패턴** (`fetch_fund_price_daily_dag.py`):
- 브라우저 1개를 생성하고 펀드 코드를 순회하며 같은 `page` 객체 재사용 → 브라우저 오버헤드 최소화

**로컬 직접 실행:**
```bash
# 최초 1회 Playwright 브라우저 설치
playwright install chromium

python -m asset_flow.crawler.fund_crawler
```

---

## managers

### `TokenManager`

KIS·Upbit API 토큰을 날짜 기반 파일(`data/tokens/YYYYMMDD_token.json`)로 관리합니다.

**주요 메서드:**

| 메서드 | 설명 |
|---|---|
| `TokenGenerator()` | 토큰 발급 및 파일 저장. 당일 파일이 이미 존재하면 스킵. 과거 토큰 파일 자동 삭제 |
| `GetTokens()` | 당일 토큰 파일 읽기. 파일이 없으면 `TokenGenerator()` 호출 후 반환 |

**발급 토큰 종류:**

| 키 | 인증 방식 | Airflow Variable |
|---|---|---|
| `KIS_STOCK` | KIS OAuth2 (Bearer) | `KIS_STOCK` |
| `KIS_ISA` | KIS OAuth2 (Bearer) | `KIS_ISA` |
| `KIS_PENSION` | KIS OAuth2 (Bearer) | `KIS_PENSION` |
| `KIS_IRP` | KIS OAuth2 (Bearer) | `KIS_IRP` |
| `UPBIT` | JWT (access_key + secret) | `UPBIT` |

**토큰 파일 경로:**
환경변수 `TOKEN_DIR`로 오버라이드 가능. 기본값: `data/tokens/`

---

### `db_manager.py`

| 함수 | 설명 |
|---|---|
| `get_fund_price(engine, product_code, standard_date)` | `market.fund_price_daily`에서 기준가·종목명 반환. 데이터 없으면 `(None, None)` |

---

## transformers

원시 API 응답(dict)을 받아 `account.asset_daily` 테이블 스키마에 맞는 DataFrame으로 변환합니다.
모든 변환 함수는 `BALANCE_COLUMNS`로 컬럼을 고정하여 반환합니다.

### `kis_transformer.py`

| 함수 | 입력 | 설명 |
|---|---|---|
| `transform_domestic_balance(raw, standard_date, config)` | `get_domestic_balance()` 응답 | 국내주식(ISA·연금저축 주식 포함) 잔고 변환. `asset_type="STOCK"` |
| `transform_overseas_balance(raw, standard_date, config)` | `get_overseas_balance()` 응답 | 해외주식 잔고 변환. `asset_type="STOCK"` |
| `transform_pension_fund_balance(raw, standard_date, config, product_code, fund_price, product_name)` | `get_account_balance()` 응답 | 연금저축 펀드 변환. API 응답 `output1[1]`(펀드/MMW 행) 고정 인덱스 접근. `asset_type="FUND"`, `multiplier=0.001` |
| `transform_cma_cash_balance(raw, standard_date, config)` | `get_account_balance()` 응답 | CMA 현금 잔고 변환. API 응답 `output1[14]`(외화단기사채 행) 고정 인덱스 접근. `asset_type="CASH"` |
| `transform_exchange_rate(raw_list, standard_date)` | `get_exchange_rate()` 응답 리스트 | 환율 변환. 달러/파운드·달러/유로는 원/달러 기준으로 교차 환산하여 원화 기준으로 통일 |

> **KIS API 고정 인덱스 주의**: `get_account_balance()` 응답은 행 순서가 고정(`KIS.ACCOUNT_BALANCE_ROW_NAMES`)되어 있어 인덱스로 접근합니다. API 스펙 변경 시 인덱스 재확인 필요.

### `upbit_transformer.py`

| 함수 | 설명 |
|---|---|
| `transform_upbit_balance(balance_raw, market_code_raw, price_raw, standard_date, account_code, account_name)` | 잔고·마켓코드·현재가 3개 응답을 병합해 암호화폐 잔고 DataFrame 생성. `asset_type="CRYPTO"` |
