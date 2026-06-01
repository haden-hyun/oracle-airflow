# Airflow Project

개인 자산 데이터를 수집·적재하는 Airflow 파이프라인 레포지토리입니다.
KIS(한국투자증권), Upbit API를 통해 매일 자산 현황을 수집하고 PostgreSQL에 저장합니다.

## 전체 프로젝트 구조

```
airflow/
├── dags/                                   # DAG 정의 파일
│   ├── make_token_dag.py
│   ├── fetch_exchange_rate_dag.py
│   ├── fetch_fund_price_daily_dag.py
│   └── fetch_asset_daily_dag.py
├── plugins/                                # 공유 라이브러리
│   └── asset_flow/                         # 핵심 비즈니스 로직
│       ├── clients/                        # API 클라이언트 (KIS, Upbit)
│       ├── config/                         # 설정 스키마
│       ├── crawler/                        # 펀드 기준가 크롤러
│       ├── managers/                       # 토큰·DB 매니저
│       └── transformers/                   # 데이터 변환
├── docker-compose.yaml
├── Dockerfile
└── requirements.txt
```

## 인프라

- **Airflow 버전**: 2.10.4
- **Executor**: LocalExecutor (단일 서버)
- **메타 DB**: PostgreSQL 13 (Docker 내부)
- **자산 DB**: PostgreSQL (호스트 서버, `host.docker.internal` 경유)
- **UI**: `http://localhost:8080`

```bash
# 초기 실행
docker-compose up -d

# 종료
docker-compose down
```

---

## asset-flow 파이프라인

개인 금융 자산(해외주식, 국내주식, ISA, 연금저축, CMA, 암호화폐)을 매일 수집해 DB에 적재하는 파이프라인입니다.

### DAG 종속성 및 스케줄

```
06:50  make_token
         │  (ExternalTaskSensor, delta=5m)
         ▼
06:55  fetch_exchange_rate      fetch_fund_price_daily
         │                              │
         └──────────(ExternalTaskSensor, delta=5m)──────────┐
                                                             ▼
07:00                                           fetch_asset_daily
```

### DAG 상세

#### `make_token` — 06:50 KST

KIS(한국투자증권) 및 Upbit API 접근 토큰을 발급하고 파일로 저장합니다.
이후 모든 DAG는 이 토큰을 읽어 API를 호출합니다.

| task_id | 역할 |
|---|---|
| `generate_tokens` | KIS·Upbit 토큰 발급 및 저장 |

---

#### `fetch_exchange_rate` — 06:55 KST

USD, JPY, GBP, EUR 환율을 KIS API로 수집해 `market.exchange_rate_daily` 테이블에 적재합니다.

| task_id | 역할 |
|---|---|
| `wait_for_token` | `make_token.generate_tokens` 완료 대기 (ExternalTaskSensor) |
| `get_standard_date` | 기준일자 파생 (`data_interval_end` - 1일) |
| `fetch_exchange_rates` | KIS API 환율 조회 |
| `transform_exchange_rates` | 원시 데이터 변환 |
| `upload_exchange_rates` | DB 적재 (upsert) |

---

#### `fetch_fund_price_daily` — 06:55 KST

Playwright로 fundguide.net을 크롤링해 연금저축 펀드 기준가를 `market.fund_price_daily` 테이블에 적재합니다.
`make_token`과 무관하게 독립 실행됩니다.

| task_id | 역할 |
|---|---|
| `get_standard_date` | 기준일자 파생 |
| `crawl_fund_prices` | Playwright 크롤링 |
| `upload_fund_prices` | DB 적재 (upsert) |

---

#### `fetch_asset_daily` — 07:00 KST

두 상류 DAG의 완료를 확인한 뒤 전체 자산 현황을 병렬 수집해 `account.asset_daily` 테이블에 적재합니다.

| task_id | 역할 |
|---|---|
| `wait_for_exchange_rate` | `fetch_exchange_rate.upload_exchange_rates` 완료 대기 |
| `wait_for_fund_price_daily` | `fetch_fund_price_daily.upload_fund_prices` 완료 대기 |
| `get_standard_date` | 기준일자 파생 |
| `get_daily_asset_group` | 자산 병렬 수집 (아래 6개 task 동시 실행) |
| ├─ `get_overseas_stock` | KIS 해외주식 잔고 조회 |
| ├─ `get_domestic_stock` | KIS 국내주식 잔고 조회 |
| ├─ `get_isa_stock` | KIS ISA 잔고 조회 |
| ├─ `get_pension_assets` | KIS 연금저축 펀드·주식 잔고 조회 |
| ├─ `get_cma_cash` | KIS CMA 현금 잔고 조회 |
| └─ `get_upbit_assets` | Upbit 암호화폐 잔고 조회 |
| `upload_asset_data` | 전체 자산 데이터 통합 후 DB 적재 |

---

## plugins/asset_flow 라이브러리

DAG에서 공통으로 사용하는 비즈니스 로직 모음입니다. `plugins/` 하위에 위치하므로 별도 설치 없이 DAG에서 직접 import할 수 있습니다.

세부 사양은 [plugins/asset_flow/README.md](plugins/asset_flow/README.md)를 참고하세요.
