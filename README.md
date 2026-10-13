# Airflow — 개인 데이터 파이프라인

- 개인 자산·시장 데이터 수집·적재용 Airflow 3.3.2
- 프로젝트 단위 구성: `plugins/<project>/` 라이브러리 + `dags/` DAG 파일
- 현재 프로젝트: **asset-flow** (KIS·Upbit API + 수기 원장 → 일별 자산 현황)

## 규칙

- dag_id = 파일명 = `{도메인}_{동사}_{대상}` (예: `account_ingest_asset_daily`)
  - 도메인 `account`·`market`·`auth` / 동사 `ingest`·`rebuild`·`upsert`·`issue`
- 프로젝트 간 import 금지, 공통 코드는 `plugins/shared/`
- 설정 분리
  - 비밀(키·계좌번호): `secrets/*.yaml` (서버 전용)
  - 비밀 아닌 설정: `dags/config/*.yaml` (커밋)
  - 인프라 비밀: `.env` (서버 전용)
- DAG 최상위(파싱 시점)에서 secrets·Variable·DB 접근 금지
- 모든 함수 docstring `Purpose / Parameter / Input / Output` (`tests/test_style.py` 강제)

## 인프라

| 항목 | 값 |
|---|---|
| 버전 | Airflow 3.3.2, Python 3.12, 이미지 `local/airflow:3.3.2-py3.12` |
| Executor | LocalExecutor (단일 서버) |
| 컴포넌트 | `api-server` · `scheduler` · `dag-processor` · `postgres` (triggerer 비활성) |
| 메타 DB | postgres:16 컨테이너, 볼륨 `airflow_pgdata16`, 포트 미노출 |
| 자산 DB | 호스트 PostgreSQL (`host.docker.internal`, conn `postgres_asset`) |
| 인증 | FAB, 관리자 = `.env` `_AIRFLOW_WWW_USER_*` |
| 알림 | Discord (`ASSET_FLOW_ENV=prod` → `discord_alert`, 그 외 → `discord_alert_test`) |
| UI | `http://<서버>:8080` |

```bash
docker compose build
docker compose up -d postgres && docker compose up airflow-init   # 최초 1회
docker compose up -d
docker compose down          # -v 금지 (메타 DB 삭제)
docker compose restart airflow-scheduler airflow-dag-processor airflow-apiserver   # plugins/ 변경 후 필수
```

- `dags/` 는 자동 재파싱, **`plugins/` 는 재시작 전까지 옛 모듈로 실행** (워커가 스케줄러에서 fork)
- 배포·재시작은 05:50~07:40 KST 피함

### `.env` (gitignore)

| 키 | 용도 |
|---|---|
| `AIRFLOW_UID` | 컨테이너 UID (1001) |
| `META_DB_PASSWORD` · `FERNET_KEY` · `AIRFLOW__API_AUTH__JWT_SECRET` · `AIRFLOW__API__SECRET_KEY` | 인프라 비밀 |
| `_AIRFLOW_WWW_USER_USERNAME` / `_PASSWORD` | 관리자 계정 |
| `AIRFLOW__API__BASE_URL` | 알림 링크 URL |
| `AIRFLOW_HOST_BIND` / `AIRFLOW_HOST_PORT` | UI 바인드 (운영 `0.0.0.0:8080`) |
| `ASSET_FLOW_ENV` | `prod` = 운영 DB 쓰기·운영 채널, 그 외 = `*_staging` DB 만 |
| `TOKEN_ISSUE_ENABLED` | `true` 일 때만 KIS 토큰 발급 (기본 false) |
| `TOKEN_HOST_DIR` / `TOKEN_MOUNT_MODE` | 토큰 디렉터리 마운트 (`./data/tokens` / `rw`) |
| `AIRFLOW_CONN_POSTGRES_ASSET` | 자산 DB URI |
| `AIRFLOW_CONN_DISCORD_ALERT` / `_TEST` | Discord 웹후크 (`{"conn_type": "http", "password": "<URL>"}`) |

## 디렉터리

```
airflow/
├── dags/
│   ├── config/asset_flow.yaml     # 비밀 아닌 설정
│   └── *.py                       # DAG 9개 (아래 표)
├── plugins/
│   ├── alerts.py                  # Discord 실패·복구 콜백
│   ├── import_error_alert.py      # DAG import 오류 → Discord
│   └── asset_flow/                # 비즈니스 로직
├── secrets/                       # asset_flow.secrets.yaml (서버 전용) + example + README
├── db/migrations/                 # Airflow 소유 market·account DDL (사람이 적용, README 참고)
├── tests/                         # pytest (순수 로직)
├── data/tokens/tokens.json        # KIS 토큰 캐시 (gitignore)
└── Dockerfile · docker-compose.yaml · requirements.txt · .env
```

# asset-flow

- 대상: 해외·국내주식, RP/발행어음, ISA, 연금저축, CMA, 암호화폐, IRP, DC, 적금, 주택청약
- 매일 T-1 기준 자산 현황을 `account.asset_daily` 에 적재

## 스케줄 (KST)

```
06:00  market_ingest_trading_holiday          (독립, 변경 없으면 skip)
06:50  auth_issue_api_tokens
         │ 센서: 유효 토큰
06:55  market_ingest_exchange_rate_daily · market_ingest_fund_price_daily
         │ 센서: 기준일 환율 4종 · 필요 펀드 기준가
07:00  account_ingest_asset_daily
         collect_account × 활성 계좌 → refresh_dashboard_views
07:10  market_ingest_product_price_daily      (센서: 토큰 · T-1 asset_daily 전 계좌)
07:20  account_ingest_dividend                (센서: 토큰 · 미국 추정만 T-1 asset_daily)
```

## DAG

| DAG | 대상 | 핵심 |
|---|---|---|
| `auth_issue_api_tokens` | `data/tokens/tokens.json` | 활성 계좌 KIS 키만 발급, 지문 변경 키만 재발급 / 키 만료 알림 D-14·D-7·D-3~ |
| `market_ingest_exchange_rate_daily` | `market.exchange_rate_daily` | USD·JPY·GBP·EUR |
| `market_ingest_fund_price_daily` | `market.fund_price_daily` | Playwright 크롤링, 실패 코드 1회 재시도, 누락 시 실패·알림 |
| `account_ingest_asset_daily` | `account.asset_daily` | 계좌당 1 태스크, 0건이면 DELETE 전 실패(기존분 보존) |
| `market_ingest_trading_holiday` | `market.trading_holiday` | 평일 휴장일 3년치, `is_manual` 보호, 변경 시 알림 |
| `market_ingest_product_price_daily` | `market.product` · `product_price_daily` · `fund_price_daily`(빈 날짜) | 원주가 1년 유지, 빈 구간·최근 5거래일 재수집, KOFIA 펀드는 스위치(`fund_nav_enabled`) |
| `account_ingest_dividend` | `market.dividend_event` · `account.dividend_receipt` | 일정 −400~+60/120일·원장 −100일 UPSERT(DELETE 없음), 미국 추정 원장(15%) |
| `account_upsert_manual_position_ledger` | `account.manual_position_ledger` | 수동: 수기 원장 입력 → 해당 계좌 재계산 |
| `account_rebuild_ledger_assets` | `account.asset_daily` | 수동: 수기 계좌 기간 재계산 (`end_date` 비우면 최종 계산일) |

## 공통 동작

- 기준일 `standard_date` = 실행일 T-1 (정기: `data_interval_end`, 수동: `run_after`)
- 정기 run 이 3시간 넘게 늦게 시작 → 수집 skip (과거 기준일 덮어쓰기 방지)
- 센서는 상류 run 이 아닌 **데이터 존재**로 판정 (30초 간격, 최대 10분) → 수동 재실행 가능
- 적재 DAG 마지막: `CALL dashboard.refresh_views()` (일부 실패해도 갱신, run 은 실패 유지)
- 적재 테이블 = Airflow Asset `outlets` (계보·이력 표시 전용, 스케줄 미사용)
- 알림: 태스크 최종 실패 시 Discord, 직전 run 실패였으면 복구 알림 (본문의 키·계좌번호는 `alerts.redact` 로 가림)
- 시세·배당 DAG 는 지연 run skip 없음(이력 API), 토큰 발급 안 함(`existing_token`)
- 수동 run 의 T = 트리거 시각 → 07:00 전 수동 실행은 asset_daily 센서가 대기

## 계좌 kind (`secrets` `accounts[].kind`)

| kind | 수집 |
|---|---|
| `kis_stock` | 위탁: 해외·국내 주식 + RP/발행어음 1행 (`CASH`/`RP_NOTE`), 예수금·외화 제외 |
| `kis_domestic_stock` | ISA 국내 주식 |
| `kis_pension_fund` | 연금저축: 펀드(평가금액 ÷ NAV) + 국내 주식 |
| `kis_cma_cash` | CMA 현금 (ISA 키 공유) |
| `upbit` | 코인 잔고 × T-1 종가 |
| `manual_fund` | IRP·DC: 원장 좌수 × NAV × multiplier |
| `manual_cash` | 적금·청약: 원장 원금 이월 |

## 수기 원장 입력 (`account_upsert_manual_position_ledger`)

- UI → Trigger DAG w/ config, 입금·결제 때마다 실행
- 정기 실행 시간(06:50~07:05) 회피

| Param | IRP·DC | 적금·청약 |
|---|---|---|
| `account_id` | `IRP` / `DC` | `INSTALLMENT_SAVINGS` / `HOUSING_SUBSCRIPTION` |
| `standard_date` | 결제 완료일 (비우면 오늘) | 납입일 |
| `holding_quantity` | 보유 좌수 (누적) | 비움 |
| `total_purchase_amount` | 누적 매입원금 | 누적 납입원금 |
| `evaluation_amount` | 평가금액 (검증용, 미저장) | 비움 |
| `allow_backdate` | 최신 원장일보다 과거 입력 시만 `true` | 같음 |

- 같은 `(standard_date, account_id)` 재실행 = 덮어쓰기 (오타 정정)
- 검증: 좌수 × NAV × multiplier vs `evaluation_amount`, 1% 초과 오차 시 실패
  - 당일 입력은 기준가 없음 → 최신 기준가로 자릿수 검증
  - `evaluation_amount` 비우면 매입원금으로 검증
- 날짜: 미래·수집 시작일 이전 실패, 누락된 과거 날짜는 `allow_backdate=true` 필요
- 재계산: 해당 계좌만 기준일 ~ 최종 계산일, 기준일이 오늘이면 skip
  - 기준가 결측일 있으면 나머지 적재 후 실패 → 기준가 보충 후 이 태스크만 재실행

## 설정

| 파일 | 내용 |
|---|---|
| `secrets/asset_flow.secrets.yaml` | `credentials[]`(키·만료일), `accounts[]`(계좌번호·kind·표시명·활성), `services` — 절차는 [secrets/README.md](secrets/README.md) |
| `dags/config/asset_flow.yaml` | 테이블명, 연금 펀드·크롤 대상, 수기 유형 product_code·multiplier, 만료 알림 규칙, MV 갱신 프로시저명, `price_ingest`, `dividend` |

- 변경은 다음 태스크부터 반영 (재시작 불필요)
- 계좌 추가·해지(`enabled: false`)·키 교체 = secrets 만 수정
- `display_name` = `account_master.account_name` (불일치 시 매일 알림)

## plugins/asset_flow

| 모듈 | 역할 |
|---|---|
| `secrets_config.py` | secrets 로더 (pydantic strict, 마스킹) + 검증 CLI |
| `app_config.py` | config yaml 로더 |
| `assets.py` | Asset 정의·이벤트 메모 |
| `finalize.py` | MV 갱신 + 상류 실패 반영 |
| `tokens.py` | KIS 토큰 (credential 단위·flock·원자적 저장) |
| `pipeline/dispatch.py` | kind → 수집 핸들러, `collect_account` |
| `pipeline/price_plan.py` | 시세 수집 창 계산·소스별 수집·지연 점검 |
| `pipeline/dividend.py` · `transformers/dividend_transformer.py` | 배당 수집 본체·변환 (미국 추정 원장) |
| `ops/key_expiry.py` | 키 만료·설정 정합성 점검 |
| `holiday.py` | 휴장일 계산·변경 비교 |
| `db_manager.py` | 자산 DB 읽기·쓰기 (쓰기 가드) |
| `runtime.py` | 기준일 계산, 지연 run skip |
| `clients/` · `transformers/` · `crawler/` · `config/` | KIS(`_request`·`_get_pages`)·Upbit·KOFIA 클라이언트, 변환, 펀드 크롤러, 상수·스키마 |

## 테스트

```bash
docker run --rm -u 1001:0 -v $PWD:/w -w /w -e HOME=/tmp --entrypoint bash local/airflow:3.3.2-py3.12 \
  -c "pip install -q --no-deps --target /tmp/pt pytest iniconfig pluggy && PYTHONPATH=/w/plugins:/tmp/pt python -m pytest -q tests"
```
