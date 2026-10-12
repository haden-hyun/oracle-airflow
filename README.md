# Airflow 개요

개인 데이터 수집·적재 파이프라인을 관리하는 Apache Airflow 레포지토리입니다.

금융 자산, 시장 데이터 등 다양한 도메인의 수집 파이프라인을 **프로젝트 단위**로 구성하며,
각 프로젝트는 독립된 `plugins/<project>/` 라이브러리와 `dags/` 내 DAG 파일 세트로 이루어집니다.

### 레포 구성 원칙

- **DAG**: `dags/` 하위에 배치. dag_id = 파일명 = `{도메인}_{동사}_{대상테이블}` (예: `account_ingest_asset_daily`).
  - 도메인: `account` · `market` · `auth` / 동사: `ingest` · `rebuild` · `upsert` · `issue`
- **라이브러리**: `plugins/<project_name>/` 하위에 비즈니스 로직을 패키지로 구성합니다.
  Airflow는 `plugins/` 디렉토리를 Python 경로에 자동으로 추가하므로, DAG에서 `from <project_name>.<module> import ...` 형태로 사용합니다.
- **격리**: 프로젝트 간 라이브러리는 서로 의존하지 않습니다. 공통 유틸리티가 필요한 경우 `plugins/shared/`에 위치시킵니다.
- **설정 분리**: 비밀(키·계좌번호) = `secrets/*.yaml`(서버 전용), 비밀 아닌 설정 = `dags/config/*.yaml`(커밋), 인프라 비밀 = `.env`(서버 전용).
- **파싱 시점 접근 금지**: DAG 최상위에서 secrets·Variable·DB 를 읽지 않습니다 (태스크 실행 중에만).

## 인프라

- **Airflow 버전**: 3.3.2 (Python 3.12, 이미지 `local/airflow:3.3.2-py3.12`)
- **Executor**: LocalExecutor (단일 서버)
- **컴포넌트**: `api-server`(UI·API) · `scheduler` · `dag-processor` · `postgres` (triggerer 는 profile 로 비활성)
- **메타 DB**: PostgreSQL 16 (Docker 내부, 볼륨 `airflow_pgdata16`, 호스트 포트 미노출)
- **자산 DB**: PostgreSQL 16 (호스트 서버, `host.docker.internal` 경유, `.env` 의 `AIRFLOW_CONN_POSTGRES_ASSET`)
- **인증**: FAB auth manager (관리자 계정은 `.env` 의 `_AIRFLOW_WWW_USER_*`)
- **알림**: Discord (`ASSET_FLOW_ENV=prod` → 운영 채널 `discord_alert`, 그 외 → 테스트 채널 `discord_alert_test`)
- **UI**: `http://<서버>:8080`

```bash
# 최초 빌드·초기화 (DB 스키마 생성 + 관리자 계정)
docker compose build
docker compose up -d postgres && docker compose up airflow-init

# 실행 / 종료
docker compose up -d
docker compose down          # -v 금지 (메타 DB 볼륨 삭제됨)
```

### 환경 변수 (`.env`, gitignore)

| 키 | 용도 |
|---|---|
| `AIRFLOW_UID` | 컨테이너 실행 UID (1001) |
| `META_DB_PASSWORD`, `FERNET_KEY`, `AIRFLOW__API_AUTH__JWT_SECRET`, `AIRFLOW__API__SECRET_KEY` | 인프라 비밀 |
| `_AIRFLOW_WWW_USER_USERNAME` / `_PASSWORD` | 관리자 계정 (airflow-init 시 생성) |
| `AIRFLOW__API__BASE_URL` | 알림 링크용 외부 URL |
| `AIRFLOW_HOST_BIND` / `AIRFLOW_HOST_PORT` | UI 바인드 (운영 `0.0.0.0:8080`) |
| `ASSET_FLOW_ENV` | `prod` 면 운영 DB 쓰기·운영 알림 채널. 그 외는 `*_staging` DB 에만 쓰기 허용 |
| `TOKEN_ISSUE_ENABLED` | `true` 일 때만 KIS 토큰 발급 (기본 false = 안전장치) |
| `TOKEN_HOST_DIR` / `TOKEN_MOUNT_MODE` | 토큰 디렉터리 마운트 (운영 `./data/tokens` / `rw`) |
| `AIRFLOW_CONN_POSTGRES_ASSET` | 자산 DB 접속 URI |
| `AIRFLOW_CONN_DISCORD_ALERT` / `_TEST` | Discord 웹후크 (`{"conn_type": "http", "password": "<URL>"}`) |

# 프로젝트

## 1. asset-flow

> 개인 자산 데이터를 수집·적재하는 Airflow 파이프라인. KIS(한국투자증권), Upbit API 수집과
> IRP·DC·적금·주택청약 등 API 미지원 자산의 수동 입력 원장을 함께 관리하며 매일 자산 현황을 PostgreSQL에 적재

### 전체 프로젝트 구조

```
airflow/
├── dags/
│   ├── config/asset_flow.yaml                  # 비밀 아닌 설정 (테이블·펀드·수기 유형·만료 알림 규칙)
│   ├── auth_issue_api_tokens.py                # 06:50 KIS 토큰 발급 + API 키 만료·설정 정합성 점검
│   ├── market_ingest_exchange_rate_daily.py    # 06:55 환율
│   ├── market_ingest_fund_price_daily.py       # 06:55 펀드 기준가 (Playwright 크롤링)
│   ├── account_ingest_asset_daily.py           # 07:00 전 계좌 자산 (계좌별 동적 매핑)
│   ├── market_ingest_trading_holiday.py        # 06:00 휴장일 달력 (변경 있을 때만 갱신)
│   ├── account_upsert_manual_position_ledger.py # 수동: 수기 원장 입력 + 해당 계좌 재계산
│   └── account_rebuild_ledger_assets.py        # 수동: 수기 계좌 asset_daily 재계산
├── plugins/
│   ├── alerts.py                               # Discord 실패·복구 콜백, post_discord
│   ├── import_error_alert.py                   # DAG import error → Discord (리스너 플러그인)
│   └── asset_flow/                             # 핵심 비즈니스 로직 (아래 표)
├── secrets/
│   ├── asset_flow.secrets.yaml                 # (서버 전용) 키·계좌번호·만료일 — 단일 진실 공급원
│   ├── asset_flow.secrets.example.yaml         # 템플릿 (커밋)
│   └── README.md                               # 작성·검증·교체 절차
├── tests/                                      # pytest (Airflow 런타임 불필요한 순수 로직 위주)
├── data/tokens/tokens.json                     # KIS 토큰 캐시 (gitignore)
├── Dockerfile, docker-compose.yaml, requirements.txt
└── .env                                        # 인프라 비밀 (gitignore)
```

### asset-flow 파이프라인

> 개인 금융 자산(해외주식, 국내주식, RP/발행어음, ISA, 연금저축, CMA, 암호화폐, IRP, DC, 적금, 주택청약)을
> 매일 수집해 DB에 적재하는 파이프라인입니다.

**DAG 종속성 및 스케줄** (KST)

```
06:00  market_ingest_trading_holiday        (독립, 변경 없으면 skip)

06:50  auth_issue_api_tokens
         │  (센서: tokens.json 에 환율용 키의 유효 토큰)
         ▼
06:55  market_ingest_exchange_rate_daily   market_ingest_fund_price_daily
         │                                          │
         └────────────────────┬─────────────────────┘
                              │  (센서: 기준일 환율 4종 · 필요 펀드 기준가가 DB 에 있음)
                              ▼
07:00  account_ingest_asset_daily
         get_standard_date → 센서 2 → collect_account × 활성 계좌
         → refresh_dashboard_views (CALL dashboard.refresh_views(), 대시보드 MV 갱신)
```

자산 적재·수기 원장 입력·재적재 DAG 는 끝에서 대시보드 MV 갱신 프로시저를 부른다 (대시보드 저장소 마이그레이션 009, Airflow 는 config 의 이름만 앎). 일부 계좌가 실패해도 갱신하고, run 은 실패로 남긴다.

센서는 상류 run 이 아니라 **데이터 존재**로 판정한다 (30초 간격, 최대 10분, 넘으면 실패 + 알림). 그래서 어느 DAG 든 수동 재실행해도 동작한다.

적재 태스크는 대상 테이블을 Airflow Asset 으로 선언한다 (`plugins/asset_flow/assets.py`, `outlets`). 성공할 때마다 UI **Assets** 화면에 갱신 이벤트(기준일·건수 메모)가 쌓인다. 계보·이력 표시 전용이고 스케줄에는 쓰지 않는다 (이벤트에 날짜 개념이 없어 과거 기준일 덮어쓰기 위험).

| DAG | 적재 대상 | 비고 |
|---|---|---|
| `auth_issue_api_tokens` | `data/tokens/tokens.json` | 활성 계좌가 쓰는 KIS 키만 발급, 키 지문 바뀌면 그 키만 재발급. `check_key_expiry` 는 발급과 병렬 (D-14·D-7·D-3~매일 알림, 알릴 것 없으면 skip) |
| `market_ingest_exchange_rate_daily` | `market.exchange_rate_daily` | USD·JPY·GBP·EUR |
| `market_ingest_fund_price_daily` | `market.fund_price_daily` | 크롤 대상 = config ∪ 계좌 펀드 ∪ `extra_fund_codes`. 실패 코드 1회 재크롤링, 수집된 코드만 교체, 누락 코드 있으면 `check_fund_coverage` 실패 + 알림 |
| `account_ingest_asset_daily` | `account.asset_daily` | 계좌당 1 태스크(수집+적재). 0건이면 DELETE 전에 실패 → 기존 적재분 보존 |
| `market_ingest_trading_holiday` | `market.trading_holiday` | 평일 휴장일 3년치. `is_manual` 행 보호, 변경 시 Discord 알림 |

**공통 동작**
- 기준일: `standard_date` = 실행일 T-1 (KST), 정기 run 은 `data_interval_end`, 수동 run 은 `run_after` 기준
- 정기 run 이 예정보다 3시간 넘게 늦게 시작하면 수집 skip (재기동·unpause 직후 과거 기준일을 현재 값으로 덮는 것 방지)
- 실패 알림: 태스크 최종 실패 시 Discord (매핑 태스크는 계좌 id 표시) / 복구 알림: 직전 같은 유형 run 이 실패였으면

**계좌 kind** (`secrets` 의 `accounts[].kind`)

| kind | 수집 내용 |
|---|---|
| `kis_stock` | 위탁: 해외·국내 주식 + RP/발행어음 1행 (`CASH`/`RP_NOTE`, 예수금·외화 미적재) |
| `kis_domestic_stock` | ISA 국내 주식 |
| `kis_pension_fund` | 연금저축: 펀드(평가금액 ÷ NAV → 좌수) + 국내 주식 |
| `kis_cma_cash` | CMA 현금 (ISA 키 공유) |
| `upbit` | 코인 잔고 × T-1 일봉 종가 |
| `manual_fund` | IRP·DC: 원장 좌수 × NAV × multiplier |
| `manual_cash` | 적금·청약: 원장 원금 이월 |

**수동 트리거 DAG** (`schedule=None`)

IRP·DC·적금·주택청약은 API가 없어 `account.manual_position_ledger` 원장을 사람이 직접 채운다.
`account_ingest_asset_daily`가 이 원장을 매일 읽어 파생시키므로 위 스케줄과는 별개로 동작한다.

| DAG | 용도 |
|---|---|
| `account_upsert_manual_position_ledger` | 입금/결제가 있을 때마다 원장에 행 추가 (날짜·좌수 검증) → 그 계좌 `asset_daily` 를 기준일부터 바로 재계산 |
| `account_rebuild_ledger_assets` | 기준가 결측 복구 후, 또는 수기 계좌 전체를 기간으로 재계산 (`end_date` 비우면 정기 DAG 최종 계산일) |

#### `account_upsert_manual_position_ledger` 사용법

입금·결제가 있을 때마다 Airflow UI에서 Trigger DAG w/ config로 실행한다. `account_id`(secrets 의 계좌 id)로
계좌를 고르면 계좌번호·펀드코드·multiplier 는 secrets·config 에서 자동으로 채워진다.

| Param | IRP·DC | 적금·청약 |
|---|---|---|
| `account_id` | `IRP` / `DC` | `INSTALLMENT_SAVINGS` / `HOUSING_SUBSCRIPTION` |
| `standard_date` | 결제 완료일 (비우면 오늘) | 납입일 |
| `holding_quantity` | 앱 화면의 보유수량(좌) | 비움 |
| `total_purchase_amount` | 누적 매입원금 | 누적 납입원금 |
| `evaluation_amount` | 앱 화면의 평가금액 (검증용, 저장 안 됨) | 비움 |
| `allow_backdate` | 최신 원장일보다 과거 날짜(빠뜨린 입력)일 때만 `true` | 같음 |

```
# 예: IRP에 새 매입 반영 (값은 예시)
account_id = IRP
standard_date = 2026-09-25
holding_quantity = 1000000
total_purchase_amount = 1500000
evaluation_amount = 1550000   # validate 태스크가 좌수 오타를 이 값으로 대조 검증
```

- 같은 `(standard_date, account_id)`로 다시 트리거하면 오타 정정으로 간주해 덮어쓴다(UPSERT).
- `total_purchase_amount`/`holding_quantity`는 회차 금액이 아니라 **그 시점까지의 누적값**이다.
- `validate` 태스크가 좌수 × NAV × multiplier와 `evaluation_amount`를 대조해 1% 초과 오차면 실패시킨다.
- 단, 당일 입력은 그날 기준가가 아직 없으므로(D 기준가는 D+1 06:55 크롤링) 역산값을 최신 기준가와
  대조하는 자릿수 검증으로 대체된다. `evaluation_amount`를 비우면 매입원금으로 검증한다.
- **날짜 검증**: 미래 날짜·수집 시작일 이전은 실패. 이 계좌 최신 원장일보다 과거이고 그 날짜 행도 없으면
  (빠뜨린 입력 또는 날짜 오타) `allow_backdate=true` 일 때만 진행한다.
- **재계산**: `rebuild_from_ledger` 가 그 계좌만 기준일 ~ 정기 DAG 최종 계산일(`asset_daily` 최대 기준일, 어제 이하)을
  다시 계산한다. 그 뒤 날짜는 정기 DAG 가 새 원장으로 계산하므로 07:00 전후 어느 때 입력해도 결과가 같다.
  기준일이 오늘이면 skip. 중간에 기준가 결측일이 있으면 나머지는 적재하고 실패 알림 → 기준가 보충 후 이 태스크만 재실행.
- 정기 실행 시간(06:50~07:05)에는 실행하지 않는다.

### 설정 파일

| 파일 | 내용 | 변경 반영 |
|---|---|---|
| `secrets/asset_flow.secrets.yaml` | `credentials[]`(키·만료일), `accounts[]`(계좌번호·kind·표시명·활성), `services`(환율·휴장일 키 참조) | 다음 태스크부터 (재시작 불필요). 작성·검증 절차는 [secrets/README.md](secrets/README.md) |
| `dags/config/asset_flow.yaml` | 테이블명, 연금 기본 펀드·크롤 대상, 수기 유형별 product_code·multiplier, 만료 알림 규칙, 대시보드 MV 갱신 프로시저 이름 | 다음 태스크부터 |

- 계좌 추가·해지(`enabled: false`)·키 교체는 secrets 파일만 고치면 된다 (DAG 코드 수정 없음).
- `display_name` 은 `account_master.account_name` 과 같아야 한다 (다르면 매일 정합성 알림).

---

### plugins/asset_flow 라이브러리

> DAG에서 공통으로 사용하는 비즈니스 로직 모음입니다. `plugins/` 하위에 위치하므로 별도 설치 없이 DAG에서 직접 import할 수 있습니다.

| 모듈 | 역할 |
|---|---|
| `secrets_config.py` | secrets 로더 (pydantic strict 검증, 중복 키 거부, 마스킹 등록) + 검증 CLI |
| `app_config.py` | `dags/config/asset_flow.yaml` 로더 |
| `assets.py` | 적재 테이블 Asset 정의 + 이벤트 메모(`mark`) — 표시 전용 |
| `finalize.py` | 적재 DAG 공용 마지막 단계: `refresh_dashboard_views`(all_done, 대시보드 MV 갱신 프로시저 CALL) + `fail_if_upstream_failed`(앞 단계 실패를 run 상태에 반영, 알림 없음) |
| `tokens.py` | KIS 토큰 v2 (credential 단위 발급·지문 비교·flock·원자적 저장) |
| `pipeline/dispatch.py` | 계좌 kind → 수집 핸들러, `collect_account`(수집 → 0건 검사 → 스코프 적재) |
| `ops/key_expiry.py` | 키 만료 판정·Upbit 만료일 대조·설정 정합성 점검·알림 embed |
| `holiday.py` | 휴장일 계산 (holidays 패키지 + 특일정보 API), 변경 비교 |
| `db_manager.py` | 자산 DB 조회·쓰기 (쓰기 가드, 날짜 변환, 오류 메시지 바인드 값 숨김) |
| `runtime.py` | KST 기준일 계산, 지연 정기 run skip |
| `clients/` · `transformers/` · `crawler/` · `config/` | KIS·Upbit API 클라이언트, 응답 → DataFrame 변환, 펀드 크롤러, API 상수·컬럼 스키마 |

### 테스트

```bash
docker run --rm -u 1001:0 -v $PWD:/w -w /w -e HOME=/tmp --entrypoint bash local/airflow:3.3.2-py3.12 \
  -c "pip install -q --no-deps --target /tmp/pt pytest iniconfig pluggy && PYTHONPATH=/w/plugins:/tmp/pt python -m pytest -q tests"
```

- 코드 규칙(`tests/test_style.py`): 모든 함수 docstring 에 `Purpose / Parameter / Input / Output`, DAG 파싱 시점 secrets 접근 금지.
