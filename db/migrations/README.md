# db/migrations — Airflow 소유 스키마 변경

- Airflow 가 채우는 원천 테이블(`market.*`, `account.*` 중 Airflow 적재분) 정의
- `dashboard.*` 는 대시보드 저장소 `db/migrations/` 소유 (Airflow 는 프로시저 CALL·읽기만)
- 컨테이너에 마운트하지 않음 → **사람이 적용** (DAG 는 DDL 실행 안 함)

## 규약

| 항목 | 규칙 |
|---|---|
| 이름 | `NNN_짧은_설명.sql`, 번호 재사용 금지 |
| 트랜잭션 | 파일 하나 = `BEGIN; … COMMIT;` |
| 재실행 | 안전해야 함 (`IF NOT EXISTS`, `CREATE OR REPLACE`) |
| 비파괴 | 적재 테이블 행 `DROP`·`TRUNCATE`·`DELETE`·`UPDATE` 금지 |
| 머리 주석 | 첫 줄 `-- NNN — 무엇을`, 이어서 왜·주의점 |
| 적용 후 | 고치지 않음, 바꾸려면 다음 번호 |
| 권한 | 단일 role `haejun` 소유 → GRANT 없음 |
| 배포 순서 | 마이그레이션 적용 → 그 테이블을 쓰는 DAG·plugins 배포 (plugins 는 재시작) |

## 적용 (스케줄러 컨테이너 psql + `AIRFLOW_CONN_POSTGRES_ASSET`)

```bash
cd ~/airflow
# 1) 접속 대상 확인 → haejun | asset
docker compose exec -T airflow-scheduler sh -c 'psql "$AIRFLOW_CONN_POSTGRES_ASSET" -Atc "select current_user, current_database()"'
# 2) 적용 (파일은 stdin 으로 전달 — 컨테이너에 db/ 가 없음)
docker compose exec -T airflow-scheduler sh -c 'psql "$AIRFLOW_CONN_POSTGRES_ASSET" -v ON_ERROR_STOP=1 -f -' < db/migrations/NNN_xxx.sql
# 3) 검증 (파일 끝 '-- 검증' 주석의 쿼리, 예)
docker compose exec -T airflow-scheduler sh -c 'psql "$AIRFLOW_CONN_POSTGRES_ASSET" -At -f -' <<< "select to_regclass('market.product')"
```

- 호스트 psql 은 `host.docker.internal` 을 못 풀어 실패 → 위 컨테이너 방식만
- `-T` 필수 (stdin 리다이렉트), `ON_ERROR_STOP=1` 로 첫 오류에서 중단 → 트랜잭션 롤백

## 컬럼 이름 규칙 (기존 account·market 테이블 관례)

- 약어 없이 단어 그대로 (`holding_quantity`, `total_evaluation_amount`)
- 끝말: `_code` · `_name` · `_type` · `_date` · `_amount` · `_price` · `_rate` · `_quantity` · `_datetime`, 불리언 `is_`
- 기록 시각: `insert_datetime` (+ 갱신 감지가 필요하면 `update_datetime`)

## 이력

| 파일 | 만드는 것 | 채우는 DAG | 적용일 |
|---|---|---|---|
| `001_market_product` | `market.product`, `market.product_price_daily` | `market_ingest_product_price_daily` (07:10) | 2026-10-14 |
| `002_dividend` | `market.dividend_event`, `account.dividend_receipt` | `account_ingest_dividend` (07:20) | 2026-10-14 |
| `003_dividend_column_names` | 배당 컬럼명 표준화 (`per_share_dividend_amount` · `dividend_type` · `exchange_rate`) | — | 2026-10-14 |

- 001 이전 테이블(asset_daily·fund_price_daily·exchange_rate_daily 등)은 이 디렉터리에 정의 없음
  - 예: `market.trading_holiday` = 대시보드 저장소 `db/migrations/006` (002 머리 주석)
- `market.product_price_daily` → `market.product` FK 있음 / `dividend_*` → `market.product` FK 없음 (배당은 product 없이도 동작)
- `account.dividend_receipt` → `account.account_master` FK 있음

### 핵심 제약

| 테이블 | PK | 비고 |
|---|---|---|
| `market.product` | `product_code` | `price_source` ∈ KIS_KR_DAILY·KIS_US_DAILY·UPBIT_DAY·FUND_NAV·NONE |
| `market.product_price_daily` | `(product_code, standard_date)` | 원주가, 거래일만, `closing_price > 0`, 펀드 제외 |
| `market.dividend_event` | `(product_code, record_date, dividend_kind)` | `dividend_kind` ∈ CASH·SPECIAL, `last_seen_date` |
| `account.dividend_receipt` | `(account_code, product_code, record_date, right_type_code)` | `right_type_code` ∈ 32·03·EST03·EST75, `EST*` ⇔ `is_estimated` |

- `last_seen_date` < 같은 종목·계좌 최신값 이고 기준일이 재조회 창 안 → 정정으로 사라진 행 (읽는 쪽이 거름)
