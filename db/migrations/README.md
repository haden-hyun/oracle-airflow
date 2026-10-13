# db/migrations — Airflow 소유 스키마 변경

- Airflow 가 채우는 원천 테이블(`market.*`, `account.*` 중 Airflow 적재분) 정의
- `dashboard.*` 는 대시보드 저장소 `db/migrations/` 소유 (Airflow 는 읽기만)
- 컨테이너에 마운트하지 않음 → 사람이 적용

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

## 적용 (스케줄러 컨테이너의 psql + postgres_asset URI)

```bash
cd ~/airflow
docker compose exec -T airflow-scheduler sh -c 'psql "$AIRFLOW_CONN_POSTGRES_ASSET" -Atc "select current_user, current_database()"'
docker compose exec -T airflow-scheduler sh -c 'psql "$AIRFLOW_CONN_POSTGRES_ASSET" -v ON_ERROR_STOP=1 -f -' < db/migrations/001_market_product.sql
```

- 호스트 psql 은 `host.docker.internal` 을 못 풀어 실패 — 위 컨테이너 방식만 사용

| 파일 | 만드는 것 | 적용일 |
|---|---|---|
| 001_market_product | `market.product`, `market.product_price_daily` | 2026-10-14 |
| 002_dividend | `market.dividend_event`, `account.dividend_receipt` | 2026-10-14 |
