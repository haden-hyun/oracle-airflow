# 변경·사고 이력

- 현재 규칙은 [README](../README.md), 여기는 날짜별 기록만 (KST)

## 2026-10-13 — Airflow 3.3.2 전환 (`a92f702`)

- 2.10.4(블루) → 3.3.2 독립 스택, 01:00 KST 운영 전환 (블루 원본: `archive/airflow-2.10` 브랜치 · `v2.10.4-final` 태그)
- 메타 DB 새로 설치 (postgres:16, 볼륨 `airflow_pgdata16`)
- 3시간 넘게 늦게 시작한 정기 run 은 수집 skip (`runtime.standard_dates`) — 현재값 API 가 과거 기준일을 덮지 않게

## 2026-10-14 — 시세·배당 DAG, KOFIA 일원화 (`9f44a62` · `2f81c06` · `bacb717`)

- 시세 `market_ingest_product_price_daily` 07:10 · 배당 `account_ingest_dividend` 07:20 추가, 마이그레이션 001~003
- 펀드 기준가 수집 KOFIA 일원화 (fundguide 는 대체·대조)
- 사고: plugins/ 변경 후 재시작 누락 → 옛 `AppConfig`(extra=forbid) 가 새 config 키 거부 → `load_app_config()` 쓰는 태스크 전부 실패
  - 재시작 3종으로 복구, README 에 plugins/ 재시작 규칙 추가

## 2026-10-17~18 — KOFIA User-Agent 차단 대응 (`8e9117f`)

- 10-17 부터 KOFIA 가 Python UA 요청을 180초 붙잡고 끊음 (curl UA ~105초, 브라우저 UA 2초 미만, IP 차단 아님)
- 브라우저 UA 전송 · 타임아웃 (10, 150) · 펀드 병렬 조회 · 07:10 은 빈 거래일 있을 때만 호출

## 2026-11-18 — KIS 환율 응답 KeyError 로 asset_daily 누락

- 11-19 07:00 run: KIS 환율 응답에 `output1` 없음 → KeyError → KIS·Upbit 계좌 11-18 asset_daily 없음 (수기 계좌만 적재)
- KIS 는 과거 잔고 API 없음 → 복구 불가

## 2026-11-23~24 — plugins/ 재시작 누락 재발 → 배포 절차 (`f072bd7` → `9446eef`)

- 11-23 라이브 트리에서 `f072bd7` 직접 커밋, 재시작 누락 → 정기 DAG 5개 이틀 실패 (KIS·Upbit 11-22·11-23 asset_daily 없음)
- 11-24 `scripts/deploy.sh`(검증·드리프트 점검) + `scripts/git-hooks/`(post-commit·post-merge·post-rewrite 자동 반영) 도입

## 2026-11-24 — 펀드 기준가 빈 날짜 자동 보충 (`3b1045a`)

- 06:55 `fund_price.fill_nav_gaps`: 최근 365일 빈 달력일 KOFIA 보충, 보충 시 Discord
- 07:10 시세 DAG FUND_NAV 경로는 `price_ingest.fund_nav_enabled: false` 로 끔 (관찰 후 삭제)

## 2026-11-25 — Upbit 가격 KST 자정 종가 통일

- 기존: asset_daily = T 07:00 현재가(미마감 일봉 trade_price), product_price_daily = 09:00 마감 공식 일봉 → 정의 불일치
- 변경: 기준일 d 가격 = d+1 00:00 KST 직전 체결가 (60분봉), 잔고·시세 공통, source 라벨 `UPBIT_DAY` 유지
- 경로: 잔고 `get_daily_candles` → `get_kst_closes`, 시세 `/v1/candles/days` → `/v1/candles/minutes/60` + `kst_day_ohlc` (KST 일별 집계)
- 시세 지연 판정의 CRYPTO +1일 허용 제거 (07:10 에 T-1 이미 마감)
