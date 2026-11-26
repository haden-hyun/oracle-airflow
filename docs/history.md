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
- 원인: EUR(FX@EUR) 만 `rt_cd=1 OPSQ0008 호출 후처리(MCI전송) 오류` (같은 아침 배당·지수에도 간헐 발생) → `get_exchange_rate` 가 rt_cd 미검사로 fetch 성공 처리 → transform KeyError. transform 재시도는 같은 XCom 재변환이라 무의미
- 조치: `get_exchange_rate` 가 rt_cd≠0 → `KisApiError`(msg_cd·msg1), output1 없음 → `RuntimeError` → fetch 재시도(5회, 최소 30·60·120·120·120초 — 11-19 오류가 약 6분 지속돼 07:05 전후까지 버티도록)가 재조회. transform `retries=0`

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
- 운영 반영 (11-24 23시대)
  - ppd 백업 `market.product_price_daily_upbit_bak_20261125` 808행 → 시세 DAG 수동 run(REST, logical_date 11-22 12:00 — 11-23 asset_daily 결측으로 센서 우회)으로 BTC·ETH 2025-10-13~11-23 407행씩 재작성
  - `scripts/oneoff/2026-11-25_upbit_kst_reprice.sql`: CRYPTO 590행 재평가(수량·매입 불변, 평가액 평균 −0.04%, 일별 −12.9~+10.4%) + UPBIT 11-18·22·23 9행 복구, 백업 `account.asset_daily_crypto_bak_20261125`
  - 11-22·23 은 다음 정상일 행이 없어 현재 잔고 = 11-21 을 API 로 확인하고 `asset_fix.upbit_live_verified` 로 적용

## 2026-11-25 — KIS asset_daily 빈 날 복구 (11-18·22·23)

- `scripts/oneoff/2026-11-25_kis_asset_daily_fill.sql`: 5계좌 88행, 수량 이어쓰기 × 그날 가격
  - 11-18 = 11-17 (11-19 와 보유 동일), 11-22·23 = 11-21 (현재 잔고와 동일, 11월 체결 0건)
  - 가격: 주식 = 그날 이하 마지막 종가 (정상 적재 4일 STOCK 전 행으로 규칙 검증), 펀드 = 그날 기준가 × 좌수 (KIS 평가액과 ±1원)
  - 연금 펀드 현재 잔고 좌수 차이는 CTRP6548R 이 11-24 기준가로 평가한 착시 (비율 = NAV 11-24/11-23)
- 제외 (사용자 결정): 중개형ISA 11-18 예수금(48,291원 또는 0원 불명), CMA 11-22·23 (11-21 0원 → 11-24 330,023원, 입금일 불명)
- 선행: 시세 KR·US 11-21~23 수동 run, 지수 `market_index` 그룹 수동 run (11-23 까지)
- KIS 11-22·23 행은 다음 정상일이 없어 `asset_fix.kis_live_verified` 로 적용 → `dashboard.refresh_views()`
- 백업 2개는 2026-12-09 이후 DROP

## 2026-11-26 — CMA 제외 · 청년미래적금 정정

- CMA: 입출금 경유 계좌(RP 아님)라 투자 자산에서 제외
  - secrets `KIS_CMA` `enabled: false` (다음 정기 run 부터 미수집, 키는 ISA 와 공유라 그대로)
  - `scripts/oneoff/2026-11-26_cma_remove.sql`: asset_daily CMA 177행 삭제, 백업 `account.asset_daily_cma_bak_20261126` (2026-12-10 이후 DROP)
  - 대시보드 저장소의 `LIQUIDITY` 범위 CMA 매핑은 그대로 (빈 범위)
- 청년미래적금 (수기 원장 DAG): 11-20 입금 50만 원 취소(누계 230만 → 180만, `allow_decrease`) · 11-23 20만 원 입금(누계 200만), 11-20~25 재계산

## 2026-11-26 — fundguide 대체·대조 제거 (KOFIA 단일 소스)

- 근거
  - 10-16~11-25 `compare_with_fundguide` 39회 성공(skip 7·상류 실패 1)·불일치 0 (README 의 1~2주 검증 조건 충족)
  - fundguide robots.txt `Disallow: /`
  - KOFIA 결측은 다음 날 06:55 `fill_nav_gaps` 가 자동 보충 (11-24 도입), T-1 누락은 `check_fund_coverage` 실패 알림
- 삭제: `compare_with_fundguide` 태스크 · `collect_fund_prices` 크롤러 대체 경로 · `plugins/asset_flow/crawler/` · `compare_sources` · `SOURCE_FUNDGUIDE` · config `funds.crawler_fallback`·`shadow_compare` · Playwright(requirements·Dockerfile Chromium 단계)
- 보충 알림 재계산 안내: min~max 한 범위 → 연속 구간별 (최근 5구간 + 외 n구간) — 흩어진 빈 날에 1년 전체 재계산을 안내하지 않게
- 배포: plugins·config 같은 커밋 → 재시작, 이미지 재빌드 (Playwright 제거)
