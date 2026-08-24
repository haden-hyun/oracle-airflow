# Handoff — 수동 자산(IRP·적금·주택청약) 일별 적재 설계

- 작성일: 2026-08-24
- 대상 DAG: `fetch_asset_daily`, `fetch_fund_price_daily`, 신규 2개
- 대상 테이블: `account.asset_daily`, 신규 `account.manual_position_ledger`
- 상태: **설계 확정 / 구현 대기**

---

## 0. 배경과 결론

KIS·Upbit API로 커버되지 않는 자산 세 가지(퇴직연금 IRP, 적금, 주택청약)를
`account.asset_daily`에 매일 적재해야 한다. 투자·자동이체가 급여일 중심이라
포지션 변동은 사실상 월 1회다.

최초 안은 **자산별 수동 DAG 3개 + forward-fill 백필**이었으나 다음 이유로 기각했다.

| 최초 안 | 문제 |
|---|---|
| 수동 DAG 3개 | 정본이 Airflow `dag_run.conf`가 된다 — 조회·정정·감사 불가, 메타DB 정리 대상 |
| | 스케줄·의존성·실패격리가 셋 다 동일. 다른 건 `asset_type` 문자열 하나뿐 |
| forward-fill 백필 | 하루 결측 시 캐리 체인 단절. 소급 정정 시 전 구간 수동 재실행 |

**확정 결론: 수동인 것은 데이터이지 파이프라인이 아니다.**
세 자산은 서로 다른 문제가 아니라 **"포지션 원장 × 일별 가격 함수"** 라는 하나의 문제다.

| 자산 | 수동 입력 (월 1회) | 매일 파생 |
|---|---|---|
| IRP | 좌수, 매입금액 | 좌수 × NAVᵗ × 0.001 |
| 적금 | 납입원금 누계 | 금액 캐리 (CMA 패턴) |
| 청약 | 납입원금 누계 | 금액 캐리 (CMA 패턴) |

신규 DAG는 3개가 아니라 **2개**(수동 입력 1 + 백필 1)이고,
나머지는 `fetch_asset_daily`에 계좌당 get/upload 페어 3쌍을 추가한다.

---

## 1. IRP 계산 방향 — 연금저축과 반대다

"연금저축처럼 매입금액·평가금액만 입력하면 된다"는 판단에는
**연금저축에서 좌수 역산이 성립하는 전제**가 빠져 있다.

```python
# plugins/asset_flow/transformers/kis_transformer.py:165
holding_quantity = total_evaluation_amount / fund_price / 0.001
```

이게 성립하는 이유는 KIS API가 **매일** 새 평가금액을 주기 때문이다.
매일 (평가금액ᴰ, NAVᴰ) 쌍이 들어오고, 좌수는 그 쌍의 파생 결과이지 입력이 아니다.

IRP는 그 쌍이 월 1회뿐이다. 나머지 29일은 평가금액이 없다.

| | 연금저축 (API) | IRP (수동) |
|---|---|---|
| 입력 | 평가금액ᴰ (매일) | 평가금액ᴰ (월 1회) |
| 매일 있는 것 | 평가금액 + NAV | **NAV뿐** |
| 계산 방향 | 평가금액 ÷ NAV → 좌수 | 좌수 × NAV → **평가금액** |
| 좌수의 위치 | 파생 결과 | **저장해야 할 상태** |

입력한 평가금액을 그대로 쓰면 한 달간 같은 숫자를 복사하는 것이 된다.
NAV가 매일 움직이는데 IRP만 고정값이면 일별 수익률이 왜곡되고,
매달 입력일마다 한 달치 변동이 계단으로 튄다.

### 앱 실측값 검증 (2026-08-24)

```
보유수량   1,023,351 좌
납입원금   1,750,060 원
평가금액   1,793,812 원

역산 NAV  = 1,793,812 / 1,023,351 / 0.001 = 1,752.88
매입단가  = 1,750,060 / 1,023,351 / 0.001 = 1,710.13
수익률    = (1,793,812 - 1,750,060) / 1,750,060 = 2.50%
```

역산 NAV가 펀드 기준가로 자연스럽고 수익률도 정확히 떨어진다.
**보유수량 = 좌수, multiplier = 0.001** — 연금저축과 동일 체계임이 확인됐다.

**앱이 보유수량을 직접 표시하므로 역산조차 불필요하다.** 좌수를 그대로 입력한다.
평가금액은 저장하지 않고 **입력 검증용으로만** 쓴다(§3 참조).

### IRP 전제 조건

- IRP는 **펀드만 매수**한다 → `asset_type='CASH'` 라인 불필요
- 따라서 `계좌 납입원금 = 펀드 매입금액` → 화면의 어느 값을 읽어도 같다
- **단, 이 등식은 결제 완료 시점에만 성립한다.** 부담금 입금 후 결제 전
  3영업일 구간에는 현금이 존재한다. "결제 완료 후 입력" 방침이
  이 구간을 회피하는 전제이므로 반드시 유지할 것

---

## 2. 신규 테이블 — `account.manual_position_ledger`

수동 자산의 **정본**. `asset_daily`는 계속 일별 스냅샷 팩트 테이블로 두고,
원장은 그 위의 입력 계층이다.

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `ledger_id` | serial PK | |
| `effective_date` | date NOT NULL | **효력일**. 이 날부터 이 포지션이 유효 |
| `account_code` | text NOT NULL | `IRP-xxxx` / `SAVINGS-xxxx` / `HOUSING-xxxx` |
| `account_name` | text | 퇴직연금IRP / 적금 / 주택청약 |
| `product_code` | text NOT NULL | IRP: 펀드 표준코드 / 적금·청약: 상품코드 |
| `product_name` | text | |
| `asset_type` | text NOT NULL | `FUND` / `SAVINGS` / `HOUSING` |
| `holding_quantity` | numeric NOT NULL | IRP: 좌수 / 적금·청약: 1 |
| `total_purchase_amount` | numeric NOT NULL | IRP: 매입금액 / 적금·청약: 납입원금 누계 |
| `multiplier` | numeric NOT NULL | IRP: 0.001 / 적금·청약: 1 |
| `currency_code` | text | `KRW` |
| `exchange_code` | text | IRP: `KRX` / 적금·청약: NULL |
| `price_date` | date NULL | **IRP 전용** — 입력한 평가금액의 NAV 기준일 |
| `status` | text NOT NULL | `ACTIVE` / `CLOSED` (만기·해지) |
| `memo` | text | |
| `insert_datetime` | timestamp DEFAULT now() | |

**제약**: `UNIQUE (account_code, product_code, effective_date)`

### 설계 원칙

1. **Append-only + 효력일** — 값이 바뀌면 UPDATE가 아니라 새 효력일 행.
   과거 상태가 보존되므로 "3개월 전 좌수"가 언제나 조회된다
2. **같은 효력일 재입력은 UPSERT** (`ON CONFLICT DO UPDATE`) — 오타 정정 경로.
   `asset_daily`의 DELETE+INSERT 멱등성과 같은 사고방식
3. **조회 규칙** — 기준일 D의 포지션 = `effective_date <= D` 중 계좌·상품별 최신 1행

```sql
SELECT DISTINCT ON (account_code, product_code) *
  FROM account.manual_position_ledger
 WHERE effective_date <= :standard_date AND status = 'ACTIVE'
 ORDER BY account_code, product_code, effective_date DESC
```

이 쿼리를 `db_manager.get_manual_positions(engine, standard_date)`로 두면
파생 태스크 3개가 공유한다.

---

## 3. 수동 입력 DAG — `upsert_manual_position` (신규 1개)

`schedule=None`, `catchup=False`. 태스크는 `validate` → `upsert` 둘.

### 입력 Params

| Param | 타입 | 예시 | 비고 |
|---|---|---|---|
| `asset_type` | enum | `IRP` / `SAVINGS` / `HOUSING` | 이후 분기를 결정 |
| `effective_date` | date | `2026-08-24` | IRP는 **결제 완료일** |
| `holding_quantity` | number | IRP `1023351` / 적금·청약 `1` | |
| `total_purchase_amount` | number | `1750060` | IRP 매입금액 / 적금·청약 납입원금 누계 |
| `evaluation_amount` | number, nullable | `1793812` | **IRP 전용, 저장 안 함 — 검증용** |
| `price_date` | date, nullable | | IRP 전용. 비우면 자동 탐색 |
| `status` | enum | `ACTIVE` / `CLOSED` | 만기·해지 시 `CLOSED` |
| `memo` | string, nullable | | |

`account_code` / `account_name` / `product_code` / `product_name` / `multiplier` /
`exchange_code`는 **입력받지 않는다.** `asset_type`별 상수이므로 Airflow Variable
(`MANUAL_IRP`, `MANUAL_SAVINGS`, `MANUAL_HOUSING`)에 두고 조회한다.
기존 `KIS_STOCK` / `KIS_ISA` Variable 패턴과 동일하고,
매달 계좌번호를 손으로 치다 오타 내는 경로가 사라진다.

### `validate` 태스크 (IRP)

```
역산 NAV = evaluation_amount / holding_quantity / 0.001
        = 1,793,812 / 1,023,351 / 0.001 = 1,752.88

→ market.fund_price_daily 에서 product_code 의 기준가 중
  1,752.88 과 일치하는 standard_date 를 찾아 price_date 확정
→ 못 찾으면 예외: 펀드코드 오류 또는 크롤링 누락
→ 오차 0.5원 초과면 예외: 좌수·평가금액 입력 오류
```

평가금액을 저장하지 않으면서 입력 검증과 `price_date` 자동 확정을 동시에 얻는다.
IRP 설계에서 가장 위험한 **"기준일을 잘못 잡으면 오차가 전 기간에 곱해지는"**
문제가 여기서 차단된다. 앱 화면의 평가금액이 며칠자 NAV 기준인지 추측하지 않는다.

적금·청약은 검증할 것이 없어 이 태스크를 통과한다.

### 실수 방지 규칙

- `total_purchase_amount`가 직전 원장값보다 **감소**하면 경고
  (적금·청약 납입원금 누계는 단조증가여야 한다)
- IRP 좌수가 감소하면 매도 여부를 `memo`에 요구

---

## 4. `fetch_asset_daily` 확장 — 원장을 읽어 매일 계산

기존 불변식 **한 `account_code` = 한 get/upload 페어**를 그대로 지켜 3쌍을 추가한다.

| 신규 태스크 | 읽는 것 | 계산 |
|---|---|---|
| `get_irp_assets` | 원장 + `fund_price_daily` | 좌수 × NAVᵗ × 0.001 |
| `get_savings_assets` | 원장 | 원금 캐리 |
| `get_housing_assets` | 원장 | 원금 캐리 |

`upload_asset_group`에 `upload_irp_assets` / `upload_savings_assets` /
`upload_housing_assets` 3개를 `upload_account.override(...)`로 추가한다.
**`upload_account` 함수 자체는 수정 불필요** — 이미 `{account_code, records}`
계약이라 그대로 재사용된다.

### 계산식 → `BalanceRecord`

**IRP**
```
unit_market_price       = NAVᵗ                    (fund_price_daily, T-1)
total_evaluation_amount = 좌수 × NAVᵗ × 0.001
unit_purchase_price     = 매입금액 ÷ 좌수 ÷ 0.001   = 1,710.13
total_purchase_amount   = 원장값 그대로             = 1,750,060
total_profit_amount     = 평가 − 매입
valuation_profit_rate   = 손익 ÷ 매입 × 100
multiplier=0.001, asset_type='FUND', currency_code='KRW', exchange_code='KRX'
```

**적금·청약**
```
holding_quantity        = 1
unit_market_price       = 원금
unit_purchase_price     = 원금
total_evaluation_amount = 원금
total_purchase_amount   = 원금
total_profit_amount     = 0      (이자는 만기에 계산되므로 미반영)
valuation_profit_rate   = 0
multiplier=1, currency_code='KRW'
```

적금·청약은 `transform_cma_cash_balance`가 이미 하는 처리
(수량 개념 없음 → `holding_quantity=1`, `unit_*_price=평가금액`)와 동일하다.

### 배치 위치

변환 로직은 DB 뷰가 아니라
`plugins/asset_flow/transformers/ledger_transformer.py` 신규 모듈에 둔다.
`kis_transformer` / `upbit_transformer`와 대칭이고 단위 테스트가 가능하다.

### 반드시 지킬 것

- **`get_X`는 원장 조회 0건이면 예외.** 빈 DataFrame을 반환하면 `upload_account`가
  DELETE만 수행해 기존 적재분이 사라진다 (모듈 docstring [빈 응답] 규칙)
- **IRP는 `wait_for_fund_price_daily` 뒤에.** 센서가 이미 있고 `get_standard_date`로
  체인되므로 추가 배선은 불필요
- **`asset_daily`에 `source` 컬럼**(`API` / `LEDGER`) 추가 권고 —
  값이 이상할 때 원인 계층을 즉시 좁힌다
- **원장은 `asset_daily`를 직접 쓰지 않는다.** 원장에 쓰는 주체와 `asset_daily`에
  쓰는 주체를 분리해야 계좌당 단일 writer 불변식이 유지된다

---

## 5. 백필 DAG — `rebuild_derived_assets` (신규 1개)

`schedule=None`, params `start_date` / `end_date`.
날짜를 순회하며 파생 3계좌만 재계산해 `delete_and_insert_account_assets`로 재적재한다.

원장 한 줄을 정정하면(예: `price_date` 오류로 좌수가 틀렸던 경우)
이 DAG로 영향 구간 전체가 복구된다. 백필이 순수 함수 `f(원장, 날짜) → 행`이므로
몇 번을 돌려도 같은 결과가 나온다.

`fetch_asset_daily`는 `catchup=False`에 T-1 하드코딩이고 KIS·Upbit는 과거 잔고
조회가 불가능하다. **백필 가능한 자산과 불가능한 자산의 경계를 DAG 단위로
못 박아 두는 것**이 이 DAG의 진짜 목적이다.

---

## 6. 구현 순서

| # | 작업 | 검증 |
|---|---|---|
| 1 | `fetch_fund_price_daily`의 `fund_codes`에 IRP 펀드코드 추가 | 크롤링 NAV ≈ 1,752.88 |
| 2 | `account.manual_position_ledger` 테이블 생성 | |
| 3 | `db_manager.get_manual_positions()` 추가 | |
| 4 | `upsert_manual_position` DAG + Variable 3개 | IRP 1행 입력 → `validate` 통과 |
| 5 | `ledger_transformer.py` | 단위 테스트 |
| 6 | `fetch_asset_daily`에 get/upload 3쌍 | 하루 적재 후 `asset_daily` 확인 |
| 7 | `rebuild_derived_assets` | 원장 효력일 이후 구간 백필 |

1번을 먼저 하는 이유는, IRP 펀드의 NAV가 며칠 쌓여 있어야 4번의 `validate`가
`price_date`를 탐색할 수 있기 때문이다.
**1번과 2~3번을 먼저 배포하고 며칠 뒤 나머지를 올리는 것이 안전하다.**

---

## 7. 미결 사항

### NAV 결측 시 IRP 평가 정책 (6번 착수 전 결정)

주말·공휴일에는 `market.fund_price_daily`에 T-1 행이 없다.
기존 `get_pension_assets`는 `get_fund_price`가 `(None, None)`을 반환하면
좌수를 0으로 만든다(`kis_transformer.py:167`). 그러나 IRP는 좌수가 원장에서 오므로
**직전 영업일 NAV로 캐리**하는 것이 맞다.

연금저축이 휴일에 실제로 어떻게 적재되고 있는지 데이터를 확인한 뒤 정하고,
두 계좌에 **동일하게** 적용할 것. 여기서 동작이 갈리면 이후 디버깅이 혼란스러워진다.

### T+3 결제 구간 자산 공백 (수용하기로 결정)

이체일 ~ 결제일 3영업일 동안 그 돈은 출금 계좌에도 IRP 좌수에도 잡히지 않는다.
월 1회·3영업일·신규 납입액 한정이라 영향은 작으나,
**매달 같은 시기에 자산 곡선이 움푹 패이는 패턴**으로 나타난다.
정확히 잡으려면 이체일에 `asset_type='CASH'` 원장 행을 넣고 결제일에 종료해야 하지만
입력이 두 번이 되므로 채택하지 않았다.
몇 달 뒤 "매달 25일쯤 자산이 왜 줄지?"를 다시 조사하지 않도록 이 문단을 근거로 남긴다.
