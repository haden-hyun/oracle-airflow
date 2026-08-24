# Handoff — 수동 자산(IRP·DC·적금·주택청약) 일별 적재 설계

- 작성일: 2026-08-24
- 대상 DAG: `fetch_asset_daily`, `fetch_fund_price_daily`, 신규 2개 + 1회성 스크립트 1개(§8)
- 대상 테이블: `account.asset_daily`, `market.fund_price_daily`, 신규 `account.manual_position_ledger`
- 상태: **설계 확정 / 구현 대기** (§6의 1·1b 완료)
- 범위: 일별 적재 설계(§1~§7) + 2026-01-28까지 과거 소급 적재(§8)

---

## 0. 배경과 결론

KIS·Upbit API로 커버되지 않는 자산 네 가지(퇴직연금 IRP·DC, 적금, 주택청약)를
`account.asset_daily`에 매일 적재해야 한다. 투자·자동이체가 급여일 중심이라
포지션 변동은 사실상 월 1회다(DC만 회사 납입 주기를 따른다).

최초 안은 **자산별 수동 DAG 3개 + forward-fill 백필**이었으나 다음 이유로 기각했다.

| 최초 안 | 문제 |
|---|---|
| 수동 DAG 3개 | 정본이 Airflow `dag_run.conf`가 된다 — 조회·정정·감사 불가, 메타DB 정리 대상 |
| | 스케줄·의존성·실패격리가 셋 다 동일. 다른 건 `asset_type` 문자열 하나뿐 |
| forward-fill 백필 | 하루 결측 시 캐리 체인 단절. 소급 정정 시 전 구간 수동 재실행 |

**확정 결론: 수동인 것은 데이터이지 파이프라인이 아니다.**
이 자산들은 서로 다른 문제가 아니라 **"포지션 원장 × 일별 가격 함수"** 라는 하나의 문제다.

| 자산 | 수동 입력 | 매일 파생 |
|---|---|---|
| IRP | 좌수, 매입금액 | 좌수 × NAVᵗ × 0.001 |
| DC | 좌수, 매입금액 | 좌수 × NAVᵗ × 0.001 (IRP와 동일) |
| 적금 | 납입원금 누계 | 금액 캐리 (CMA 패턴) |
| 청약 | 납입원금 누계 | 금액 캐리 (CMA 패턴) |

신규 DAG는 자산 수와 무관하게 **2개**(수동 입력 1 + 백필 1)이고,
나머지는 `fetch_asset_daily`에 계좌당 get/upload 페어 4쌍을 추가한다.

### DC 추가가 이 설계의 검증 사례다

DC(확정기여형 퇴직연금)는 설계 확정 후에 추가됐는데,
**신규 DAG 0개·신규 컬럼 0개·`ledger_transformer` 수정 0줄**로 끝났다.
필요한 것은 `account_master` 1행, Param enum 1값, get/upload 페어 1쌍뿐이다.
DC 펀드가 저축IRP와 **같은 `K55105D43299`** 라서 `fund_codes` 변경조차 없다.

자산 유형이 늘어날 때 파이프라인이 아니라 데이터만 늘어난다는 것이 §0 결론의 요지다.

---

## 1. IRP·DC 계산 방향 — 연금저축과 반대다

이 절의 논리는 IRP와 DC에 그대로 적용된다. 둘 다 좌수를 수동 입력받아
같은 펀드(`K55105D43299`)의 NAV로 매일 평가하므로, 이하 "IRP"는 DC를 포함해 읽는다.
차이는 §1 끝의 "DC가 IRP와 다른 점" 두 가지뿐이다.


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

### DC가 IRP와 다른 점

계산식·검증·백필은 완전히 동일하다. 운영상 차이만 둘이다.

- **납입 주체가 회사다.** 사용자 출금 계좌에서 나가는 돈이 아니므로
  §7의 T+3 논의가 아예 무관하다. 없던 자산이 새로 생기는 것이다
- **납입 주기가 회사 정책을 따른다.** 월 1회가 아니라 분기·연 1회일 수 있다.
  입력 주기가 불규칙해도 원장 설계상 문제없다 — `standard_date` 기준
  최신 1행을 집어오므로 다음 입력까지 직전 좌수가 계속 유효하다
- 퇴직 시 전액 IRP로 이전된다. 그때 `account_master.is_active=false`로 종료한다

---

## 2. 신규 테이블 — `account.manual_position_ledger`

수동 자산의 **정본**. `asset_daily`는 계속 일별 스냅샷 팩트 테이블로 두고,
원장은 그 위의 입력 계층이다.

**원칙: 사람만 알 수 있는 값만 저장한다.** 나머지는 조인하거나 계산한다.

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `standard_date` | date NOT NULL | **효력 기준일**. 이 날부터 이 포지션이 유효. IRP는 **결제 완료일** |
| `account_code` | text NOT NULL | `43904978-29`(IRP) / `266-021-430970`(DC) / `230-389-0337643`(적금) / `339-2472-2043-41`(청약) |
| `product_code` | text NULL | IRP·DC 펀드 표준코드(둘 다 `K55105D43299`). 적금·청약 NULL |
| `holding_quantity` | numeric NULL | IRP·DC 좌수. 적금·청약 NULL (변환 시 1) |
| `total_purchase_amount` | numeric NOT NULL | IRP·DC 매입금액 / 적금·청약 납입원금 누계 |
| `insert_datetime` | timestamp DEFAULT now() | 소급 입력 여부 판별용 |

**PK**: `(standard_date, account_code)`

`asset_daily`의 멱등 적재 스코프가 `(standard_date, account_code)`이고
(`db_manager.py:69`) 모든 transformer가 `standard_date`를 쓰므로 원장도 같은 규약을 따른다.
**한 계좌 = 한 상품**을 전제하므로 `product_code`는 PK에 넣지 않는다.
IRP·DC가 2개 이상 펀드를 담는 날이 오면 그때 PK를 확장한다 — 지금 넣으면
적금·청약에 의미 없는 NOT NULL 값을 만들어야 한다.

IRP와 DC가 **같은 `product_code`를 공유**하는 것은 문제되지 않는다.
원장 조회도 `asset_daily` 적재도 `account_code` 단위로 분리되고,
`get_fund_price`는 두 계좌에 같은 NAV를 반환하면 그만이다.

### 제거한 컬럼과 그 값의 출처

| 제거 | 대신 어디서 오나 |
|---|---|
| `ledger_id` | 참조하는 테이블 없음. 자연키로 충분 |
| `account_name` / `product_name` | `account_master` / Airflow Variable (조회 시 조인) |
| `asset_type` | **`account_master.account_type`** — `IRP` / `DC` / `INSTALLMENT_SAVINGS` / `HOUSING_SUBSCRIPTION`이 이미 있다 |
| `multiplier` / `currency_code` / `exchange_code` | `account_type`별 상수. `ledger_transformer`가 채운다 (`kis_transformer`가 `0.001`을 하드코딩하는 것과 동일) |
| `price_date` | **불필요해졌다** — 아래 참조 |
| `status` | **`account_master.is_active`** — 계좌 생명주기는 계좌 테이블의 책임이다 |
| `memo` | 시스템이 읽지 않는 자유 텍스트 |

`asset_type`을 지우면서 §4의 vocabulary 매핑 문제도 같이 사라진다.
분기 기준이 `account_master.account_type` 하나로 단일화되기 때문이다.

### `price_date`가 사라진 이유

당초 `price_date`는 "앱 화면의 평가금액이 며칠자 NAV 기준인지"를 역산해 못 박는 컬럼이었다.
그러나 **평가금액을 애초에 저장하지 않기로 한 이상**(§1) 이 질문 자체가 성립하지 않는다.

매일의 계산은 `좌수 × 그날의 NAV`다. 좌수는 원장에서, NAV는 `fund_price_daily`에서 온다.
**과거 어느 날의 NAV도 필요하지 않다.** 결제일에 좌수를 등록하면 그 날부터 매일
그 날짜의 NAV가 적용되므로, 기준일 정합성은 저절로 맞는다.

`price_date`는 저장하지 않는 값(평가금액)을 검증하기 위해 저장하는 컬럼이었다 — 순환이다.
검증 자체는 §3에 태스크 로직으로 남기고, 그 결과는 테이블에 남기지 않는다.

### 설계 원칙

1. **Append-only + 효력 기준일** — 값이 바뀌면 UPDATE가 아니라 새 `standard_date` 행.
   과거 상태가 보존되므로 "3개월 전 좌수"가 언제나 조회된다
2. **같은 기준일 재입력은 UPSERT** (`ON CONFLICT DO UPDATE`) — 오타 정정 경로.
   충돌 타깃이 곧 PK다. `asset_daily`의 DELETE+INSERT 멱등성과 같은 사고방식
3. **조회 규칙** — 기준일 D의 포지션 = `standard_date <= D` 중 계좌별 최신 1행

원장의 `standard_date`는 `asset_daily`의 그것과 의미가 다르다.
`asset_daily`는 **그 날의 스냅샷**이고, 원장은 **그 날부터 유효한 상태**다.
그래서 원장은 매일 행이 생기지 않고(월 1회), 아래 쿼리로 날짜를 채워 넣는다.

### `db_manager.get_manual_positions(engine, standard_date)`

**하는 일**: 기준일 D 시점에 유효한 수동 자산 포지션을 계좌별 1행씩(현재 4계좌) DataFrame으로 반환한다.

```sql
SELECT DISTINCT ON (l.account_code)
       l.standard_date, l.account_code, l.product_code,
       l.holding_quantity, l.total_purchase_amount,
       m.account_name, m.account_type
  FROM account.manual_position_ledger l
  JOIN account.account_master m USING (account_code)
 WHERE l.standard_date <= :standard_date
   AND m.is_active
 ORDER BY l.account_code, l.standard_date DESC
```

**왜 함수로 빼는가** — `get_irp_assets` / `get_dc_assets` /
`get_savings_assets` / `get_housing_assets` 4개 태스크가 같은 조회를 한다. `DISTINCT ON` + `<=` + `is_active` 조합은
한 곳만 틀려도 조용히 다른 날짜의 포지션을 집어오므로, 세 번 복붙할 성질의 쿼리가 아니다.
`get_fund_price(engine, product_code, standard_date)`(`db_manager.py:36`)와 같은 위치·같은 역할이다.

**계약**: 조회 0건이면 빈 DataFrame을 반환한다. 예외는 호출자(`get_X`)가 던진다(§4 [빈 응답]).

---

## 3. 수동 입력 DAG — `upsert_manual_position` (신규 1개)

`schedule=None`, `catchup=False`. 태스크는 `validate` → `upsert` 둘.

### 입력 Params

**8개 → 4개.** 사람이 앱 화면에서 눈으로 읽어야만 알 수 있는 값만 남긴다.

| Param | 타입 | 기본값 | 설명 |
|---|---|---|---|
| `account_code` | enum(4) | — | 계좌 선택. 이 하나로 `account_type`·펀드코드·상수가 모두 결정된다 |
| `standard_date` | date | 오늘 | IRP·DC는 **결제 완료일**, 적금·청약은 납입일 |
| `holding_quantity` | number, nullable | — | **IRP·DC만.** 앱의 "보유수량" |
| `total_purchase_amount` | number | — | IRP·DC 매입금액 / 적금·청약 납입원금 누계 |
| `evaluation_amount` | number, nullable | — | **IRP·DC만. 저장 안 함 — 검증 전용**(아래) |

`asset_type` / `status` / `memo` / `price_date` params는 삭제했다.
`asset_type`은 `account_code`로 `account_master`를 조회하면 나오고,
`status`는 `account_master.is_active`로 옮겼으며, 나머지 둘은 §2에서 컬럼째 사라졌다.

`product_code` / `multiplier` / `exchange_code`는 `account_type`별 상수라
Airflow Variable(`MANUAL_ASSET_CONFIG`)에 두고 조회한다. Variable을 계좌별로
쪼갤 이유가 없어졌다 — 계좌마다 다른 값이 `product_code` 하나뿐이고,
IRP와 DC는 그것마저 같다.

### 입력값 예시를 Param description에 박아둔다

월 1회 입력이라 **매번 "이 칸에 뭘 넣더라"를 다시 떠올려야 한다.**
Airflow `Param`은 `description`을 트리거 UI에 그대로 렌더하므로 여기에 실측 예시를 넣는다.

```python
params={
    'account_code': Param(
        '43904978-29',
        enum=['43904978-29', '266-021-430970',
              '230-389-0337643', '339-2472-2043-41'],
        description=(
            '43904978-29 = 저축IRP(한국투자증권) / '
            '266-021-430970 = 확정기여형DC(신한은행) / '
            '230-389-0337643 = 청년미래적금(신한) / '
            '339-2472-2043-41 = 청년주택드림청약(농협)'
        ),
    ),
    'holding_quantity': Param(
        None, type=['null', 'number'],
        description='[IRP·DC 전용] 앱 > 보유수량. 예) 1023351 (좌)',
    ),
    'total_purchase_amount': Param(
        None, type=['null', 'number'],
        description='IRP·DC: 앱 > 납입원금. 예) 1750060 / 적금·청약: 지금까지 넣은 원금 총액',
    ),
    'evaluation_amount': Param(
        None, type=['null', 'number'],
        description='[IRP·DC 전용·검증용] 앱 > 평가금액. 예) 1793812. 저장되지 않는다',
    ),
}
```

`fetch_fund_price_daily`의 `fund_codes` Param이 이미 `description`을 쓰고 있어
새로운 패턴이 아니다.

### `validate` 태스크 — 축소하되 없애지는 않는다

`price_date` 역산·탐색 로직은 §2와 함께 사라졌다. 남는 검증은 하나뿐이다.

```
IRP·DC: 좌수 × (standard_date의 NAV) × 0.001  ≈  evaluation_amount ?
        1,023,351 × 1,752.88 × 0.001 = 1,793,812  → 입력값과 일치

→ 오차 1% 초과면 예외 (좌수 자릿수 오타)
→ evaluation_amount 미입력이면 검증을 건너뛴다 (경고만)
→ NAV 행이 없으면(크롤링 실패) 검증을 건너뛴다 — §7 참조
```

`price_date`를 찾는 게 아니라 **이미 아는 날짜의 NAV로 입력값을 확인만** 한다.
IRP·DC 좌수는 한 번 틀리면 그 값이 이후 전 기간에 곱해지므로, 이 계좌들에서
자릿수 오타를 잡아줄 장치는 이것 하나뿐이다(§6 참조 — 단위 테스트도 없다).
`evaluation_amount` param을 남긴 유일한 이유다.

적금·청약은 검증할 것이 없어 통과한다. 다만 `total_purchase_amount`가
직전 원장값보다 **감소하면 경고**한다 — 납입원금 누계는 단조증가여야 한다.

---

## 4. `fetch_asset_daily` 확장 — 원장을 읽어 매일 계산

기존 불변식 **한 `account_code` = 한 get/upload 페어**를 그대로 지켜 4쌍을 추가한다.

| 신규 태스크 | 읽는 것 | 계산 |
|---|---|---|
| `get_irp_assets` | 원장 + `fund_price_daily` | 좌수 × NAVᵗ × 0.001 |
| `get_dc_assets` | 원장 + `fund_price_daily` | 좌수 × NAVᵗ × 0.001 |
| `get_savings_assets` | 원장 | 원금 캐리 |
| `get_housing_assets` | 원장 | 원금 캐리 |

`get_irp_assets`와 `get_dc_assets`는 계산이 동일하므로 `ledger_transformer`
함수 하나를 공유하고 `account_code`만 다르게 넘긴다. **태스크를 합치지는 않는다** —
불변식이 계좌당 페어를 요구하고, 한쪽 실패가 다른 쪽 적재를 막지 않아야 한다.

`upload_asset_group`에 `upload_irp_assets` / `upload_dc_assets` /
`upload_savings_assets` / `upload_housing_assets` 4개를 `upload_account.override(...)`로
추가한다. **`upload_account` 함수 자체는 수정 불필요** — 이미 `{account_code, records}`
계약이라 그대로 재사용된다.

### 계산식 → `BalanceRecord`

**IRP·DC** (동일)
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
multiplier=1, asset_type='CASH', currency_code='KRW'
```

적금·청약은 `transform_cma_cash_balance`가 이미 하는 처리
(수량 개념 없음 → `holding_quantity=1`, `unit_*_price=평가금액`)와 동일하다.

### `account_type` → `asset_daily.asset_type` 매핑

원장이 `asset_type`을 저장하지 않으므로(§2) 분기 기준은 `account_master.account_type`이다.
`ledger_transformer`가 이를 `asset_daily`의 분류값으로 변환한다.

| `account_master.account_type` | → `asset_daily.asset_type` | multiplier | exchange_code |
|---|---|---|---|
| `IRP` | `FUND` | 0.001 | `KRX` |
| `DC` | `FUND` | 0.001 | `KRX` |
| `INSTALLMENT_SAVINGS` | `CASH` | 1 | NULL |
| `HOUSING_SUBSCRIPTION` | `CASH` | 1 | NULL |

적금·청약을 `CASH`로 매핑하는 이유는 계산이 CMA와 동일하기 때문이다.
`INSTALLMENT_SAVINGS`를 `asset_daily`에 그대로 흘려보내면 기존 집계 쿼리가 이 자산을 놓친다.
`currency_code`는 셋 다 `KRW` 상수다.

### 배치 위치

변환 로직은 DB 뷰가 아니라
`plugins/asset_flow/transformers/ledger_transformer.py` 신규 모듈에 둔다.
`kis_transformer` / `upbit_transformer`와 대칭이고, API 호출이 없는 순수 함수라
입력만 주면 결과를 눈으로 확인할 수 있다.

### 반드시 지킬 것

- **`get_X`는 원장 조회 0건이면 예외.** 빈 DataFrame을 반환하면 `upload_account`가
  DELETE만 수행해 기존 적재분이 사라진다 (모듈 docstring [빈 응답] 규칙)
- **IRP·DC는 `wait_for_fund_price_daily` 뒤에.** 센서가 이미 있고 `get_standard_date`로
  체인되므로 추가 배선은 불필요
- **`asset_daily`에 `source` 컬럼**(`API` / `LEDGER`) 추가 권고 —
  값이 이상할 때 원인 계층을 즉시 좁힌다
- **원장은 `asset_daily`를 직접 쓰지 않는다.** 원장에 쓰는 주체와 `asset_daily`에
  쓰는 주체를 분리해야 계좌당 단일 writer 불변식이 유지된다

---

## 5. 백필 DAG — `rebuild_derived_assets` (신규 1개)

`schedule=None`, params `start_date` / `end_date`.
날짜를 순회하며 파생 4계좌만 재계산해 `delete_and_insert_account_assets`로 재적재한다.

**하는 일**: `start_date`~`end_date` 각 날짜 D에 대해
`get_manual_positions(engine, D)` → `ledger_transformer` → `upload_account`를 반복한다.
`fetch_asset_daily`의 하루치 파생 로직을 날짜 루프로 감싼 것이고, 로직은 재사용한다.

**언제 쓰나** — 이게 없으면 각각 `asset_daily`에 직접 SQL을 쳐야 한다.

| 상황 | 왜 필요한가 |
|---|---|
| 좌수·원금 오타 정정 | 잘못된 값이 이미 며칠~몇 주 적재됨. 원장 UPSERT만으로는 과거 `asset_daily`가 안 고쳐진다 |
| 계좌 최초 등록 | 적금·청약·DC는 이미 몇 달~몇 년 납입됐다. 등록일 이후만 적재하면 자산 곡선이 그 날 갑자기 솟는다 |
| NAV 크롤링 누락 복구 | `fund_price_daily`에 구멍이 났다가 나중에 채워진 구간의 IRP·DC 재계산 |

백필이 순수 함수 `f(원장, 날짜) → 행`이므로 몇 번을 돌려도 같은 결과가 나온다.

`fetch_asset_daily`는 `catchup=False`에 T-1 하드코딩이고 KIS·Upbit는 과거 잔고
조회가 불가능하다. **백필 가능한 자산과 불가능한 자산의 경계를 DAG 단위로
못 박아 두는 것**이 이 DAG의 진짜 목적이다.

---

## 6. 구현 순서

| # | 작업 | 검증 |
|---|---|---|
| ~~1~~ | ~~`fetch_fund_price_daily`의 `fund_codes`에 IRP 펀드코드 추가~~ | **완료** — `K55105D43299` 수집 중 |
| ~~1b~~ | ~~적금·청약 `account_master` 등록~~ | **완료** |
| 1c | DC `account_master` 등록 (`266-021-430970`, `account_type='DC'`) | `fund_codes` 변경 불필요 — IRP와 같은 펀드 |
| 2 | `account.manual_position_ledger` 테이블 생성 | |
| 3 | `db_manager.get_manual_positions()` 추가 | IRP 1행 수동 INSERT 후 조회 |
| 4 | `upsert_manual_position` DAG + Variable 1개 | IRP 실측값 입력 → `validate` 통과 |
| 5 | `ledger_transformer.py` | 실측값 재현 (아래) |
| 6 | `fetch_asset_daily`에 get/upload 4쌍 | 하루 적재 후 `asset_daily` 확인 |
| 7 | `rebuild_derived_assets` | 적금·청약·DC 과거 구간 소급 적재 |

1번이 이미 끝났으므로 IRP·DC 펀드 NAV가 쌓이는 대기 시간은 없다.
1c와 2번부터 바로 시작한다.

### 단위 테스트 — 인프라를 새로 만들지는 않는다

현재 레포에 테스트 인프라가 없다. `requirements.txt`에 `pytest`가 없고
`tests/` 디렉터리도 없으며, `dags/test_slack_alert_dag.py`는 이름만 test일 뿐
Slack 알림 확인용 DAG다. **이 작업 하나를 위해 테스트 프레임워크를 도입하는 것은
과하다** — `kis_transformer` / `upbit_transformer`도 테스트 없이 운영 중이고,
`ledger_transformer`만 테스트가 있으면 규약이 어긋난다.

대신 **§1의 앱 실측값을 재현하는 1회성 검증**으로 대체한다. 5번 작업 시
아래를 직접 실행해 눈으로 확인하고, 결과를 이 문서에 기록한다.

```
입력: holding_quantity=1023351, total_purchase_amount=1750060, NAV=1752.88
기대: total_evaluation_amount = 1,793,812
      unit_purchase_price    = 1,710.13
      valuation_profit_rate  = 2.50
```

이 값이 맞으면 IRP 계산식은 검증된 것이다. 적금·청약은 항등 변환이라
`total_purchase_amount`가 그대로 나오는지만 보면 된다.

테스트 인프라는 **레포 전체 차원에서 별도로 판단할 일**이지 이 작업의 하위 항목이 아니다.
`ledger_transformer`를 순수 함수로 두는 것(§4 배치 위치)은 나중에 인프라가 생겼을 때
바로 테스트할 수 있게 하려는 것이고, 지금 당장 테스트를 쓰라는 뜻은 아니다.

---

## 7. 미결 사항

### ~~주말·공휴일 NAV 결측~~ — 해당 없음 (2026-08-24 확인)

당초 "휴일에는 `fund_price_daily`에 T-1 행이 없다"고 적었으나 **전제가 틀렸다.**
크롤러는 영업일 여부를 전혀 따지지 않는다.

- `fetch_fund_price_daily`는 `schedule='55 6 * * *'` — 주말·공휴일 포함 매일 실행
- `get_standard_date()`는 단순 `data_interval_end - 1일`
  (`fetch_fund_price_daily_dag.py:75`) — 영업일 보정이 없다
- `fetch_fund_quote()`는 fundguide.net이 **그 시점에 표시 중인 기준가**를 긁는다.
  휴일에도 사이트는 직전 영업일 NAV를 그대로 노출한다

일요일 06:55 실행 → `standard_date=토요일`, `standard_price=금요일 NAV`로 **행이 생성된다.**
캐리포워드가 구조적으로 이미 내장돼 있어 휴일 결측은 발생하지 않는다.

### NAV 행 결측 시 IRP·DC 평가 정책 (6번 착수 전 결정)

위 확인의 결과, 남는 진짜 리스크는 휴일이 아니라 **크롤링 실패**다.
Playwright 타임아웃·사이트 개편·펀드코드 오류 시 `crawl_fund_prices`가
해당 코드를 `[SKIP]`으로 건너뛰어(`fetch_fund_price_daily_dag.py`) 그 날 행이 아예 없다.

`kis_transformer.py:167`의 `(None, None)` → 좌수 0 분기는 **휴일 대응이 아니라
이 크롤링 실패 대응이다.** 원래 문단이 두 상황을 뭉뚱그린 것이 오류였다.

IRP·DC에는 좌수 0이 명백히 틀리다 — 좌수는 원장에서 오는 확정값이라
자산이 하루 0원으로 찍힐 이유가 없다. 두 갈래 중 **전자를 권한다.**

| 안 | 내용 | 평가 |
|---|---|---|
| A (권장) | `get_fund_price`를 `standard_date <= D ORDER BY DESC LIMIT 1`로 직전 유효 NAV 조회 | 크롤링 실패가 며칠 이어져도 적재가 계속된다 |
| B | 예외를 던져 재시도 | 실패가 지속되면 그동안 IRP·DC뿐 아니라 **그 날 전체 적재가 멈춘다** |

연금저축에도 동일하게 적용할지는 별도 판단이 필요하다. 연금저축은 좌수가
평가금액의 파생값이라 NAV가 없으면 계산 자체가 불가능하다 — 상황이 다르다.

### ~~T+3 결제 구간 자산 공백~~ — 발생하지 않음 (2026-08-24 정정)

당초 "이체일~결제일 3영업일 동안 자산 곡선이 움푹 패인다"고 적었으나 **틀렸다.**

**추적 대상 계좌 중 돈을 잃는 계좌가 없기 때문이다.** `account_master`에 있는 것은
한국투자증권 계좌·업비트·적금·청약·DC뿐이고, IRP 부담금이 빠져나가는 급여통장은
여기 없다. 적금·청약도 "납입원금 누계"로만 잡히지 입출금 계좌로 추적되지 않는다.

실제로 일어나는 일은 **자산 증가가 3영업일 늦게 반영되는 것**뿐이다.
없던 돈이 늦게 나타나는 것이라 곡선이 패이지 않고, 계단이 며칠 밀릴 뿐이다.
결제일 이후에 입력하므로 그 전까지는 직전 좌수가 그대로 유효하다(§2 조회 규칙).

DC는 회사가 납입하므로 이 논의 자체가 무관하다.

**재검토 조건**: 급여통장 등 입출금 계좌를 `account_master`에 추가하는 시점.
그때는 실제로 추적 계좌에서 돈이 빠져나가므로 공백이 생긴다.

---

## 8. 과거 구간 소급 적재 — 2026-01-28까지

§5의 `rebuild_derived_assets`를 **최초로 크게 쓰는 작업**이다.
목표는 4개 수동 계좌의 `asset_daily`를 2026-01-28부터 채우는 것이고,
선행 조건이 하나 있다.

**시작일 근거**: `asset_daily`의 실제 최소 `standard_date`가 **2026-01-28**이다(확인 완료).
KIS·업비트 계좌가 이 날부터 적재돼 있으므로 수동 계좌도 같은 날에 맞춘다.
**더 앞으로 당기지 않는다** — 수동 계좌만 존재하는 날짜가 생기면
그 구간의 자산 총계가 실제보다 작게 잡혀, 나중에 "1월 초에 자산이 왜 이것뿐이지?"를
다시 조사하게 된다. 전 계좌가 같은 날 시작하는 편이 총계 해석에 일관된다.

| 구간 | `fund_price_daily` | 원장 | 가능 여부 |
|---|---|---|---|
| 2026-05-23 ~ 현재 | 있음 | 입력하면 됨 | 바로 가능 |
| 2026-01-28 ~ 2026-05-22 | **없음** | 입력하면 됨 | **IRP·DC 불가** / 적금·청약 가능 |

적금·청약은 NAV가 필요 없으므로(원금 캐리) **8-2만으로 2026-01-28까지 즉시 채워진다.**
IRP·DC는 8-1이 끝나야 한다.

### 8-1. 펀드 기준가 과거 수집 (2026-01-28 ~ 2026-05-22)

**현재 크롤러로는 불가능하다.** 기존 방식을 그대로 과거에 적용할 수 없다.

```python
# plugins/asset_flow/crawler/fund_crawler.py:51
BASE_URL = "https://www.fundguide.net/Fund/SimpleSearch?search_key={code}"
```

이 페이지는 **조회 시점의 최신 기준가 한 건**만 노출한다. 날짜 파라미터가 없고,
`fetch_fund_quote()`는 화면에 떠 있는 값을 긁을 뿐이다.
`to_fund_price_record(raw, standard_date)`가 `standard_date`를 인자로 받는 것은
**레이블을 붙이는 것이지 그 날짜의 값을 가져오는 것이 아니다.**
과거 날짜를 넘기며 반복 호출하면 **전 구간에 오늘 NAV가 복사된다** — 조용히 틀린 데이터다.

따라서 별도 수집 경로가 필요하다.

| 안 | 방법 | 평가 |
|---|---|---|
| A (권장) | 금융투자협회 전자공시(`dis.kofia.or.kr`)에서 기간 조회 후 CSV 다운로드 → 변환 INSERT | 기준가 원천 공시처. 일자별 시계열을 한 번에 받는다 |
| B | fundguide의 기준가 추이/차트 엔드포인트를 찾아 크롤링 | 재사용 가능하나 엔드포인트 탐색·검증 비용. 실패하면 A로 회귀 |
| C | 운용사(펀드 판매사) 앱·홈페이지에서 수기 수집 | 약 100영업일치. 현실적이지 않다 |

**A안을 권장한다.** 이 수집은 **1회성**이므로 DAG가 아니라
`scripts/backfill_fund_price.py` 같은 일회용 스크립트로 처리하고,
끝나면 스크립트를 남기되 스케줄에 태우지 않는다.
`fetch_fund_price_daily`(매일·최신값)와 성격이 달라 같은 DAG에 넣으면
"이 DAG는 최신값을 긁는다"는 단순한 계약이 깨진다.

**반드시 지킬 것**

- 대상 펀드는 3개다 — `K553W5E17401`(연금저축), `K55105D43299`(IRP·DC 공용).
  IRP·DC가 같은 코드이므로 실제 수집 코드는 2개다
- `standard_date`는 **공시된 기준일 그대로** 넣는다. T-1 오프셋을 또 적용하면 안 된다.
  기존 DAG의 T-1은 "어제 값을 오늘 긁는다"는 실행 타이밍 보정이지 데이터 속성이 아니다
- 휴일 행 처리를 기존과 맞춘다. 크롤러는 휴일에도 직전 영업일 NAV로 행을 만든다(§7).
  공시 CSV는 영업일만 주므로 **휴일을 직전 영업일 값으로 채워 넣어야** 시계열이 일관된다.
  안 채우면 그 날 IRP·DC 적재가 §7의 "NAV 행 결측" 경로를 타게 된다
- 적재 전 `2026-05-23` 이후 구간과 **하루 겹쳐서 값을 대조**한다.
  공시값과 크롤링값이 다르면 그 차이의 원인을 먼저 밝힌다

### 8-2. 원장에 매수 이력 입력 — 적립식이라 가능하다

**4개 계좌 모두 적립식이므로 매수(납입) 이력만 있으면 원장이 완성된다.**
§2의 append-only + 효력 기준일 설계가 정확히 이 상황을 위한 것이다.
매수일마다 원장 1행을 넣으면, `get_manual_positions(D)`가
`standard_date <= D` 중 최신 1행을 집어 **매수와 매수 사이의 모든 날짜를 자동으로 메운다.**
forward-fill 로직을 따로 짤 필요가 없다(§0에서 기각한 이유가 이것이다).

```
원장 (매수 이력만)              →  asset_daily (매일)
2026-01-25  좌수 210,000          2026-01-25 ~ 02-24  좌수 210,000 × 그날 NAV
2026-02-25  좌수 421,300          2026-02-25 ~ 03-24  좌수 421,300 × 그날 NAV
2026-03-25  좌수 633,900          ...
```

**가장 틀리기 쉬운 지점: 원장은 누계다**

원장의 `holding_quantity` / `total_purchase_amount`는 **그 시점까지의 누계**이지
그 회차의 매수분이 아니다. 거래내역은 보통 회차별 금액으로 표시되므로
**입력 전에 누적합으로 변환해야 한다.**

| 매수일 | 회차 매수금액 | 원장 `total_purchase_amount` |
|---|---|---|
| 2026-01-25 | 350,000 | 350,000 |
| 2026-02-25 | 350,000 | **700,000** (350,000 아님) |
| 2026-03-25 | 350,000 | **1,050,000** |

좌수도 같다. 회차별 좌수가 아니라 그 시점 잔고 좌수를 넣는다.
이 변환을 놓치면 자산이 매달 초기화되는 그래프가 나오는데,
값이 그럴듯해 보여서 한참 뒤에야 발견된다.

**검증**: 마지막 행의 누계가 §1 앱 실측값(좌수 1,023,351 / 원금 1,750,060)과
일치하는지 확인한다. 일치하면 중간 회차도 맞다고 볼 수 있다.

**2026-01-28 이전 개설 계좌**

적금·청약·DC는 2026-01-28 이전부터 납입했을 수 있다. 이 경우
**2026-01-28 시점의 잔고를 첫 원장 행으로 넣는다**(`standard_date='2026-01-28'`).
그 이전 회차를 일일이 넣을 필요는 없다 — 백필 시작일이 2026-01-28이므로
그 날의 누계만 정확하면 이후가 전부 맞는다.
첫 행이 2026-01-28보다 늦으면 그 앞 구간은 `get_manual_positions`가 0건을 반환해
`asset_daily`에 행이 생기지 않고, 자산 곡선이 첫 행 날짜에 갑자기 솟는다.

**입력 경로**: 회차가 수십 건이므로 `upsert_manual_position` DAG를 회차마다
돌리는 것은 비현실적이다. **1회성 INSERT문으로 직접 넣는다.**
DAG는 앞으로의 월 1회 입력용이고, 과거 이력은 일괄 적재 대상이다.
단 `validate`를 건너뛰게 되므로 위 누계 검증을 반드시 수동으로 수행한다.

### 8-3. `rebuild_derived_assets` 실행

`start_date='2026-01-28'`, `end_date=어제`로 1회 실행한다.

| 순서 | 대상 | 선행 조건 |
|---|---|---|
| 1 | 적금·청약 | 8-2만 완료되면 됨 |
| 2 | IRP·DC | 8-1 + 8-2 완료 |

**분리 실행을 권한다.** 적금·청약은 NAV와 무관하므로 8-1을 기다릴 이유가 없고,
먼저 돌려서 `rebuild_derived_assets` 자체의 동작을 검증해두면
IRP·DC 백필 때 실패 원인이 NAV 쪽인지 DAG 쪽인지 바로 갈린다.

**안전성** — `delete_and_insert_account_assets`의 DELETE 스코프가
`(standard_date, account_code)`이므로(`db_manager.py:83`) KIS·업비트 계좌의
기존 적재분은 건드리지 않는다. 몇 번을 다시 돌려도 안전하다.

**검증**

- 백필 마지막 날의 IRP 값이 §1 실측값과 일치하는가
- 2026-01-28 ~ 어제 사이에 **행이 빠진 날짜가 없는가**
  (`GROUP BY standard_date` 후 계좌 수 확인)
- 자산 곡선에 계단이나 급락이 있는가 — 있다면 그 날짜의 원장 행을 의심한다

### 구현 순서에 추가

§6의 7번(`rebuild_derived_assets`) 완료 후 이어서 수행한다.

| # | 작업 | 비고 |
|---|---|---|
| 8 | 8-1 펀드 기준가 과거 수집 | IRP·DC 백필의 선행 조건. 8-2와 병렬 가능 |
| 9 | 8-2 원장 매수 이력 INSERT | 누계 변환 주의 |
| 10 | 8-3 적금·청약 백필 | 8-1 없이 가능 |
| 11 | 8-3 IRP·DC 백필 | 8·9 완료 후 |

### 미확인 사항

- **매수 이력 확보 가능 범위** — 4개 계좌 모두 2026-01-28까지 거래내역 조회가
  되는지 확인. 안 되는 계좌는 그 계좌의 백필 시작일이 뒤로 밀린다.
  이 경우 그 계좌만 시작일이 다르다는 사실을 이 문서에 기록할 것 —
  나중에 자산 곡선에서 그 계좌만 늦게 나타나는 이유를 다시 찾지 않도록 한다
