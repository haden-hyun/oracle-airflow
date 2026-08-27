# Handoff — 연금저축 계좌 분리(세액공제/비공제) 및 펀드 미보유 처리

- 작성일: 2026-08-27
- 대상 DAG: `fetch_asset_daily`, `make_token_dag`(간접)
- 대상 테이블: `account.asset_daily` (스키마 변경 없음)
- 상태: **구현 완료 / 실행 검증 대기** (2026-08-28 07:00 정기 실행에서 확인)
- 신규 계좌: `43896035-22` (연금저축 세액공제 비대상)

---

## 0. 배경과 결론

세액공제를 받지 않는 연금저축 계좌를 신규 개설했다. 기존 계좌와 세제 취급이 다르므로
**자산 원장에서도 분리해서 봐야 한다**. 계좌가 하나 늘었을 뿐이지만, 기존 코드는
"연금저축 = 계좌 1개"를 세 군데에 전제로 박아두고 있었다.

| 전제가 박힌 위치 | 형태 |
|---|---|
| `token_manager.py` | 토큰 딕셔너리 키 `KIS_PENSION` 하나 |
| `fetch_asset_daily_dag.py` | `Variable.get('KIS_PENSION')` 하드코딩 + 단일 태스크 |
| `kis_transformer.py` | 펀드를 **반드시 보유한다**는 전제 (미보유 시 0 나눗셈) |

세 번째가 이번 작업에서 가장 실질적인 변경이다. 신규 계좌는 주식만 매수했고 펀드는
아직 없는데, 기존 코드는 펀드 평가금액 0을 처리할 수 없었다.

**결론: 계좌 추가는 데이터 문제이고, 펀드 미보유는 코드 문제였다.**
전자는 Variable 이름을 인자로 빼는 것으로, 후자는 보유 판정을 앞으로 당기는 것으로 끝난다.

---

## 1. Airflow Variable 리네임

| 이전 | 이후 | 계좌 |
|---|---|---|
| `KIS_PENSION` | `KIS_PENSION_DEDUCTIBLE` | 기존 (세액공제 대상) |
| — | `KIS_PENSION_NON_DEDUCTIBLE` | 신규 `43896035-22` (비대상) |

두 Variable 모두 각자의 `appkey`/`secret`을 갖는다. **계좌가 다르므로 토큰도 별도 발급한다.**

`_get_kis_token(variable_name)`은 인자를 그대로 `Variable.get()`에 넘기므로,
`TokenGenerator`의 딕셔너리에서 **키와 인자를 같은 문자열로 맞추는 것이 유일한 규약**이다.
DAG 쪽 `tokens[variable_name]` 조회도 이 규약에 의존한다.

```python
# plugins/asset_flow/managers/token_manager.py
"KIS_PENSION_DEDUCTIBLE":     self._get_kis_token("KIS_PENSION_DEDUCTIBLE"),
"KIS_PENSION_NON_DEDUCTIBLE": self._get_kis_token("KIS_PENSION_NON_DEDUCTIBLE"),
```

동일 appkey는 KIS 토큰 발급에 1분 제한이 있으나, 두 계좌의 appkey가 다르므로
`TokenGenerator`의 연속 발급(이제 6회)에 영향이 없다.

---

## 2. 태스크 분리 — 함수 1개, 인스턴스 2개

두 계좌는 **로직이 100% 동일하고 Variable 이름만 다르다.** 함수를 복제하면
이후 모든 수정이 두 곳에 반영돼야 하므로, `upload_account`가 이미 쓰고 있는
`.override(task_id=...)` 패턴을 그대로 적용했다.

```python
'pension_deductible':     get_pension_assets.override(
    task_id='get_pension_deductible')(dates, 'KIS_PENSION_DEDUCTIBLE'),
'pension_non_deductible': get_pension_assets.override(
    task_id='get_pension_non_deductible')(dates, 'KIS_PENSION_NON_DEDUCTIBLE'),
```

계좌당 get/upload 페어 불변식은 유지된다. `account_code`가 서로 다르므로
(`upload_account`의 DELETE 스코프는 `(standard_date, account_code)`) 두 upload가
서로의 적재분을 지우지 않는다.

### task_id가 바뀐다 — 히스토리 단절

| 이전 | 이후 |
|---|---|
| `get_pension_assets` | `get_pension_deductible` |
| `upload_pension_assets` | `upload_pension_deductible` |

계좌가 둘이 된 이상 `..._assets`라는 이름은 어느 계좌인지 말해주지 않는다.
**Airflow UI에서 기존 두 태스크의 실행 히스토리는 끊긴다.** 적재된 데이터에는 영향이 없다
(`account_code`는 Variable 값에서 파생되고 값은 그대로다). 과거 그래프에는 옛 태스크가
회색으로 남는다.

---

## 3. 펀드 미보유 처리 — 검증 순서가 핵심이다

### 문제

`transform_pension_fund_balance`는 KIS가 좌수를 주지 않으므로 NAV로 역산한다.

```python
holding_quantity    = 평가금액 / NAV / 0.001
unit_purchase_price = 매입금액 / holding_quantity / 0.001   # ← 좌수 0이면 0으로 나눈다
```

펀드를 보유하지 않으면 평가금액이 0이고, 좌수도 0이 되어 매입단가가 `NaN`이 된다.

### 기각한 안 — 분모 가드

`holding_quantity == 0`일 때 `unit_purchase_price`를 0으로 채우는 방안을 먼저 검토했으나 기각했다.
**`holding_quantity`는 입력이 아니라 역산 결과다.** 좌수 0은 "평가금액이 0"이라는 뜻이고,
그 행은 좌수 0 / 매입단가 0 / 평가금액 0짜리 빈 행이다. 가드를 넣으면 `NaN` 대신 0이
적재될 뿐 의미 없는 행이 매일 쌓인다.

**보유하지 않은 자산은 행을 만들지 않는다.**

### 적용안 — 3단 조기 반환 + 검증 순서 역전

```python
rows = raw.get("output1") or []
if len(rows) < 2:                                   # ① 펀드/MMW 행 자체가 없음
    return pd.DataFrame(columns=BALANCE_COLUMNS)
...
if not (df["total_evaluation_amount"] > 0).all():   # ② 평가금액 0 = 미보유
    return pd.DataFrame(columns=BALANCE_COLUMNS)

if not fund_price:                                  # ③ NAV 검증은 보유 확정 뒤
    raise ValueError(...)
...
if not (df["holding_quantity"] > 0).all():          # ④ 역산 결과 0 (반올림 경계)
    return pd.DataFrame(columns=BALANCE_COLUMNS)
```

**①이 필요한 이유**: `df.loc[[1]]`은 고정 인덱스 접근이라 output1 길이가 2 미만이면
`KeyError`다. "값이 없으면"이 곧 예외가 된다.

**③의 위치가 이 변경의 요점이다.** 이전 코드는 NAV 검증이 함수 맨 앞(파싱 이전)에 있었다.
그대로 두면 **펀드를 하나도 안 든 계좌가 그날 NAV 크롤링 실패만으로 죽는다** — 쓰지도 않는
값 때문이다. 보유 판정을 앞으로 당겨야 "펀드 없음"이 "NAV 없음"보다 먼저 결정된다.

**④는 반올림 경계**다. 평가금액이 `NAV × 0.001`의 절반 미만이면 `round(0)`으로 좌수가 0이
되는데, 이 경우 매입금액은 0이 아닐 수 있어 `NaN`이 아닌 `inf`가 나온다. NAV 1,000원 기준
평가금액 0.5원 미만이라 실무상 발생하지 않지만, 잔량 소수점 처리에서 걸릴 수 있다.

**기존 계좌 동작은 불변이다.** 펀드를 보유 중이면 ①②④에 걸리지 않고 ③의 NAV 검증으로
넘어가며, NAV 결측 시 실패하는 기존 정책("직전 값으로 채우지 않는다")이 그대로 유지된다.

### 첫 매수가 코드 변경 없이 흡수된다

신규 계좌는 **주식을 이미 매수했으므로** `pension_df`가 비지 않는다.
→ `AirflowException("데이터 없음")` 미발생. 오늘은 주식 행만 적재되고,
펀드 첫 매수 다음 날부터 펀드 행이 자연히 추가된다. 이것이 "한 번에 적용" 요구의 실현이다.

주식조차 없었다면 매일 실패 알림이 쌓였을 것이므로, 이 설계는 **주식 보유를 전제로 한다.**

---

## 4. 펀드 코드는 Param 공유(A안)

두 계좌가 **같은 펀드(`K553W5E17401`)** 를 운용하므로 DAG Param `pension_fund_code`를
두 태스크가 공유한다. 코드 변경 없음.

| 검토한 안 | 판정 |
|---|---|
| **A. Param 공유** | **채택** — 지금 사실에 맞고 수정량 0 |
| B. 계좌 Variable에 `fund_code` 키 | 기각 — 계좌별 분기가 필요해지면 그때 |
| C. Param을 `{account_code: fund_code}` dict | 기각 — 설정이 Variable/Param 두 곳으로 갈라짐 |

**한쪽 계좌만 다른 펀드로 갈아타는 순간 A안은 깨진다.** 그 지점을 Param description에
남겨두었다: `"세액공제·비공제 계좌 공용 — 한쪽만 다른 펀드로 갈아타면 계좌별로 분리해야 한다"`.

이 결정 덕분에 `fetch_fund_price_daily_dag.py`의 `fund_codes`와
`fund_crawler.py:132`의 `CODES`(두 곳에 중복 정의된 상태)는 **손대지 않았다.**

---

## 5. 변경 파일

| 파일 | 변경 |
|---|---|
| `plugins/asset_flow/managers/token_manager.py` | 토큰 키 리네임 + 신규 1건 추가, `GetTokens` docstring |
| `dags/fetch_asset_daily_dag.py` | `get_pension_assets(dates, variable_name)` 인자화, override 2회, upload 페어 2쌍, 모듈 docstring·`DAG_DOC`(자산 10→11, 태스크 9→10) |
| `plugins/asset_flow/transformers/kis_transformer.py` | 조기 반환 3곳 + NAV 검증 위치 이동, docstring |
| `plugins/asset_flow/README.md` | 토큰 테이블 2행, transformer 설명 |

**무수정**: `fetch_fund_price_daily_dag.py`, `fund_crawler.py`, `db_manager.py`,
`upload_account`, `account.asset_daily` 스키마.

---

## 6. 검증 결과

`transform_pension_fund_balance` 4개 분기를 스텁 응답으로 확인했다.

| 케이스 | 기대 | 결과 |
|---|---|---|
| 펀드/MMW 행 없음 (output1 1행) | 빈 DF | (0, 16) |
| 평가금액 0 + NAV 결측 | 빈 DF (예외 아님) | (0, 16) |
| 정상 보유 (평가 1,100,000 / NAV 1,234.56) | 좌수 891,006 / 수익률 10.0 | 일치 |
| 보유 + NAV 결측 | `ValueError` | 발생 |

세 번째의 `account_code`가 `43896035-22`로 생성되는 것도 함께 확인했다.

**미검증**: 로컬 Airflow가 sqlite 상대경로 설정 오류로 기동되지 않아 DagBag 임포트
테스트는 하지 못했다. 문법 검사(`ast.parse`)만 통과한 상태다.

---

## 7. 배포 시 주의 — 토큰 파일 캐시

`TokenGenerator()`는 **당일 토큰 파일이 존재하면 즉시 return한다**(`token_manager.py:110`).
따라서 옛 키(`KIS_PENSION`)만 들어 있는 오늘자 파일이 남아 있으면
`tokens['KIS_PENSION_DEDUCTIBLE']`에서 `KeyError`가 나고, 재시도 3회도 같은 이유로 실패한다.

- **내일(08-28) 06:50 `make_token_dag`가 새 파일을 생성한 뒤 실행** → 조치 불필요
- **오늘 중 수동 테스트** → `data/tokens/20260827_token.json` 삭제 후 실행

---

## 8. 다음 단계

1. **2026-08-28 07:00 정기 실행 확인**
   - `get_pension_deductible`: 펀드 + 주식 모두 적재 (기존과 동일 건수)
   - `get_pension_non_deductible`: **주식만** 적재, 로그에 `(펀드: 0, 주식: N)`
2. `account.asset_daily`에서 `account_code = '43896035-22'` 행 확인
3. 비공제 계좌 펀드 첫 매수 후, 다음 날 펀드 행이 자동으로 붙는지 확인 (코드 변경 없어야 정상)

### 남은 기술 부채 (이번 범위 밖)

- **예수금 미적재**: 두 연금저축 계좌 모두 예수금을 자산으로 잡지 않는다. 입금 후 미매수
  구간의 현금이 스냅샷에서 누락된다. `transform_cma_cash_balance`가 같은 TR(CTRP6548R)에서
  현금을 뽑는 검증된 함수라 재사용 가능하나, 적용하면 **과거 적재분과 집계 기준이 달라져**
  자산이 갑자기 늘어 보이는 구간이 생긴다. 두 계좌 동시 적용 여부를 함께 결정해야 한다.
- **`output1[1]` 고정 인덱스**: 한 계좌에 펀드를 2종 이상 보유하면 한 종목만 잡히거나
  잘못된 행을 읽는다. 현재는 계좌당 펀드 1종 전제가 유효하다.
- **펀드 코드 중복 정의**: `fetch_fund_price_daily_dag.py:76`(Param)과
  `fund_crawler.py:132`(`CODES`)에 같은 목록이 두 번 적혀 있다.
