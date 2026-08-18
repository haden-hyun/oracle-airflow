# Handoff — `fetch_asset_daily` 위탁계좌 일별 적재 누락

- 작성일: 2026-08-18
- 대상 DAG: `fetch_asset_daily` (`dags/fetch_asset_daily_dag.py`)
- 대상 테이블: `account.asset_daily`
- 상태: **원인 규명 완료 / 코드 수정 미착수**

---

## 1. 증상

위탁계좌(`KIS_STOCK`, `account_code = <위탁계좌>`)의 국내주식과 해외주식이
일자별로 **둘 중 하나만** 적재된다. 어떤 날은 국내만, 어떤 날은 해외만 남는다.

최초 가설은 "휴장일에 KIS 잔고 API가 빈 `output1`을 반환한다"였으나, **데이터로 반증되었다.**

---

## 2. 원인 (확정)

**같은 계좌를 대상으로 하는 두 upload 태스크의 동시 쓰기 경합(race condition).**

`get_overseas_stock`과 `get_domestic_stock`은 모두 `Variable.get('KIS_STOCK')`을 읽으므로
`account_code`가 동일하다.

```python
# dags/fetch_asset_daily_dag.py:127-128 (get_overseas_stock)
config = Variable.get('KIS_STOCK', deserialize_json=True)
account_code = f"{config['account']}-{config['product_code']}"   # <위탁계좌>

# dags/fetch_asset_daily_dag.py:146-147 (get_domestic_stock)
config = Variable.get('KIS_STOCK', deserialize_json=True)
account_code = f"{config['account']}-{config['product_code']}"   # <위탁계좌>  ← 동일
```

그런데 적재 함수의 DELETE 스코프는 `(standard_date, account_code)`이다.

```sql
-- plugins/asset_flow/managers/db_manager.py:81
DELETE FROM account.asset_daily
 WHERE standard_date = :std_date AND account_code = :account_code
```

`upload_overseas_stock`과 `upload_domestic_stock`이 **병렬 실행**되면서 서로가 방금 INSERT한 행을 DELETE한다.

계좌 단위 스코핑은 "다른 계좌를 건드리지 않는다"는 목적은 달성했지만,
**한 계좌가 두 개의 get/upload 페어로 쪼개져 있는 경우**를 전제에서 놓쳤다.

### PostgreSQL Read Committed 동작

- 두 트랜잭션이 거의 동시에 시작 → 각 DELETE의 스캔 스냅샷에 상대 INSERT 행이 아직 없음 → **둘 다 생존**
- 시작 시점이 조금이라도 벌어짐 → 후행 DELETE가 선행 INSERT 행을 보고 지움 → **후행만 생존**

승자는 그날의 API 응답 속도가 정한다. 따라서 요일·휴장일과 무관하다.

---

## 3. 증거

### 3.1 발생 시점이 리팩터링 커밋과 정확히 일치

| 항목 | 날짜 |
|---|---|
| 누락 시작 `standard_date` | `2026-06-21` |
| 커밋 `fd75504 refactor: 자산 적재를 계좌 단위 get/upload 페어로 재구성` | `2026-06-21` |

리팩터링 **이전**에는 단일 `upload_asset_data` 태스크가 6개 결과를 `pd.concat`으로
합쳐 **하나의 트랜잭션**으로 DELETE + INSERT 했다. 충돌 여지가 없었다.

### 3.2 리팩터링 이전에는 주말·휴장일에도 정상 적재 (휴장일 가설 반증)

```
2026-03-01 (일)  KRX 2, NASD 5, NYSE 1
2026-03-07 (토)  KRX 2, NASD 5, NYSE 1
2026-03-08 (일)  KRX 2, NASD 5, NYSE 1
```

`2026-01-28 ~ 2026-06-20` 전 구간에서 주말·휴장일 포함 매일 국내·해외가 모두 존재한다.
잔고 조회는 장 상태와 무관하게 정상 응답한다.

### 3.3 누락에 요일 패턴이 없음

```
2026-07-21(화) 국내만    2026-07-17(금) 해외만
2026-07-22(수) 국내만    2026-07-18(토) 해외만
2026-07-23(목) 국내만    2026-07-19(일) 해외만
2026-07-24(금) 국내만    2026-07-20(월) 해외만
```

### 3.4 `insert_datetime`이 경합을 확정

`2026-06-21` 이후 58일 중 **43일이 한쪽만(XOR), 15일만 둘 다**.
둘 다 살아남은 15일은 예외 없이 두 INSERT 시각이 **밀리초 이내**다.

| standard_date | 국내 insert | 해외 insert | 간격 | 결과 |
|---|---|---|---|---|
| 2026-06-26 | `07:00:05.386255` | `07:00:05.384689` | 1.6 ms | 둘 다 생존 |
| 2026-07-08 | `07:00:05.974308` | `07:00:05.963655` | 10.7 ms | 둘 다 생존 |
| 2026-08-04 | `07:00:05.572488` | `07:00:05.552974` | 19.5 ms | 둘 다 생존 |
| 2026-06-22 | — | `07:00:05.530474` | — | 해외만 |
| 2026-06-23 | `07:00:05.045028` | — | — | 국내만 |

---

## 4. 영향 범위

| 계좌 | 영향 | 사유 |
|---|---|---|
| `KIS_STOCK` (위탁) | **영향 있음** | 국내·해외 두 태스크가 같은 `account_code` 공유 |
| `KIS_ISA` | 없음 | 단일 get/upload 페어 |
| `KIS_PENSION` | 없음 | 펀드·주식을 **한 태스크 안에서** concat (`dag:213`) |
| `KIS_CMA` | 없음 | 계좌 설정 분리 (단, ISA와 토큰 공유 — 계좌번호가 같아지면 재발) |
| `UPBIT` | 없음 | 단일 페어 |

`KIS_PENSION`이 무사한 이유가 곧 해법이다. 같은 계좌의 자산을 한 태스크에서 합쳐 반환한다.

---

## 5. 오탐으로 확인된 항목

- **`2026-08-11`부터 국내 3종목 → 1종목** (`[0180V0, 278530, 292150]` → `[0228G0]`)
  → 버그 아님. 실제 포트폴리오 교체로 확인됨.
- **`2026-06-06`, `2026-06-13` (토) 전체 0건**
  → 리팩터링 이전 시점이라 이 경합과 무관한 별건. DAG 실행 실패로 추정. 미조사.

---

## 6. 결정 사항 및 적용 내역

상태: **적용 완료 / 배포 및 관찰 대기**

### 6.1 위탁계좌 수집 태스크 통합 (원인 제거)

`dags/fetch_asset_daily_dag.py`

`get_overseas_stock` + `get_domestic_stock` → **`get_stock_account`** 단일 태스크.
해외·국내를 `pd.concat`으로 합쳐 계좌당 하나의 payload를 반환한다.
`get_pension_assets`가 이미 쓰던 패턴과 동일하다.

배선: `get_results` 키 `'overseas_stock'`/`'domestic_stock'` → `'stock'`,
upload 태스크 → `upload_stock_account`. 태스크 수 get 6→5, upload 6→5.

모듈 docstring에 **[불변식] 하나의 `account_code`는 정확히 하나의 get/upload 쌍을
가진다**를 이유와 함께 명시했다. 계좌 추가 시 같은 실수를 막기 위함이다.

### 6.2 계좌별 0건 fail (수집 실패 탐지)

`get_stock_account`, `get_isa_stock`, `get_pension_assets`, `get_cma_cash`,
`get_upbit_assets` **5개 전부**에 적용. Upbit 예외 없음.

```python
if df.empty:
    raise AirflowException("... 데이터 없음 — 빈 응답")
```

기존에는 `payload = {"account_code": ..., "records": []}`가 **딕셔너리라 truthy**여서
`upload_account`의 `if not payload` 가드를 통과했고, DELETE만 수행되어 기존 적재분이
조용히 사라졌다. 예외로 바꾸면 `retries=3`이 일시 장애를 흡수하고, 최종 실패 시
XCom이 없어 가드가 발동해 기존 데이터가 보존된다.

### 6.3 DELETE rowcount 로깅 (재발 감지)

`plugins/asset_flow/managers/db_manager.py`

```python
result = conn.execute(text(f"DELETE FROM ..."), {...})
if result.rowcount:
    print(f"[재적재] {standard_date} {account_code}: 기존 {result.rowcount}건 삭제 후 재적재")
```

정상 첫 실행이면 0이어야 한다. 6월에 이 로그가 있었다면 XOR 43일 전부(74%)가
사고 당일 잡혔을 것이다. 정당한 재실행에서도 뜨므로 fail이 아닌 로그로 둔다.

### 6.4 검토 후 채택하지 않은 항목

- **UPSERT 전환** — PK `(standard_date, account_code, product_code)`가 있어 가능하지만,
  stale row(같은 날 재실행 시 사라진 종목) 정리를 하려면 결국 `account_code` 스코프
  DELETE가 필요해 **동일한 경합이 남는다.** 원인은 SQL이 아니라 태스크 구조였다.
- **`get_pension_assets`의 concat 빈 프레임 필터** — pandas 2.1.4 실측 결과
  `pd.concat([정상DF, pd.DataFrame()])`는 컬럼·dtype을 보존하고 FutureWarning도 없다.
  제안이 잘못되어 철회.
- **DAG 말미 검증 태스크** — 효과는 크지만 태스크 통합으로 원인이 제거된 뒤에는
  중복 투자. 며칠 관찰 후 재검토.

---

## 7. 검증 상태

| 항목 | 결과 |
|---|---|
| `py_compile` (양 파일) | 통과 |
| DagBag 파싱 | `IMPORT ERRORS: none` |
| get/upload 태스크 5:5 대응 | 확인 |
| `get_results` 키 ↔ 배선 일치 | 확인 |
| `pd.concat` 4가지 경우 | 둘다/해외만/국내만 → 컬럼 보존, 둘다없음 → `empty=True` → raise |
| 실제 DAG 실행 | **미검증 (배포 후 확인 필요)** |

---

## 8. 알려진 트레이드오프

- **한쪽 시장만 빈 응답이면 fail하지 않는다.** 통합 후 0건 판정은 계좌 단위이므로
  "해외 7건 + 국내 0건"은 7건만 적재되고 통과한다. 국내 종목 전량 매도가 정상
  상태인 이상 이게 옳지만, 겉보기 증상이 원래 버그와 같아진다. 수집 로그에
  `(해외: N, 국내: M)`을 남겨 추적 가능하게 해두었다.
- **국내 API만 실패해도 해외까지 재시도된다.** 잔고 조회는 멱등이라 부작용 없음.
- **Airflow UI에서 기존 4개 태스크 이력이 orphan 처리된다.** 과거 실행 기록 조회에만 영향.

---

## 9. 배포 후 검증 절차

1. Airflow UI에서 `get_stock_account` → `upload_stock_account` 단일 경로 확인
2. 1회 수동 실행 후 국내·해외가 **동시에** 존재하는지 확인

```sql
SELECT standard_date, exchange_code, count(*)
  FROM account.asset_daily
 WHERE account_code = '<위탁계좌>'
   AND standard_date >= current_date - 3
 GROUP BY 1, 2 ORDER BY 1, 2;
```

3. 2~3일 관찰하여 XOR 패턴 소멸 확인
4. 태스크 로그에 `[재적재]`가 뜨지 않는지 확인 (첫 실행 기준)

---

## 10. 남은 과제 (이번 수정 범위 밖)

- **`requirements.txt` 버전 핀 부재** — `pandas`, `numpy` 등 전부 핀이 없어 Docker
  재빌드 시 pandas 3.x가 유입될 수 있다. 위 concat 실측은 2.1.4 기준이다.
  **우선순위 높음.**
- **`2026-06-21 ~ 2026-08-17` 소실분 백필** — 잔고 API는 요청 파라미터에 날짜가 없어
  과거 시점 조회가 불가능하다. 원본 복원 불가. 인접 영업일 보정 vs 결손 유지 판단 필요.
- **`2026-06-06`, `2026-06-13` 전체 결손** 원인 조사 (리팩터링 이전 건, 별개 사유).
- **UPSERT 전환 검토** — 도입 시 "종목 감소 재시도" 처리 방식 설계 필요.
