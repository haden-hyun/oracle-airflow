# Handoff — `fetch_asset_daily` 위탁계좌 일별 적재 누락

- 작성일: 2026-08-18
- 대상 DAG: `fetch_asset_daily` (`dags/fetch_asset_daily_dag.py`)
- 대상 테이블: `account.asset_daily`
- 상태: **원인 규명·코드 수정·소실분 백필 완료 / 배포 및 관찰 대기**

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

## 10. 소실분 백필 (완료)

실행일: 2026-08-18 / 대상: `2026-06-21 ~ 2026-08-17`, `<위탁계좌>`

### 10.1 방식

carry-forward. 직전에 해당 시장 데이터가 있던 날의 행을 복사하고 `standard_date`만
교체했다. 잔고 API는 요청 파라미터에 날짜가 없어 과거 시점 조회가 불가능하므로
원본 복원은 불가능하며, **적재된 208행은 모두 추정치다.**

시장 구분은 `exchange_code = 'KRX'`(국내) / 그 외(해외)로 판정했다.

### 10.2 결과

| 항목 | 값 |
|---|---|
| 적재 행 수 | **208** (국내 50행·18일 / 해외 158행·23일) |
| 전체 행 수 | 1695 → 1903 |
| `insert_datetime` | `2026-08-18 08:34:12.364480` (전 행 동일, 백필분 식별자) |
| 최대 gap | 4일 |

### 10.3 제외 구간 — `2026-08-11`, `2026-08-12` 해외 (10행)

리밸런싱 신규 매수가 결손 구간에 걸쳐 있어 **의도적으로 비워 두었다.**

```
08-10 실측  AAPL 1, GOOGL 2, NVDA 3, PLTR 5, SPCX 1          $2,691
08-11 결손  ← 매수 진행
08-12 결손  ← 매수 진행
08-13 실측  AAPL 3, GOOGL 3, NVDA 4, PLTR 5, SPCX 1, ORCL 2  $4,205
```

08-10에서 복사하면 ORCL이 누락되고 총액이 56% 과소평가된다. 매수가 이틀에 걸쳐
진행되어 08-13에서 역방향 복사해도 과대평가다. **어느 스냅샷도 실제를 대표하지
않아 결손으로 남겼다.**

제외 규칙: **종목 구성이 바뀐 구간만 제외**한다(없는 종목이 생기거나 있는 종목이
빠지는 명백한 오류). 수량만 변한 구간은 평가금액 오차와 같은 성격이라 포함했다.

### 10.4 포함했으나 수량이 부정확한 구간 (23행)

결손 기간 중 매매가 있었으나 종목 구성은 유지된 경우다.

| 구간 | 수량 변화 | 총액 차이 | 행 |
|---|---|---:|---:|
| 07-17 ~ 07-20 KR | 278530 11→14, 292150 70→80 | +10.3% | 12 |
| 08-13 KR | 0228G0 271→279 | +9.6% | 1 |
| 06-22 KR | 0180V0 92→82, 292150 68→70 | −8.8% | 3 |
| 06-23 OV | JOBY 40→41 | −4.2% | 7 |

나머지 185행은 수량이 정확하고 평가금액만 직전 영업일 값이다.

### 10.5 검증 결과 (전 항목 통과)

- 신규 PK 208 / 삭제된 PK 0 / PK 중복 0
- **기존 1695행 값 변경 0건, `insert_datetime` 변경 0건** — `DELETE` 없이 순수 INSERT
- 208행 전부 13개 컬럼이 소스일과 완전 일치, 불일치 0건
- 잔존 결손은 08-11·08-12 해외뿐 (의도한 제외와 일치)

### 10.6 롤백

208행 전부 `insert_datetime`이 동일하므로 `=` 비교가 안전하다.

```sql
DELETE FROM account.asset_daily
 WHERE account_code    = '<위탁계좌>'
   AND insert_datetime = TIMESTAMP '2026-08-18 08:34:12.364480'
   AND standard_date BETWEEN '2026-06-21' AND '2026-08-17';
```

### 10.7 조회 측 후속 조치 필요

- **08-11 ~ 08-12는 해외가 비어 있어** 대시보드에서 총액이 급락으로 보인다.
  직전 영업일 보간 또는 구간 분리 표시가 필요하다.
- 백필분 208행은 추정치다. `insert_datetime`만으로 구분하는 것은 향후 재적재와
  뒤섞일 수 있으므로 `is_estimated boolean` 컬럼 추가를 권한다.

---

## 11. 남은 과제

- **`requirements.txt` 버전 핀 부재** — `pandas`, `numpy` 등 전부 핀이 없어 Docker
  재빌드 시 pandas 3.x가 유입될 수 있다. 위 concat 실측은 2.1.4 기준이다.
  **우선순위 높음.**
- **`2026-06-06`, `2026-06-13` 전체 결손** 원인 조사 (리팩터링 이전 건, 별개 사유).
- **`is_estimated` 컬럼 추가** — 백필 208행이 추정치라는 사실을 데이터에 남긴다.
- **UPSERT 전환 검토** — 도입 시 "종목 감소 재시도" 처리 방식 설계 필요.
- **미배포** — 이 브랜치가 머지·배포되기 전까지는 매일 결손이 계속 발생한다.
