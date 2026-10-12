# Handoff — 증시 휴장일 달력 DAG (`refresh_market_holiday`)

> 사본 — 원본은 Airflow 쪽 `~/airflow/dag_handoff.md`(2026-10-09). DAG를 구현하는 쪽은 원본을 기준으로 고치고, 이 저장소의 테이블 규약(`db/migrations/006_market_holiday.sql`)이나 `scripts/refresh_market_calendar.py`가 바뀌면 이 사본과 원본을 함께 고친다.

- 작성일: 2026-10-09
- 대상 DAG: **신규** `refresh_market_holiday` (아직 없음 — 이 문서대로 만든다)
- 대상 테이블: `account.market_holiday` (자산 대시보드 저장소 `db/migrations/006_market_holiday.sql`, **이미 적용됨**)
- 현재 상태: 테이블에 2025–2027년치가 들어 있다(국내 52일 · 미국 31일). 지금은 대시보드 저장소의
  `scripts/refresh_market_calendar.py` 가 채운다. 이 DAG가 그 역할을 넘겨받는다.
- 상태: **설계 완료 / 구현 대기**

---

## 0. 배경과 결론

`fetch_asset_daily` 는 시계열을 쓰기 편하도록 **휴장과 관계없이 매일** 적재한다. 휴장일 행은
직전 거래일 가격이 이월된 값이다. 대시보드는 그날이 휴장이었는지 알아야 "전일 대비"를 바르게
읽을 수 있는데, 지금까지는 토·일만 휴장으로 봤다. 그 결과 2026-01-28 ~ 10-08 적재분에서
**국내 평일 휴장 13일, 미국 평일 휴장 6일**이 정상 거래일처럼 표시됐다(설·추석·선거일·성금요일 등).

휴장일 달력은 1년에 몇 번 바뀌지 않는다. 그래서 **매일 적재와 분리해 주 1회** 갱신한다.

**결론: 적재는 그대로 두고, 달력만 주 1회 DAG로 다시 계산해 덮어쓴다.**

---

## 1. 테이블 규약

```sql
account.market_holiday (
    market          varchar(4)   -- 'KR' = 한국거래소(XKRX), 'US' = 뉴욕증권거래소(XNYS)
    holiday_date    date
    name            varchar(120) -- 화면에 그대로 보이는 한국어 이름 (예: '추석', '추수감사절')
    source          varchar(32)  -- 'holidays 0.106' | 'kasi' | 'manual'
    is_manual       boolean      -- 사람이 넣은 행. DAG는 절대 지우거나 덮어쓰지 않는다
    insert_datetime timestamp
    PRIMARY KEY (market, holiday_date)
)
```

지켜야 할 규약 세 가지:

| 규약 | 이유 |
|---|---|
| **평일만 담는다** (토·일 제외) | 주말은 대시보드 코드(`app/market.py`)가 따로 판단한다. 주말 휴일까지 넣으면 중복이다 |
| **`is_manual = true` 행은 건드리지 않는다** | 라이브러리·API보다 늦게 반영되는 임시휴장은 사람이 직접 넣는다 |
| **3년치(작년·올해·내년)를 매번 통째로 다시 쓴다** | 증분 비교보다 단순하고, 출처가 날짜를 철회한 경우도 자연스럽게 반영된다 |

대시보드는 해당 연도 행이 하나도 없으면 "휴장일 달력 없음" 경고를 띄운다. 그러니 내년 1월 초에
**내년치가 미리 들어가 있어야 한다** — 3년치를 쓰는 이유다.

---

## 2. 데이터 출처

### 2-1. `holidays` 패키지 (필수)

```python
import holidays
kr = holidays.financial_holidays("XKRX", years=[y-1, y, y+1], language="ko")  # 이름이 한국어
us = holidays.financial_holidays("XNYS", years=[y-1, y, y+1])                 # 이름이 영어
```

- XKRX: 공휴일 + 대체공휴일 + 선거일 + 근로자의 날 + **연말휴장일(12/31)** 까지 포함한다.
- XNYS: 이름이 영어로만 나온다. 아래 표로 한국어로 옮긴다. `" (observed)"` 가 붙으면 `"… 대체 휴장"`.
- 여러 이름이 겹치면 `"A; B"` 형태로 온다. `"; "` 로 나눠 각각 옮긴다.

| 영어 | 한국어 |
|---|---|
| New Year's Day | 신정 |
| Martin Luther King Jr. Day | 마틴 루서 킹 데이 |
| Washington's Birthday | 대통령의 날 |
| Good Friday | 성금요일 |
| Memorial Day | 현충일(메모리얼 데이) |
| Juneteenth National Independence Day | 준틴스 |
| Independence Day | 독립기념일 |
| Labor Day | 노동절 |
| Thanksgiving Day | 추수감사절 |
| Christmas Day | 크리스마스 |

⚠ **패키지는 설치한 버전에 날짜가 고정돼 있다.** 같은 버전으로 매주 돌려도 결과는 같다.
새로 지정된 임시공휴일을 반영하려면 패키지를 올리거나 2-2를 쓴다.

### 2-2. 공공데이터포털 특일정보 API (선택, 권장)

임시공휴일은 보통 시행 1~2주 전에 발표된다. 패키지 업데이트를 기다리지 않고 잡으려면 이 API를 붙인다.

- 엔드포인트: `https://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService/getRestDeInfo`
- 파라미터: `serviceKey`, `solYear`(YYYY), `solMonth`(MM), `_type=json`, `numOfRows=50`
- 응답: `response.body.items.item` — **결과가 1건이면 dict, 여러 건이면 list** 로 온다. 둘 다 처리할 것
- 항목: `locdate`(20261009), `dateName`('한글날'), `isHoliday`('Y')
- 올해·내년 × 12개월 = 24회 호출. `isHoliday == 'Y'` 이고 평일인 날만 KR 행으로 쓴다(`source='kasi'`)
- 키: Airflow Variable **`DATA_GO_KR_KEY`**. **없으면 이 단계는 건너뛰고 성공 처리**한다(실패시키지 않는다)

국내 공휴일은 곧 한국거래소 휴장일이므로 그대로 KR 휴장으로 쓴다. 반대로 연말휴장일처럼
공휴일이 아닌 거래소 휴장은 API에 없으니 2-1이 반드시 먼저 있어야 한다.

---

## 3. DAG 설계

| 항목 | 값 |
|---|---|
| dag_id | `refresh_market_holiday` |
| 스케줄 | 매주 월요일 06:00 KST — `'0 6 * * 1'`, `catchup=False` |
| 연결 | `postgres_asset` (다른 DAG와 동일) |
| 알림 | `callbacks.slack_failure_callback` / `slack_recovery_callback` (기존 규약) |
| 의존성 | 없음. `fetch_asset_daily` 와 독립 |
| tags | `market`, `calendar`, `holiday`, `weekly` |

주 1회인 이유: 월 1회면 1~2주 전에 발표되는 임시공휴일을 놓칠 수 있고, 매일은 의미가 없다.

### 태스크

| Task | 입력 → 출력 | 내용 |
|---|---|---|
| `build_from_library` | — → 레코드 리스트 | 2-1로 3년치 평일 휴장일 계산 |
| `fetch_kasi_holidays` | — → 레코드 리스트 | 2-2. 키 없으면 `[]`. `retries=3`, 지수 백오프 |
| `upsert_holidays` | 두 리스트 → `{before, after}` | 아래 SQL. 한 트랜잭션 |
| `report_changes` | diff → 로그 | 추가·삭제된 날짜를 출력. 변경이 있으면 Slack으로 알려도 좋다 |

레코드 형태: `[market, 'YYYY-MM-DD', name, source]` — XCom으로 넘기므로 날짜는 문자열로.

### 멱등 적재 SQL

```sql
-- 한 트랜잭션 안에서
DELETE FROM account.market_holiday
 WHERE NOT is_manual AND holiday_date BETWEEN :y_minus_1_0101 AND :y_plus_1_1231;

INSERT INTO account.market_holiday (market, holiday_date, name, source)
VALUES (...)                               -- 라이브러리 행을 먼저, KASI 행을 나중에
ON CONFLICT (market, holiday_date) DO NOTHING;
```

- 라이브러리 행을 먼저 넣으므로 같은 날짜는 **라이브러리 이름이 이긴다**. KASI는 라이브러리에 없는 날만 채운다.
- `ON CONFLICT DO NOTHING` 이 수동 행(`is_manual`)도 보호한다(위 DELETE에서 지워지지 않았으므로 충돌).

---

## 4. 배포 절차

1. `requirements.txt` 에 `holidays>=0.106` 추가 → 이미지 재빌드(`docker compose build && docker compose up -d`)
   - **순서 주의**: DAG 파일을 먼저 넣으면 스케줄러가 `ModuleNotFoundError: holidays` 로 임포트 에러를 띄운다
2. (선택) Variable `DATA_GO_KR_KEY` 등록 — 공공데이터포털 "한국천문연구원_특일 정보" 활용 신청 후 일반 인증키(Decoding)
3. `dags/refresh_market_holiday_dag.py` 작성 — 기존 `fetch_exchange_rate_dag.py` 의 `@dag`/`@task` 형식, `DAG_DOC` 형식을 따른다
4. UI에서 수동 1회 트리거 → 아래 5장 검증
5. **대시보드 저장소 후속 작업**(DAG가 1회 성공한 뒤):
   - `scripts/daily.sh` 의 `refresh_market_calendar.py` 호출 줄 삭제
   - `deploy/asset-dashboard-refresh.service` 의 `ExecStart=-…refresh_market_calendar.py` 줄 삭제
   - `scripts/refresh_market_calendar.py` 는 수동 복구용으로 남긴다 (같은 규약으로 동작)

---

## 5. 검증

```sql
-- 2026년 평일 휴장일 수: KR 17 · US 10 이어야 한다
SELECT market, count(*) FROM account.market_holiday
 WHERE extract(year FROM holiday_date) = 2026 GROUP BY market;

-- 3년치 전체: 현재 기준 KR 52 · US 31 (KASI 행이 추가되면 KR이 늘 수 있다)
SELECT market, source, count(*) FROM account.market_holiday GROUP BY 1, 2 ORDER BY 1, 2;

-- 주말 행이 없어야 한다 (0건)
SELECT * FROM account.market_holiday WHERE extract(isodow FROM holiday_date) > 5;
```

2026년 기대값:

- KR(17): 01-01 신정연휴, 02-16~18 설 연휴, 03-02 삼일절 대체, 05-01 노동절, 05-05 어린이날,
  05-25 부처님오신날 대체, 06-03 지방선거일, 07-17 제헌절, 08-17 광복절 대체, 09-24~25 추석,
  10-05 개천절 대체, 10-09 한글날, 12-25 기독탄신일, 12-31 연말휴장일
- US(10): 01-01, 01-19, 02-16, 04-03, 05-25, 06-19, 07-03(독립기념일 대체 휴장), 09-07, 11-26, 12-25

대시보드 쪽 확인: `/api/data-health` 의 `markets.today` 가 오늘 날짜에 맞는 휴장 여부를 돌려주는지,
`calendar_ok` 가 `true` 인지.

---

## 6. 수동 행 넣는 법 (임시휴장)

```sql
INSERT INTO account.market_holiday (market, holiday_date, name, source, is_manual)
VALUES ('KR', '2027-06-01', '임시공휴일', 'manual', true);
```

잘못 넣은 수동 행은 사람이 지운다. DAG는 수동 행을 지우지 않는다.

---

## 7. 범위 밖 / 남은 이슈

- **미국 조기 폐장(반일장)** — 추수감사절 다음 날, 크리스마스이브 등. `holidays` 에 없다.
  휴장이 아니라 종가가 일찍 나오는 날이라 이월 판단에는 영향이 없다. 필요하면 `market_session` 류 테이블을 따로 둔다.
- **KRX 개장 지연**(수능일 10시 개장 등)도 휴장이 아니므로 다루지 않는다.
- 이전 초안 코드: 자산 대시보드 세션 스크래치패드의 `refresh_market_holiday_dag.py` (이 문서로 대체, 참고용).
