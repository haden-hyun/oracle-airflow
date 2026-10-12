# 작업 I: secrets YAML 값만 바꾸면 전 과정이 따라오는 구조 — 엔드투엔드 설계와 실패 모드

- 작성일: 2026-10-12
- 대상 저장소: `oracle-airflow`. 읽기 전용으로 분석했고, 이 보고서 외의 파일은 수정하지 않았다.
- 기준 버전: **Airflow 3.3.2**(`apache-airflow-core==3.3.2`, `apache-airflow-task-sdk==1.3.2`). 현재 운영은 `apache/airflow:2.10.4` docker-compose(LocalExecutor)다.
- "소스 확인" 표시는 PyPI wheel 두 개를 스크래치패드에 풀어 직접 읽은 항목이다.
- 선행 보고서와의 관계:
  - [F Variable·명명](airflow3_F_variables_naming.md): **§1.3의 `dags/config/asset_flow.yaml`(git 커밋 대상)에 계좌번호를 두는 안은 폐기한다.** 사용자 결정 (1)에 따라 계좌번호도 비밀이기 때문이다. F의 "Connection(`login`/`password`)으로 KIS 키 보관" 안도 이 보고서의 단일 secrets 파일 안으로 대체한다(§2.1에서 근거 제시). F의 함정 분석(마스킹 키워드, 모든 컨테이너 마운트, `.gitignore`)은 그대로 유효하다.
  - [D 백업·Discord](airflow3_D_backup_discord.md): 실패 알림은 D §5.2의 `requests` 직접 호출 콜백과 Connection `discord_alert`를 그대로 쓴다. 만료 알림과 import error 알림도 같은 전송 함수(`_post_discord`)를 재사용한다.
  - [E 병행 구축](airflow3_E_parallel_cutover.md): 섀도 기간의 토큰 RO 마운트·발급 금지 원칙(E §4.3)과 충돌하지 않도록 토큰 캐시 형식 변경을 **블루(2.10.4)에 먼저** 넣는다(§6).

---

## 0. 요약 (TL;DR)

| # | 결론 |
|---|---|
| 1 | 하드코딩 지점은 **16곳**이다(§1 표). 가장 큰 것은 ① `token_manager`의 발급 목록·`REQUIRED_TOKEN_KEYS`, ② `fetch_asset_daily`의 계좌별 태스크 10쌍과 `override` 20회, ③ `upsert_manual_position`의 `ACCOUNT_CODE_ENUM`(**계좌번호 4개가 git과 직렬화 DAG에 평문으로 있음**), ④ 원장 계좌를 `account_type == "IRP"` + `iloc[0]`으로 "유형당 1계좌"라고 가정하는 6곳이다. |
| 2 | 목표 구조는 **서버의 단일 파일 `secrets/asset_flow.yaml`(gitignore, RO 마운트)**에 `credentials`(키·만료일)와 `accounts`(별칭·유형·계좌번호·참조 키)를 두는 것이다. 코드는 이 파일을 **태스크 실행 중에만** 읽는다. |
| 3 | 수집 DAG는 **파싱 시점 for 루프가 아니라 동적 태스크 매핑**으로 바꾼다. 계좌 목록은 런타임 태스크(`list_accounts`)가 YAML에서 읽어 **비밀이 아닌 별칭만** XCom으로 넘기고, `collect_account.expand(alias=...)` + `map_index_template`으로 UI에 `kis_stock`, `irp` 같은 이름을 표시한다. 이렇게 하면 ① 계좌 추가·삭제에 코드 수정이 없고, ② task_id가 고정되며, ③ DAG 버전이 바뀌지 않고, ④ **YAML이 깨져도 import error가 아니라 태스크 실패가 되어 Discord 알림이 간다.** |
| 4 | 파싱 시점에 secrets를 읽으면 안 되는 이유: Param 기본값·enum·`doc_md`·op_kwargs 등 파싱 시점 값은 **직렬화 DAG(JSON)로 메타DB에 저장되고, 3.x는 DAG 버전마다 그 사본을 보존**하므로 한번 들어간 계좌번호는 이후 버전에서 지워도 남는다. 현재 `ACCOUNT_CODE_ENUM`이 바로 이 상태다. |
| 5 | **키 교체 함정**: 현재 토큰 캐시(`data/tokens/YYYYMMDD_token.json`)는 "당일 파일이 있으면 즉시 return"(`token_manager.py:116-117`)이라 **YAML에서 appkey를 바꿔도 그날 자정까지 옛 키 토큰이 쓰인다.** 그런데 `KISApiClient`는 헤더에 **새** appkey와 **옛** 토큰을 함께 보내므로(`kis_client.py:31-39`) 당일 수집이 인증 오류로 실패한다. 해결책은 자격증명별 **키 지문(sha256 앞 16자)**을 토큰과 함께 저장하고, 불일치하면 **그 자격증명만** 파일 잠금 아래에서 재발급하는 것이다. 매핑 태스크가 병렬로 동시에 불일치를 감지해도 KIS 1분 1회 제한에 걸리지 않게 `fcntl.flock` + 잠금 후 재확인을 쓴다. |
| 6 | 업무 DB 키 `account_code`는 **계좌번호에서 파생된다**(KIS: `f"{account}-{product_code}"`, `kis_transformer.py:54,92,153,221`). 따라서 계좌번호를 바꾸면 **새 계좌로 취급되어 시계열이 끊긴다.** 진짜 새 계좌면 그게 맞고, 같은 계좌의 번호 정정이면 1회성 마이그레이션 SQL(§3.3)을 쓴다. 계좌 삭제는 수집만 멈추고 과거 행은 남는다. 삭제 대신 `enabled: false`를 권장한다. |
| 7 | YAML 파손 시 3.x 동작(소스 확인): 파싱 중 예외 → `ParseImportError` 기록 + **해당 DAG를 `is_stale=True`로 표시** → 스케줄러가 stale DAG에는 run을 만들지 않는다 → **run이 없으니 콜백도 없다.** 대책은 ① 파싱 시점에 YAML을 읽지 않는 설계(근본), ② 3.3.2에 있는 **import error 리스너 훅**(`on_new_dag_import_error`, `on_existing_dag_import_error`)을 플러그인으로 등록해 dag-processor가 Discord로 보내게 하는 것(코드 버그 대비)이다. `on_existing_*`는 재파싱할 때마다(기본 30초) 호출되므로 **스택트레이스 해시로 중복을 막아야 한다.** |
| 8 | 만료 알림: 매일 09:00 `ops_check_credentials` DAG가 YAML의 `expires_on`을 읽어 **D-14부터 매일 1건**(만료 지나면 빨간색, 태스크 실패) Discord로 보낸다. Upbit는 공식 API `GET /v1/api_keys`가 키별 `expire_at`을 주므로 **YAML 값과 교차 검증**한다. KIS는 appkey 만료일 조회 API를 찾지 못했으므로 YAML 값을 신뢰원으로 쓴다. |
| 9 | 변경 절차: 서버에서 사본 편집 → `python -m asset_flow.config.validate <사본>` → 통과 시 원본 백업 후 **같은 디렉터리 안에서 `mv`로 원자적 교체** → 다음 태스크부터 자동 반영. **파일 단위 bind mount는 금지**(원자적 교체 후 컨테이너가 옛 inode를 계속 본다). 디렉터리를 RO로 마운트한다. |
| 10 | 구현 순서: Phase 0(2.10.4 블루)에 로더·검증 CLI·토큰 지문·만료 알림을 먼저 넣고(3.x와 무관한 변경), 동적 매핑 재구성과 import error 리스너는 그린(`feat/airflow3`)에 넣어 E의 섀도 검증 S4(계좌별 `asset_daily` 동등성)로 확인한다. 섀도 기간에는 그린에서 **토큰 발급을 코드로 금지**(`TOKEN_ISSUE_ENABLED=false`)해야 한다(§6.2). |

---

## 1. 현재 코드의 하드코딩 지점 전수 조사

`grep -rn "Variable\|KIS_\|UPBIT\|account_type\|override(task_id"`와 파일 정독으로 찾았다. "YAML만으로 변경 가능?" 열은 **목표 구조에서** 코드 수정 없이 흡수되는지를 뜻한다.

| # | 위치 | 하드코딩 내용 | 계좌를 추가하거나 키를 바꾸면 생기는 일 | 목표 구조에서의 처리 |
|---|---|---|---|---|
| H1 | `plugins/asset_flow/managers/token_manager.py:27-30` | `REQUIRED_TOKEN_KEYS = {KIS_STOCK, KIS_ISA, KIS_PENSION_DEDUCTIBLE, KIS_PENSION_NON_DEDUCTIBLE, KIS_IRP, UPBIT}` | 계좌를 추가하려면 코드를 고쳐야 한다. **사용처 없는 `KIS_IRP`의 키가 만료되면 전체 발급이 실패한다**(F §1.1). | 필요한 자격증명 = "활성 계좌가 참조하는 자격증명 ∪ 시장 데이터용 자격증명"을 **YAML에서 계산**한다. 쓰지 않는 키는 발급하지 않는다. |
| H2 | 같은 파일 `:123-132` | 발급 목록 dict 6줄(`KIS_GOLD` 주석 포함). 하나라도 실패하면 `raise`해서 **파일 자체를 쓰지 않는다** | 키 하나가 만료되면 모든 계좌의 수집이 멈춘다. | 자격증명 목록을 순회하며 **자격증명별로** 발급하고 저장한다. 실패한 것만 실패로 기록하고 나머지는 저장한다(§3.1). |
| H3 | 같은 파일 `:63`, `:100` | `json.loads(Variable.get(name))`. 키 이름 `appkey`/`secret` | Variable 이름이 곧 토큰 dict 키다. | 로더 `credential(alias)`로 대체한다. |
| H4 | 같은 파일 `:116-117` | 당일 파일이 있으면 즉시 return한다. 키 비교가 없다 | **키 교체가 당일에는 반영되지 않는다**(§3.1). | 키 지문 비교 후 불일치하는 것만 재발급한다. |
| H5 | `dags/fetch_asset_daily_dag.py:182,184` / `:210,212` / `:284,287` / `:303,306` | `Variable.get('KIS_STOCK'…)`, `tokens['KIS_STOCK']`, `KIS_ISA`, `KIS_CMA` + `tokens['KIS_ISA']`(CMA가 ISA 토큰을 빌려 쓰는 암묵 규약), `UPBIT` | 계좌를 추가하려면 get 함수를 하나 더 만들어야 한다. | 계좌 유형(`kind`)별 처리 함수 디스패치(§2.5). 토큰 공유는 YAML의 `credential:` 참조로 **명시**한다. |
| H6 | 같은 파일 `:221-274`, `:406-409` | `get_pension_assets(dates, variable_name)` + `.override(task_id='get_pension_deductible')` 2회 | 연금 계좌를 하나 더 두려면 override를 하나 더 써야 한다. | 매핑된 `collect_account` 하나로 흡수한다. |
| H7 | 같은 파일 `:335`, `:355`, `:375`, `:394` | `positions["account_type"] == "IRP"` / `"DC"` / `"INSTALLMENT_SAVINGS"` / `"HOUSING_SUBSCRIPTION"` 다음에 `.iloc[0]` | **유형당 1계좌만 처리된다.** IRP를 두 개 두면 하나는 조용히 빠진다. | 원장 계좌도 YAML의 `accounts`에 별칭 단위로 두고, `account_code`로 원장 행을 고른다. |
| H8 | 같은 파일 `:403-416`, `:455-464` | get 결과 dict 10키 + `upload_account.override(task_id=…)` 10회 | 계좌 하나당 2줄씩 추가해야 하고 task_id 짝을 맞춰야 한다. | 매핑 1개로 대체한다(task_id가 고정됨). |
| H9 | 같은 파일 `:117-120` | Param `pension_fund_code='K553W5E17401'`(공제·비공제 공용) | 한쪽만 펀드를 바꾸면 깨진다(Param description에 경고가 있음). | 계좌별 `fund_code` 필드로 옮긴다. |
| H10 | `dags/fetch_exchange_rate_dag.py:124,126` | 환율 조회에 `KIS_STOCK` 키와 토큰을 쓴다 | 위탁계좌 키를 없애면 환율 수집도 멈춘다. | YAML `market_data.credential`로 지정한다. |
| H11 | `dags/fetch_fund_price_daily_dag.py:75-78`, `plugins/asset_flow/crawler/fund_crawler.py:132` | `fund_codes=['K553W5E17401','K55105D43299']`(이중 정의) | 펀드를 바꾸면 계좌 쪽과 크롤 목록을 따로 고쳐야 한다. | 크롤 목록 = **계좌들의 `fund_code` 합집합**을 런타임에 계산한다(Param은 "추가 코드" 용도로만 남김). |
| H12 | `dags/upsert_manual_position_dag.py:37-42`, `:149-159` | `ACCOUNT_CODE_ENUM`: **실제 계좌번호 4개**, Param 기본값과 description에도 계좌번호 | 계좌를 추가하려면 코드를 고쳐야 한다. **사용자 결정 (1) 위반**: git 이력과 직렬화 DAG에 계좌번호가 있다. | Param을 **별칭**(`irp`, `dc` …) 자유 문자열로 바꾸고, 런타임에 YAML로 검증해 `account_code`로 해석한다(§2.6). |
| H13 | 같은 파일 `:226-231` | `Variable 'MANUAL_ASSET_CONFIG'`(유형별 `product_code`·`multiplier`), `account_type in ('IRP','DC')` | 유형 키가 고정돼 있다. | 계좌별 `fund_code`·`multiplier`를 YAML로 옮긴다. `kind`별 검증 분기는 코드에 둔다(유형은 코드 개념). |
| H14 | `dags/rebuild_derived_assets_dag.py:109-232` | `rebuild_irp/dc/savings/housing` 4태스크, 각각 `account_type == …` + `iloc[0]` | H7과 같다. | `rebuild_account.expand(alias=원장 계좌 별칭)`. |
| H15 | `plugins/asset_flow/transformers/ledger_transformer.py:19-24` | `_CASH_PRODUCT_CODE`, `_FUND_MULTIPLIER = 0.001` | multiplier가 `MANUAL_ASSET_CONFIG`와 이중으로 관리된다(F §1.1). | 계좌의 `multiplier`를 인자로 받는다. `_CASH_PRODUCT_CODE`는 유형 상수라 코드에 둔다. |
| H16 | `plugins/asset_flow/transformers/kis_transformer.py:157` | `print(f"[{account_code}] 계좌잔고 원시응답 output1 …: {rows}")`(커밋 `6bb1393` 디버그 로깅) | **계좌번호가 태스크 로그에 평문으로 남는다.** | 계좌번호를 `mask_secret`에 등록하면 `***`로 가려진다(§2.4). 디버그가 끝나면 이 줄은 제거한다. |

부수 사항(하드코딩은 아니지만 자동화를 막는 것):

- `token_manager.py:38` `TOKEN_DIR` 기본값이 상대경로다. E §3.1대로 compose에 절대경로를 명시한다.
- `get_X`가 반환하는 XCom에 `account_code`(=계좌번호 파생값)와 `records` 전체가 들어간다(`fetch_asset_daily_dag.py:200` 등). 그래서 **메타DB `xcom` 테이블과 UI XCom 화면에 계좌번호가 남는다.** 목표 구조에서는 수집과 적재를 한 태스크에서 하고, XCom에는 별칭과 건수만 반환한다(§2.3).
- `callbacks.py:21` `SLACK_WEBHOOK_URL`: D에서 Discord Connection으로 교체한다. **알림 경로는 secrets YAML에 의존하면 안 된다**(YAML이 깨졌을 때 알림까지 죽기 때문이다. §4).

---

## 2. 설정 기반으로 바꾸는 방법

### 2.1 secrets 파일 하나 — 형식과 위치

**결정**: Airflow secrets backend(Connection/Variable)를 쓰지 않고, **애플리케이션 전용 YAML 한 파일**을 로더가 직접 읽는다.

| 비교 | (A) F안: Connection(키) + git YAML(계좌) | (B) Variable 하나에 JSON 문자열 | **(C) 전용 파일 `secrets/asset_flow.yaml` + 자체 로더(권장)** |
|---|---|---|---|
| 계좌번호 비밀 처리 | git YAML에 있음 → **결정 (1) 위반** | 가능 | 가능 |
| 편집 대상 파일 수 | 2개 + Connection | 1개(그러나 JSON 문자열을 YAML 안에 넣어야 함, F 함정 1) | **1개** |
| 만료일 저장 | Connection `extra`에 넣어야 해서 어색함 | 가능 | 자연스러운 필드 |
| 스키마 검증 CLI | 두 출처를 합쳐야 함 | 가능 | **로더 그대로 재사용** |
| 2.10.4와 3.3.2 공통 | `airflow.sdk.Connection`과 2.x API가 다름 | 다름 | **같은 코드**(순수 Python + PyYAML) |
| 마스킹 | Connection password는 자동 | 키워드 의존 | 로더가 `mask_secret`을 명시적으로 호출 |
| Airflow 추상화 이점(백엔드 교체로 Vault 이전) | 있음 | 있음 | 없음. 필요해지면 로더 내부만 바꾼다 |

(C)를 고른 이유는 사용자 목표인 "**한 파일의 값만** 바꾸면 된다"와 만료일 필드에 가장 직접적으로 맞기 때문이다. F §1.2(c)의 LocalFilesystemBackend도 결국 "gitignore된 파일 + RO 마운트"였으므로 보안 수준은 같다. Discord webhook만은 D §5.1대로 **Connection `discord_alert`(`.env`의 `AIRFLOW_CONN_DISCORD_ALERT`)**로 둔다. 알림 경로를 이 YAML과 분리하기 위해서다.

### 2.2 스키마 예시 (`secrets/asset_flow.yaml`, 서버에만 있음)

```yaml
# secrets/asset_flow.yaml — gitignore, chmod 640, owner ${AIRFLOW_UID}:0, 디렉터리째 RO 마운트
# 이 파일만 고치면 토큰 발급·수집·만료 알림이 따라온다. 수정 후 반드시 validate 실행(§5).
version: 1

credentials:                     # 자격증명 = 토큰 발급 단위. 별칭은 비밀 아님(UI·로그에 나와도 됨)
  kis_stock:
    provider: kis
    appkey: "PSxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    secret: "xxxxxxxx…(약 180자)"
    expires_on: 2027-03-31       # KIS Developers 포털에 표시된 서비스 만료일(사람이 옮겨 적음)
  kis_isa:
    provider: kis
    appkey: "…"
    secret: "…"
    expires_on: 2027-03-31
  kis_pension_deductible:     {provider: kis, appkey: "…", secret: "…", expires_on: 2027-05-01}
  kis_pension_non_deductible: {provider: kis, appkey: "…", secret: "…", expires_on: 2027-08-27}
  upbit:
    provider: upbit
    appkey: "…"                  # access_key
    secret: "…"
    expires_on: 2027-01-15       # GET /v1/api_keys의 expire_at과 교차검증(§2.7)

market_data:
  credential: kis_stock          # 환율 조회용 (H10)

accounts:                        # 별칭 = 매핑 표시명·Param 값. 소문자/숫자/_ 만
  kis_stock:
    kind: kis_brokerage          # 디스패치 키 (§2.5)
    credential: kis_stock
    account: "12345678"          # CANO — 비밀
    product_code: "01"
    name: 한국투자 위탁
  kis_isa:
    kind: kis_isa
    credential: kis_isa
    account: "…"
    product_code: "01"
    name: 한국투자 ISA
  pension_deductible:
    kind: kis_pension
    credential: kis_pension_deductible
    account: "…"
    product_code: "22"
    name: 연금저축(세액공제)
    fund_code: K553W5E17401      # H9 — 계좌별
  pension_non_deductible:
    kind: kis_pension
    credential: kis_pension_non_deductible
    account: "…"
    product_code: "22"
    name: 연금저축(비공제)
    fund_code: K553W5E17401
  cma:
    kind: kis_cma
    credential: kis_isa          # CMA는 ISA 키·토큰 공유 — 현재 암묵 규약(H5)을 명시화
    account: "…"
    product_code: "…"
    name: 한국투자 CMA
  upbit:
    kind: upbit
    credential: upbit
    account_code: UPBIT          # DB 키를 직접 지정(현재 기본값과 동일)
    name: 업비트
  irp:
    kind: ledger_fund            # 원장 기반(API 없음) — credential 없음
    account_code: "…"            # 원장·account_master의 키 (현재 ACCOUNT_CODE_ENUM 값)
    name: 저축IRP
    account_type: IRP            # account_master.account_type
    fund_code: K55105D43299
    multiplier: 0.001
  dc:          {kind: ledger_fund, account_code: "…", name: 확정기여형DC, account_type: DC, fund_code: K55105D43299, multiplier: 0.001}
  savings:     {kind: ledger_cash, account_code: "…", name: 청년미래적금,   account_type: INSTALLMENT_SAVINGS}
  housing:     {kind: ledger_cash, account_code: "…", name: 청년주택드림청약, account_type: HOUSING_SUBSCRIPTION}
  # 해지/보류: 지우지 말고 enabled: false (§3.3)
  # old_isa: {kind: kis_isa, enabled: false, …}

alerts:
  expiry_warn_days: 14
```

- KIS 계좌의 `account_code`는 생략하면 지금처럼 `"{account}-{product_code}"`로 파생한다(`kis_transformer.py:54`와 동일). 명시하면 그 값을 쓴다(§3.3의 번호 변경 대응).
- `credentials` 별칭과 `accounts` 별칭은 **비밀이 아니다.** UI의 매핑 이름, XCom, 로그, Discord에 그대로 나간다. 그래서 별칭에 계좌번호 일부를 넣지 않는다(검증 CLI가 숫자 6자리 이상 연속을 거부한다).

### 2.3 수집 태스크 생성 방식: 파싱 시점 for 루프 vs 동적 태스크 매핑

#### 비교

| 기준 | (A) 파싱 시점 for 루프 (`for alias in aliases: collect.override(task_id=f"collect_{alias}")`) | **(B) 동적 태스크 매핑 (`collect_account.expand(alias=list_accounts())`)** |
|---|---|---|
| 계좌 목록을 읽는 시점 | **dag-processor가 파싱할 때**(기본 30초마다, F §1.2(a)) | **DAG run 실행 중 태스크 안**에서 |
| 계좌 추가·삭제 시 | 다음 파싱부터 태스크가 생기거나 사라진다. **새 DAG 버전**이 생긴다(3.x: 직렬화 해시 변경). | 다음 run부터 매핑 개수가 바뀐다. **DAG 구조는 그대로라 버전이 바뀌지 않는다.** |
| task_id 안정성 | 별칭이 바뀌면 task_id가 바뀌어 이력이 끊긴다(handoff.md §2의 연금 분리 때와 같은 현상). | task_id `collect_account`는 영구 고정이다. 계좌는 `map_index`로 구분하고, UI에는 `map_index_template`으로 렌더링된 별칭이 표시된다. |
| UI 가독성 | 태스크 이름에 계좌가 바로 보인다. | 매핑된 인스턴스 목록에 `kis_stock`, `irp` …가 보인다(Named mapping, 2.9+). |
| 계좌별 재시도 | 해당 task clear | 해당 map index clear(UI에서 인스턴스 선택). 동등하다. |
| YAML이 깨졌을 때 | **DAG import error → stale → run 없음 → 알림 없음**(§4.1) | `list_accounts` 태스크 실패 → `on_failure_callback` → **Discord 알림** |
| 파싱 시점 비밀 노출 위험 | 파싱 코드가 secrets 파일을 열어야 한다(별칭만 쓰더라도 dag-processor에 파일 마운트가 필수가 되고, 실수 한 번이면 직렬화 DAG로 들어간다). | 파싱 코드는 파일을 열지 않는다. |
| 비밀 아닌 목록 파일을 git에 따로 두는 변형(A') | 목록은 git, 값은 secrets → **계좌를 추가할 때 두 파일을 고쳐야 한다**(목표 위반). | — |
| 업스트림 실패 격리 | 태스크별 독립 | 매핑 인스턴스별 독립(한 계좌 실패가 다른 계좌를 막지 않음) |
| 동시성 제어 | 개별 설정 | `max_active_tis_per_dagrun`으로 KIS 동시 호출 수를 한 번에 제한 |
| 2.10.4 지원 | 지원 | `expand`(2.3+), `map_index_template`(2.9+) 모두 지원 |

**권장: (B) 동적 매핑.** 결정적 이유는 두 가지다. 첫째, 사용자 목표("YAML만")를 만족하면서 파싱 시점에 secrets 파일을 전혀 열지 않는 유일한 방식이다. 둘째, YAML 파손이 **조용한 정지**(§4.1)가 아니라 **알림이 가는 태스크 실패**가 된다.

#### 파싱 시점에 secrets를 읽으면 안 되는 이유 (구체적으로)

1. **직렬화 DAG에 들어간다.** 파싱 시점에 평가된 값(Param 기본값·`enum`·`description`, `doc_md`, operator 인자 `op_args`/`op_kwargs`, `expand`의 리터럴 리스트, task_id, `map_index_template` 원문)은 직렬화 JSON으로 메타DB `serialized_dag`에 저장되고 UI의 DAG 상세·코드 화면과 REST API로 노출된다. 현재 `ACCOUNT_CODE_ENUM`과 Param description의 계좌번호가 정확히 이 경로로 메타DB에 들어가 있다.
2. **3.x는 DAG 버전별로 직렬화본을 보존한다.** 구조가 바뀔 때마다 새 `dag_version`이 생기고, 옛 run은 자기 버전을 가리킨다. 그래서 나중에 코드에서 계좌번호를 지워도 **과거 버전의 직렬화본에 계속 남는다.** 지우려면 `airflow db clean` 수준의 정리가 필요하다.
3. `FERNET_KEY`는 Variable/Connection만 암호화하고 직렬화 DAG는 암호화하지 않는다. 따라서 D §3의 메타DB 덤프에도 평문으로 들어간다.
4. 파싱은 30초마다 반복되므로 YAML 오류가 곧 import error가 된다(§4.1).

→ **원칙**: 파싱 시점 코드는 secrets 파일을 import하거나 열지 않는다. 별칭조차 런타임에 얻는다. 검증 CLI(§5)에 "DAG 파일이 `asset_flow.config.loader`를 모듈 최상위에서 호출하지 않는지" 정적 검사를 넣을 수도 있다(선택).

#### 목표 DAG 골격 (Airflow 3 SDK, 초안 — 저장소에는 쓰지 않음)

```python
# dags/fetch_asset_daily_dag.py (발췌)
from airflow.sdk import dag, task, get_current_context

@dag(dag_id="fetch_asset_daily", schedule="0 7 * * *", ...)
def fetch_asset_daily_dag():
    dates = get_standard_date()

    @task
    def list_accounts() -> list[str]:
        """YAML을 읽어 활성 수집 대상 별칭만 반환한다. 계좌번호는 XCom에 넣지 않는다."""
        from asset_flow.config.loader import load_config     # 검증 실패 → 예외 → 태스크 실패 → Discord
        cfg = load_config()
        return sorted(a.alias for a in cfg.enabled_accounts())  # 정렬 → map_index 순서 안정화

    @task(
        map_index_template="{{ account_alias }}",
        max_active_tis_per_dagrun=4,                 # KIS 동시 호출 제한
        retries=3, retry_delay=timedelta(seconds=20), retry_exponential_backoff=True,
    )
    def collect_account(alias: str, dates: dict) -> dict:
        get_current_context()["account_alias"] = alias   # Named mapping 표시명
        from asset_flow.config.loader import load_config
        from asset_flow.pipeline.dispatch import run_account
        acct = load_config().account(alias)            # 여기서 계좌번호·키를 mask_secret 등록
        n = run_account(acct, dates)                   # 수집 → 0건 검사 → (standard_date, account_code) DELETE+INSERT
        return {"alias": alias, "rows": n}             # XCom에는 별칭과 건수만

    [wait_for_exchange_rate, wait_for_fund_price_daily] >> dates
    collect_account.partial(dates=dates).expand(alias=list_accounts())
```

- **get/upload 분리를 하나로 합친 이유**: 지금의 `upload_account(trigger_rule=ALL_DONE)`는 "get이 최종 실패하면 payload가 None이라 DELETE하지 않는다"는 성질에 기대고 있다(`fetch_asset_daily_dag.py:418-433`). 매핑 태스크 그룹(`@task_group` + `expand`)으로 짝을 유지할 수도 있지만, 실패한 매핑 인스턴스 다음에 오는 ALL_DONE 하류가 None을 받는지는 실측해 봐야 한다. 한 태스크 안에서 "수집 → 0건이면 예외 → 그다음에만 DELETE+INSERT" 순서로 처리하면 **같은 불변식**(빈 결과로 기존 적재분을 지우지 않음, 계좌당 하나의 writer)을 코드 구조로 보장한다. 재시도하면 수집부터 다시 하지만, 수집은 조회 API라 멱등이다.
- 부수 효과: 계좌번호와 records가 XCom에 저장되지 않는다(§1 부수 사항 해결).
- 실패 알림: D §5.2 콜백 embed에 `ti.rendered_map_index`(3.x RuntimeTaskInstance 필드, 소스 확인)를 `계좌` 필드로 추가하면 "`collect_account` [pension_deductible] 실패"처럼 계좌가 보인다.

### 2.4 로더와 마스킹

```python
# plugins/asset_flow/config/loader.py (초안)
"""secrets/asset_flow.yaml 로더. 반드시 태스크·콜백·CLI 실행 중에만 호출한다(파싱 시점 금지)."""
from __future__ import annotations
import dataclasses, datetime as dt, hashlib, os, re
from pathlib import Path
import yaml

PATH = Path(os.getenv("ASSET_FLOW_SECRETS", "/opt/airflow/secrets/asset_flow.yaml"))
KINDS = {"kis_brokerage", "kis_isa", "kis_pension", "kis_cma", "upbit", "ledger_fund", "ledger_cash"}
ALIAS_RE = re.compile(r"^[a-z][a-z0-9_]{1,40}$")

class ConfigError(ValueError):
    """검증 실패. 메시지에 비밀 값을 넣지 않는다(키 경로만)."""

def _mask(*values):
    try:                                            # 3.x
        from airflow.sdk.log import mask_secret     # 소스 확인: task-sdk 1.3.2 airflow/sdk/log.py:250
    except ImportError:                             # 2.10.4
        from airflow.utils.log.secrets_masker import mask_secret
    for v in values:
        if v:
            mask_secret(str(v))

def fingerprint(appkey: str, secret: str) -> str:
    """키 지문 — 토큰 캐시 무효화용. 원문 복원 불가(sha256), 16자로 충분."""
    return hashlib.sha256(f"{appkey}\0{secret}".encode()).hexdigest()[:16]

def load_config(path: Path = PATH, *, mask: bool = True) -> "AssetFlowConfig":
    with path.open(encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    cfg = AssetFlowConfig.parse(raw)               # 구조·참조·형식 검증 → ConfigError
    if mask:
        for c in cfg.credentials.values():
            _mask(c.appkey, c.secret)
        for a in cfg.accounts.values():
            _mask(a.account, a.account_code if a.kind != "upbit" else None)
    return cfg
```

- **마스킹 범위**: 3.x `mask_secret`은 태스크 프로세스와 supervisor 양쪽에 등록된다(소스 확인: `SUPERVISOR_COMMS.send(MaskSecret(...))`). 계좌번호와 `account_code`를 등록하면 H16의 디버그 출력, `base_client.safe_request`의 에러 로그, 예외 메시지에서 `***`로 바뀐다. 짧은 값(`"01"`, `"22"`)은 마스킹하면 로그 전체가 망가지므로 **상품코드는 등록하지 않는다.**
- 업무 DB `account.asset_daily.account_code`에는 계좌번호 파생값이 그대로 저장된다. DB 키이므로 피할 수 없다. "비밀"의 범위를 **git·직렬화 DAG·XCom·로그·알림에서 제외**하는 것으로 정의한다. DB 키까지 대체키(별칭)로 바꾸려면 §3.3의 마이그레이션이 필요하며, 이 보고서는 권장하지 않는다(대시보드·원장 전부 영향).
- `version: 1` 필드를 두어 나중에 스키마가 바뀌면 로더가 명확한 오류를 내게 한다.

### 2.5 계좌 유형별 처리 함수 디스패치

```python
# plugins/asset_flow/pipeline/dispatch.py (초안)
HANDLERS = {
    "kis_brokerage": collect_kis_brokerage,   # 기존 get_stock_account: 해외 + 국내
    "kis_isa":       collect_kis_domestic,    # 기존 get_isa_stock
    "kis_pension":   collect_kis_pension,     # 기존 get_pension_assets, fund_code는 계좌에서
    "kis_cma":       collect_kis_cma,         # 기존 get_cma_cash
    "upbit":         collect_upbit,           # 기존 get_upbit_assets
    "ledger_fund":   collect_ledger_fund,     # 기존 get_irp/dc_assets, iloc[0] 대신 account_code로 선택
    "ledger_cash":   collect_ledger_cash,     # 기존 get_savings/housing_assets
}

def run_account(acct, dates) -> int:
    df = HANDLERS[acct.kind](acct, dates)     # 각 함수는 기존 본문을 그대로 옮긴 것(config dict 모양 유지)
    if df.empty:
        raise AirflowException(f"[{acct.alias}] 데이터 없음 — 빈 응답")   # 기존 불변식
    return delete_and_insert_account_assets(..., account_code=acct.account_code, records=df.to_dict("records"))
```

- 핸들러에 넘기는 `config`는 지금 Variable과 **같은 모양의 dict**(`appkey`, `secret`, `account`, `product_code`, `type`)로 만든다. 그러면 `KISApiClient`와 `kis_transformer`를 고치지 않아도 된다(F §1.4와 같은 전략, `type` ← `name`).
- 토큰은 `TokenManager().token_for(acct.credential)`로 얻는다(§3.1). CMA는 YAML상 `credential: kis_isa`이므로 자동으로 ISA 토큰을 쓴다.
- **새 계좌 유형**(예: KIS 금현물 `KIS_GOLD`)이 생기면 핸들러 추가라는 코드 변경이 필요하다. 이것은 "YAML만"의 범위 밖이 맞다. 같은 유형의 계좌를 늘리거나 줄이는 것만 YAML로 흡수한다.
- `kind`와 `account_master.account_type` 대응은 코드 상수로 검증한다(`ledger_fund` ↔ IRP/DC, `ledger_cash` ↔ INSTALLMENT_SAVINGS/HOUSING_SUBSCRIPTION).

### 2.6 나머지 DAG 처리

| DAG | 변경 |
|---|---|
| `make_token` | `TokenGenerator()` → `ensure_tokens(required_credentials(cfg))`. 자격증명별로 발급하며, 일부가 실패하면 성공분은 저장하고 마지막에 실패 목록과 함께 예외를 던진다. 매핑까지 할 필요는 없다(발급은 순차가 안전함, 1분 제한). |
| `fetch_exchange_rate` | `cfg.market_data.credential`의 토큰·키를 쓴다. |
| `fetch_fund_price_daily` | 크롤 대상 = `{a.fund_code for a in enabled accounts if a.fund_code}` ∪ Param `extra_fund_codes`(기본 `[]`). 런타임 태스크에서 계산한다. |
| `upsert_manual_position` | Param `account_alias`(자유 문자열, 기본 `"irp"`, description은 "secrets YAML의 원장 계좌 별칭")로 바꾼다. `validate`에서 `cfg.account(alias)`로 `account_code`·`fund_code`·`multiplier`를 해석한다. 없는 별칭이면 **유효 별칭 목록을 담은 예외**를 던진다. `ACCOUNT_CODE_ENUM`은 삭제한다. UI 드롭다운은 사라지지만, 별칭은 사람이 외울 수 있는 짧은 값이다. 드롭다운이 꼭 필요하면 enum을 **유형/별칭 코드 상수**로 두는 절충안이 있으나 계좌를 추가할 때 코드를 고쳐야 한다. |
| `rebuild_derived_assets` | `rebuild_account.expand(alias=list_ledger_accounts())`. 날짜 루프 본문은 그대로 둔다. |
| `ops_check_credentials`(신규) | §2.7 |
| `account_master` 동기화(선택) | 원장 계좌의 `account_code`·`account_name`·`account_type`·`is_active`를 YAML 기준으로 upsert하는 `sync_account_master` 태스크를 `ops_check_credentials`에 둔다. 그러면 원장 계좌를 추가할 때 DB를 직접 조작할 필요가 없다. **`account_master`의 전체 컬럼과 제약은 저장소에 DDL이 없어 확인하지 못했다**(§7). KIS 계좌가 `account_master`에 있는지도 미확인이다. |

### 2.7 만료일 저장과 2주 전 알림

**DAG `ops_check_credentials`**(매일 09:00 KST, 07시 체인과 분리. 토큰 발급 실패와 알림 경로를 서로 독립시키기 위함)

```python
@task
def check_expiry():
    cfg = load_config()
    today = pendulum.now("Asia/Seoul").date()
    warn = cfg.alerts.expiry_warn_days                       # 14
    rows = []
    for alias, c in cfg.credentials.items():
        if not cfg.is_referenced(alias):                     # 활성 계좌·시장데이터가 안 쓰는 키는 제외
            continue
        days = (c.expires_on - today).days
        if days <= warn:
            rows.append((alias, c.provider, c.expires_on, days))
    upbit_mismatch = cross_check_upbit(cfg)                  # 아래
    if rows or upbit_mismatch:
        post_expiry_embed(rows, upbit_mismatch)              # D §5.2 _post_discord 재사용, 1건으로 묶음
    if any(d < 0 for *_, d in rows):
        raise AirflowException("만료된 자격증명 있음 — secrets YAML 갱신 필요")  # 빨간 실패 알림도 발생
```

| 남은 일수 | 동작 | 색 |
|---|---|---|
| 15일 이상 | 없음 | — |
| 14 ~ 1 | **매일 1건**(모든 해당 키를 한 embed에 필드로 묶음). 제목 예: "🔑 자격증명 만료 임박 2건" | 노랑 `0xFBC02D` |
| 0(당일) | 위와 같고 제목에 "오늘 만료" | 주황 |
| 음수(만료) | 알림 + **태스크 실패**(D의 실패 알림이 한 번 더 감) | 빨강 |

- **매일 보내는 이유**: 사람이 갱신해 YAML의 `expires_on`을 고치는 순간 알림이 멈추므로, "갱신 완료"를 확인하는 장치가 된다. D-14, 7, 3, 1에만 보내는 방식은 놓치기 쉽다. 하루 1건이라 소음도 작다.
- **Upbit 교차 검증**: Upbit 공식 API `GET /v1/api_keys`는 계정의 모든 키에 대해 `access_key`와 `expire_at`(KST, `yyyy-MM-dd'T'HH:mm:ss+09:00`)을 돌려준다. 별도 권한 없이 JWT 인증만 필요하다. YAML의 `appkey`와 같은 `access_key` 항목을 찾아 `expire_at` 날짜가 `expires_on`과 다르면 "YAML 만료일 불일치"를 알린다. **Upbit은 사실상 자동 확인이 된다.** 다만 공식 문서의 필드 설명에 "Deprecated일시"라는 표현이 섞여 있어, 첫 실행에서 포털에 표시된 만료일과 같은지 확인해야 한다.
- **KIS**: appkey 자체의 만료일을 조회하는 공식 API를 찾지 못했다. 토큰 발급 응답의 `access_token_token_expired`는 **토큰**(약 1일) 만료일이지 appkey 만료일이 아니다. 그래서 KIS는 사람이 포털에서 보고 적은 `expires_on`을 신뢰원으로 쓴다. 보조 장치로, 토큰 발급이 인증 오류로 실패하면 make_token의 예외 메시지에 "`expires_on`={값}, 만료 여부 확인"을 덧붙인다.
- `expires_on`이 없거나 날짜 형식이 아니면 검증 실패(로드 단계)로 처리한다. 만료일 없는 키를 허용하면 알림이 조용히 빠지기 때문이다. 정말 만료가 없는 키라면 `expires_on: never`를 명시하게 한다.

---

## 3. 키 교체·계좌 변경 시 함정

### 3.1 토큰 캐시가 옛 키 토큰을 계속 쓰는 문제

**현재 동작(코드 근거)**

1. `TokenGenerator()`는 `data/tokens/{오늘}_token.json`이 있으면 즉시 return한다(`token_manager.py:116-117`).
2. `GetTokens()`는 키 **이름** 집합만 검사한다(`:161`). 값이 어떤 appkey로 발급됐는지는 모른다.
3. `KISApiClient._build_headers`는 `authorization: Bearer {파일의 토큰}`과 `appKey`/`appSecret: {현재 config}`를 **함께** 보낸다(`kis_client.py:31-39`).

→ 오전에 YAML의 KIS appkey를 새 키로 바꾸면, 그날 남은 시간 동안(그리고 다음 날 06:50 전까지) 모든 KIS 호출이 "새 appkey + 옛 키로 발급된 토큰" 조합이 된다. 토큰은 발급한 appkey에 묶여 있으므로(KIS 공식 예제는 appkey별로 토큰을 받아 저장한다) 인증 오류가 날 것으로 예상된다. **재시도 3회도 같은 이유로 실패하고, handoff.md §7의 "토큰 파일을 수동 삭제" 조치가 필요해진다.** 키가 유출돼 급히 교체해야 할 때 가장 나쁜 방식으로 실패하는 셈이다.

**해결: 자격증명별 지문 저장 + 불일치하는 것만 재발급**

토큰 파일 형식(날짜별 파일 대신 단일 파일, 만료 시각 기준):

```json
{
  "format": 2,
  "credentials": {
    "kis_stock": {"token": "eyJ…", "fp": "3f9a1c0b7e2d4a51", "issued_at": "2026-10-13T06:50:02+09:00",
                  "expires_at": "2026-10-14T06:50:01+09:00"},
    "kis_isa":   {"token": "…", "fp": "…", "issued_at": "…", "expires_at": "…"}
  }
}
```

```python
# TokenManager.token_for(alias) — 개념 코드
def token_for(self, alias: str) -> str:
    cred = self.cfg.credentials[alias]
    if cred.provider == "upbit":
        return make_upbit_jwt(cred)                 # 발급 API 없음 → 캐시하지 않고 사용 시점 서명
    fp = fingerprint(cred.appkey, cred.secret)
    entry = self._read().get(alias)
    if self._valid(entry, fp):                      # fp 일치 + expires_at까지 1시간 이상 남음
        return entry["token"]
    if not ISSUE_ENABLED:                           # 섀도(그린) 모드: 발급 금지 (E §4.3, §6.2)
        raise AirflowException(f"[{alias}] 유효 토큰 없음, TOKEN_ISSUE_ENABLED=false")
    with self._lock():                              # fcntl.flock(data/tokens/.lock, LOCK_EX)
        entry = self._read().get(alias)             # ★ 잠금 획득 후 재확인 — 다른 매핑 인스턴스가 방금 발급했을 수 있음
        if self._valid(entry, fp):
            return entry["token"]
        token, expires_at = issue_kis_token(cred)   # EGW00133(1분 1회) 응답이면 61초 대기 후 1회 재시도
        self._write_atomic(alias, token, fp, expires_at)   # tmp 파일 → os.replace
        return token
```

| 설계 요소 | 이유 |
|---|---|
| **지문 = sha256(appkey‖secret) 앞 16자** | 토큰 파일에 키 원문을 두지 않으면서 교체를 감지한다. appkey(36자)와 secret(약 180자)은 엔트로피가 커서 해시로 역산할 수 없다. secret만 바꿔도 감지된다. |
| **자격증명 단위로 무효화** | 키 하나를 교체해도 나머지 4개 KIS 토큰은 재발급하지 않는다. 불필요한 발급 알림톡과 1분 제한 노출이 줄어든다. |
| **파일 잠금 + 잠금 후 재확인** | 동적 매핑으로 `collect_account`가 최대 `max_active_tis_per_dagrun`개 동시에 돈다. 같은 자격증명(예: ISA와 CMA가 공유하는 `kis_isa`)을 쓰는 두 인스턴스가 **동시에** 불일치를 감지하면 둘 다 발급을 시도해 두 번째가 1분 제한에 걸린다. LocalExecutor는 같은 컨테이너의 프로세스들이므로 `flock`으로 충분하다. |
| **KIS 1분 1회 제한** | 공식 예제 주석에는 "유효기간 1일, 6시간 이내 발급 신청 시 기존 토큰 값과 동일, 발급 시 알림톡 발송"만 있고, 1분 제한은 제3자 자료(E §11-1)에만 있다. 교체 직후에는 새 appkey로 처음 발급하므로 제한과 충돌하지 않는다. 문제는 같은 새 키를 1분 안에 두 번 발급하는 경우뿐이고, 위 잠금이 이를 막는다. 방어적으로 제한 응답에는 61초 대기 후 1회만 재시도한다. |
| **만료 시각 기준** | 날짜 파일명 방식은 06:50 발급 토큰을 자정에 버리고 다음 날 다시 발급한다. KIS 응답의 `access_token_token_expired`를 저장하면 "남은 시간 1시간 미만이면 갱신"으로 일관되게 처리할 수 있다. `make_token`의 06:50 사전 발급은 그대로 둔다(07시 체인 전에 실패를 드러내는 역할). |
| **원자적 쓰기** | 쓰는 도중에 다른 프로세스가 읽어 JSON 파싱 오류가 나는 것을 막는다(`os.replace`). |
| **Upbit은 캐시하지 않음** | Upbit JWT는 발급 API 호출이 아니라 로컬 서명이다(`token_manager.py:98-107`). 서명된 JWT를 파일에 두면 **파일 유출 = 키 유출**이 된다(만료 claim 없음). 사용 시점에 서명하면 키 교체가 즉시 반영된다. 요청마다 새 `nonce`로 서명하는 것이 Upbit 인증 방식의 취지에 맞다. 현재 하루 동안 같은 JWT를 재사용하는 동작이 허용되는지는 공식 문서로 확인하지 못했다. |
| **옛 형식 파일 호환** | `format` 키가 없는 옛 파일은 "모든 항목 지문 없음 = 불일치"로 간주해 자연스럽게 재발급한다. **단, 섀도 그린이 블루 파일을 RO로 읽는 기간에는 이 규칙 때문에 발급을 시도하게 된다** → §6.2에서 순서로 해결한다. |

**교체 후 옛 키 처리**: KIS·Upbit 포털에서 옛 키를 즉시 폐기하는 것이 원칙이다. 다만 교체 직후 첫 수집이 성공하는 것을 확인한 뒤에 폐기한다(검증 CLI의 `--online` 옵션, §5).

### 3.2 계좌 추가·삭제

| 시나리오 | 목표 구조에서의 동작 | 주의 |
|---|---|---|
| 같은 유형의 계좌 추가(예: 두 번째 ISA) | YAML에 `credentials`(새 키라면)와 `accounts`를 추가 → validate → 다음 run에 매핑이 1개 늘어나고, 새 자격증명은 첫 사용 시 발급된다. | 처음 수집하는 날 `asset_daily`에 새 `account_code` 행이 생긴다. 과거 소급은 API 계좌라면 불가능하다(`rebuild_derived_assets` 독스트링에 적힌 현행 제약과 같음). 원장 계좌라면 `upsert_manual_position` 후 `rebuild_derived_assets`로 소급한다. |
| 계좌 해지 | `enabled: false`로 둔다. 매핑에서 빠지고, 참조가 없어진 자격증명은 발급과 만료 알림에서도 빠진다. | **과거 `asset_daily` 행은 그대로 남는다**(DELETE 스코프가 `(standard_date, account_code)`이므로 다른 날짜와 계좌는 건드리지 않는다). "최신 기준일의 전 계좌 합계" 같은 쿼리에서는 자연스럽게 빠진다. 원장 계좌는 `account_master.is_active=false`도 맞춘다(동기화 태스크가 있으면 자동). |
| YAML에서 계좌 항목 자체를 삭제 | 해지와 같지만 별칭 기록이 사라진다. | 같은 별칭을 나중에 다른 계좌에 재사용하면 UI 매핑 이력이 헷갈린다. **삭제보다 `enabled: false`를 권장한다.** |
| 별칭 변경 | DB에는 영향이 없다(DB 키는 `account_code`). UI의 매핑 이름만 바뀐다. | `upsert_manual_position`을 실행할 때 쓰는 별칭이 바뀌므로 README 사용법을 갱신한다. |
| 계좌 순서 변경 | `list_accounts`가 정렬하므로 map_index 순서는 별칭 사전순으로 고정된다. 계좌를 추가하면 뒤쪽 인덱스가 밀린다. | UI는 렌더링된 이름으로 보므로 문제없다. map_index 숫자로 이력을 추적하지 않는다. |

### 3.3 계좌번호 변경과 `account_code` 연속성 (코드 확인 결과)

**업무 DB 키는 계좌번호 기반이다.**

- KIS 계좌: `account_code = f"{config['account']}-{config['product_code']}"`. `transform_domestic_balance`, `transform_overseas_balance`, `transform_pension_fund_balance`, `transform_cma_cash_balance`(`kis_transformer.py:54`, `:92`, `:153`, `:221`)와 DAG의 `get_*`(`fetch_asset_daily_dag.py:183`, `:211`, `:243`, `:285`)가 같은 식으로 만든다. `delete_and_insert_account_assets`의 DELETE 스코프도 이 값이다(`db_manager.py:107`).
- Upbit: `config.get('account', 'UPBIT')`(`fetch_asset_daily_dag.py:304`). 계좌번호가 아니라 고정 문자열이다.
- 원장 계좌: `manual_position_ledger.account_code`와 `account_master.account_code`(JOIN 키, `db_manager.py:75-76`). 값은 실제 계좌번호 형식이다(예: `'<계좌번호>-<상품코드>'`, `'<은행 계좌번호>'`, `upsert_manual_position_dag.py:37-42`).

**따라서 YAML에서 계좌번호를 바꾸면 다음 run부터 새 `account_code`로 적재된다.** 옛 코드의 시계열은 그 날짜에서 끝나고 새 코드의 시계열이 시작된다. 바뀐 날 같은 날짜에 두 코드가 함께 생기지는 않는다(옛 코드는 더 이상 쓰이지 않음).

| 상황 | 권장 처리 |
|---|---|
| **실제로 다른 계좌**(해지 후 신규 개설) | 그대로 둔다. 시계열이 끊기는 것이 사실과 맞다. 옛 계좌는 `enabled: false`로 별도 항목에 남긴다. |
| **같은 계좌의 번호 정정**(오타, 은행 측 번호 체계 변경) | 1회성 마이그레이션을 한다. ① YAML을 고치기 **전에** 같은 트랜잭션에서 `UPDATE account.asset_daily SET account_code = :new WHERE account_code = :old;`를 실행하고, 원장 계좌라면 `manual_position_ledger`와 `account_master`도 함께 바꾼다(FK·PK 제약에 따라 순서 조정. DDL 미확인). ② YAML을 고친다. ③ 다음 run의 DELETE 스코프가 새 코드로 정상 동작하는지 확인한다. 이 작업은 드물기 때문에 DAG로 자동화하지 않고 런북 SQL로 둔다. |
| 번호는 바뀌었지만 DB 키를 유지하고 싶다 | YAML에 `account_code: "<옛 값>"`을 명시해 파생을 끈다(§2.2). 이렇게 하면 API 호출에는 새 번호를 쓰고 DB 키는 옛 값을 유지한다. 그러나 **DB 키와 실제 번호가 달라져 나중에 혼동하기 쉽다.** 마이그레이션 SQL이 가능하면 그쪽을 우선한다. |
| 상품코드(`product_code`)만 변경 | 계좌번호 변경과 같다(`account_code`에 포함되므로). |

검증 CLI는 **변경 전 YAML(백업)과 비교해 `account_code`가 바뀐 계좌가 있으면 경고**를 출력한다. "의도한 번호 변경인가? 마이그레이션 SQL을 먼저 실행했는가?"를 묻는 것이다(§5).

---

## 4. YAML이 깨졌을 때의 실패 모드

### 4.1 파싱 시점 오류 = 조용한 정지 (3.3.2 소스 확인)

`airflow/dag_processing/collection.py::_update_import_errors`(core 3.3.2)를 보면:

1. 파일 파싱 중 예외가 나면 `ParseImportError` 행을 만들거나 갱신한다.
2. 그다음 해당 파일의 DAG에 대해 `UPDATE dag SET has_import_errors=True, is_stale=True`를 실행한다.
3. 스케줄러가 run을 만들 DAG를 고르는 쿼리(`models/dag.py`의 `dags_needing_dagruns` 계열)는 `is_stale == false`만 대상으로 한다.

→ **run이 생기지 않으므로 태스크도 실패 콜백도 없다.** UI 상단의 import error 배너 말고는 신호가 없다. 07시 체인이 다음 날 아침에야 "데이터가 없다"는 형태로 발견된다. 현재 2.10.4의 `DAGS_ARE_PAUSED_AT_CREATION` 함정(F §2.6)과 같은 종류의 "알림 없는 정지"다.

이 보고서의 설계에서 이 경로가 열리는 경우는 **DAG·플러그인 코드 자체의 버그**뿐이다. YAML은 파싱 시점에 읽지 않기 때문이다. 그래도 코드 배포 실수는 생기므로 아래 4.2의 감시를 둔다.

### 4.2 import error를 Discord로 알리는 수단 (3.x)

| 수단 | 동작 위치 | 장점 | 단점 | 판정 |
|---|---|---|---|---|
| **(a) 리스너 플러그인 `on_new_dag_import_error` / `on_existing_dag_import_error`** | dag-processor(매니저 프로세스, `manager.py:1345` → `update_dag_parsing_results_in_db` → `_update_import_errors`) | 3.3.2 코어에 훅이 있다(소스 확인: `airflow/listeners/spec/importerrors.py`). `integrate_listener_plugins`로 플러그인의 `listeners`가 등록된다. 별도 DAG나 API 인증이 필요 없고 즉시 알린다. | `on_existing_*`는 **이미 있는 오류 파일을 다시 파싱할 때마다** 호출된다(기본 30초). 중복 제거가 필수다. 매니저 루프 안(DB 세션 중)에서 동기 실행되므로 HTTP timeout을 짧게(3초) 잡는다. 훅 예외는 `log.exception("error calling listener")`로 삼켜진다(소스 확인). | **권장** |
| (b) 감시 DAG가 REST API `GET /api/v2/importErrors`를 폴링 | 태스크 | 2.x/3.x 공통 개념 | 3.x 태스크는 ORM을 못 쓰므로 API 사용자와 JWT 발급이 필요하다. 감시 DAG 자체가 import error면 무력하다. 폴링 주기만큼 지연된다. | 보조 |
| (c) 호스트 cron이 `airflow dags list-import-errors -o json`을 실행하고 결과가 있으면 curl로 Discord 전송 | 호스트 | Airflow가 반쯤 죽어도 동작한다. | Airflow 밖 구성요소가 늘어난다. | 선택(아래 (d)와 함께) |
| (d) 07:30 "데이터 도착" 외부 체크 | 호스트 cron | import error, paused, 스케줄러 정지 등 **원인과 무관하게 "결과 없음"을 잡는다.** | 원인을 알려 주지 않는다. | 권장(최후 방어선) |

**(a) 초안**

```python
# plugins/import_error_alert.py
"""DAG import error → Discord. dag-processor 프로세스에서 실행된다."""
import hashlib, json, os, time
from pathlib import Path
from airflow.listeners import hookimpl
from airflow.plugins_manager import AirflowPlugin

_STATE = Path(os.getenv("IMPORT_ERROR_ALERT_STATE", "/opt/airflow/logs/.import_error_alert.json"))
_REMIND_SEC = 6 * 3600          # 같은 오류는 6시간마다 재알림

def _should_send(filename: str, stacktrace: str) -> bool:
    key = hashlib.sha256(f"{filename}\0{stacktrace}".encode()).hexdigest()
    state = json.loads(_STATE.read_text()) if _STATE.exists() else {}
    if time.time() - state.get(key, 0) < _REMIND_SEC:
        return False
    state[key] = time.time()
    _STATE.write_text(json.dumps(state))
    return True

def _notify(filename, stacktrace, new: bool):
    try:
        if not _should_send(filename, stacktrace):
            return
        from callbacks import post_discord_simple    # D §5.2 _post_discord 래퍼, timeout (3,3), 재시도 1회
        post_discord_simple(
            title=f"🟠 DAG import error{' (신규)' if new else ''}: {Path(filename).name}",
            description="```\n" + stacktrace[-1500:].replace("```", "'''") + "\n```",
            color=0xFB8C00,
        )
    except Exception:
        pass                                          # 리스너는 절대 예외를 내지 않는다

class _Listener:
    @hookimpl
    def on_new_dag_import_error(self, filename, stacktrace):
        _notify(filename, stacktrace, new=True)

    @hookimpl
    def on_existing_dag_import_error(self, filename, stacktrace):
        _notify(filename, stacktrace, new=False)

class ImportErrorAlertPlugin(AirflowPlugin):
    name = "import_error_alert"
    listeners = [_Listener()]
```

- **webhook URL 출처**: dag-processor 매니저 프로세스는 태스크 실행 컨텍스트가 아니다. 따라서 D §5.1의 `.env` `AIRFLOW_CONN_DISCORD_ALERT`(환경변수)를 직접 파싱하는 경로가 가장 확실하다. 이 경로도 secrets YAML과는 무관하다.
- 복구 알림("import error 해소")이 필요하면 `ParseImportError`가 사라지는 시점에 대한 훅이 없으므로 (b) 또는 (c)로 처리한다. 필수는 아니다.
- 2.10.4: 같은 이름의 리스너 훅이 2.10 계열에 추가됐다고 알려져 있지만 **이번에 소스로 확인하지는 않았다.** Phase 0에서 도입하려면 2.10.4 wheel로 확인한 뒤 결정한다. 3.x에서만 도입해도 이 설계상 위험은 작다(YAML은 파싱과 무관하므로).

### 4.3 런타임 검증 실패 → 태스크 실패 → 알림

| 실패 | 어디서 드러나는가 | 알림 |
|---|---|---|
| YAML 문법 오류, 파일 없음, 권한 오류 | 각 DAG의 첫 런타임 태스크(`list_accounts`, `generate_tokens`, `check_expiry`)에서 `ConfigError`/`OSError` | D의 `on_failure_callback` → 🔴 Discord. **알림 경로(Connection `discord_alert`)는 YAML과 무관하므로 반드시 도달한다.** |
| 참조 오류(없는 `credential`, 알 수 없는 `kind`), 중복 별칭·`account_code` | 같음(로드 시 검증) | 같음 |
| 키가 틀림, 만료됨 | `make_token`(06:50) 발급 실패 | 같음. 메시지에 `expires_on` 힌트 포함 |
| 계좌번호가 틀림 | 해당 매핑 인스턴스(KIS 응답 오류 또는 0건 → 예외) | 해당 계좌 인스턴스만 실패. 나머지 계좌는 적재. 기존 적재분은 보존(빈 결과로 DELETE하지 않음) |
| 일부 계좌 항목만 깨짐 | 설계 선택: **로드 단계는 전체 실패**(하나라도 깨지면 `list_accounts` 실패)로 한다. | 부분 성공을 허용하면 "깨진 계좌가 조용히 빠지는" 실패가 생긴다. 하루 수집이 늦어지는 편이 낫다. 검증 CLI가 사전에 막으므로 실제로는 드물다. |
| 편집 중 반쯤 쓰인 파일을 읽음 | 원자적 교체 절차(§5)로 예방 | — |

**알림 독립성 원칙**: 실패 알림, 만료 알림, import error 알림 셋 다 **secrets YAML을 읽지 않고도** Discord URL을 얻어야 한다. 이 원칙이 깨지면 "YAML이 깨짐 → 알림도 깨짐 → 조용한 실패"가 된다. 만료 알림 태스크는 YAML을 읽지만, YAML이 깨지면 그 태스크의 **실패 콜백**이 알림을 보낸다.

---

## 5. 변경 반영 절차 (서버 운영 런북)

### 5.1 파일 배치와 권한

```text
~/oracle-airflow3/               # E의 그린(전환 후 운영) 디렉터리
├── secrets/                     # gitignore. 디렉터리째 RO 마운트 → /opt/airflow/secrets
│   ├── asset_flow.yaml          # 640, ${AIRFLOW_UID}:0
│   ├── asset_flow.example.yaml  # (커밋) 값 없는 템플릿 — 키 구조 문서 역할
│   └── backup/                  # 700. 교체 전 사본 보관 (컨테이너에 노출 불필요하면 별도 경로)
```

```yaml
# docker-compose (x-airflow-common에 추가)
environment:
  ASSET_FLOW_SECRETS: /opt/airflow/secrets/asset_flow.yaml
  TOKEN_DIR: /opt/airflow/data/tokens
volumes:
  - ./secrets:/opt/airflow/secrets:ro     # ★ 디렉터리 마운트. 파일 단위 마운트 금지
```

- **파일 단위 bind mount 금지**: `- ./secrets/asset_flow.yaml:/opt/...:ro`처럼 파일을 마운트하면 컨테이너는 마운트 시점의 inode를 본다. 호스트에서 `mv`로 원자적 교체를 하면 새 inode가 생기므로 **컨테이너는 옛 내용을 계속 읽는다**(Docker bind mount의 일반 동작). 편집기도 대부분 "새 파일에 쓰고 rename"하므로 똑같이 걸린다. 디렉터리를 마운트하면 이름 조회가 매번 일어나 즉시 반영된다.
- 마운트 대상: F §1.2(c) 함정 2와 같이 scheduler(LocalExecutor 태스크), dag-processor(DAG 레벨 콜백), api-server, triggerer. 공통 앵커로 모두 마운트한다.
- `.gitignore`에 `secrets/*`, `!secrets/*.example.yaml`을 추가한다(F §1.6과 같음). `git check-ignore -v secrets/asset_flow.yaml`로 확인한다.

### 5.2 검증 CLI: `python -m asset_flow.config.validate`

```bash
# 컨테이너 안에서 실행 (로더와 같은 코드·같은 Python 환경)
docker compose exec airflow-scheduler \
  python -m asset_flow.config.validate /opt/airflow/secrets/staging/asset_flow.yaml \
    --previous /opt/airflow/secrets/asset_flow.yaml [--online]
```

| 검사 | 실패/경고 | 비고 |
|---|---|---|
| YAML 문법, `version` | 실패 | 줄 번호를 표시하고 값은 출력하지 않는다. |
| 필수 필드, `kind` ∈ 허용 집합, `provider` ∈ {kis, upbit} | 실패 | |
| 별칭 정규식, 숫자 6자리 이상 연속 금지 | 실패 | 별칭으로 계좌번호가 새는 것을 막는다. |
| `credential` 참조 존재, `market_data.credential` 존재 | 실패 | |
| `account_code`(파생 포함) 중복 | 실패 | 계좌당 하나의 writer라는 불변식 보호(`fetch_asset_daily_dag.py:34-36`) |
| KIS `account` 8자리 숫자, `product_code` 2자리 숫자 | 실패 | CANO·ACNT_PRDT_CD 형식 |
| `fund_code` 필요한 kind(`kis_pension`, `ledger_fund`)에 존재, `multiplier > 0` | 실패 | |
| `expires_on` 날짜 또는 `never` | 실패 | |
| `expires_on`이 이미 지남 / 14일 이내 | 실패 / 경고 | |
| 같은 appkey가 두 자격증명에 중복 | 경고 | 1분 발급 제한을 공유하게 된다. |
| `--previous` 대비 `account_code`가 바뀐 계좌 | **경고 + 확인 프롬프트** | §3.3 마이그레이션 SQL을 먼저 실행했는지 묻는다. |
| `--previous` 대비 지문이 바뀐 자격증명 | 정보 | "다음 사용 시 재발급됨"을 안내한다. |
| 파일 권한(그룹/기타 쓰기 금지, 기타 읽기 금지) | 경고 | |
| `--online`: Upbit `GET /v1/api_keys`로 키 유효성과 `expire_at` 비교 | 실패/경고 | 조회 API이고 부작용이 없다. |
| `--online --kis`: 지문이 바뀐 KIS 자격증명만 토큰 발급 시험 | 실패 | **발급 1회 + 알림톡이 발생한다.** 얻은 토큰을 토큰 파일에 저장해 낭비를 없앤다. 기본값은 끈다. |

출력에는 별칭과 키 경로만 쓰고 값은 쓰지 않는다. 종료 코드는 0(통과), 1(실패), 2(경고만 있음)로 나눈다.

### 5.3 변경 절차

```bash
cd ~/oracle-airflow3/secrets
TS=$(date +%Y%m%d_%H%M%S)
install -m 600 asset_flow.yaml staging/asset_flow.yaml   # ① 사본 (staging/도 RO 마운트 아래라 컨테이너에서 검증 가능)
$EDITOR staging/asset_flow.yaml                           # ② 사본만 편집
docker compose exec airflow-scheduler python -m asset_flow.config.validate \
  /opt/airflow/secrets/staging/asset_flow.yaml --previous /opt/airflow/secrets/asset_flow.yaml   # ③ 검증
cp -p asset_flow.yaml backup/asset_flow.yaml.$TS          # ④ 원본 백업 (backup/는 700)
chmod 640 staging/asset_flow.yaml && chown "${AIRFLOW_UID:-50000}:0" staging/asset_flow.yaml
mv staging/asset_flow.yaml asset_flow.yaml                # ⑤ 같은 파일시스템 안 rename = 원자적
# ⑥ 반영 확인(선택): ops_check_credentials 수동 실행 → 만료 요약 1건 수신
```

- **반영 시점**: 로더는 태스크마다 파일을 새로 읽는다(프로세스 단위 `lru_cache`만 사용). 따라서 ⑤ 이후 시작하는 태스크부터 반영된다. 재시작은 필요 없다. 이미 실행 중인 run은 태스크별로 옛 값과 새 값이 섞일 수 있으므로 **06:45~07:15에는 편집하지 않는다.**
- **키 교체 시 추가 단계**: ⑤ 이후 `make_token`을 수동 실행해 새 키로 발급되는지 확인한다(지문 불일치 → 해당 자격증명만 재발급). 성공을 확인한 뒤 포털에서 옛 키를 폐기한다.
- **롤백**: `cp -p backup/asset_flow.yaml.<TS> staging/ && mv staging/asset_flow.yaml.<TS> asset_flow.yaml`. 토큰은 지문 불일치로 자동 재발급된다(옛 키가 아직 폐기되지 않았다면).
- **백업 보존**: 최근 10개 + 30일. `backup/`에는 평문 키가 있으므로 서버 밖으로 보낼 때는 D §3처럼 `age`/`gpg`로 암호화한다. 메타DB 덤프와 달리 이 파일은 작아서 비밀번호 관리자에 첨부 보관하는 방법도 있다.
- **실수 방지 장치**: ① 원본 직접 편집 금지(사본 → 검증 → mv), ② 원본은 640이고 쓰기 권한은 소유자만, ③ 검증 CLI의 `--previous` 비교로 `account_code` 변경을 경고, ④ `asset_flow.example.yaml`(커밋)로 구조를 문서화, ⑤ 편집 금지 시간대 준수.

---

## 6. 최종 아키텍처와 구현 순서

### 6.1 아키텍처 (텍스트 다이어그램)

```text
                       ┌──────────────────────────────────────────────────────────┐
  사람(서버 SSH) ────▶ │ secrets/asset_flow.yaml  (gitignore, 640, 디렉터리 RO 마운트) │
  사본 편집 → validate │  credentials{appkey,secret,expires_on}                   │
  → backup → mv        │  accounts{alias,kind,credential,account,product_code,…}  │
                       └───────────────▲──────────────────────────────────────────┘
                                       │ 태스크 실행 중에만 읽음 (파싱 시점 X)
                                       │ load_config() → 검증 → mask_secret(키·계좌번호)
   ┌───────────────────────────────────┼────────────────────────────────────────────┐
   │ Airflow 3.3.2 (LocalExecutor)     │                                            │
   │                                   │                                            │
   │ 06:50 make_token ── generate_tokens: required_credentials(cfg) 순회            │
   │                         │        └▶ TokenManager: 지문 비교 → 불일치만 재발급   │
   │                         ▼            (flock + 재확인, KIS 1분 1회 방어)         │
   │              data/tokens/tokens.json {alias: token, fp, expires_at}  (Upbit 제외)│
   │                         │                                                       │
   │ 06:55 fetch_exchange_rate (market_data.credential)   fetch_fund_price_daily     │
   │                         │                             (fund_codes = 계좌 fund_code 합집합)
   │                         ▼                                                       │
   │ 07:00 fetch_asset_daily                                                         │
   │   get_standard_date → list_accounts() ──▶ ["cma","dc","housing","irp",…] (별칭만)│
   │                         └▶ collect_account.expand(alias)  [map_index_template]  │
   │                              HANDLERS[kind] → 0건이면 예외 → DELETE+INSERT       │
   │                              (standard_date, account_code)                      │
   │                                                                                 │
   │ 09:00 ops_check_credentials: 만료 D-14~ 요약 1건, Upbit /v1/api_keys 교차검증   │
   │        (선택) sync_account_master                                               │
   │ 수동  upsert_manual_position(account_alias) / rebuild_derived_assets.expand     │
   │                                                                                 │
   │ dag-processor: ImportErrorAlertPlugin (on_new/existing_dag_import_error, 중복제거)│
   └──────────┬───────────────────────────┬──────────────────────────────────────────┘
              │ on_failure_callback        │ 만료 요약 / import error
              ▼                            ▼
        Connection discord_alert  (.env AIRFLOW_CONN_DISCORD_ALERT — YAML과 독립)
              │
              ▼
           Discord           + 호스트 cron 07:30 "오늘 asset_daily 도착?" 최후 방어선
```

### 6.2 구현 순서와 3.x 전환 계획(C/E)과의 결합

원칙은 세 가지다. ① 3.x와 무관하고 2.10.4에서 동작하는 변경은 **블루에 먼저** 넣는다(C Phase 0, E §6.2-2). 그러면 그린과의 차이가 줄고, 토큰 파일 형식을 두 스택이 공유할 수 있다. ② task_id·DAG 구조가 바뀌는 변경은 **그린 브랜치에서** 하고 섀도 S4(계좌별 `asset_daily` 동등성)로 검증한다. 블루(옛 코드)와 그린(새 코드)이 같은 날 같은 값을 내면 재구성이 맞다는 증거가 된다. ③ 단계마다 Variable 폴백을 남겨 롤백할 수 있게 한다.

| 단계 | 스택/시점 | 작업 | 검증 | 롤백 |
|---|---|---|---|---|
| **I-1** | 블루 2.10.4, C Phase 0 | `secrets/asset_flow.yaml` 작성(현재 Variable 값 + 만료일을 포털에서 확인해 기입), `.gitignore`, **디렉터리 RO 마운트**, `loader.py`(2.x `mask_secret` 폴백), `validate` CLI | CLI 통과, 컨테이너에서 경로 확인 | 마운트 제거 |
| **I-2** | 블루 | 기존 DAG의 `Variable.get('KIS_*')` 호출을 로더 어댑터 `legacy_config(variable_name)`로 교체한다. 어댑터는 YAML에서 같은 모양의 dict를 만들고, 없으면 Variable로 폴백한다. **태스크 구조는 그대로 둔다.** | 07시 체인 3일 성공, 로그에서 계좌번호가 `***`로 가려짐 | 어댑터가 Variable 폴백 |
| **I-3** | 블루 | **TokenManager v2**: 자격증명별 지문, `tokens.json`, flock, 발급 목록을 `required_credentials(cfg)`로 계산(**`KIS_IRP` 자연 제외**), Upbit 사용 시점 서명, `TOKEN_ISSUE_ENABLED` 플래그(기본 true). 옛 키 이름(`KIS_STOCK` 등) → 자격증명 별칭 매핑은 어댑터가 담당한다. | 키 하나를 교체하는 리허설: 그 자격증명만 재발급되고 나머지는 재사용되는지 로그로 확인 | 코드 revert. 옛 날짜 파일 방식으로 복귀 |
| **I-4** | 블루 | `ops_check_credentials` DAG(만료 요약 + Upbit 교차검증), D의 Discord 콜백(이미 Phase 0 계획) | 일부러 `expires_on`을 10일 뒤로 바꾼 사본으로 수동 실행 → 노란 알림 수신 | DAG pause |
| **I-5** | 블루 | `upsert_manual_position`: `ACCOUNT_CODE_ENUM` 제거, `account_alias` Param. H16 디버그 print 제거 | 수동 실행 1회(스테이징 원장 또는 같은 값 재입력) | revert |
| — | **E 단계 0~1** | `v2.10.4-final` 태그는 I-1~I-5 **이후**에 단다. 그린 clone 생성 | | |
| **I-6** | 그린 `feat/airflow3` | `fetch_asset_daily` 동적 매핑 재구성(`list_accounts` + `collect_account.expand`, 디스패치), `rebuild_derived_assets` 매핑, `fund_codes` 런타임 계산, 환율 `market_data.credential`. Variable 폴백 제거 | 섀도 S4: 블루와 그린의 `asset_daily`를 계좌별로 비교(같은 `account_code` 집합, 금액은 실행 시각 차이 범위) | 그린 브랜치 revert(블루는 무영향) |
| **I-7** | 그린 | `ImportErrorAlertPlugin`(3.x 소스 확인된 훅) | 일부러 문법 오류 DAG 파일을 넣어 알림 1건 수신 → 30초 뒤 재파싱에도 중복이 없는지 → 파일 삭제 | 플러그인 파일 삭제 |
| **I-8** | 그린 섀도 기간 | `.env`에 **`TOKEN_ISSUE_ENABLED=false`**. 블루 `data/tokens`를 RO 마운트(E §4.3)하고 secrets 디렉터리도 블루 것을 RO로 공유. 두 스택이 같은 `tokens.json` 형식(I-3)과 같은 지문을 쓰므로 그린은 재발급을 시도하지 않는다. 혹시 불일치하면 발급 대신 **즉시 실패**한다. | 그린 컨테이너에서 KIS 발급 호출이 0건(로그 grep) | — |
| **I-9** | 전환(E 단계 4) | `TOKEN_ISSUE_ENABLED` 줄을 삭제(기본 true), 그린 자체의 `data/tokens`, secrets 디렉터리 이동(또는 그린 경로를 정식화). F의 dag_id 변경을 함께 하면 `list_accounts` 등 새 task_id와 함께 이력이 한 번에 끊긴다(F §2.7의 "한 번에 비용 지불" 논리와 같음). | E 4-h + D+1 체인 | E §8 |
| **I-10** | 안정화 후 | 메타DB의 `KIS_*`, `UPBIT`, `MANUAL_ASSET_CONFIG` Variable 삭제(F §1.5 단계 6). 평문 노출 이력이 있으므로 KIS/Upbit 키를 재발급하고 YAML만 교체한다. **이것이 "YAML만 바꾸면 된다"의 첫 실전 검증이 된다.** | 교체 당일 수집 성공, 옛 키 폐기 | 백업 YAML로 복귀 |

> **I-3을 블루에 먼저 넣어야 하는 이유**: E §4.3의 섀도 방식은 "그린이 블루의 당일 토큰 파일을 RO로 읽는다"에 기대고 있다. 만약 그린만 지문 형식이고 블루는 옛 날짜 파일이라면, 그린은 "지문 없음 = 불일치"로 판단해 발급을 시도한다. 그러면 RO 쓰기 실패 이전에 **KIS 발급 API가 먼저 호출되어**(E §4.3 마지막 문단과 같은 위험) 블루의 1분 제한과 6시간 규칙에 간섭한다. 두 스택의 파일 형식을 같게 맞추고, 그린에는 발급 금지 플래그를 더한다(이중 방어).

### 6.3 작업량 추정

| 묶음 | 내용 | 추정 |
|---|---|---|
| I-1~I-2 | 로더·CLI·어댑터·마운트 | 1인일 |
| I-3 | TokenManager v2 + 리허설 | 1인일 |
| I-4~I-5 | 만료 DAG, upsert Param | 0.5인일 |
| I-6~I-7 | 매핑 재구성, 리스너 | 1~1.5인일(섀도 검증은 E 일정에 포함) |
| 합계 | | **3.5~4인일**. C/E 일정 안에서 Phase 0에 약 2.5인일이 더해진다. |

---

## 7. 확인이 더 필요한 사항

1. **KIS appkey 만료일**: 포털에서 확인할 수 있는 만료일(서비스 유효기간)의 존재와 연장 절차를 공식 문서로 찾지 못했다. 공식 예제에는 토큰 정책(1일 유효, 6시간 이내 재신청 시 같은 토큰, 발급 시 알림톡)만 있다. 사용자가 포털 화면의 값을 `expires_on`에 적는 것을 전제로 했다.
2. **KIS 토큰과 appkey의 결합 강도**: 다른 appkey 헤더와 옛 토큰을 섞어 보내면 어떤 오류 코드가 나는지 실측이 필요하다(§3.1의 "인증 오류 예상"). I-3 리허설에서 확인한다.
3. **Upbit `/v1/api_keys`의 `expire_at`**: 공식 문서 필드 설명에 "Deprecated일시"라는 표현이 섞여 있다. 첫 실행 결과를 Upbit 마이페이지의 만료일과 대조한다. 같은 JWT를 하루 동안 재사용하는 현재 방식이 nonce 정책상 허용되는지도 미확인이다(목표 구조에서는 요청마다 서명하므로 무관해진다).
4. **`account_master` DDL**: 컬럼·PK·FK를 저장소에서 찾지 못했다. `sync_account_master`와 §3.3 마이그레이션 SQL의 정확한 순서는 DDL을 확인한 뒤 확정한다. KIS 계좌도 `account_master`에 있는지 확인해야 한다.
5. **2.10.4의 import error 리스너 훅** 존재 여부(3.3.2만 소스 확인). I-7을 블루로 당길지 결정할 때 필요하다.
6. **매핑 태스크 그룹 + ALL_DONE 하류의 None 전달**: 이 보고서는 수집과 적재를 한 태스크로 합쳐 이 질문을 피했다. get/upload를 UI에서 분리해 보고 싶다면 스테이징에서 실측한다.
7. **`mask_secret`이 UI XCom·rendered fields 화면까지 가리는지**: 로그 마스킹은 소스로 확인했다. 설계상 XCom에 계좌번호를 넣지 않으므로 영향은 작다.
8. **git 이력의 계좌번호**: `ACCOUNT_CODE_ENUM`과 README 예시(`README.md:106`)의 계좌번호는 이미 커밋 이력에 있다. 코드에서 제거해도 이력에는 남는다. 이력 재작성(`git filter-repo`)은 원격·클론 전체에 영향을 주므로 사용자 결정 사항이다. 원격이 비공개로 유지된다면 "앞으로 추가하지 않음"으로 충분할 수 있다.

---

## 출처 (2026-10-12 접속)

**Airflow (3.3.2)**
- Dynamic Task Mapping(`expand`, `partial`, `expand_kwargs`, Named mapping `map_index_template`, `max_active_tis_per_dagrun`): https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html
- Listeners(플러그인 등록, import error 이벤트): https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/listeners.html
- Plugins(`AirflowPlugin.listeners`): https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/plugins.html
- DAG 직렬화·버저닝: https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-serialization.html , https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-versioning.html
- Best Practices(top-level code에서 Variable/외부 자원 접근 금지): https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html
- Masking sensitive data(`mask_secret`): https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/mask-sensitive-values.html
- Configuration Reference(`[dag_processor] min_file_process_interval`): https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html
- REST API v2 `importErrors`: https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html
- 소스 확인(PyPI wheel을 스크래치패드에 풀어 읽음):
  - `apache-airflow-core==3.3.2`: `airflow/listeners/spec/importerrors.py`(`on_new_dag_import_error`, `on_existing_dag_import_error`), `airflow/listeners/listener.py`(`get_listener_manager` → `integrate_listener_plugins`), `airflow/dag_processing/collection.py::_update_import_errors`(재파싱마다 existing 훅 호출, `has_import_errors=True, is_stale=True` 갱신, 훅 예외는 `log.exception`으로 삼킴), `airflow/dag_processing/manager.py:1345`(매니저에서 호출), `airflow/models/dag.py`(run 생성 대상 쿼리의 `is_stale == false`)
  - `apache-airflow-task-sdk==1.3.2`: `airflow/sdk/log.py:250`(`mask_secret` → supervisor에도 `MaskSecret` 전송), `airflow/sdk/execution_time/task_runner.py`(`rendered_map_index`, `_render_map_index`, early rendering), `airflow/sdk/bases/operator.py`(`map_index_template` 인자)

**KIS / Upbit**
- KIS 공식 예제 `kis_auth.py`(토큰 유효 1일, 6시간 이내 재신청 시 동일 토큰, 발급 시 알림톡, `access_token_token_expired`): https://github.com/koreainvestment/open-trading-api/blob/main/examples_user/kis_auth.py
- KIS 토큰 1분 1회 제한(제3자 자료, E §11-1과 같은 미확인 항목): https://algolab.co.kr/blog/kis-api-access-token-expiry-refresh-2026
- Upbit API 키 리스트 조회 `GET /v1/api_keys`(`access_key`, `expire_at`): https://docs.upbit.com/kr/reference/list-api-keys

**Docker**
- Bind mounts(파일 단위 마운트와 inode): https://docs.docker.com/engine/storage/bind-mounts/

**저장소 근거(읽기 전용)**
- `plugins/asset_flow/managers/token_manager.py`(27-30, 38, 61-140, 142-167), `plugins/asset_flow/clients/kis_client.py`(22-39), `plugins/asset_flow/clients/upbit_client.py`, `plugins/asset_flow/clients/base_client.py`(291-307)
- `dags/fetch_asset_daily_dag.py`(34-38 불변식, 112-121 Param, 166-416 get 10종, 418-464 upload·override), `dags/fetch_exchange_rate_dag.py`(118-130), `dags/fetch_fund_price_daily_dag.py`(70-79), `dags/upsert_manual_position_dag.py`(37-42, 148-184, 214-231), `dags/rebuild_derived_assets_dag.py`(109-232), `dags/make_token_dag.py`
- `plugins/asset_flow/transformers/kis_transformer.py`(54, 92, 153, 157, 221), `ledger_transformer.py`(19-24), `upbit_transformer.py`, `plugins/asset_flow/managers/db_manager.py`(61-120), `plugins/asset_flow/crawler/fund_crawler.py`(132), `plugins/callbacks.py`(20-27), `docker-compose.yaml`, `.gitignore`, `handoff.md`(§2, §7)
- 선행 보고서: `docs/airflow3_F_variables_naming.md`, `docs/airflow3_D_backup_discord.md`, `docs/airflow3_E_parallel_cutover.md`
