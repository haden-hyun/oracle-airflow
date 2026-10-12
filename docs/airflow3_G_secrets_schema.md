# 작업 G: secrets YAML 스키마 설계 검토 (계좌번호·앱키·만료일 단일 진실 공급원)

- 작성일: 2026-10-12
- 대상 저장소: `oracle-airflow`. 읽기 전용으로 분석했고, 이 보고서 외의 파일은 수정하지 않았다.
- 기준 버전: **Airflow 3.3.2**(`apache-airflow-core==3.3.2`, `apache-airflow-task-sdk==1.3.2`)
- 선행 보고서: [F Variable·명명](airflow3_F_variables_naming.md) §1, [D Discord](airflow3_D_backup_discord.md) §5, [E 병행 전환](airflow3_E_parallel_cutover.md) §4
- 근거 표기
  - **소스 확인**: PyPI wheel(`apache-airflow-core==3.3.2`, `apache-airflow-task-sdk==1.3.2`)을 스크래치패드에 풀어서 직접 읽은 내용
  - **실측**: 로컬 PyYAML 6.0.1과 pydantic 2.13.4로 스크래치패드에서 직접 실행한 결과. 3.3.2 공식 constraints는 PyYAML 6.0.3, pydantic 2.13.5다.
- 사용자 결정사항(이 보고서의 전제)
  1. **계좌번호도 비밀이다.** F §1.3에서 `dags/config/asset_flow.yaml`에 계좌번호를 두자고 한 안은 폐기한다.
  2. 키(KIS appkey/secret, Upbit access/secret)마다 **만료일을 함께 저장**하고, **만료 14일 전부터 Discord로 알림**을 받는다.
  3. 최종 목표: 서버의 **secrets YAML 값만 바꾸면** 토큰 발급, 수집 DAG, 만료 알림이 모두 자동으로 따라오는 구조.

---

## 0. 요약 (TL;DR)

| # | 질문 | 결론 |
|---|---|---|
| 1 | (a) dict인가 list인가 | **list of objects + 필수 `id` 필드**로 한다. PyYAML `safe_load`는 중복 키를 **오류 없이 마지막 값으로 덮어쓴다**(실측). 계좌 블록을 복사해 붙이고 키 이름을 안 바꾸면 기존 계좌가 **조용히 사라진다.** 로더에 중복 키 검사를 넣으면 dict도 막을 수 있지만, list+id로 두면 로더와 무관하게 스키마 검증 단계에서 다시 한 번 잡힌다. 그리고 순서(처리·표시 순서)가 명시적이다. 두 형식의 실질 차이는 크지 않으며, **결정적인 것은 "중복 키 거부 로더 + id 유일성 검증"을 둘 다 두는 것**이다. |
| 2 | (b) 정규화 여부 | **정규화한다.** `credentials`(앱키 단위: 키·시크릿·만료일)와 `accounts`(계좌 단위: 계좌번호·상품코드·표시명)를 나누고, 계좌가 `credential: <id>`로 참조한다. **만료일은 앱키의 속성**이므로 계좌마다 키를 중복 기재하면 CMA와 ISA의 만료일이 서로 어긋날 수 있다. **토큰 발급 대상도 "활성 계좌가 참조하는 credential"에서 자동으로 계산**되므로, F §1.1에서 발견한 `KIS_IRP` 데드 의존성이 구조적으로 사라진다. |
| 3 | (c) 저장 방식 | **독자 스키마의 단일 파일 `secrets/asset_flow.secrets.yaml` + 전용 로더**를 권장한다. LocalFilesystemBackend 형식은 다음 문제 때문에 이 목적에 맞지 않는다(소스 확인). ① Connection은 허용 키가 11개로 고정돼 있어 만료일·계좌번호를 `extra`에 넣어야 한다. ② extra 값은 **키 이름이 민감 키워드일 때만** 마스킹된다. ③ 스키마 검증·참조 무결성 훅이 없다. ④ **YAML 파싱 오류가 나면 Connection 조회는 DEBUG 로그만 남기고 다음 백엔드(메타DB)로 넘어간다.** ⑤ 파일을 문자열로 읽어 파싱하므로 오류 메시지에 **비밀이 든 원문 줄이 그대로 들어간다**(실측). ⑥ 수기 원장 계좌(IRP·DC·적금·청약)는 자격증명이 없어 Connection의 의미와도 맞지 않는다. |
| 4 | Discord webhook | secrets 파일에 **넣지 않는다.** D §5.1대로 `.env`의 `AIRFLOW_CONN_DISCORD_ALERT`(Connection password, 자동 마스킹)에 둔다. **secrets 파일이 깨졌다는 사실을 알릴 채널이 그 파일에 의존하면 안 되기 때문이다.** 결과적으로 KIS/Upbit용 `[secrets] backend`(LocalFilesystemBackend) 설정은 **필요 없어진다**. F §1.2(c)의 함정 1과 "모든 서비스에 마운트" 요구도 함께 사라진다. |
| 5 | (d) 검증 | **pydantic v2를 쓴다.** Airflow 3.3.2 core가 `pydantic>=2.11.0`, `jsonschema>=4.19.1`, `pyyaml>=6.0.3`을 필수 의존성으로 요구하고, 공식 constraints는 `pydantic==2.13.5`, `jsonschema==4.26.0`이다(소스·constraints 확인). 따라서 **추가로 설치할 것이 없다.** pydantic은 참조 무결성 같은 교차 검증을 코드로 쓸 수 있고, `model_json_schema()`로 jsonschema도 뽑아 편집기 자동완성에 쓸 수 있다. 실패하면 **fail-closed**한다: 값을 담지 않은 `SecretsConfigError`로 태스크를 실패시키고, 실패 콜백이 Discord로 알린다. |
| 6 | 검증 함정(실측) | YAML 1.1 규칙 때문에 따옴표 없는 `01`은 `1`, `00000000`은 `0`, `012345`는 **8진수 5349**가 된다. 날짜 `2027-02-30`은 `YAMLError`가 아닌 `ValueError`를 낸다. pydantic 기본 오류 메시지에는 `input_value=`로 **입력값이 노출된다.** 각각 `strict=True`, timestamp 해석 제거, `hide_input_in_errors=True`로 막았고, 12개 오류 사례가 모두 값 노출 없이 거부되는 것을 실측했다(§5.4). |
| 7 | (e) 경계 재정의 | 원칙은 **"계좌 1개에 귀속되는 값은 secrets, 계좌 유형·상품·테이블 단위 값은 config"**다. secrets에는 앱키, 시크릿, 만료일, CANO, 상품코드(ACNT_PRDT_CD), 수기 계좌번호, 계좌 표시명, 계좌→키 참조를 둔다. `dags/config/asset_flow.yaml`에는 테이블명, 펀드코드(연금·크롤링), 계좌유형별 multiplier·product_code, 만료 경고 일수 같은 운영 파라미터를 둔다. 계좌 별칭 `id`(`KIS_ISA` 등)는 비밀이 아니므로 XCom·Param·로그에 쓴다. |
| 8 | 마스킹 | 로더가 검증을 통과한 직후 app_key, app_secret, CANO, 수기 계좌번호를 `mask_secret(value)`(name 없이)로 등록한다. 그러면 `12345678-01` 같은 합성 코드도 로그에 `***-01`로 찍힌다. 다만 **마스킹은 로그와 렌더링 값에만 적용된다.** XCom(메타DB)과 업무 DB(`account.asset_daily.account_code`)에는 계좌번호가 평문으로 남는다(소스 확인: XCom 경로에 redact 없음). **XCom으로는 계좌 `id`만 넘기도록** 후속 수정을 권장한다(§7.3). |
| 9 | 자동 추종 | 파일을 고쳐 계좌를 추가하거나, 키를 교체하거나, 만료일을 갱신하거나, 계좌를 비활성화하면 **다음 태스크 실행부터 반영된다**(로더가 매 태스크 프로세스에서 다시 읽음). 이를 위해 세 가지 코드 변경이 필요하다. ① 토큰 발급 목록을 파일에서 계산하고, 키 지문이 바뀌면 그 키만 재발급한다. ② 수집 태스크를 계좌 `kind`별 dynamic task mapping으로 바꾼다. ③ 매일 06:30 `ops_check_credentials`가 검증과 만료 알림을 한다(§8). |

---

## 1. 현재 비밀이 흩어진 지점 (코드 근거)

| 위치 | 담긴 비밀 | 근거 | 새 구조에서 |
|---|---|---|---|
| 메타DB Variable `KIS_STOCK` / `KIS_ISA` / `KIS_PENSION_DEDUCTIBLE` / `KIS_PENSION_NON_DEDUCTIBLE` / `KIS_CMA` / `KIS_IRP` / `UPBIT` | appkey, secret, `account`(CANO), `product_code`, `type` | `token_manager.py:61-107`(`json.loads(Variable.get(...))`), `fetch_asset_daily_dag.py:182,210,242,284,303`, `fetch_exchange_rate_dag.py:124` | secrets 파일로 옮긴 뒤 삭제 |
| git: `upsert_manual_position_dag.py:37-42` `ACCOUNT_CODE_ENUM` | IRP·DC·적금·청약 **계좌번호 4개 평문** | 파일 직접 확인 | secrets `accounts[kind=manual_*]`로 옮기고, Param은 계좌 `id`로 바꿈(§7.2). **git 이력에는 남는다**(§10-3). |
| XCom(메타DB) | `get_*` 태스크 반환값 `{"account_code": "<CANO>-<상품코드>", "records": [...]}` | `fetch_asset_daily_dag.py:183-...`, 반환 dict | 마스킹 대상이 아님. id 전달로 줄일 수 있음(§7.3) |
| 업무 DB `account.asset_daily`, `account.account_master`, `account.manual_position_ledger` | `account_code` 컬럼(계좌번호를 포함한 업무 키) | `upsert_manual_position_dag.py:215-218`, transformer `account_code=f"{config['account']}-{config['product_code']}"` | **이 보고서 범위 밖.** 업무 키라 그대로 둔다. secrets 파일의 `account_code`와 일치해야 하므로 교차 검증만 추가한다(§8.2). |
| `data/tokens/YYYYMMDD_token.json` | Bearer 토큰(계좌번호는 없음) | `token_manager.py:137-138` | 유지(이미 gitignore). 키 지문 필드를 추가한다(§8.1). |
| 태스크 로그 | `print("Response body:", response.text)` 등 | `token_manager.py:85` | 로더가 등록한 마스크로 가린다(§6) |

`KISApiClient`는 매 요청 헤더에 `appKey`/`appSecret`을 싣고(`kis_client.py:31-39`), 파라미터로 `CANO`/`ACNT_PRDT_CD`를 보낸다(`kis_client.py:51-54`). 그래서 **계좌 단위 설정 dict 하나에 키와 계좌번호가 함께 들어 있어야 한다.** 이 호출 규약(`config['appkey']`, `config['secret']`, `config['account']`, `config['product_code']`, `config['type']`)은 `kis_transformer.py:54-55`도 쓰므로, 로더가 **같은 모양의 dict를 만들어 주면** 클라이언트와 transformer는 바꾸지 않아도 된다(§7.1 `kis_account_config`).

---

## 2. (a) 계좌를 이름 키 dict로 vs list of objects로

### 2.1 PyYAML 동작 실측

```text
>>> yaml.safe_load('a: 1\na: 2\n')
{'a': 2}                                   # 중복 키: 오류 없이 마지막 값이 이김
>>> yaml.safe_load('p: 01\nq: 08\nn: 012345\nx: 00000000\nd: 2026-10-12')
{'p': 1, 'q': '08', 'n': 5349, 'x': 0, 'd': datetime.date(2026, 10, 12)}
#  01 → 1 (앞자리 0 소실) / 08 → 문자열(8진수로 무효라서) / 012345 → 8진수 5349 / 날짜 자동 변환
```

- YAML 명세는 매핑 키가 유일해야 한다고 정하지만, PyYAML(SafeLoader)은 이를 강제하지 않는다. 오래된 이슈로 남아 있다.
- Airflow `LocalFilesystemBackend`도 같은 `yaml.safe_load`를 쓴다(소스 확인: `airflow/utils/yaml.py`가 `CSafeLoader`/`SafeLoader` 래퍼이고, `local_filesystem._parse_yaml_file`이 이를 호출). **중복 감지는 `.env` 형식에만 있다**(`VariableNotUnique`, `ConnectionNotUnique`). YAML·JSON 파일의 중복 conn_id는 조용히 덮어쓴다.

### 2.2 비교

| 기준 | dict (`accounts: {KIS_ISA: {...}}`) | **list + id (`accounts: [{id: KIS_ISA, ...}]`)** |
|---|---|---|
| 중복 감지 | 일반 `safe_load`로는 **불가능**하다. 파싱이 끝나면 이미 dict 하나로 합쳐져 있으므로 jsonschema·pydantic도 알 수 없다. 커스텀 로더(중복 키 거부)가 있어야만 잡힌다. | 커스텀 로더가 없어도 **pydantic의 id 유일성 검증**에서 잡힌다(실측). 커스텀 로더도 함께 두면 항목 안의 필드 중복(`app_secret`을 두 번 쓴 경우)까지 막는다. |
| 대표 사고: 블록을 복사하고 키 이름을 안 바꿈 | 기존 계좌가 **새 값으로 조용히 대체**된다. 기존 계좌 수집이 멈추고, 새 계좌 값이 옛 이름으로 적재된다. | `accounts.id 중복: ['KIS_ISA']`로 **로드가 실패**한다. |
| 순서 | Python 3.7+ dict도 삽입 순서를 유지하므로 실질 차이는 없다. 다만 "순서가 의미 있다"는 것이 형식에 드러나지 않는다. | 순서가 처리·표시 순서라는 것이 자연스럽다(dynamic mapping의 map_index 순서). |
| 추가·삭제 diff | 블록 단위로 추가·삭제된다. | 같다(`- id:`로 시작하는 블록). **id가 블록 안에 있어서** 블록을 잘라 옮겨도 정보가 온전하다. |
| 이름 변경 | 키를 바꾼다. | `id` 값을 바꾼다. 둘 다 토큰 키와 XCom 별칭이 바뀌므로 영향은 같다. |
| 조회 | `d[key]` O(1) | 로드할 때 `{a.id: a}` 인덱스를 한 번 만든다(항목이 10여 개라 비용은 무의미). |
| id 형식 검증 | 키에 정규식을 걸려면 별도 코드가 필요하다. | 일반 필드라서 `^[A-Z][A-Z0-9_]{1,63}$` 제약을 바로 건다. |
| Airflow 관례 | `connections.yaml`/`variables.yaml`과 같은 모양이다. | Airflow 형식과는 다르다. §4에서 독자 스키마로 정하므로 단점이 아니다. |

**결정: list + 필수 `id`, 그리고 중복 키를 거부하는 YAML 로더를 함께 쓴다.** 비용은 같고, 방어선이 둘이 된다. `credentials`도 같은 형식으로 둔다.

---

## 3. (b) 자격증명과 계좌의 정규화

### 3.1 현재 관계 (코드 근거)

| 계좌 | 앱키(토큰 키) | 근거 |
|---|---|---|
| 위탁 `KIS_STOCK` | `KIS_STOCK` | `tokens['KIS_STOCK']` |
| ISA `KIS_ISA` | `KIS_ISA` | `tokens['KIS_ISA']` |
| 연금 공제/비공제 | 각자 | `tokens[variable_name]` |
| **CMA `KIS_CMA`** | **`KIS_ISA`를 빌려 씀** | `fetch_asset_daily_dag.py:287` `KISApiClient(tokens['KIS_ISA'], cma_config)`. 헤더의 appKey는 `KIS_CMA` Variable의 값이므로, 두 Variable의 appkey가 같아야 한다는 **암묵 규약**이 있다(F §1.1 ※). |
| `KIS_IRP` | 발급만 하고 쓰지 않음 | `REQUIRED_TOKEN_KEYS`에 포함, 사용처 없음(F §1.1 부수 발견 1) |
| 환율 | `KIS_STOCK` 재사용 | `fetch_exchange_rate_dag.py:124-126` |
| Upbit | `UPBIT` | `tokens['UPBIT']` |
| IRP·DC·적금·청약 | 없음(원장 기반) | `get_irp_assets` 등이 업무 DB만 읽음 |

### 3.2 비교

| 기준 | 계좌마다 키 중복 기재 | **정규화(credentials ↔ accounts 참조)** |
|---|---|---|
| CMA/ISA 공유 | 같은 키를 두 번 적는다. **키를 교체할 때 한쪽만 바꾸면** CMA가 ISA 토큰에 다른 appKey 헤더를 붙여 실패한다(현행과 같은 함정). | `credential: KIS_ISA` 한 줄로 공유가 **명시**된다. 키를 교체할 때 한 곳만 고치면 된다. |
| 만료일 | 키의 속성인데 계좌마다 적게 되어 **같은 키에 만료일이 둘** 생긴다. 알림도 중복되거나 어긋난다. | credential에 한 번만 적는다. 알림 단위가 "키" 1개다. |
| 토큰 발급 단위 | 계좌 단위로 생각하게 된다(CMA 토큰을 따로 발급하는 실수 위험). | **credential 단위로 발급**하고 계좌는 참조로 찾는다. 현행 `tokens[...]` 키 규약과 정확히 일치한다. |
| 미사용 키 | `KIS_IRP`처럼 남아 있어도 알아채기 어렵다. | `credentials_in_use()` = 활성 계좌나 `services`가 참조하는 키만 발급한다. **참조되지 않는 키는 발급하지 않으므로**, 그 키가 만료돼도 다른 수집이 실패하지 않는다. |
| 수기 원장 계좌 | 키 필드가 비어 있는 이질적 항목이 된다. | `credential` 없이 `kind: manual_*`로 표현하고, 필드 조합 규칙을 `kind`별로 검증한다. |
| 단점 | 한 블록만 보면 된다. | 참조를 따라가야 한다. 대신 **참조 무결성 검증**(없는 id 참조, provider 불일치)이 필요하며, §5에서 구현했다. |

**결정: 정규화한다.** 추가로 계좌가 아닌 용도의 키 참조(환율 조회)도 같은 파일의 `services.exchange_rate_credential`에 둔다. 그래야 **모든 참조가 한 파일 안에서 함께 검증**된다. 이 참조를 config 파일에 두면 두 파일을 오가는 참조가 생겨 단일 진실 공급원 원칙이 깨진다.

> KIS appkey가 계좌 단위로 발급되는지, CMA처럼 다른 계좌의 키를 써도 계속 허용되는지는 KIS 정책 확인이 필요하다(F §3-2). 스키마는 "공유한다/안 한다"를 모두 표현할 수 있으므로 정책과 무관하게 유지된다.

---

## 4. (c) 저장 방식

### 4.1 후보

| 후보 | 내용 |
|---|---|
| **c1** | Airflow `LocalFilesystemBackend` 형식. `connections.yaml`(conn_id → login=appkey, password=secret, extra={expires_on, cano, product_code, ...}) + `variables.yaml` |
| **c2** | 독자 스키마의 단일 파일 `secrets/asset_flow.secrets.yaml` + 전용 로더(pydantic 검증) |
| c3 | c2 파일을 읽어 Airflow Connection으로 보여 주는 커스텀 `BaseSecretsBackend` 어댑터 |

### 4.2 c1(Connection extra)의 제약 (소스 확인)

1. **필드가 고정돼 있다.** `_create_connection`은 dict 키가 `Connection.__init__` 인자(`conn_id, conn_type, description, host, login, password, schema, port, extra, uri, team_name`) + `extra_dejson`의 부분집합이 아니면 `AirflowException("The object have illegal keys")`를 낸다. 그래서 `expires_on`·`cano`는 `extra`(JSON)에 넣어야 하고, 타입은 JSON 문자열이 된다. 날짜가 맞는 형식인지는 아무도 검증하지 않는다.
2. **extra 마스킹은 키 이름에 달려 있다.**
   - `Connection.extra_dejson`은 `mask_secret(dict)`를 호출한다. `add_mask`는 dict를 `(값, 키이름)`으로 재귀하면서 **키 이름이 민감 키워드에 해당할 때만** 패턴을 등록한다.
   - 기본 키워드는 `access_key, access_token, api_key, apikey, auth_header, authorization, bearer, connection_string, dsn, passphrase, passwd, password, private_key, proxy, proxy_password, proxies, secret, service_account, service_key, token, keyfile_dict, webhook_url`이다. 판정은 소문자화와 `\W+`→`_` 정규화 뒤 **부분 문자열 포함**으로 한다.
   - 그래서 `cano`, `account`, `appkey`는 가려지지 않는다. `AIRFLOW__CORE__SENSITIVE_VAR_CONN_NAMES`에 추가하면 가릴 수 있지만, 설정 하나를 빠뜨리면 그대로 노출된다.
   - 참고로 `_mask_connection_secrets`의 `mask_secret(conn.extra)`는 **extra JSON 문자열 전체**를 하나의 패턴으로 등록하므로, 개별 값 보호에는 도움이 되지 않는다.
3. **검증 훅이 없다.** 필수 키, 형식(8자리·2자리·ISO 날짜), 참조 무결성(계좌 → 키)을 강제할 자리가 없다. 결국 소비하는 코드 쪽에 검증을 흩어 놓게 된다.
4. **파싱 오류가 조용히 폴백된다.**
   - Task SDK `_get_connection`은 백엔드 예외를 `log.debug(..., exc_info=True)`로만 남기고 **다음 백엔드(env → 메타DB)로 넘어간다**(`execution_time/context.py:221-227`). 기본 로그 레벨에서는 흔적이 없다.
   - 메타DB에 옛 값이 남아 있으면 그 값이 쓰이고, 없으면 "conn_id isn't defined"라는 **원인과 다른 메시지**가 나온다.
   - Variable은 `log.exception`으로 남기는데(`:343-347`), 이번에는 아래 5번 문제가 생긴다.
5. **오류 메시지에 비밀 원문이 섞인다.** `_parse_yaml_file`은 `f.read()`로 **문자열을 읽어** `safe_load`한다. PyYAML은 문자열 입력의 오류 메시지에 해당 줄 스니펫을 붙인다. 실측 결과는 아래와 같다.
   ```text
   while scanning a quoted scalar
     in "<unicode string>", line 1, column 4:
       a: "TOPSECRET          ← 비밀이 든 줄이 그대로 출력
   ```
   이 메시지는 `AirflowFileParseException.__str__`에 그대로 들어간다. 로드에 실패했으니 그 값은 아직 마스크로 등록되지 않은 상태라 **가려지지도 않는다.** 반면 파일 **스트림**을 직접 넘기면 스니펫이 붙지 않는다(실측: `in "bad.yaml", line 1, column 4`까지만).
6. **의미가 맞지 않는다.** IRP·DC·적금·청약 계좌는 자격증명이 없는 "계좌번호만 있는 비밀"이다. 이를 Connection으로 만들면 `conn_type`도 없는 가짜 연결이 된다. Variable로 두면 다시 JSON 문자열 함정(F §1.2(c) 함정 1)에 걸린다.
7. 장점도 있다. Airflow 표준 경로(`Connection.get`, `BaseHook`)이고, password가 자동 마스킹되며, 나중에 Vault로 옮길 때 백엔드 클래스만 바꾸면 된다.

### 4.3 비교표

| 기준 | c1 LocalFS 형식 | **c2 독자 스키마 + 전용 로더** | c3 c2 + 커스텀 SecretsBackend |
|---|---|---|---|
| 만료일·계좌번호 같은 메타데이터 | extra JSON(타입 없음) | 1급 필드(타입·형식 검증) | 1급 필드 |
| 스키마·참조 무결성 검증 | 없음 | **있음**(fail-closed) | 있음 |
| 중복 키 | 조용히 덮어씀 | 거부 | 거부 |
| 파싱 오류 시 | DEBUG 로그 후 메타DB 폴백, 메시지에 원문 노출 | **즉시 실패**, 위치만 보고 | 백엔드 예외도 Airflow가 삼키므로 c1과 같은 폴백 문제 |
| 마스킹 | password 자동. 나머지는 키 이름에 의존 | **로더가 명시적으로 전부 등록** | 둘 다 |
| 비밀과 비밀 아닌 값의 혼재 | conn 단위로 분리되지만 계좌 메타(표시명 등)가 extra에 섞임 | 파일 하나에 "계좌 단위 값"만 둔다(§6). 비밀 아닌 운영 설정은 config로 분리 | c2와 같음 |
| 단일 진실 공급원 | `connections.yaml` + `variables.yaml` + config의 계좌→conn 매핑으로 **3곳** | **1곳** | 1곳 |
| 마운트 범위 | `[secrets] backend`라 **모든 Airflow 서비스**(F §1.2(c) 함정 2) | **로더를 호출하는 태스크 프로세스가 도는 컨테이너만**(LocalExecutor → scheduler) | 모든 서비스 |
| Airflow 기능 연동(UI 연결 테스트, Hook) | 됨 | 안 됨. 쓰는 곳이 KIS/Upbit 자체 클라이언트뿐이라 필요 없음 | 됨 |
| 구현량 | 거의 없음 | 로더 약 200줄 + 테스트 | c2 + 백엔드 클래스, 설정 |
| 외부 시크릿 매니저로 이전 | 백엔드 교체 | 로더의 "원문 dict 읽기"만 교체(검증·마스킹은 재사용) | 백엔드 교체 |

### 4.4 권장: c2

- 사용자 목표 3(**한 파일만 바꾸면 모두 따라옴**)과 결정 2(**만료일 1급 관리**)를 직접 만족하는 것은 c2뿐이다.
- c1의 장점(Hook 연동)은 이 프로젝트에서 쓸 곳이 없다. KIS와 Upbit은 `requests` 기반 자체 클라이언트를 쓴다.
- c3은 c2의 검증 이점을 Airflow의 "예외를 삼키는" 경로 뒤에 숨기므로 오히려 나쁘다. Hook 연동이 실제로 필요해질 때 다시 검토한다.
- **Discord webhook은 c2 파일에 넣지 않는다.** D §5.1대로 `.env`의 `AIRFLOW_CONN_DISCORD_ALERT`(conn_type http, password = URL)에 둔다.
  - 실패 알림은 secrets 파일이 깨졌을 때 **바로 그 사실을 알려야** 하므로 독립 경로여야 한다.
  - Connection password라서 자동 마스킹되고, 사용자가 말한 관리 대상(계좌번호·앱키·만료일)에도 해당하지 않는다.
- 결과적으로 **`AIRFLOW__SECRETS__BACKEND`(LocalFilesystemBackend)를 설정하지 않는다.** F §1.3의 `secrets/connections.yaml`·`variables.yaml`과 compose의 `AIRFLOW__SECRETS__BACKEND*`는 이 안으로 대체한다(§9).

---

## 5. (d) 스키마 검증

### 5.1 도구 선택

| 항목 | pydantic v2 | jsonschema |
|---|---|---|
| 3.3.2 이미지 포함 여부 | ✅ core `Requires-Dist: pydantic>=2.11.0`, task-sdk `pydantic>2.11.0`, constraints `pydantic==2.13.5` | ✅ core·task-sdk `jsonschema>=4.19.1`, constraints `jsonschema==4.26.0` |
| 필수키·타입·정규식 | ✅ | ✅ |
| 날짜 실재성(2027-02-30 거부) | ✅ (`date.fromisoformat`) | `format: date`는 format checker를 켜야 강제된다 |
| **목록 안 id 유일성** | ✅ validator 코드 | ❌ `uniqueItems`는 객체 **전체**의 동일성만 본다. id만 같은 경우는 잡지 못한다 |
| **참조 무결성**(계좌 → credential, provider 일치) | ✅ `model_validator` | ❌ 표준 키워드로는 불가능 |
| kind별 필드 조합 | ✅ | `if/then`이나 `oneOf`로 가능하지만 장황하다 |
| 오류 메시지 값 숨김 | ✅ `hide_input_in_errors=True` | 직접 포맷해야 한다 |
| 결과물 | 타입이 있는 불변 객체(`frozen=True`) | 검증만 하고 결과는 dict |
| 편집기 지원 | `SecretsConfig.model_json_schema()`로 JSON Schema 생성 → `# yaml-language-server: $schema=...` | 그대로 사용 |

**결정: pydantic v2.** jsonschema로는 핵심 요구(유일성, 참조 무결성)를 표현할 수 없다. JSON Schema가 필요하면 pydantic에서 생성한다.

### 5.2 검증 규칙

| 대상 | 규칙 | 구현 |
|---|---|---|
| 루트 | `version: 1` 고정, 정의되지 않은 키 금지 | `Literal[1]`, `extra="forbid"` |
| id | `^[A-Z][A-Z0-9_]{1,63}$`, credentials·accounts 각각 유일 | `Ident`, `_integrity` |
| app_key / app_secret | 문자열, 8자 이상(빈 값·자리표시자 실수 방지) | `KeyStr` |
| expires_on | **키 필수.** 값은 `YYYY-MM-DD` 또는 `null`(만료 없음을 확인한 경우) | `ISODate \| None`, 기본값 없음 |
| CANO | 정확히 숫자 8자리 **문자열** | `^\d{8}$` + `strict` |
| 상품코드 | 정확히 숫자 2자리 **문자열** | `^\d{2}$` + `strict` |
| 수기 계좌번호 | 숫자와 하이픈, 숫자로 시작·끝, 7~26자 | `^\d[\d-]{5,24}\d$` |
| kind별 필드 조합 | kis: credential+cano+product_code 필수, account_no 금지 / upbit: credential만 / manual: account_no+account_type 필수, credential·cano 금지 | `Account._shape` |
| 참조 | `account.credential`이 존재하고 provider가 `kind`와 일치. `services.exchange_rate_credential`은 KIS 키 | `_integrity` |
| account_code 유일 | 같은 계좌번호가 두 번 나오면 거부 | `_integrity` |
| 업무 DB 정합(교차 시스템) | 활성 계좌의 `account_code`가 `account.account_master`에 `is_active`로 있어야 함 | 로더 밖. `ops_check_credentials`에서 검사(§8.2) |

### 5.3 YAML 단계 처리

- **중복 키 거부**: 매핑 생성자를 바꿔 같은 키가 두 번 나오면 `중복 키 'display_name': 72행 (처음 71행)` 형태로 실패시킨다. 값은 출력하지 않는다.
- **timestamp 암묵 해석 제거**: 날짜를 문자열로 받아 `_iso_date` 한 곳에서만 검사한다. 기본 SafeLoader는 `2027-02-30`에서 `YAMLError`가 아닌 `ValueError`를 던져 오류 처리 경로가 둘로 갈라진다(실측). 이 처리 덕분에 따옴표가 있는 날짜와 없는 날짜를 똑같이 받는다.
- **스트림 파싱**: `yaml.load(open(...))`로 파싱해 오류 메시지에 원문 줄이 붙지 않게 한다(§4.2-5).
- **strict 모드**: 따옴표 없는 `01`·`00000000`이 int로 바뀐 것을 **자동 보정하지 않고 거부**한다. 보정하면 `00000000`이 `0`이 된 뒤 원래 값을 되살릴 수 없고, 8진수 `012345`→5349처럼 **다른 값으로 조용히 바뀐** 경우도 있다.

### 5.4 실측 결과 (프로토타입 = §7.1 코드, 템플릿 = §7.4)

```text
REJECT dup-id                | 스키마 검증 실패 1건 — <root>: Value error, accounts.id 중복: ['IRP']
REJECT dup-field             | 중복 키 'display_name': 72행 (처음 71행)
REJECT unquoted01            | accounts.0.product_code: Input should be a valid string
REJECT unquoted-cano-leading0| accounts.0.cano: Input should be a valid string
REJECT bad-ref               | accounts[KIS_CMA].credential: 정의되지 않은 'KIS_IAS'
REJECT missing-expiry        | credentials.4.expires_on: Field required
REJECT manual-with-cano      | accounts.6: kind=manual_fund: 필수 누락 [], 허용 안 됨 ['cano']
REJECT yaml-syntax           | YAML 문법 오류 34행 (expected <block end>, but found '<scalar>')
REJECT dup-account-no        | accounts: 같은 계좌번호(account_code)가 두 번 이상 등장
REJECT bad-date              | credentials.0.issued_on: Value error, day is out of range for month
REJECT typo-field            | credentials.0.issue_on: Extra inputs are not permitted
REJECT cano-7                | accounts.1.cano: String should match pattern '^\d{8}$'
```

`hide_input_in_errors=True`를 넣기 전에는 같은 오류에 `input_value={'version': 1, 'credentia...`처럼 **입력 dict가 잘린 채로 포함**됐다. 이 설정은 필수다.

### 5.5 검증 실패 시 동작

| 시점 | 동작 | 이유 |
|---|---|---|
| 파일을 수정한 직후(사람) | `docker compose exec airflow-scheduler python -m asset_flow.secrets_config`로 `OK`와 임박 만료 목록을 확인한다. | 06:50 체인 전에 오류를 잡는다. 컨테이너 안에서 실행하므로 이미지의 pydantic·PyYAML 버전과 같다. |
| 06:30 `ops_check_credentials` | 로드에 실패하면 태스크가 실패하고, `on_failure_callback`이 Discord로 알린다. 메시지는 `SecretsConfigError`(경로·행·필드만)다. | 정기 체인보다 20분 먼저 알린다. |
| 각 수집·토큰 태스크 | 로드에 실패하면 **즉시 실패(fail-closed)**하고 KIS/Upbit을 호출하지 않는다. 기존 retries에 따라 재시도한 뒤 실패 알림을 보낸다. | 일부 계좌만 건너뛰고 성공 처리하면 자산 합계가 **알림 없이 빠진다.** 계좌 단위로 부분 성공시키는 것보다 전체 실패가 안전하다. |
| **폴백 금지** | 검증에 실패해도 메타DB Variable이나 이전 파일로 넘어가지 않는다. 전환 기간 폴백(F §1.4)은 "파일에 **해당 id가 없을 때**"만 허용하고, "파일이 **잘못됐을 때**"는 허용하지 않는다. | c1의 조용한 폴백이 바로 피하려던 문제다. |
| 만료된 키 | 토큰 발급 전에 `expires_on < 오늘`이면 해당 credential만 `SecretsConfigError("KIS_ISA 만료(2027-03-31)")`로 실패시키고, 이 키를 쓰는 계좌만 실패한다(§8.1). | KIS의 401 응답보다 원인이 분명하다. 다른 키의 수집은 계속된다. |

---

## 6. (e) 비밀과 비밀 아닌 값의 경계 재정의

### 6.1 원칙

1. **계좌 1개에 귀속되는 값은 secrets**에 둔다. 계좌번호를 비밀로 정했으므로, 계좌번호를 키로 쓰거나 계좌와 1:1로 묶인 값을 config에 두면 결국 계좌번호(또는 그 별칭과의 대응표)를 두 파일에서 관리하게 된다. 표시명처럼 비밀이 아닌 계좌 속성도 **계좌 블록과 같이 움직이도록** secrets에 둔다.
2. **계좌 유형, 상품(펀드), 테이블 단위 값과 운영 파라미터는 config**(`dags/config/asset_flow.yaml`, 커밋)에 둔다. 코드와 함께 리뷰되고 GitDagBundle 버저닝을 따른다(F §1.2(a)).
3. **API 경로·TR_ID 같은 상수는 코드**(`asset_flow/config/kis.py`, `upbit.py`)에 그대로 둔다.
4. 계좌 **`id`**(`KIS_ISA`, `IRP`)는 비밀이 아닌 **별칭**이다. XCom, Param, 로그, map_index 이름에 계좌번호 대신 이것을 쓴다.

### 6.2 값별 분류

| 값 | 현재 위치 | 새 위치 | 이유 |
|---|---|---|---|
| KIS appkey/appsecret, Upbit access/secret | Variable | **secrets** `credentials[]` | 자격증명 |
| 키 만료일·발급일 | 없음 | **secrets** `credentials[].expires_on/issued_on` | 키와 같이 교체되는 값 |
| CANO(8자리), 상품코드(2자리) | Variable `account`/`product_code` | **secrets** `accounts[]` | 결정 1. 상품코드 단독은 비밀이 아니지만 CANO와 짝으로 계좌를 식별하고 같이 바뀐다. 2자리라 마스킹 대상도 아니다(최소 길이 5, 소스 확인). |
| IRP·DC·적금·청약 계좌번호 | **git** `ACCOUNT_CODE_ENUM` | **secrets** `accounts[kind=manual_*].account_no` | 결정 1 |
| 계좌 표시명(`type` → `account_name`) | Variable | **secrets** `accounts[].display_name` | 계좌 1:1 속성(원칙 1) |
| 계좌 → 키 참조(CMA→ISA), 환율용 키 | 코드에 암묵적으로 있음 | **secrets** `accounts[].credential`, `services` | 참조 무결성을 한 파일에서 검증 |
| 계좌 활성 여부 | 코드(태스크 존재 여부) | **secrets** `accounts[].enabled` | "파일만 바꾸면" 목표 |
| 계좌 수집 방식 `kind` | 코드(태스크 함수별) | **secrets** `accounts[].kind` | 계좌 추가를 파일만으로 하기 위함. 값 자체는 비밀이 아니다. |
| account_type(IRP/DC/...) | 업무 DB `account_master` | **secrets**(수기 계좌만) + 업무 DB | 수기 계좌의 원장 조회 키. 업무 DB와 교차 검증 |
| 테이블 schema/name | DAG Param | **config** `tables` | 비밀 아님, 구조 |
| 연금 펀드코드 `K553W5E17401`, 크롤링 대상 펀드코드 | DAG Param | **config** `funds` | 상품 단위. 공개 펀드 표준코드 |
| 계좌유형별 product_code(펀드)·multiplier | Variable `MANUAL_ASSET_CONFIG` + `ledger_transformer._FUND_MULTIPLIER` | **config** `manual_asset_types` | 유형 단위(F §1.1 부수 발견 2의 이중 관리 해소) |
| 만료 경고 일수(14), 점검 시각 | 없음 | **config** `credential_check` | 운영 파라미터 |
| Discord webhook URL | Variable `SLACK_WEBHOOK_URL` | **`.env` `AIRFLOW_CONN_DISCORD_ALERT`** | 알림 경로 독립(§4.4) |
| FERNET/JWT/API 비밀, DB 비밀번호 | compose | **`.env`** | 인프라 비밀(작업 B) |
| `postgres_asset` 접속 정보 | 메타DB Connection | 메타DB Connection 유지 또는 `.env` `AIRFLOW_CONN_POSTGRES_ASSET` | E §4.5의 스테이징 오버라이드와 맞춤. 이 보고서 범위 밖 |

### 6.3 수정된 `dags/config/asset_flow.yaml` (커밋 대상, F §1.3 예시 대체)

```yaml
# 비밀이 아닌 설정만 둔다. 계좌번호·앱키·계좌 표시명은 secrets/asset_flow.secrets.yaml.
# 계좌 단위 값을 여기에 두지 않는다 (작업 G §6.1 원칙 1).
tables:
  asset_daily:          {schema: account, table_name: asset_daily}
  exchange_rate_daily:  {schema: market,  table_name: exchange_rate_daily}
  fund_price_daily:     {schema: market,  table_name: fund_price_daily}

funds:
  pension_fund_code: K553W5E17401          # kind=kis_pension_fund 공용
  crawl_codes: [K553W5E17401, K55105D43299]

manual_asset_types:                         # account_master.account_type 단위 (기존 MANUAL_ASSET_CONFIG)
  IRP:                  {product_code: K55105D43299, multiplier: 0.001}
  DC:                   {product_code: K55105D43299, multiplier: 0.001}
  INSTALLMENT_SAVINGS:  {product_code: "<상품코드>", multiplier: 1.0}
  HOUSING_SUBSCRIPTION: {product_code: "<상품코드>", multiplier: 1.0}

credential_check:
  warn_days: 14                             # 만료 14일 전부터 매일 알림
```

> `manual_asset_types`의 키는 계좌가 아니라 **계좌 유형**이다. secrets의 수기 계좌 `account_type`이 이 키를 참조한다. 이 참조는 두 파일에 걸쳐 있으므로 `ops_check_credentials`가 교차 검사한다(§8.2). 유형 정의는 코드·리뷰 쪽 값이라 config에 두는 것이 맞다.

---

## 7. 최종 권장 스키마와 로더

### 7.1 로더 (`plugins/asset_flow/secrets_config.py` 제안, 실측에 쓴 코드 그대로)

```python
"""
자산 수집 자격증명·계좌의 단일 진실 공급원 로더 (secrets/asset_flow.secrets.yaml)

- 파일 경로: 환경변수 ASSET_FLOW_SECRETS (기본 /opt/airflow/secrets/asset_flow.secrets.yaml)
- 검증 실패 시 SecretsConfigError를 낸다. 메시지에는 경로·행·필드 위치만 담고 값은 담지 않는다(fail-closed).
- 검증을 통과하면 비밀 값(app_key/app_secret/계좌번호)을 mask_secret으로 등록한다.
"""
from __future__ import annotations

import datetime as dt
import os
import re
from pathlib import Path
from typing import Annotated, Literal

import yaml
from pydantic import (BaseModel, BeforeValidator, ConfigDict, Field,
                      StringConstraints, ValidationError, model_validator)

DEFAULT_PATH = "/opt/airflow/secrets/asset_flow.secrets.yaml"
EXPIRY_WARN_DAYS = 14


class SecretsConfigError(RuntimeError):
    """secrets 파일 읽기/검증 실패. 메시지에는 위치만 담고 값은 담지 않는다."""


# ── YAML: 중복 키 거부 + 암묵 timestamp 해석 제거 ─────────────────────
class _UniqueKeyLoader(yaml.SafeLoader):
    pass


# 날짜를 문자열로 받아 ISO 검증 한 곳(_iso_date)에서만 처리한다.
# (기본 SafeLoader는 2027-02-30 같은 값에서 YAMLError가 아닌 ValueError를 낸다)
_UniqueKeyLoader.yaml_implicit_resolvers = {
    ch: [(tag, rx) for tag, rx in rs if tag != "tag:yaml.org,2002:timestamp"]
    for ch, rs in yaml.SafeLoader.yaml_implicit_resolvers.items()
}


def _mapping_without_duplicates(loader, node, deep=False):
    seen: dict = {}
    for key_node, _ in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in seen:
            raise SecretsConfigError(
                f"중복 키 {key!r}: {key_node.start_mark.line + 1}행 (처음 {seen[key] + 1}행)")
        seen[key] = key_node.start_mark.line
    return loader.construct_mapping(node, deep=deep)


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _mapping_without_duplicates)


# ── 스키마 ─────────────────────────────────────────────────────────
def _iso_date(v):
    if isinstance(v, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", v):
        return dt.date.fromisoformat(v)          # 2027-02-30 → ValueError → 검증 오류로 수집됨
    raise ValueError("YYYY-MM-DD 형식의 날짜여야 함")


ISODate = Annotated[dt.date, BeforeValidator(_iso_date)]
Ident = Annotated[str, StringConstraints(pattern=r"^[A-Z][A-Z0-9_]{1,63}$")]
KeyStr = Annotated[str, StringConstraints(min_length=8)]
Cano = Annotated[str, StringConstraints(pattern=r"^\d{8}$")]
ProductCode = Annotated[str, StringConstraints(pattern=r"^\d{2}$")]
ManualAccountNo = Annotated[str, StringConstraints(pattern=r"^\d[\d-]{5,24}\d$")]
Provider = Literal["kis", "upbit"]
Kind = Literal["kis_stock", "kis_domestic_stock", "kis_pension_fund", "kis_cma_cash",
               "upbit", "manual_fund", "manual_cash"]
_KIND_PROVIDER: dict[str, str | None] = {
    "kis_stock": "kis", "kis_domestic_stock": "kis", "kis_pension_fund": "kis",
    "kis_cma_cash": "kis", "upbit": "upbit", "manual_fund": None, "manual_cash": None,
}


class _Strict(BaseModel):
    # strict: "01"을 따옴표 없이 써서 int 1이 되면 거부 / extra=forbid: 오타 필드 거부
    # hide_input_in_errors: 검증 오류 메시지에 입력값(키·계좌번호)을 넣지 않음
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True,
                              hide_input_in_errors=True)


class Credential(_Strict):
    id: Ident                                    # = 토큰 딕셔너리 키 (KIS_STOCK 등)
    provider: Provider
    app_key: KeyStr                              # KIS appkey / Upbit access key
    app_secret: KeyStr                           # KIS appsecret / Upbit secret key
    expires_on: ISODate | None                   # 키 생략 불가. null = "만료 없음"을 확인한 경우
    issued_on: ISODate | None = None
    note: str | None = None


class Account(_Strict):
    id: Ident                                    # 비밀이 아닌 별칭 (XCom·Param·로그에 쓰는 이름)
    kind: Kind                                   # 수집 로직 선택자
    display_name: str = Field(min_length=1)      # asset_daily.account_name
    enabled: bool = True
    credential: Ident | None = None              # API 계좌만
    cano: Cano | None = None                     # KIS 종합계좌번호 8자리
    product_code: ProductCode | None = None      # KIS 계좌상품코드 2자리
    account_no: ManualAccountNo | None = None    # 수기 원장 계좌(은행 등) 번호 원문
    account_type: Ident | None = None            # 수기 원장 계좌: account_master.account_type

    @model_validator(mode="after")
    def _shape(self):
        provider = _KIND_PROVIDER[self.kind]
        if provider == "kis":
            need, forbid = ("credential", "cano", "product_code"), ("account_no", "account_type")
        elif provider == "upbit":
            need, forbid = ("credential",), ("cano", "product_code", "account_no", "account_type")
        else:
            need, forbid = ("account_no", "account_type"), ("credential", "cano", "product_code")
        missing = [f for f in need if getattr(self, f) is None]
        extra = [f for f in forbid if getattr(self, f) is not None]
        if missing or extra:
            raise ValueError(f"kind={self.kind}: 필수 누락 {missing}, 허용 안 됨 {extra}")
        return self

    @property
    def account_code(self) -> str:
        """asset_daily / account_master 의 account_code 규약과 같다."""
        if self.cano:
            return f"{self.cano}-{self.product_code}"
        if self.account_no:
            return self.account_no
        return self.id                           # Upbit: 기존 규약 'UPBIT'


class Services(_Strict):
    exchange_rate_credential: Ident              # 환율 조회에 쓸 KIS 키


class SecretsConfig(_Strict):
    version: Literal[1]
    credentials: list[Credential] = Field(min_length=1)
    accounts: list[Account] = Field(min_length=1)
    services: Services

    @model_validator(mode="after")
    def _integrity(self):
        for name, items in (("credentials", self.credentials), ("accounts", self.accounts)):
            ids = [i.id for i in items]
            dup = sorted({i for i in ids if ids.count(i) > 1})
            if dup:
                raise ValueError(f"{name}.id 중복: {dup}")
        codes = [a.account_code for a in self.accounts]
        if len(codes) != len(set(codes)):
            raise ValueError("accounts: 같은 계좌번호(account_code)가 두 번 이상 등장")
        creds = {c.id: c for c in self.credentials}
        for a in self.accounts:
            if a.credential is None:
                continue
            c = creds.get(a.credential)
            if c is None:
                raise ValueError(f"accounts[{a.id}].credential: 정의되지 않은 {a.credential!r}")
            if c.provider != _KIND_PROVIDER[a.kind]:
                raise ValueError(f"accounts[{a.id}]: kind와 credential provider 불일치")
        ex = creds.get(self.services.exchange_rate_credential)
        if ex is None or ex.provider != "kis":
            raise ValueError("services.exchange_rate_credential: KIS credential id여야 함")
        return self

    # ── 조회 헬퍼 ──
    def credential(self, cred_id: str) -> Credential:
        try:
            return next(c for c in self.credentials if c.id == cred_id)
        except StopIteration:
            raise SecretsConfigError(f"credentials에 없는 id: {cred_id!r}") from None

    def account(self, account_id: str) -> Account:
        try:
            return next(a for a in self.accounts if a.id == account_id)
        except StopIteration:
            raise SecretsConfigError(f"accounts에 없는 id: {account_id!r}") from None

    def active_accounts(self, *kinds: str) -> list[Account]:
        return [a for a in self.accounts if a.enabled and (not kinds or a.kind in kinds)]

    def credentials_in_use(self) -> list[Credential]:
        """토큰 발급 대상 = 활성 계좌나 services가 참조하는 credential만 (미사용 키는 발급하지 않음)."""
        used = {a.credential for a in self.active_accounts() if a.credential}
        used.add(self.services.exchange_rate_credential)
        return [c for c in self.credentials if c.id in used]

    def expiring(self, today: dt.date, warn_days: int = EXPIRY_WARN_DAYS) -> list[tuple[Credential, int]]:
        """(credential, 남은 일수). 만료 warn_days일 전부터 이미 만료된 것까지, 남은 일수 오름차순."""
        out = [(c, (c.expires_on - today).days) for c in self.credentials
               if c.expires_on is not None and (c.expires_on - today).days <= warn_days]
        return sorted(out, key=lambda x: x[1])


def _secret_values(cfg: SecretsConfig) -> list[str]:
    vals = [v for c in cfg.credentials for v in (c.app_key, c.app_secret)]
    vals += [a.cano for a in cfg.accounts if a.cano]
    vals += [a.account_no for a in cfg.accounts if a.account_no]
    return vals


def _register_masks(cfg: SecretsConfig) -> None:
    try:
        from airflow.sdk.log import mask_secret   # Airflow 3.x
    except ImportError:                            # Airflow 밖(로컬 CLI 검증)
        return
    for v in _secret_values(cfg):
        mask_secret(v)                             # name 없이 호출 → 키 이름과 무관하게 항상 등록


def load_secrets(path: str | os.PathLike | None = None, *, mask: bool = True) -> SecretsConfig:
    p = Path(path or os.getenv("ASSET_FLOW_SECRETS", DEFAULT_PATH))
    try:
        with p.open(encoding="utf-8") as f:        # 스트림으로 파싱 → YAML 오류 메시지에 원문 줄이 붙지 않음
            raw = yaml.load(f, Loader=_UniqueKeyLoader)
    except FileNotFoundError:
        raise SecretsConfigError(f"secrets 파일 없음: {p}") from None
    except SecretsConfigError as e:
        raise SecretsConfigError(f"{p}: {e}") from None
    except yaml.MarkedYAMLError as e:
        line = e.problem_mark.line + 1 if e.problem_mark else "?"
        raise SecretsConfigError(f"{p}: YAML 문법 오류 {line}행 ({e.problem})") from None
    try:
        cfg = SecretsConfig.model_validate(raw)
    except ValidationError as e:
        locs = "; ".join(f"{'.'.join(map(str, er['loc'])) or '<root>'}: {er['msg']}" for er in e.errors())
        raise SecretsConfigError(f"{p}: 스키마 검증 실패 {e.error_count()}건 — {locs}") from None
    if mask:
        _register_masks(cfg)
    return cfg


if __name__ == "__main__":                         # python -m asset_flow.secrets_config [path]
    import sys
    cfg = load_secrets(sys.argv[1] if len(sys.argv) > 1 else None, mask=False)
    print(f"OK: credentials={len(cfg.credentials)} accounts={len(cfg.accounts)}")
    for c, d in cfg.expiring(dt.date.today()):
        print(f"  WARN {c.id}: 만료일 {c.expires_on} (남은 {d}일)")
```

설계 메모
- `from None`으로 예외 체인을 끊는다. 원래의 `ValidationError`나 `MarkedYAMLError` 트레이스백이 로그에 붙어 값이 새는 것을 막기 위해서다.
- `lru_cache`를 **쓰지 않는다.** 3.x LocalExecutor는 태스크마다 새 프로세스를 띄우므로, 매 태스크가 파일을 한 번 읽는다(수 ms). 파일을 고치면 **재시작 없이 다음 태스크부터 반영**되는 것이 목표 3의 핵심이다. 한 태스크 안에서 여러 번 부르는 경우는 호출부에서 변수로 재사용한다.
- 로컬 검증 CLI(`mask=False`)는 Airflow 3 런타임이 없는 곳에서도 돌아간다. `_register_masks`는 `ImportError`만 삼킨다.

### 7.2 로더 인터페이스 (호출부용 얇은 함수, 같은 모듈)

```python
def kis_account_config(account_id: str, cfg: SecretsConfig | None = None) -> dict:
    """현행 Variable(KIS_*)과 같은 모양: {'appkey','secret','account','product_code','type'}.
    KISApiClient·kis_transformer를 고치지 않고 그대로 쓰기 위한 어댑터."""

def upbit_account_config(account_id: str = "UPBIT", cfg: SecretsConfig | None = None) -> dict:
    """{'appkey','secret','account','type'} — token_manager._get_upbit_token / get_upbit_assets 호환."""

def token_key(account_id: str, cfg: SecretsConfig | None = None) -> str:
    """계좌 → 토큰 딕셔너리 키(= credential id). KIS_CMA → 'KIS_ISA'."""

def credentials_for_tokens(cfg: SecretsConfig | None = None) -> list[Credential]:
    """TokenManager가 발급할 목록. REQUIRED_TOKEN_KEYS 상수를 대체."""

def credential_fingerprint(cred: Credential) -> str:
    """sha256(app_key)[:12]. 토큰 파일에 저장해 키 교체를 감지(§8.1). 원문은 저장하지 않음."""

def manual_account_ids(cfg: SecretsConfig | None = None) -> list[str]:
    """upsert_manual_position Param 검증용 활성 수기 계좌 id 목록."""
```

호출부 변경 예시(현행 → 신규):

```python
# fetch_asset_daily.get_cma_cash
cma_config = Variable.get('KIS_CMA', deserialize_json=True)
kis = KISApiClient(tokens['KIS_ISA'], cma_config)
# ↓
cfg = load_secrets()
cma_config = kis_account_config('KIS_CMA', cfg)
kis = KISApiClient(tokens[token_key('KIS_CMA', cfg)], cma_config)

# token_manager._get_kis_token(variable_name) / TokenGenerator
info = json.loads(Variable.get(variable_name))
tokens = {"KIS_STOCK": ..., "KIS_IRP": ..., "UPBIT": ...}        # 하드코딩
# ↓
for cred in credentials_for_tokens(cfg):                          # 파일에서 계산
    tokens[cred.id] = issue(cred)                                 # cred.app_key / cred.app_secret

# fetch_exchange_rate.fetch_exchange_rates
config = Variable.get('KIS_STOCK', deserialize_json=True)
# ↓
cred_id = cfg.services.exchange_rate_credential                   # 계좌가 아니라 키만 필요
```

`upsert_manual_position`의 Param:

```python
# 현행: enum=ACCOUNT_CODE_ENUM (계좌번호 4개가 git·직렬화 DAG·UI 폼에 노출)
'account_code': Param(..., enum=ACCOUNT_CODE_ENUM)
# ↓ 계좌 id(비밀 아님)를 받고, 태스크 안에서 secrets로 계좌번호를 찾는다
'account_id': Param("IRP", type="string", pattern=r"^[A-Z][A-Z0-9_]{1,63}$",
                    description="secrets 파일의 수기 계좌 id (IRP, DC, INSTALLMENT_SAVINGS, HOUSING_SUBSCRIPTION)")
# validate 태스크: acct = cfg.account(p['account_id']); kind가 manual_*인지 확인 → acct.account_code로 account_master 조회
```

- 드롭다운(enum)을 유지하려면 DAG 파싱 시점에 secrets를 읽어야 한다. 그러면 **dag-processor에도 secrets를 마운트**해야 하고, 파일이 깨지면 DAG import error가 된다. 이 경우 **실패 콜백이 없어 Discord 알림 없이** UI에만 표시된다. 그래서 문자열 Param과 실행 시점 검증을 권장한다.
- id 목록을 `enum`으로 박아 두는 방법도 있다(id는 비밀이 아니므로). 다만 계좌를 추가할 때 코드도 같이 고쳐야 해 목표 3과 어긋난다.

### 7.3 XCom으로 계좌번호를 넘기지 않기 (권장 후속 수정)

- 현재 `get_*`는 `{"account_code": "<CANO>-<상품코드>", "records": [...]}`를 반환하고, 이 값이 메타DB `xcom` 테이블에 평문으로 저장된다. 3.3.2 소스의 XCom 조회 경로(`api_fastapi/core_api/routes/public/xcom.py`, `sdk/bases/xcom.py`)에는 `redact`/`mask` 호출이 없다. 그래서 **UI의 XCom 탭에도 그대로 보인다**(소스 확인).
- 권장안은 이렇다. `get_*`는 `{"account_id": "KIS_ISA", "records": [...]}`를 반환하고 records의 `account_code` 컬럼은 비운다. `upload_account`가 `cfg.account(account_id).account_code`로 채워 DELETE/INSERT한다.
- 이렇게 하면 메타DB(XCom)에는 계좌번호가 남지 않고, 업무 DB에만 남는다.
- 업무 DB의 `account_code`는 업무 키이므로 이 보고서에서 바꾸지 않는다. 계좌번호를 업무 DB에서도 빼야 한다면 대리키 도입이 필요하다. 이는 DB 스키마 변경이라 별도 작업이다.

### 7.4 `.example` 템플릿 (커밋 대상, 실측에 쓴 파일)

```yaml
# secrets/asset_flow.secrets.example.yaml  (커밋하는 템플릿 — 실제 값 금지)
# 실제 파일: secrets/asset_flow.secrets.yaml (gitignore, chmod 640, 서버에만 둠)
# 규칙 1: 숫자처럼 보이는 값(계좌번호·상품코드)은 반드시 "따옴표"로 감싼다.
# 규칙 2: 날짜는 YYYY-MM-DD. 따옴표는 있어도 없어도 된다.
# 규칙 3: 수정한 뒤 검증한다:
#   docker compose exec airflow-scheduler python -m asset_flow.secrets_config
version: 1

credentials:                       # 앱키 단위. id = 토큰 딕셔너리 키
  - id: KIS_STOCK
    provider: kis
    app_key: "<KIS_APPKEY>"
    app_secret: "<KIS_APPSECRET>"
    issued_on: 2026-03-31
    expires_on: 2027-03-31         # 포털에 표시된 만료일. 만료 14일 전부터 Discord 알림
  - id: KIS_ISA
    provider: kis
    app_key: "<KIS_APPKEY>"
    app_secret: "<KIS_APPSECRET>"
    expires_on: 2027-03-31
  - id: KIS_PENSION_DEDUCTIBLE
    provider: kis
    app_key: "<KIS_APPKEY>"
    app_secret: "<KIS_APPSECRET>"
    expires_on: 2027-03-31
  - id: KIS_PENSION_NON_DEDUCTIBLE
    provider: kis
    app_key: "<KIS_APPKEY>"
    app_secret: "<KIS_APPSECRET>"
    expires_on: 2027-03-31
  - id: UPBIT
    provider: upbit
    app_key: "<UPBIT_ACCESS_KEY>"
    app_secret: "<UPBIT_SECRET_KEY>"
    expires_on: 2027-03-31         # 만료가 없다고 확인했으면 null (키 자체는 생략 불가)

accounts:                          # 계좌 단위. 순서 = 처리·표시 순서
  - id: KIS_STOCK
    kind: kis_stock                # 해외+국내 잔고
    credential: KIS_STOCK
    cano: "00000000"
    product_code: "01"
    display_name: 한국투자 위탁
  - id: KIS_ISA
    kind: kis_domestic_stock
    credential: KIS_ISA
    cano: "00000001"
    product_code: "00"
    display_name: 한국투자 ISA
  - id: KIS_PENSION_DEDUCTIBLE
    kind: kis_pension_fund
    credential: KIS_PENSION_DEDUCTIBLE
    cano: "00000002"
    product_code: "22"
    display_name: 연금저축(세액공제)
  - id: KIS_PENSION_NON_DEDUCTIBLE
    kind: kis_pension_fund
    credential: KIS_PENSION_NON_DEDUCTIBLE
    cano: "00000003"
    product_code: "22"
    display_name: 연금저축(비공제)
  - id: KIS_CMA
    kind: kis_cma_cash
    credential: KIS_ISA            # CMA는 ISA 앱키·토큰 공유 (현행 tokens['KIS_ISA'])
    cano: "00000004"
    product_code: "00"
    display_name: 한국투자 CMA
  - id: UPBIT
    kind: upbit
    credential: UPBIT
    display_name: 업비트
  - id: IRP
    kind: manual_fund              # 원장 좌수 × NAV
    account_type: IRP
    account_no: "00000005-29"
    display_name: 저축IRP
  - id: DC
    kind: manual_fund
    account_type: DC
    account_no: "000-000-000006"
    display_name: 확정기여형DC
  - id: INSTALLMENT_SAVINGS
    kind: manual_cash              # 원장 원금 캐리
    account_type: INSTALLMENT_SAVINGS
    account_no: "000-000-0000007"
    display_name: 청년미래적금
  - id: HOUSING_SUBSCRIPTION
    kind: manual_cash
    account_type: HOUSING_SUBSCRIPTION
    account_no: "000-0000-0000-08"
    display_name: 청년주택드림청약

services:
  exchange_rate_credential: KIS_STOCK   # fetch_exchange_rate가 쓰는 키
```

- 템플릿 자체도 검증을 통과해야 한다. 그래야 CI에서 `python -m asset_flow.secrets_config secrets/asset_flow.secrets.example.yaml`로 **스키마 회귀 테스트**를 할 수 있다. 그래서 자리표시자 계좌번호도 형식에 맞는 더미 값(`"00000001"`)으로 넣었다. 자리표시자끼리 같으면 account_code 중복 검사에 걸리므로 서로 다르게 두었다.
- `KIS_IRP` credential은 넣지 않았다. 사용처가 없기 때문이다(F §1.1). 만약 넣더라도 참조하는 계좌가 없으면 `credentials_in_use()`에서 빠져 발급되지 않는다.
- `account_no` 예시의 하이픈 위치는 현행 `ACCOUNT_CODE_ENUM`의 형식(은행별로 다름)을 따른다. **account_code가 업무 DB의 값과 글자 하나까지 같아야 한다.**

### 7.5 파일 배치, 권한, `.gitignore` (제안 — 저장소에는 반영하지 않음)

```text
oracle-airflow/
├── .env                                   # (gitignore) 인프라 비밀 + AIRFLOW_CONN_DISCORD_ALERT
├── secrets/
│   ├── asset_flow.secrets.yaml            # (gitignore) 단일 진실 공급원 — 서버에만
│   ├── asset_flow.secrets.example.yaml    # (커밋) 템플릿, CI 스키마 테스트 입력
│   └── README.md                          # (커밋) 수정 → 검증 절차
├── dags/config/asset_flow.yaml            # (커밋) 비밀 아닌 설정 (§6.3)
└── plugins/asset_flow/secrets_config.py   # (커밋) 로더
```

```gitignore
secrets/*
!secrets/*.example.yaml
!secrets/README.md
```

```yaml
# docker-compose (3.x) — secrets는 태스크가 도는 서비스에만 (LocalExecutor → scheduler)
services:
  airflow-scheduler:
    environment:
      ASSET_FLOW_SECRETS: /opt/airflow/secrets/asset_flow.secrets.yaml
    volumes:
      - ./secrets/asset_flow.secrets.yaml:/opt/airflow/secrets/asset_flow.secrets.yaml:ro
```

- 호스트에서는 `chown ${AIRFLOW_UID}:0 secrets/asset_flow.secrets.yaml && chmod 640 ...`를 실행한다.
- api-server, dag-processor, triggerer에는 마운트하지 않는다. 실패 콜백이 dag-processor에서 돌더라도 webhook은 `.env` Connection에서 오므로 secrets가 필요 없다.
- **파일 단위 bind mount의 함정**: 편집기가 "새 파일로 저장 후 rename"하는 방식(vim 기본값 등)이면 inode가 바뀐다. 그러면 컨테이너는 **옛 inode를 계속 본다.** 디렉터리 단위 마운트(`./secrets:/opt/airflow/secrets:ro`)를 쓰거나, 파일 단위를 쓸 경우 수정 후 검증 CLI 출력으로 반영 여부를 확인한다. 디렉터리 마운트가 더 안전하므로 이를 권장한다(그 경우 `.example`도 같이 보이지만 무해하다).
- E의 블루/그린 섀도 기간에는 그린에 **스테이징 전용 secrets 파일**을 둔다(E §4.5와 같은 원칙). 운영 키를 그대로 복사하면 KIS 토큰 경합(E §4.3)이 다시 생긴다.

---

## 8. "파일만 바꾸면 따라오는" 동작

### 8.1 토큰 발급 (`TokenManager`)

| 변경 | 동작 |
|---|---|
| 발급 목록 | `REQUIRED_TOKEN_KEYS` 상수를 없애고 `credentials_for_tokens(cfg)`의 id 집합을 쓴다. |
| 토큰 파일 형식 | `{"KIS_ISA": {"token": "...", "fp": "<sha256(app_key)[:12]>", "issued_at": "..."}, ...}` |
| 키 교체 감지 | `GetTokens()`가 파일의 `fp`를 현재 credential 지문과 비교한다. **다르거나 없는 id만** 재발급하고 파일을 원자적으로 교체한다(임시 파일 → `os.replace`). 현행처럼 키 하나가 빠졌다고 **전체를 삭제하고 재발급하지 않는다.** KIS 발급 한도(1분 1회로 알려짐, E §6-1 미확인)를 덜 건드린다. |
| 만료 키 | 발급 전에 `expires_on < today`이면 그 credential만 실패로 기록한다. 그 키를 참조하는 계좌 태스크만 `SecretsConfigError`로 실패하고 나머지는 계속한다. |
| 미사용 키 | 참조되지 않으므로 발급하지 않는다(`KIS_IRP` 문제 해소). |
| 호환 | 토큰 딕셔너리 키 = credential id = 현행 Variable 이름으로 유지하므로, `tokens['KIS_ISA']` 형태의 호출부는 그대로 동작한다. |

### 8.2 만료 알림 DAG `ops_check_credentials` (F §2.5 명명 규칙 적용)

- 스케줄: 매일 **06:30 KST**. `make_token`(06:50)보다 앞서 돌아 파일 오류를 먼저 알린다.
- `on_failure_callback`: D §5.2의 `discord_failure_callback`
- 태스크
  1. `validate`: `load_secrets()`를 호출한다. 실패하면 태스크가 실패하고 Discord 실패 알림이 간다.
  2. `cross_check`: config의 `manual_asset_types`에 수기 계좌의 `account_type`이 있는지, 업무 DB `account.account_master`에 활성 계좌의 `account_code`가 `is_active`로 있는지 확인한다. 불일치하면 실패한다.
  3. `notify_expiring`: `cfg.expiring(today_kst, warn_days=config.credential_check.warn_days)` 결과가 있으면 D의 `_post_discord`로 embed 1건을 보낸다. 남은 일수가 0 이하이면 빨강, 그 외에는 주황이다. 필드에는 **credential id·만료일·남은 일수만** 담고 키 값은 담지 않는다. 결과가 없으면 아무것도 보내지 않는다.
- **매일 반복 알림**: 만료 14일 전부터 파일의 `expires_on`이 갱신될 때까지 매일 1회 보낸다. 사용자가 키를 재발급하고 파일의 `app_key`/`app_secret`/`expires_on`을 고치면 **다음 날부터 알림이 자동으로 멈추고**, 토큰은 지문 변경으로 자동 재발급된다. 알림 중복을 막기 위한 상태 저장은 필요 없다(일 1회 스케줄 자체가 빈도 제한이다).
- 만료 알림은 경고이므로 `notify_expiring`은 **성공으로 끝낸다.** 실패로 처리하면 "실패 → 복구" 알림 쌍이 매일 생겨 잡음이 된다.

```python
# 알림 본문 예 (D §5.2 embed 구조 재사용)
{"title": "🔑 API 키 만료 임박 2건", "color": 0xFB8C00,
 "fields": [{"name": "KIS_ISA", "value": "2027-03-31 (D-11) · 사용 계좌: KIS_ISA, KIS_CMA", "inline": False},
            {"name": "UPBIT",   "value": "2027-03-31 (D-11) · 사용 계좌: UPBIT",            "inline": False}],
 "footer": {"text": "secrets/asset_flow.secrets.yaml 의 app_key/app_secret/expires_on 갱신"}}
```

### 8.3 수집 DAG

- 계좌 추가를 "파일만"으로 하려면 태스크 목록이 파일에서 나와야 한다. **파싱 시점에 파일을 읽지 않고** 실행 시점 dynamic task mapping을 쓴다.
  ```python
  @task
  def list_accounts(kinds: list[str]) -> list[str]:
      return [a.id for a in load_secrets().active_accounts(*kinds)]   # XCom에는 id만

  collect_kis_stock.expand(account_id=list_accounts(["kis_stock", "kis_domestic_stock"]))
  collect_kis_cash.expand(account_id=list_accounts(["kis_cma_cash"]))
  # ... kind별 1개 태스크. map_index_template="{{ task.op_kwargs['account_id'] }}" 로 UI 표시
  ```
- 이렇게 바꾸면 task_id가 바뀌어 이력이 끊긴다(F §2.6과 같은 성격). 그래서 **3.x 전환 PR의 dag_id 변경 커밋과 같은 배포 창**에 넣는 것을 권장한다.
- 1단계로 태스크 구조는 그대로 두고 Variable 호출만 `kis_account_config(id)`로 바꾸는 방법도 있다. 이 경우 "키·계좌번호·만료일 변경"은 파일만으로 따라오고, "계좌 추가"만 코드가 필요하다.

### 8.4 변경 시나리오별 반영

| 사용자가 secrets에서 바꾸는 것 | 반영되는 곳 | 코드·재시작 |
|---|---|---|
| 키 교체(app_key/app_secret/expires_on) | 다음 태스크에서 지문 불일치 → 해당 키만 재발급, 만료 알림 중단 | 필요 없음 |
| 만료일만 갱신(연장) | 다음 06:30 점검부터 알림 중단 | 필요 없음 |
| 계좌 비활성화(`enabled: false`) | 다음 run부터 mapping에서 빠짐. 그 계좌만 쓰던 키는 발급도 중단 | 필요 없음(§8.3 적용 후) |
| 같은 kind의 계좌 추가 | 다음 run부터 mapping 대상 | 필요 없음(§8.3 적용 후). 단 **업무 DB `account_master`에 행이 있어야** 함(§8.2-2가 알려줌) |
| 새 kind의 계좌(새 상품 유형) | — | 수집 로직 코드 필요(당연) |
| 파일 문법·형식 오류 | 06:30 검증 실패 알림, 이후 태스크 fail-closed | 파일 수정 |

---

## 9. F·D·E 보고서 대비 변경점

| 항목 | 이전 권고 | 이 보고서 |
|---|---|---|
| 계좌번호 위치 | F §1.3: `dags/config/asset_flow.yaml`(커밋) | **secrets 파일**(결정 1) |
| KIS/Upbit 키 형태 | F §1.2(d): LocalFS `connections.yaml`의 Connection(login/password) | **독자 스키마 secrets 파일**(§4) |
| `[secrets] backend` | F §1.3: LocalFilesystemBackend를 모든 서비스에 | **설정하지 않음.** secrets 파일은 scheduler에만 마운트 |
| `AIRFLOW__CORE__SENSITIVE_VAR_CONN_NAMES=appkey,webhook` | F §1.3 | 필요 없음(로더가 값 단위로 마스킹. webhook은 Connection password) |
| Discord webhook | D §5.1: Connection `discord_alert`(`.env` 권장) | **그대로 따름.** secrets 파일에 넣지 않음 |
| 로더의 Variable 폴백 | F §1.4: 파일에 없으면 Variable | "id가 없을 때"만 폴백, **검증 실패 시 폴백 금지**(§5.5) |
| `lru_cache` | F §1.4: 설정 파일 캐시 | secrets는 캐시하지 않음(파일 변경 즉시 반영) |
| `MANUAL_ASSET_CONFIG` | F §1.3: config `manual_assets`(계좌유형 키) | 유지(이름만 `manual_asset_types`). 계좌번호와 분리됨 |
| 이전 절차 | F §1.5 단계 1: vars.json을 config/connections/variables로 분할 | vars.json을 **secrets 파일 1개 + config 1개**로 분할. 단계 4의 검증에 `python -m asset_flow.secrets_config` 추가 |

---

## 10. 남은 확인 사항과 위험

1. **키 만료 정책(공식 원문 미확인)**: KIS appkey/appsecret와 Upbit Open API 키에 정해진 유효기간이 있는지 공식 공지에서 확인하지 못했다. 검색 결과는 제3자 자료뿐이었고, "재발급 전까지 유지"라는 설명도 있었다. 스키마는 `expires_on`을 **사용자가 포털에서 확인한 값으로 직접 적는** 방식이고, 만료가 없으면 `null`을 명시하게 했으므로 정책과 무관하게 동작한다. 실제 값은 KIS Developers 포털과 Upbit 마이페이지의 Open API 관리 화면에서 확인한다.
2. **KIS 앱키 공유(CMA→ISA) 허용 여부**: F §3-2와 같다. 스키마는 어느 쪽이든 표현할 수 있다.
3. **git 이력의 계좌번호**: `ACCOUNT_CODE_ENUM`을 HEAD에서 지워도 이력에는 남는다. 원격 저장소를 공개할 가능성이 있다면 `git filter-repo` 등으로 이력을 다시 써야 한다. 협업자 clone에도 영향이 있으므로 별도로 결정해야 한다.
4. **마스킹의 한계**
   - 마스킹은 로그와 렌더링 템플릿 필드에만 적용된다. XCom(§7.3), 업무 DB, `airflow tasks test` 같은 Airflow 밖 출력에는 적용되지 않는다.
   - 길이가 5 미만인 값은 마스킹하지 않는다(`[logging] min_length_masked_secret` 기본값 5, 소스 확인). 상품코드 2자리는 그래서 가려지지 않으며, 비밀이 아니므로 의도한 동작이다.
   - 숫자 8자리 CANO는 로그 속 **같은 숫자열**(예: 금액 `112345678`)도 가린다. 무해한 오탐이다.
   - `mask_secret`은 task-sdk에서 supervisor 프로세스에도 전달된다(소스 확인: `MaskSecret` 메시지).
5. **Upbit 토큰 캐시**: 현행 `_get_upbit_token`은 nonce를 담은 JWT를 하루 1회 만들어 재사용한다. 이 보고서의 스키마와는 무관하지만, "토큰 발급 자동 추종"을 구현할 때 Upbit은 **요청마다 JWT를 새로 서명**하는 편이 맞는지 Upbit 인증 문서로 따로 확인할 것을 권한다.
6. **2.10.4에서 로더 사용**: 프로토타입은 pydantic 2.13 기준으로 실측했다. 2.10.4 이미지의 pydantic 버전은 확인하지 않았으므로, F §1.5 단계 2대로 로더는 3.x 전환 PR에만 넣는다.
7. **파일 단위 bind mount의 inode 문제**(§7.5): 디렉터리 마운트로 피한다.

---

## 출처

**Airflow 공식 문서 (3.3.2, stable)**
- Local Filesystem Secrets Backend: https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/secrets-backend/local-filesystem-secrets-backend.html
- Secrets Backend 개요(탐색 순서): https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/secrets-backend/index.html
- Masking sensitive data: https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/mask-sensitive-values.html
- Configuration Reference(`[core] sensitive_var_conn_names`, `[logging] min_length_masked_secret`): https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html
- Managing Connections: https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html
- Params: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/params.html
- Dynamic Task Mapping(`map_index_template`): https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html
- Installation from PyPI(constraints 파일 사용): https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html
- 3.3.2 constraints(`pydantic==2.13.5`, `jsonschema==4.26.0`, `PyYAML==6.0.3`): https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt

**소스 확인 (PyPI wheel을 스크래치패드에서 압축만 풀어 읽음)**
- `apache-airflow-core==3.3.2`
  - `METADATA`: `Requires-Dist: pydantic>=2.11.0`, `jsonschema>=4.19.1`, `pyyaml>=6.0.3`
  - `airflow/secrets/local_filesystem.py`: `_parse_yaml_file`(문자열 read 후 `yaml.safe_load`), `_create_connection`(허용 키 검사), `load_variables`·`load_connections_dict`(`.env`만 중복 감지)
  - `airflow/utils/yaml.py`(CSafeLoader 래퍼)
  - `airflow/models/connection.py`(`__init__` 인자)
  - `airflow/exceptions.py`(`AirflowFileParseException.__str__`)
  - `config_templates/config.yml`(`min_length_masked_secret` 기본값 5, 3.0.0+)
  - `api_fastapi/core_api/routes/public/xcom.py`(redact 없음)
- `apache-airflow-task-sdk==1.3.2`
  - `METADATA`: `pydantic>2.11.0`, `jsonschema>=4.19.1`
  - `_shared/secrets_masker/secrets_masker.py`(`DEFAULT_SENSITIVE_FIELDS`, `should_hide_value_for_key` 부분 문자열 판정, `add_mask` dict 재귀·최소 길이)
  - `log.py`(`mask_secret` → supervisor `MaskSecret` 전달)
  - `execution_time/context.py`(`_mask_connection_secrets`, Connection 백엔드 예외 `log.debug` 후 다음 백엔드로, Variable `log.exception`, `_mask_and_deserialize_variable`)
  - `definitions/connection.py`(`extra_dejson` → `mask_secret(dict)`)
  - `bases/xcom.py`(redact 없음)

**YAML / 검증 라이브러리**
- YAML 1.2.2 명세(매핑 키 유일성): https://yaml.org/spec/1.2.2/#mapping
- YAML 1.1 int 타입(`0` 접두 8진수): https://yaml.org/type/int.html
- YAML 1.1 timestamp 타입: https://yaml.org/type/timestamp.html
- PyYAML 중복 키 미검출 이슈: https://github.com/yaml/pyyaml/issues/165
- pydantic Strict Mode: https://docs.pydantic.dev/latest/concepts/strict_mode/
- pydantic `ConfigDict.hide_input_in_errors`: https://docs.pydantic.dev/latest/api/config/#pydantic.config.ConfigDict.hide_input_in_errors
- pydantic JSON Schema 생성: https://docs.pydantic.dev/latest/concepts/json_schema/
- JSON Schema `uniqueItems`(객체 전체 동일성만 비교): https://json-schema.org/understanding-json-schema/reference/array#uniqueItems

**키 정책 관련 (공식 원문 미확인 — §10-1)**
- AlgoLab, KIS API 키 발급 가이드(제3자): https://algolab.co.kr/blog/kis-api-key-guide-30min
- 업비트 개발자센터 공지(키 보안): https://docs.upbit.com/kr/changelog/안내-open-api-키-정보-보안에-신경써주시기-바랍니다

**선행 보고서**
- `docs/airflow3_F_variables_naming.md`, `docs/airflow3_D_backup_discord.md`, `docs/airflow3_E_parallel_cutover.md`

**저장소 코드 근거**
- `plugins/asset_flow/managers/token_manager.py`, `plugins/asset_flow/clients/kis_client.py`, `plugins/asset_flow/clients/upbit_client.py`, `plugins/asset_flow/transformers/kis_transformer.py`, `dags/fetch_asset_daily_dag.py`, `dags/fetch_exchange_rate_dag.py`, `dags/upsert_manual_position_dag.py`, `plugins/callbacks.py`, `docker-compose.yaml`, `.gitignore`
