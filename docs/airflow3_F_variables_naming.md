# 작업 F: Variable 관리 방식 개선과 DAG 파일명·dag_id 평가

- 작성일: 2026-10-12
- 대상 저장소: `oracle-airflow` (읽기 전용으로 분석했고, 이 보고서 외의 파일은 수정하지 않음)
- 기준 버전: **Airflow 3.3.2** (2026-09-17, 최신 안정판), `apache-airflow-task-sdk` 1.3.2
  - 요청서에 적힌 "3.10"이라는 버전은 존재하지 않아 3.3.2를 기준으로 썼습니다. 3.1.x와 다른 부분은 `> 3.1.x 주석`으로 따로 표시했습니다.
- 현재 구성: `apache/airflow:2.10.4` docker-compose(LocalExecutor, 메타DB는 compose 내부 `postgres:13`), 자산 업무 DB는 호스트의 별도 PostgreSQL(`postgres_asset`), 실패 알림은 `plugins/callbacks.py` + Variable `SLACK_WEBHOOK_URL`
- 선행 보고서와의 관계:
  - 작업 A(코드 영향): `airflow.sdk.Variable`로 이전, `default_var` → `default`
  - 작업 B(인프라): `.env` 분리, `FERNET_KEY=''` 때문에 Variable이 평문 저장됨, dag-processor 추가
  - 작업 C(전략): Phase 0~4 단계적 전환
  - 이 보고서는 위 결론을 전제로 하며, 서로 충돌하는 권고는 없습니다.
- "소스 확인" 표시: PyPI의 `apache-airflow-core==3.3.2`·`apache-airflow-task-sdk==1.3.2` wheel을 스크래치패드에 풀어 직접 읽고 확인한 내용입니다.

---

## 0. 요약 (TL;DR)

| # | 결론 |
|---|---|
| 1 | Variable은 8개이고, 그중 **자격증명(appkey/secret, webhook URL)과 일반 설정(계좌번호·상품코드·표시명·펀드코드·multiplier)이 한 JSON 안에 섞여 있습니다.** 그런데 `FERNET_KEY=''`(작업 B §0-7)라서 메타DB와 백업 dump에 전부 평문으로 남아 있습니다. |
| 2 | **권장 구조는 "비밀은 LocalFilesystemBackend(`secrets/*.yaml`, gitignore), 비밀이 아닌 설정은 repo의 `dags/config/asset_flow.yaml`"입니다.** KIS/Upbit 키는 **Connection**(`login`=appkey, `password`=secret)으로 옮기고, Slack webhook은 secrets 파일의 Variable로 둡니다. Vault 같은 외부 시크릿 매니저는 1인·단일 서버 구성에는 과합니다. |
| 3 | 로더는 기존 Variable과 **같은 모양의 dict를 돌려주도록** 만듭니다. 이렇게 하면 `KISApiClient`·transformer·token 딕셔너리 키 규약(`KIS_STOCK` 등)을 고치지 않아도 됩니다. 옮기는 동안에는 파일에 없으면 Variable로 폴백해 롤백할 수 있게 둡니다. |
| 4 | **함정 3가지(소스 확인)**: ① secrets YAML에서 JSON Variable을 YAML dict로 쓰면 `deserialize_json=True`에서 예외가 나는데, 이 예외가 **삼켜지고 다음 백엔드(메타DB의 옛 값)로 조용히 넘어갑니다.** ② `appkey`, `SLACK_WEBHOOK_URL`은 기본 마스킹 키워드에 걸리지 않아 **로그에 평문으로 찍힐 수 있습니다.** ③ 태스크 프로세스(LocalExecutor면 scheduler 컨테이너)와 dag-processor(DAG 레벨 콜백) **양쪽에 secrets 파일을 마운트해야 합니다.** |
| 5 | 부수 발견: `KIS_IRP` 토큰은 발급만 되고 쓰는 태스크가 없습니다. 그런데도 `REQUIRED_TOKEN_KEYS`에 들어 있어서 **KIS_IRP 키가 만료되면 모든 KIS 수집이 함께 실패합니다.** 또 IRP·DC multiplier가 `MANUAL_ASSET_CONFIG`와 `ledger_transformer._FUND_MULTIPLIER`(0.001) 두 곳에 있습니다. |
| 6 | DAG 이름: `fetch_` 4개 중 **실제로 "가져오기만" 하는 DAG는 없습니다.** 모두 수집·변환·적재 ETL이고, `fetch_asset_daily`의 10개 계좌 중 4개는 API가 아니라 원장에서 파생됩니다. `_daily`는 스케줄이 아니라 **대상 테이블명**을 따른 것이라 `fetch_exchange_rate`(테이블 `exchange_rate_daily`)만 빠져 있습니다. 파일 접미사 `_dag.py`는 7개 모두 일관됩니다. |
| 7 | 권장 명명은 **B안 `<도메인>_<동사>_<대상테이블>`**입니다. 도메인은 DB 스키마 `market`/`account`, 그리고 `auth`/`ops`로 정하고, 동사는 `ingest`/`rebuild`/`upsert`/`issue`/`test`만 씁니다. 예: `fetch_asset_daily` → `account_ingest_asset_daily`. 전체 매핑은 §2.5에 있습니다. |
| 8 | dag_id를 바꾸면 **실행 이력·XCom·로그·DAG 버전이 새 ID에서 0부터 시작합니다.** 옛 ID는 stale DAG로 남습니다. `ExternalTaskSensor.external_dag_id` 3곳을 같은 커밋에서 바꿔야 하고, **`DAGS_ARE_PAUSED_AT_CREATION=true` 때문에 새 DAG가 paused 상태로 생기므로 unpause를 빠뜨리면 다음 날 07시 체인이 알림 없이 통째로 멈춥니다.** |
| 9 | 시점: **3.x 전환(작업 C Phase 3) 때 별도 커밋으로 함께 배포하는 것이 유리합니다.** 신규 구축(메타DB를 새로 만드는 경우)이라면 무조건 함께 바꾸는 것이 좋습니다. 이유는 3.x 전환으로 logical_date 의미·UI URL·복구 알림 기준선이 어차피 끊기고, 센서 체인 재검증과 unpause 단계도 전환 때 이미 계획돼 있기 때문입니다. |

---

## 1. Variable 관리 방식 개선

### 1.1 현재 Variable 목록과 구조 (코드 근거)

| Variable | 형식 | 키 | 읽는 곳 | 비밀 키 | 비밀 아닌 키 |
|---|---|---|---|---|---|
| `KIS_STOCK` | JSON | `appkey`, `secret`, `account`, `product_code`, `type` | `token_manager._get_kis_token`, `fetch_asset_daily.get_stock_account`, `fetch_exchange_rate.fetch_exchange_rates` | `appkey`, `secret` | `account`(CANO), `product_code`(ACNT_PRDT_CD), `type`(계좌 표시명 → `account_name`) |
| `KIS_ISA` | JSON | 위와 동일 | token, `get_isa_stock` | 〃 | 〃 |
| `KIS_PENSION_DEDUCTIBLE` | JSON | 위와 동일 | token, `get_pension_assets(variable_name)` | 〃 | 〃 |
| `KIS_PENSION_NON_DEDUCTIBLE` | JSON | 위와 동일 | token, `get_pension_assets(variable_name)` | 〃 | 〃 |
| `KIS_CMA` | JSON | 위와 동일 | `get_cma_cash`. **토큰은 `tokens['KIS_ISA']`를 빌려 씀** | 〃 (※) | 〃 |
| `KIS_IRP` | JSON | `appkey`, `secret` (+ 계좌 키가 있을 수 있음) | `token_manager`에서만 읽음 | `appkey`, `secret` | — |
| `UPBIT` | JSON | `appkey`(access_key), `secret`, `account`(선택, 기본 `UPBIT`), `type`(선택, 기본 `업비트`) | `token_manager._get_upbit_token`, `get_upbit_assets` | `appkey`, `secret` | `account`, `type` |
| `MANUAL_ASSET_CONFIG` | JSON | `{IRP, DC, INSTALLMENT_SAVINGS, HOUSING_SUBSCRIPTION}` → 각각 `product_code`(4개 모두 필수, payload에 저장됨), `multiplier`(IRP·DC 검증에 사용) | `upsert_manual_position.validate` | 없음 | 전부 |
| `SLACK_WEBHOOK_URL` | 문자열 | — | `callbacks._send_slack` (없으면 알림을 **조용히 건너뜀**) | URL 자체가 자격증명 | — |

근거 코드: `plugins/asset_flow/managers/token_manager.py:62-111`, `plugins/asset_flow/clients/kis_client.py:31-38`(헤더에 `appKey`/`appSecret`), `plugins/asset_flow/transformers/kis_transformer.py:54-55`(`account`·`product_code`·`type`), `dags/fetch_asset_daily_dag.py:179-304`, `dags/fetch_exchange_rate_dag.py:121-126`, `dags/upsert_manual_position_dag.py:226-277`, `plugins/callbacks.py:20-24`.

(※) `KISApiClient`는 **요청 헤더에 config의 `appkey`/`secret`을 함께 보냅니다.** 그런데 CMA는 ISA 토큰을 쓰므로, `KIS_CMA`의 appkey/secret은 사실상 `KIS_ISA`와 같은 값이어야 합니다. KIS 토큰이 appkey에 묶인다는 전제에서 나온 추정이며, KIS 정책 확인이 필요합니다. 새 구조에서는 이 중복을 "CMA는 ISA Connection을 참조"로 명시해 없앱니다.

**Variable 밖에 흩어진 비밀 아닌 설정**도 같은 정리 대상입니다.

| 위치 | 값 |
|---|---|
| `fetch_asset_daily` Param | `daily_asset={schema:account, table_name:asset_daily}`, `pension_fund_code='K553W5E17401'` |
| `fetch_exchange_rate` Param | `exchange_rate={schema:market, table_name:exchange_rate_daily}` |
| `fetch_fund_price_daily` Param | `fund_codes=['K553W5E17401','K55105D43299']` |
| `upsert_manual_position_dag.py:37-42` | `ACCOUNT_CODE_ENUM`. **계좌번호 4개가 이미 git에 하드코딩돼 있음** |
| `ledger_transformer.py:24` | `_FUND_MULTIPLIER = 0.001` (`MANUAL_ASSET_CONFIG.multiplier`와 이중 관리) |
| `asset_flow/config/kis.py`, `upbit.py` | API 경로·TR_ID 상수. 코드 성격이므로 그대로 둠 |

**부수 발견 (조치 권장)**
1. **`KIS_IRP` 데드 의존성**: IRP 평가는 원장 × NAV로 하므로(`get_irp_assets`) `tokens['KIS_IRP']`를 쓰는 곳이 없습니다. 그런데도 `TokenGenerator`가 발급하고 `REQUIRED_TOKEN_KEYS`에 포함돼 있습니다. 그래서 KIS_IRP appkey가 만료되거나 삭제되면 `TokenGenerator` 전체가 예외를 내고, **위탁·ISA·연금·CMA·환율 수집이 모두 실패합니다.** 쓰지 않는다면 발급 목록에서 빼는 편이 낫습니다(별도 PR).
2. **multiplier 이중 관리**: `upsert_manual_position`은 Variable의 multiplier로 검증하고, `fetch_asset_daily`/`rebuild_derived_assets`는 코드 상수 0.001로 평가합니다. 이를 설정 파일 한 곳으로 모읍니다(§1.4).
3. **평문 노출 경로**: `FERNET_KEY=''`이므로 `variable` 테이블에 평문으로 저장돼 있습니다(작업 B). 따라서 작업 B §7 단계 1의 `pg_dumpall` 백업 파일에도 KIS appkey/secret이 평문으로 들어갑니다. 백업 파일은 권한을 제한하고, 전환 후 롤백 기간이 끝나면 지웁니다.
4. `data/tokens/YYYYMMDD_token.json`에도 Bearer 토큰이 평문으로 있습니다. `data/`는 이미 gitignore 대상이므로 유지하면 되고, 이 보고서 범위에서는 변경하지 않습니다.

### 1.2 대안 비교 (Airflow 3.3.2 기준)

#### (a) 비밀 아닌 설정을 repo의 YAML/JSON 파일로 (`dags/config/`)

- 공식 Dynamic Dag Generation 문서는 메타데이터를 "Dag 폴더 안의 파일"(JSON, YAML)로 두고 `__file__` 기준 경로로 읽으라고 권합니다.
- Variable처럼 네트워크·DB를 호출하지 않으므로 **파싱 비용이 사실상 없습니다.** dag-processor 기본 `min_file_process_interval=30`초마다 파싱하더라도 작은 YAML을 읽는 정도라 부담이 없습니다.
  - 다만 Best Practices의 top-level 원칙은 그대로 적용됩니다. 값을 쓰는 곳이 태스크 안이라면 **태스크 안에서 읽는 것이 기본**이고, DAG 구조(Param 기본값, 태스크 생성)에 쓸 때만 모듈 레벨에서 읽습니다.
- **DAG 버저닝 관점** (3.0+):
  - 설정값이 DAG 구조에 들어가면(Param 기본값, 동적 태스크 목록) 값을 바꿀 때 직렬화 DAG 해시가 바뀌어 **새 DAG 버전으로 기록됩니다.** 이는 의도한 변경이므로 오히려 추적성이 생깁니다.
  - 태스크 안에서만 읽으면 DAG 버전은 그대로이고, `LocalDagBundle`은 "do not support versioning of the bundle"이므로 항상 디스크의 최신 파일로 실행됩니다.
  - 나중에 `GitDagBundle`을 도입하면 번들(= repo의 dags 하위 파일 전체)이 커밋 단위로 고정됩니다. 그러면 clear/rerun 시 **그 시점의 설정 파일까지 재현**됩니다(3.3부터 rerun에 번들 버전 적용, 작업 C §3.2). Variable은 버전이 없어 항상 현재 값을 읽는다는 점과 대비됩니다.
- `.yaml`은 `.py`가 아니므로 DAG 파일로 파싱되지 않습니다. 디렉터리를 명시적으로 빼고 싶다면 `.airflowignore`에 `config/`를 추가합니다(3.x 기본 문법은 glob).
- 장점: 코드 리뷰와 git 이력, 스테이징·운영에 같은 파일, 롤백이 쉬움.
- 단점: 값을 바꾸려면 배포(git pull)가 필요하고 UI에서 즉시 수정할 수 없음. 이 저장소의 설정은 계좌 개설·펀드 교체 정도로 드물게 바뀌므로 단점이 작습니다.

#### (b) `AIRFLOW_VAR_*` 환경변수와 `.env`

- 형식은 `AIRFLOW_VAR_{KEY}`(대문자)이고, JSON은 문자열로 넣습니다(`AIRFLOW_VAR_KIS_STOCK='{"appkey":"..."}'`). UI와 `airflow variables list`에 나오지 않습니다.
- 3.x 워커의 기본 탐색 순서는 `EnvironmentVariablesBackend → ExecutionAPISecretsBackend`입니다(소스 확인: `airflow.sdk.execution_time.secrets.DEFAULT_SECRETS_SEARCH_PATH_WORKERS`). 따라서 태스크 프로세스에서 바로 읽힙니다.
- 단점:
  - 중첩 JSON을 compose/.env에서 인용부호 처리하기 번거롭습니다.
  - **컨테이너의 모든 프로세스 환경과 `docker inspect`에 노출됩니다.**
  - 값을 바꾸려면 컨테이너를 다시 만들어야 합니다.
  - 7개 계좌 JSON을 env로 관리하면 가독성이 떨어집니다.
- 판단: 작업 B대로 **인프라 비밀(FERNET_KEY, JWT_SECRET, API SECRET_KEY)은 `.env`**에 두고, 업무 자격증명은 (c)로 둡니다.

#### (c) LocalFilesystemBackend (`variables.yaml` / `connections.yaml`)

- 설정 키(3.3.2): `[secrets] backend = airflow.secrets.local_filesystem.LocalFilesystemBackend`, `[secrets] backend_kwargs = {"variables_file_path": ..., "connections_file_path": ...}`. 둘 다 1.10.10에 도입됐습니다.
  - 키 하나만 덮어쓰려면 `AIRFLOW__SECRETS__BACKEND_KWARG__<KEY>`를 씁니다.
  - 워커 전용 설정 `[workers] secrets_backend / secrets_backend_kwargs`(3.0.0+)가 있습니다. **비워 두면 워커도 `[secrets] backend`를 그대로 씁니다**(소스 확인: `_shared/configuration/parser.py::_get_custom_secret_backend`).
- 탐색 순서: **커스텀 백엔드 → 환경변수 → 메타DB**이며, 바꿀 수 없습니다.
  - 3.x 워커 기준 소스로는 `LocalFilesystemBackend → EnvironmentVariablesBackend → ExecutionAPISecretsBackend`이고, 마지막 단계에서 api-server가 다시 자기 탐색 순서로 메타DB까지 조회합니다.
  - **UI의 Admin > Variables에는 메타DB 값만 보입니다.** 같은 키가 메타DB에 남아 있으면 파일 값이 이기므로 혼동하기 쉽습니다(§1.5 단계 6에서 삭제).
- 파일 형식: JSON, YAML, `.env`를 지원합니다. 파일은 **조회할 때마다 다시 읽습니다**(`_local_variables`가 캐시 없는 property, 소스 확인). 파일을 고치면 컨테이너를 재시작하지 않아도 다음 태스크부터 반영됩니다.
- 공식 문서는 이 백엔드를 개발용 또는 Kubernetes Secret·사이드카용으로 소개합니다. 하지만 1인 단일 서버 compose 환경에서는 **"gitignore된 파일 + 읽기 전용 마운트"**가 Vault의 실용적 대체재입니다.
- **함정 (소스 확인)**:
  1. `load_variables()`는 YAML 값을 그대로 돌려줍니다. JSON Variable(`KIS_STOCK` 등)을 YAML **매핑**으로 쓰면 `Variable.get(..., deserialize_json=True)`가 dict에 `json.loads`를 시도해 예외가 납니다. 이 예외는 `_get_variable`의 `except Exception`에 삼켜지고, 로그만 남긴 채 **다음 백엔드(env → 메타DB)로 넘어갑니다.** 그 결과 메타DB의 옛 값이 조용히 쓰일 수 있습니다. Variable로 둘 JSON은 YAML 안에서 **JSON 문자열**로 써야 합니다. 이 문제는 자격증명을 Connection으로 옮기면 피할 수 있습니다(§1.3).
  2. 백엔드는 **조회하는 프로세스가 도는 컨테이너**에서 파일을 읽습니다. 각 컨테이너별로 필요한 이유는 다음과 같습니다.
     - scheduler: LocalExecutor 태스크가 여기서 실행됩니다.
     - dag-processor: DAG 레벨 `on_success_callback`이 여기서 실행됩니다(작업 A §4.3).
     - api-server: Execution API 폴백 경로와 UI의 연결 테스트에 씁니다.
     - triggerer: deferrable 태스크를 도입할 경우에 필요합니다.
     
     따라서 **모든 Airflow 서비스에 같은 경로로 마운트**해야 합니다.
- > 3.1.x 주석: `[workers] secrets_backend`와 워커 기본 탐색 경로 구조는 3.0.0부터 같습니다. 위 "예외를 삼키고 다음 백엔드로 넘어가는" 동작은 task-sdk 1.3.2 소스로만 확인했으므로, 3.1.x에서는 같은 방식으로 한 번 더 확인하기를 권합니다.

#### (d) KIS appkey/secret을 Connection으로 (`login`/`password` 또는 extra JSON)

- 이점:
  - Connection `password`는 **태스크에서 접근할 때 항상 마스킹**됩니다(Masking 문서).
  - 자격증명과 계좌 설정을 구조적으로 분리할 수 있습니다.
  - Connection 하나를 여러 계좌가 참조할 수 있어 CMA → ISA 키 재사용을 명시적으로 표현할 수 있습니다.
- 구성: `conn_type: generic`(코어 `providers_manager`에 "Generic"으로 등록돼 있음, 소스 확인), `login`=appkey, `password`=secret.
  - extra JSON 방식(`{"appkey":..., "appsecret":...}`)도 가능합니다. 하지만 extra 키 마스킹은 **키 이름이 민감 키워드를 포함할 때만** 적용되고, 기본 키워드에 `appkey`는 없습니다(`apikey`, `api_key`, `secret`, `token`, `password` 등만 있음). 그래서 `login`/`password` 방식이 더 안전합니다.
  - `login`(appkey)은 기본적으로 마스킹되지 않습니다. 로더에서 `airflow.sdk.log.mask_secret(conn.login)`을 호출하거나 `AIRFLOW__CORE__SENSITIVE_VAR_CONN_NAMES=appkey,webhook`을 추가합니다.
- 읽기 API(3.x): `from airflow.sdk import Connection; Connection.get(conn_id)` → `.login`, `.password`, `.extra_dejson` (소스 확인: task-sdk 1.3.2 `definitions/connection.py`). `BaseHook.get_connection(conn_id)`도 있습니다.
- 정의 위치: (c)의 `connections.yaml`, `AIRFLOW_CONN_KIS_STOCK='{"conn_type":"generic","login":"...","password":"..."}'`(JSON 형식은 2.3.0+), 또는 UI/메타DB.

#### (e) 외부 시크릿 매니저 (HashiCorp Vault, AWS SSM/Secrets Manager 등)

- provider 백엔드를 `[secrets] backend`에 설정해 씁니다. 키 회전, 감사 로그, 접근 정책이 장점입니다.
- **이 프로젝트에는 과합니다.**
  - 1인이 운영하는 단일 서버라 Vault를 띄우면 운영 대상 서비스(언실, 백업, 인증서)가 하나 늘어납니다. 그리고 **07시 체인의 새로운 단일 장애점**이 됩니다.
  - 클라우드 매니저는 자격증명을 보관할 클라우드 계정·IAM 키가 다시 필요해, 결국 "비밀을 지키는 비밀"을 로컬에 두게 됩니다.
  - 비밀 개수가 7개 내외이고 접근 주체도 한 명입니다.
- 다시 검토할 시점: 다중 사용자·다중 서버, 감사 요구, 또는 이미 조직의 Vault가 있을 때. 백엔드 클래스만 바꾸면 되므로 (c) 구조에서 넘어가는 비용은 작습니다.

#### (f) `airflow variables import`로 파일 일괄 등록

- 문법(3.3.2): `airflow variables import [-a {overwrite,fail,skip}] file`. 지원 형식은 `.env`/`.json`/`.yaml`/`.yml`이고 기본 동작은 `overwrite`입니다. Connection은 `airflow connections import [--overwrite] file`입니다.
- 장점: 파일 하나로 메타DB를 재현할 수 있어 신규 구축과 스테이징 시드에 편합니다.
- 한계: 등록한 뒤의 **진실 공급원(source of truth)은 다시 메타DB**가 됩니다. UI 수정과 파일 사이에 드리프트가 생기고, `FERNET_KEY`가 없으면 평문으로 저장됩니다.
- 판단: **이전 단계의 백업과 시드 용도로만** 씁니다(`airflow variables export`는 JSON만 지원하며 평문이 나오므로 취급에 주의). 운영의 상시 경로로는 (c)를 씁니다.

#### 비교표

| 기준 | 메타DB Variable(현행) | (a) repo YAML | (b) env/.env | (c) LocalFS 백엔드 | (d) Connection | (e) Vault 등 | (f) import |
|---|---|---|---|---|---|---|---|
| 비밀 저장 적합성 | 낮음(Fernet 없으면 평문) | **부적합** | 중 | **높음**(파일 권한·gitignore) | 높음(password 마스킹) | 매우 높음 | 낮음(결과는 메타DB) |
| 비밀 아닌 설정 적합성 | 중 | **높음** | 낮음 | 중 | — | 낮음 | 중 |
| 버전 관리·리뷰 | 없음 | **git** | 없음 | 없음(의도적) | 정의 위치에 따름 | 버전 기능 있음 | 파일만 git 가능 |
| DAG 버저닝 연동 | 없음 | 있음(구조에 쓰일 때 / GitDagBundle) | 없음 | 없음 | 없음 | 없음 | 없음 |
| 파싱 비용(top-level) | API 호출 | 파일 읽기 | 없음 | 파일 읽기 | API 호출 | 네트워크 | — |
| 변경 반영 | 즉시(UI) | 배포 | 컨테이너 재생성 | 즉시(파일 재조회) | 정의 위치에 따름 | 즉시 | 재실행 필요 |
| 운영 부담 | 낮음 | 낮음 | 낮음 | 낮음 | 낮음 | **높음** | 낮음 |

### 1.3 권장 구조: 비밀과 비밀 아닌 설정 분리

원칙은 다음과 같습니다.
1. **자격증명**(재발급이 필요하고 유출 시 피해가 있는 값)은 repo 밖 `secrets/`의 LocalFilesystemBackend로 둡니다. 형태는 Connection을 우선하고, 단일 문자열만 Variable로 둡니다.
2. **비밀 아닌 설정**(코드와 함께 바뀌는 값)은 repo의 `dags/config/asset_flow.yaml` 단일 파일에 둡니다. 계좌 → Connection 참조 관계도 여기에 적습니다.
3. **인프라 비밀**(Fernet/JWT/API 키)은 작업 B대로 `.env`에 둡니다.
4. 메타DB Variable은 전환 기간의 폴백으로만 쓰고, 이후 삭제합니다.

> 계좌번호(`account`, CANO)는 자격증명은 아니지만 **개인 식별정보**입니다. `ACCOUNT_CODE_ENUM`으로 IRP·DC·적금·청약 계좌번호가 이미 git에 있으므로, 이 저장소의 GitHub 원격(`haden-hyun`)이 **비공개라면** 설정 파일에 둡니다. 공개 전환 가능성이 있다면 `account`도 `secrets/`로 옮깁니다. Connection의 `schema` 필드에 넣으면 됩니다.

#### 디렉터리 예시

```text
oracle-airflow/
├── .env                          # (gitignore) FERNET_KEY, JWT_SECRET 등 인프라 비밀 — 작업 B §7-4
├── secrets/                      # (gitignore) LocalFilesystemBackend 원본, 서버에만 존재
│   ├── connections.yaml          #   KIS 5개 키 + Upbit
│   ├── variables.yaml            #   SLACK_WEBHOOK_URL
│   ├── connections.example.yaml  #   (커밋) 키 이름만 있는 템플릿
│   └── variables.example.yaml    #   (커밋)
├── dags/
│   ├── config/
│   │   └── asset_flow.yaml       # (커밋) 비밀 아닌 설정 — 계좌·펀드·테이블·multiplier
│   └── *.py
└── plugins/asset_flow/config/
    └── loader.py                 # 설정 로더 (장기적으로 dags/common/ 이동 — 작업 A §7)
```

#### 예시: `dags/config/asset_flow.yaml` (커밋 대상)

```yaml
# 비밀이 아닌 설정만 둔다. appkey/secret/webhook은 절대 넣지 않는다 (secrets/ 참고).
# 최상위 키 이름(KIS_STOCK 등)은 token_manager의 토큰 딕셔너리 키 규약과 같다.
kis_accounts:
  KIS_STOCK:
    conn_id: kis_stock          # secrets/connections.yaml의 Connection ID
    account: "12345678"         # CANO (문자열로 — 앞자리 0 보존)
    product_code: "01"          # ACNT_PRDT_CD
    type: "한국투자 위탁"        # account_name
  KIS_ISA:
    conn_id: kis_isa
    account: "..."
    product_code: "01"
    type: "한국투자 ISA"
  KIS_PENSION_DEDUCTIBLE:
    conn_id: kis_pension_deductible
    account: "..."
    product_code: "22"
    type: "연금저축(세액공제)"
  KIS_PENSION_NON_DEDUCTIBLE:
    conn_id: kis_pension_non_deductible
    account: "..."
    product_code: "22"
    type: "연금저축(비공제)"
  KIS_CMA:
    conn_id: kis_isa            # CMA는 ISA 키·토큰을 공유 — 현재 암묵 규약을 명시화
    token_key: KIS_ISA
    account: "..."
    product_code: "..."
    type: "한국투자 CMA"

upbit:
  conn_id: upbit
  account: UPBIT
  type: 업비트

funds:
  pension_fund_code: K553W5E17401   # 연금저축 공제·비공제 공용
  crawl_codes: [K553W5E17401, K55105D43299]

manual_assets:                       # 기존 MANUAL_ASSET_CONFIG
  IRP:                  {product_code: K55105D43299, multiplier: 0.001}
  DC:                   {product_code: K55105D43299, multiplier: 0.001}
  INSTALLMENT_SAVINGS:  {product_code: "...",        multiplier: 1.0}
  HOUSING_SUBSCRIPTION: {product_code: "...",        multiplier: 1.0}

tables:
  asset_daily:         {schema: account, table_name: asset_daily}
  exchange_rate_daily: {schema: market,  table_name: exchange_rate_daily}
  fund_price_daily:    {schema: market,  table_name: fund_price_daily}
```

> `product_code`, 계좌번호 같은 실제 값은 현재 Variable에서 옮겨 적습니다. 위의 `"..."`는 자리표시자입니다. IRP·DC 펀드코드 `K55105D43299`는 `fetch_asset_daily_dag.py`의 DC docstring과 `fund_codes` Param에서 확인한 값입니다.

#### 예시: `secrets/connections.yaml` (gitignore, 서버에만 있음)

```yaml
kis_stock:
  conn_type: generic
  description: KIS 위탁계좌 OpenAPI appkey/appsecret
  login: "<APPKEY>"
  password: "<APPSECRET>"
kis_isa:
  conn_type: generic
  login: "<APPKEY>"
  password: "<APPSECRET>"
kis_pension_deductible:
  conn_type: generic
  login: "<APPKEY>"
  password: "<APPSECRET>"
kis_pension_non_deductible:
  conn_type: generic
  login: "<APPKEY>"
  password: "<APPSECRET>"
kis_irp:                      # §1.1 부수 발견 1 — 사용처가 없으면 삭제 검토
  conn_type: generic
  login: "<APPKEY>"
  password: "<APPSECRET>"
upbit:
  conn_type: generic
  login: "<ACCESS_KEY>"
  password: "<SECRET_KEY>"
```

#### 예시: `secrets/variables.yaml` (gitignore)

```yaml
# 문자열 값만 둔다. JSON Variable을 쓸 경우 반드시 JSON "문자열"로 (YAML 매핑 금지 — §1.2(c) 함정 1)
SLACK_WEBHOOK_URL: "https://hooks.slack.com/services/T000/B000/XXXX"
```

#### compose 설정 (작업 B §7 단계 5에 추가할 부분, 초안)

```yaml
x-airflow-common:
  environment:
    AIRFLOW__SECRETS__BACKEND: airflow.secrets.local_filesystem.LocalFilesystemBackend
    AIRFLOW__SECRETS__BACKEND_KWARGS: >-
      {"variables_file_path": "/opt/airflow/secrets/variables.yaml",
       "connections_file_path": "/opt/airflow/secrets/connections.yaml"}
    # 기본 키워드에 없는 이름도 마스킹 (appkey는 'apikey'와 다름, webhook URL 보호)
    AIRFLOW__CORE__SENSITIVE_VAR_CONN_NAMES: 'appkey,webhook'
    ASSET_FLOW_CONFIG: /opt/airflow/dags/config/asset_flow.yaml
  volumes:
    - ./secrets:/opt/airflow/secrets:ro   # api-server·scheduler·dag-processor·triggerer 모두(공통 앵커)
```

- 호스트 권한: `chown ${AIRFLOW_UID}:0 secrets/*.yaml && chmod 640 secrets/*.yaml`. compose가 `user: "${AIRFLOW_UID:-50000}:0"`으로 실행되기 때문입니다.
- `variables.yaml`을 읽는 경로도 같으므로, 지금 `callbacks.py`의 `Variable.get("SLACK_WEBHOOK_URL")` 호출은 **코드를 고치지 않아도** 파일 값을 읽습니다. `default_var` → `default` 수정은 작업 A에서 다룹니다.

### 1.4 로더 코드 (Airflow 3 SDK 기준, 초안)

```python
# plugins/asset_flow/config/loader.py
"""
비밀 아닌 설정(dags/config/asset_flow.yaml)과 자격증명(Connection)을 합쳐
기존 Variable(KIS_* / UPBIT / MANUAL_ASSET_CONFIG)과 같은 모양의 dict를 돌려준다.
호출부(KISApiClient, transformers, token_manager)는 반환 모양이 같으므로 바뀌지 않는다.

반드시 태스크(또는 콜백) 실행 중에 호출한다 — Connection 조회는 Task SDK 경로를 탄다.
"""
from __future__ import annotations

import functools
import os
from pathlib import Path

import yaml
from airflow.sdk import Connection, Variable
from airflow.sdk.log import mask_secret

_CONFIG_PATH = Path(os.getenv("ASSET_FLOW_CONFIG", "/opt/airflow/dags/config/asset_flow.yaml"))


@functools.lru_cache(maxsize=1)
def settings() -> dict:
    """설정 파일은 태스크 프로세스당 1회만 읽는다(작은 로컬 파일, 네트워크 없음)."""
    with _CONFIG_PATH.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def _credentials(conn_id: str) -> tuple[str, str]:
    conn = Connection.get(conn_id)
    mask_secret(conn.login)          # login은 기본 마스킹 대상이 아님
    return conn.login, conn.password


def kis_account(key: str) -> dict:
    """예: kis_account("KIS_STOCK") → {"appkey","secret","account","product_code","type"}"""
    acct = settings()["kis_accounts"].get(key)
    if acct is None:
        # 전환 기간 폴백: 파일에 아직 없는 계좌는 기존 Variable에서 읽는다 (롤백 경로)
        return Variable.get(key, deserialize_json=True)
    appkey, secret = _credentials(acct["conn_id"])
    return {
        "appkey": appkey,
        "secret": secret,
        "account": acct["account"],
        "product_code": acct["product_code"],
        "type": acct["type"],
    }


def kis_token_key(key: str) -> str:
    """토큰 딕셔너리 키. CMA처럼 다른 계좌 토큰을 빌려 쓰는 경우를 설정으로 명시한다."""
    return settings()["kis_accounts"][key].get("token_key", key)


def upbit_account() -> dict:
    cfg = settings()["upbit"]
    appkey, secret = _credentials(cfg["conn_id"])
    return {"appkey": appkey, "secret": secret, "account": cfg["account"], "type": cfg["type"]}


def manual_asset_config() -> dict:
    return settings()["manual_assets"]
```

호출부 변경 예시:

```python
# fetch_asset_daily.get_cma_cash  (Before → After)
cma_config = Variable.get('KIS_CMA', deserialize_json=True)
kis = KISApiClient(tokens['KIS_ISA'], cma_config)
# ↓
from asset_flow.config.loader import kis_account, kis_token_key
cma_config = kis_account('KIS_CMA')
kis = KISApiClient(tokens[kis_token_key('KIS_CMA')], cma_config)

# token_manager._get_kis_token(variable_name)
info = json.loads(Variable.get(variable_name))
# ↓
info = kis_account(variable_name)          # appkey/secret 키 이름 동일

# ledger_transformer — multiplier 이중 관리 해소(선택)
_FUND_MULTIPLIER = 0.001
# ↓ transform_fund_position(row, ..., multiplier=manual_asset_config()[row["account_type"]]["multiplier"])
```

- 모듈 레벨에서 `settings()`를 호출하는 것은 DAG 구조에 값이 필요할 때만 허용합니다. 예: `fund_codes` Param 기본값을 `settings()["funds"]["crawl_codes"]`로 두는 경우입니다. 이때 YAML을 바꾸면 새 DAG 버전이 생기며, 이는 의도된 동작입니다.
- `PyYAML`은 Airflow 코어 의존성이라 공식 이미지에 이미 포함돼 있습니다.
- 3.x LocalExecutor는 태스크마다 별도 프로세스에서 실행하므로 `lru_cache`는 태스크 1회 범위로만 유지됩니다. 따라서 오래된 값이 남지 않습니다.

### 1.5 기존 Variable에서 옮기는 절차

작업 C의 단계에 맞춥니다. **로더의 폴백 덕분에 각 단계를 독립적으로 롤백할 수 있습니다.**

| 단계 | 시점 | 작업 | 롤백 |
|---|---|---|---|
| 0 | 지금(2.10.4) | `docker compose exec airflow-scheduler airflow variables export /tmp/vars.json` → 호스트로 복사. **평문 파일이므로 작업이 끝나면 삭제합니다.** | — |
| 1 | Phase 0 | `vars.json`을 나눕니다. 비밀 아닌 키는 `dags/config/asset_flow.yaml`(커밋), appkey/secret은 `secrets/connections.yaml`, webhook은 `secrets/variables.yaml`로 보냅니다. `.gitignore`(§1.6)를 먼저 커밋합니다. | 파일 삭제 |
| 2 | Phase 0 또는 3.x 전환 PR | 로더를 추가하고 호출부를 교체합니다(Variable 폴백 유지). 2.10.4에서는 `airflow.sdk`가 없으므로 **로더만 3.x 전환 PR에 넣고**, 2.x 단계에서는 설정 파일만 준비해 두는 것이 단순합니다. | git revert |
| 3 | 3.x compose 적용 | `AIRFLOW__SECRETS__BACKEND(_KWARGS)`와 `./secrets:ro` 마운트를 모든 Airflow 서비스에 추가합니다(§1.3). | env 제거 → 메타DB Variable로 폴백 |
| 4 | 검증 | ① `airflow variables get SLACK_WEBHOOK_URL`과 `airflow connections get kis_stock`이 파일 값을 돌려주는지 확인합니다(CLI가 백엔드를 거치는지는 3.3.2에서 실측 필요). ② `test_slack_alert`로 실패 → 복구 알림을 확인합니다. dag-processor 경로도 함께 검증됩니다. ③ `make_token`을 수동 실행해 토큰 6개 발급을 확인합니다. ④ 태스크 로그에서 appkey/secret/webhook이 `***`로 가려졌는지 확인합니다. | 3과 동일 |
| 5 | 3일간 관찰 | 07시 체인과 `upsert_manual_position` 수동 실행. 로그에 `Unable to retrieve variable from secrets backend`가 없는지 확인합니다(함정 1 감시). | 3과 동일 |
| 6 | 정리 | 메타DB의 `KIS_*`, `UPBIT`, `MANUAL_ASSET_CONFIG`, `SLACK_WEBHOOK_URL`을 `airflow variables delete <key>`로 삭제하고, 로더의 Variable 폴백도 제거합니다. **삭제해야 "UI에 보이는 값 ≠ 실제 값" 혼동과 평문 잔존이 사라집니다.** 이어서 작업 B의 `rotate-fernet-key`를 적용하고, 평문이 담긴 옛 dump는 롤백 기간이 끝나면 폐기합니다. | 단계 0의 export로 재등록 |
| 7 | 선택 | 평문 노출 이력이 있으므로 KIS appkey/secret을 재발급하고 `secrets/connections.yaml`만 교체합니다. 재시작은 필요 없습니다. | — |

스테이징(작업 C Phase 2)에서는 `secrets/`를 **복사하지 말고**, 스테이징 전용 파일을 따로 둡니다. KIS 토큰 경합 주의는 작업 C §5.1을 따릅니다.

### 1.6 `.gitignore` 처리 (제안 — 저장소에는 반영하지 않음)

현행 `.gitignore`는 `.env`와 `data/`(토큰 파일)를 이미 제외합니다. 다음을 추가합니다.

```gitignore
# Airflow LocalFilesystemBackend — 자격증명 원본 (서버에만 존재)
secrets/*
!secrets/*.example.yaml
!secrets/README.md

# 일시적 export 산출물 (평문)
vars*.json
*_variables_export.json
```

- 커밋하기 전에 `git check-ignore -v secrets/connections.yaml`로 실제로 제외되는지 확인합니다.
- `dags/config/asset_flow.yaml`은 **커밋 대상**입니다. 실수로 자격증명을 넣지 않도록, pre-commit에 `appkey|appsecret|hooks.slack.com` 패턴 검사를 거는 것을 권장합니다(예: `detect-secrets` 또는 간단한 grep 훅).

---

## 2. DAG 파일명·dag_id 평가

### 2.1 현재 DAG가 실제로 하는 일 (코드로 확인)

| dag_id (파일) | 스케줄 | 태스크 흐름 | 적재 대상 | 성격 |
|---|---|---|---|---|
| `make_token` (`make_token_dag.py`) | 06:50 | `generate_tokens` | `data/tokens/*.json`(파일) | 인증 준비(ETL 아님) |
| `fetch_exchange_rate` (`fetch_exchange_rate_dag.py`) | 06:55 | `wait_for_token` → `get_standard_date` → `fetch_exchange_rates` → `transform_exchange_rates` → `upload_exchange_rates` | `market.exchange_rate_daily` | **E+T+L** |
| `fetch_fund_price_daily` (`fetch_fund_price_daily_dag.py`) | 06:55 | `get_standard_date` → `crawl_fund_prices` → `upload_fund_prices` | `market.fund_price_daily` | **E(크롤링)+T+L** |
| `fetch_asset_daily` (`fetch_asset_daily_dag.py`) | 07:00 | 센서 2 → `get_standard_date` → `get_*` 10개 → `upload_*` 10개 | `account.asset_daily` | **E+T+L**. 10개 중 4개(IRP·DC·적금·청약)는 API가 아니라 원장에서 파생 |
| `rebuild_derived_assets` | 수동 | `rebuild_irp/dc/savings/housing` | `account.asset_daily` | 백필·재계산 |
| `upsert_manual_position` | 수동 | `validate` → `upsert` | `account.manual_position_ledger` | 수동 입력 적재 |
| `test_slack_alert` | 수동 | `task_will_succeed`, `task_will_fail` | — | 운영 점검 |

DAG의 `tags`도 이미 ETL임을 말하고 있습니다. 수집 DAG 3개에 `ingestion`이 붙어 있고, 도메인은 `market`/`account`/`auth`로 태깅돼 있습니다.

### 2.2 문제점

1. **`fetch_`가 범위를 과소 표현합니다.** "fetch"는 보통 추출(E)을 뜻하는데, 4개 DAG 모두 변환과 DB 적재까지 하는 ETL입니다. 게다가 `fetch_exchange_rate` DAG 안에 같은 동사의 **태스크 `fetch_exchange_rates`**(추출 단계만)가 있어, 같은 단어가 DAG 수준과 태스크 수준에서 다른 범위를 가리킵니다. `fetch_asset_daily`는 40%가 원장 파생이라 "fetch"와 더 거리가 멉니다.
2. **`_daily`의 기준이 섞여 있습니다.** `fetch_fund_price_daily`와 `fetch_asset_daily`의 `_daily`는 스케줄이 아니라 **적재 테이블명**(`fund_price_daily`, `asset_daily`)에서 왔습니다. 반면 `fetch_exchange_rate`는 테이블이 `exchange_rate_daily`인데도 접미사가 없습니다. 같은 매일 실행인 `make_token`에도 없습니다. 스케줄 주기를 이름에 넣으면 스케줄이 바뀔 때 이름이 거짓이 되므로, **"테이블명을 그대로 쓴다"는 규칙으로 통일**하는 것이 맞습니다.
3. **도메인 접두사가 없습니다.** 3.x UI의 DAG 목록은 기본적으로 dag_id 순으로 정렬됩니다. 그래서 `account`(자산 4개)와 `market`(시장 2개)이 시각적으로 묶이지 않고, 태그 필터에만 의존하게 됩니다.
4. **동사 어휘가 통제되지 않습니다.** DAG 동사는 `make`/`fetch`/`rebuild`/`upsert`/`test`이고, 태스크의 추출 동사는 `fetch`/`crawl`/`get`이 섞여 있습니다. `make_token`의 `make`는 실제로 하는 일(KIS 5개·Upbit 1개 토큰 **발급**)을 흐리게 표현합니다.
5. 파일 접미사 `_dag.py`: 7개 모두 `<dag_id>_dag.py`로 **일관됩니다.** `dags/` 폴더가 이미 DAG임을 알려 주므로 중복이긴 하지만, 일관성이 지켜져 있어 문제는 아닙니다. 정리한다면 `<dag_id>.py`(파일명 = dag_id)로 맞추면 grep과 탐색이 가장 쉽습니다.

### 2.3 업계 관례

- **Airflow 공식 문서는 dag_id 명명 규칙을 정하지 않습니다.** 제약은 `^[\w.-]+$`, 250자 이하뿐입니다(소스 확인: task-sdk `KEY_REGEX`, core `ID_LEN=250`). Astronomer 포럼과 가이드도 "프로젝트 안의 기존 관례를 따르라" 수준입니다. 따라서 아래는 일반적인 데이터 엔지니어링 관례를 정리한 것입니다.
  - **도메인_동사_대상**: `sales_ingest_orders`처럼 소유 도메인으로 묶습니다.
  - **계층 접두사**: `ingest_`/`transform_`/`export_`, 또는 medallion의 `bronze_`/`silver_`를 앞에 붙여 파이프라인 단계로 묶습니다. dbt의 `stg_<source>__<entity>`처럼 이중 밑줄로 구분자를 강조하는 방식도 널리 쓰입니다.
  - **대상 테이블 = 이름**: "DAG 하나가 테이블 하나를 소유한다"는 구조에서 쓰며, 소유 관계 추적이 쉽습니다.

### 2.4 대안 비교

| 기준 | A안: 최소 교정 (`<동사>_<대상>`) | **B안: 도메인 접두사 (`<도메인>_<동사>_<대상테이블>`)** | C안: 계층 접두사 (`<계층>_<스키마>_<테이블>`) |
|---|---|---|---|
| 예 (`fetch_asset_daily`) | `ingest_asset_daily` | `account_ingest_asset_daily` | `ingest_account_asset_daily` |
| `fetch_` 문제 해결 | ○ | ○ | ○ |
| `_daily` 일관성 | ○ (테이블명 규칙) | ○ | ○ |
| UI 정렬 시 묶임 | 동사별(ingest끼리) | **도메인별**(account/market) — 태그와 일치 | 계층별 |
| 수동·운영 DAG 표현 | 자연스러움 | 자연스러움 (`account_upsert_...`, `ops_test_...`) | 어색함 (`manual_account_manual_position_ledger`) |
| 변경 폭 | 작음 | 중간 | 중간 |
| 규모 확장(DAG 20+) | 약함 | **강함** | 강함(웨어하우스형일 때) |

**권장: B안.** 이유는 다음과 같습니다.
1. 이미 쓰고 있는 `tags`(`account`, `market`, `auth`)와 DB 스키마 구조를 그대로 이름에 반영하므로 새로 익힐 개념이 없습니다.
2. 수동 DAG(`upsert`, `rebuild`)와 운영 DAG(`test`)까지 한 규칙으로 표현할 수 있습니다.
3. 동사를 통제된 어휘로 제한할 수 있습니다.

**규칙 정의**
- `<도메인>`: `market` | `account` (DB 스키마) | `auth` (외부 API 인증) | `ops` (운영·점검)
- `<동사>`:
  - `ingest`: 외부(API/크롤링)에서 추출해 적재. 파생 계산 포함
  - `rebuild`: 기존 원천으로 재계산·백필
  - `upsert`: 사람이 입력한 값을 적재
  - `issue`: 자격증명·토큰 발급
  - `test`: 점검
- `<대상>`: 적재 테이블명(`_daily` 등 테이블 접미사 포함). 테이블이 없으면 산출물 이름.
- 파일명: `<dag_id>.py` (`_dag` 접미사 제거). 파일명만 바꾸는 것은 선택 사항입니다.
- 이중 밑줄(`account__ingest_asset_daily`)은 도메인이 4개로 고정돼 있어 굳이 필요하지 않습니다. 기존 snake_case 스타일을 유지합니다.

### 2.5 권장 매핑표 (B안)

| 현재 dag_id | 현재 파일 | 권장 dag_id | 권장 파일 | 비고 |
|---|---|---|---|---|
| `make_token` | `make_token_dag.py` | `auth_issue_api_tokens` | `auth_issue_api_tokens.py` | KIS·Upbit 토큰 발급(산출물은 파일) |
| `fetch_exchange_rate` | `fetch_exchange_rate_dag.py` | `market_ingest_exchange_rate_daily` | `market_ingest_exchange_rate_daily.py` | 테이블명 `exchange_rate_daily`에 맞춰 `_daily` 추가 |
| `fetch_fund_price_daily` | `fetch_fund_price_daily_dag.py` | `market_ingest_fund_price_daily` | `market_ingest_fund_price_daily.py` | |
| `fetch_asset_daily` | `fetch_asset_daily_dag.py` | `account_ingest_asset_daily` | `account_ingest_asset_daily.py` | 원장 파생 포함. "ingest" 정의에 포함됨 |
| `rebuild_derived_assets` | `rebuild_derived_assets_dag.py` | `account_rebuild_ledger_assets` | `account_rebuild_ledger_assets.py` | 대상이 "원장 기반 4계좌"임을 명시 |
| `upsert_manual_position` | `upsert_manual_position_dag.py` | `account_upsert_manual_position_ledger` | `account_upsert_manual_position_ledger.py` | 테이블명 그대로 |
| `test_slack_alert` | `test_slack_alert_dag.py` | `ops_test_slack_alert` | `ops_test_slack_alert.py` | |

> task_id(`fetch_exchange_rates`, `crawl_fund_prices`, `get_*` 등)는 **바꾸지 않기를 권장합니다.** task_id는 DAG 안에서만 유효하므로 혼동이 작습니다. 반면 바꾸면 TI 이력과 그래프 버전이 다시 끊기고, `ExternalTaskSensor.external_task_id`(`generate_tokens`, `upload_exchange_rates`, `upload_fund_prices`)까지 함께 고쳐야 합니다.

### 2.6 dag_id 변경의 영향

| 영역 | 영향 | 대응 |
|---|---|---|
| 실행 이력 (DagRun/TI/XCom/로그) | 모두 `dag_id`를 키로 씁니다. 새 ID는 **이력 0건인 새 DAG**이고, 옛 ID는 파일이 사라지면 dag-processor가 **stale**(3.x `is_stale`)로 표시합니다. 이력은 메타DB에 남고 로그도 `logs/dag_id=<옛ID>/`에 남습니다. | 옛 이력이 필요 없어지면 `airflow dags delete <옛ID>`로 정리합니다. 롤백 기간이 끝난 뒤에 합니다. |
| DAG 버전(3.x) | 새 ID는 v1부터 시작합니다. 옛 버전 이력은 옛 ID에 남습니다. | — |
| **`ExternalTaskSensor.external_dag_id`** | 3곳: `fetch_exchange_rate.wait_for_token`(→`make_token`), `fetch_asset_daily.wait_for_exchange_rate`, `wait_for_fund_price_daily`. 상류만 바꾸고 하류를 그대로 두면 **600초 뒤 `AirflowSensorTimeout`**이 납니다. 센서는 `dag_id` + `logical_date − 5분`으로 매칭합니다(작업 A §5). | 7개 DAG를 **한 커밋으로 동시에** 바꿉니다. 상류·하류의 첫 run이 같은 날 생기도록 배포합니다. |
| **paused 상태** | `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION='true'`이므로 **새 ID 7개가 모두 paused로 생성됩니다.** 실행이 아예 생기지 않으므로 실패 콜백도 호출되지 않아 **Slack 알림 없이 07시 체인이 멈춥니다.** | 배포 체크리스트에 "새 ID 4개 unpause(스케줄 DAG), 수동 DAG 3개 unpause"를 넣습니다. 같은 날 `airflow dags list`로 `is_paused=False`를 확인합니다. |
| unpause 직후 즉시 실행 | `catchup=False`라도 unpause 시점에 직전 스케줄 1회가 바로 생성될 수 있습니다(timetable별 동작은 3.3.2에서 실측 필요). | 적재가 `(standard_date, account_code)` 단위 DELETE 후 INSERT로 멱등이라 데이터 위험은 낮습니다. 상류부터(`auth` → `market` → `account`) 순서대로 unpause하면 센서 대기도 줄어듭니다. |
| 콜백 (`callbacks.py`) | Slack 메시지의 dag_id 표기가 바뀝니다. 과거 메시지의 "UI에서 확인" 링크(옛 ID)는 stale DAG 페이지로 연결됩니다. 복구 알림 판단(작업 A §4.4 대안 A `ti.get_previous_dagrun()`, 대안 B REST)은 dag_id 기준이라 **새 ID의 첫 성공 run은 직전 run이 없어 복구 알림을 보내지 않습니다.** 정상 동작입니다. | 없음. 콜백 코드에 dag_id 하드코딩이 없으므로 수정할 필요가 없습니다. |
| Variable/Connection/Pool | dag_id와 무관합니다(§1의 새 구조도 무관). | — |
| 업무 DB | `account.asset_daily` 등에는 dag_id 컬럼이 없습니다. | — |
| 문서·주석 | dag_id 문자열이 DAG 7개 파일(docstring, `DAG_DOC`, 센서), `README.md`(13곳), `handoff.md`, `plugins/asset_flow/README.md`, `plugins/CALLBACKS_GUIDE.md`, `token_manager.py` 주석에 있습니다. | 같은 커밋에서 일괄 치환하고 `grep`으로 0건인지 확인합니다. |
| 외부 참조 | 북마크, `/api/v2/dags/{dag_id}/...` 호출 스크립트. 저장소 안에는 없습니다(작업 B §6.2). | 외부 스크립트를 따로 확인합니다. |

### 2.7 변경 시점

| 시점 | 장점 | 단점 | 평가 |
|---|---|---|---|
| ① 지금(2.10.4, 작업 C Phase 0) | 변경 축이 분리됩니다(이름만 바뀜). | 배포와 관찰 사이클이 한 번 더 늘어납니다. "운영 영향 없음"이 원칙인 Phase 0에 paused·센서 위험을 들여옵니다. 2.x 이력도 한 번 더 끊깁니다. | △ |
| **② 3.x 전환(Phase 3) 때, 별도 커밋으로 같은 배포 창에** | 3.x 전환으로 logical_date 의미(CronTriggerTimetable), UI URL, 복구 알림 기준선이 **어차피 끊깁니다.** 센서 체인 재검증과 unpause 단계가 Phase 3에 이미 있어 **검증 비용을 한 번만 냅니다.** 스테이징(Phase 2)에서 새 이름으로 미리 검증할 수 있습니다. | 전환 당일 변경점이 늘어납니다. 다만 이름 변경은 기계적이고 grep으로 검증할 수 있으며, 커밋을 분리해 원인을 추적할 수 있습니다. 롤백할 때도 2.10.4 dump는 옛 ID, 옛 코드와 함께 복원되므로 정합성이 유지됩니다. | **권장** |
| ③ 3.x를 신규 구축(메타DB 새로 생성, 이력 미이관) | 잃을 이력이 없으므로 이름 변경 비용이 0에 가깝습니다. | — | **무조건 함께 변경** |
| ④ 바꾸지 않음 | 비용 없음 | `fetch_`/`_daily` 혼선이 남고, DAG가 늘수록 비용이 커집니다. | 최소한 A안 수준의 교정이라도 권장 |

**결론**: 작업 C의 Phase 2(스테이징)에서 새 이름으로 검증합니다. Phase 3에서는 "3.x 코드 전환 커밋"과 "dag_id 일괄 변경 커밋"을 분리하되 같은 배포 창에 적용합니다. 배포 체크리스트에는 다음을 넣습니다.
- 새 ID unpause
- 센서 3곳 매칭 확인
- `grep -rn "fetch_asset_daily\|make_token\|..."` 0건 확인

Variable → 설정 파일·secrets 이전(§1.5 단계 2~3)도 같은 3.x 전환 PR에 들어가므로, 07시 체인 재검증(작업 C Phase 2 검증 항목 1~7)을 한 번 수행하면 세 가지 변경을 모두 덮을 수 있습니다.

---

## 3. 확인이 더 필요한 사항

1. `airflow variables get` / `airflow connections get` CLI가 3.3.2에서 `[secrets] backend`를 거쳐 조회하는지. §1.5 단계 4에서 실측합니다. 안 된다면 `test_slack_alert`와 `make_token` 실행으로 대체 검증합니다.
2. `KIS_CMA`의 appkey/secret이 `KIS_ISA`와 같아야 하는지(KIS 토큰-appkey 바인딩 정책).
3. GitHub 원격 저장소의 공개 범위. 계좌번호를 설정 파일에 둘지(§1.3 주석) 결정하는 데 필요합니다.
4. 새 dag_id를 unpause한 직후 CronTriggerTimetable(`catchup=False`)이 직전 스케줄 run을 즉시 만드는지 스테이징에서 확인합니다.
5. "secrets 백엔드 예외를 삼키고 다음 백엔드로 넘어가는" 동작이 3.1.x에서도 같은지. task-sdk 1.3.2만 소스로 확인했습니다.

---

## 출처

- Local Filesystem Secrets Backend (3.3.2): https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/secrets-backend/local-filesystem-secrets-backend.html
- Secrets Backend 개요(탐색 순서, `[workers] secrets_backend`): https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/secrets-backend/index.html
- Masking sensitive data(기본 민감 키워드, `mask_secret`): https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/mask-sensitive-values.html
- Managing Variables(`AIRFLOW_VAR_*`): https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html
- Managing Connections(`AIRFLOW_CONN_*`, JSON 형식): https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html
- CLI Reference(`variables import -a {overwrite,fail,skip}`, `connections import --overwrite`, `variables export`): https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html
- Configuration Reference(`[secrets] backend/backend_kwargs/use_cache`, `[workers] secrets_backend*`, `[core] sensitive_var_conn_names`, `[dag_processor] min_file_process_interval`, `dag_bundle_config_list`): https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html
- Best Practices(top-level code, Variables at top level): https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html
- Dynamic Dag Generation(Dag 폴더 내 YAML/JSON, `__file__`, `.airflowignore`): https://airflow.apache.org/docs/apache-airflow/stable/howto/dynamic-dag-generation.html
- Dag Bundles(LocalDagBundle 비버저닝, GitDagBundle): https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-bundles.html
- Task SDK API(1.3.2, `airflow.sdk.Connection`/`Variable`): https://airflow.apache.org/docs/task-sdk/stable/api.html
- 명명 관례 참고(공식 규칙 없음을 확인): Astronomer 포럼 https://forum.astronomer.io/t/what-is-the-best-practice-of-giving-id/210 , Astronomer authoring-dags 스킬 https://tessl.io/registry/skills/github/astronomer/agents/authoring-dags , dbt 구조 가이드(`stg_<source>__<entity>`) https://docs.getdbt.com/best-practices/how-we-structure/2-staging
- 소스 확인에 쓴 PyPI wheel(스크래치패드에서 압축만 풀어 읽음):
  - `apache-airflow-core==3.3.2`: `airflow/secrets/local_filesystem.py`(`load_variables`, `_local_variables` property), `airflow/api_fastapi/execution_api/routes/variables.py`, `airflow/providers_manager.py`(generic 타입), `airflow/models/base.py`(`ID_LEN`)
  - `apache-airflow-task-sdk==1.3.2`: `execution_time/secrets/__init__.py`(`DEFAULT_SECRETS_SEARCH_PATH_WORKERS`), `execution_time/supervisor.py`(`ensure_secrets_backend_loaded`), `execution_time/context.py`(`_get_variable`, `_mask_and_deserialize_variable`), `_shared/configuration/parser.py`(`_get_custom_secret_backend` 워커 → secrets 폴백), `definitions/connection.py`(`Connection.get`, `extra_dejson`), `definitions/_internal/node.py`(`KEY_REGEX`)
- 선행 보고서: `docs/airflow3_A_code_impact.md`, `docs/airflow3_B_infra.md`, `docs/airflow3_C_release_strategy.md`
