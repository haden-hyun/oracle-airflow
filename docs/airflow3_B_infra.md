# 작업 B — Airflow 3 전환 시 인프라·배포·운영 구성 영향 분석

- 작성일: 2026-10-12
- 대상 저장소: `oracle-airflow`. 읽기 전용으로 분석했고 저장소 파일은 수정하지 않았다.
- 현재 구성: `apache/airflow:2.10.4`(기본 Python 3.12), LocalExecutor, docker-compose(webserver/scheduler/init + `postgres:13`), TaskFlow DAG 7개, `plugins/callbacks.py`, `plugins/asset_flow`
- 기준 문서: 2026-10-12 시점 Airflow **3.3.2** 문서(stable, 2026-09-17 릴리스). 출처는 문서 맨 끝 §9에 모았다.

---

## 0. 요약 (TL;DR)

| # | 영역 | 결론 | 심각도 |
|---|---|---|---|
| 1 | 컴포넌트 | `webserver`를 `api-server`로 바꾸고, **`dag-processor`를 별도 서비스로 추가해야 한다(필수)**. `triggerer`는 권장한다. 헬스체크 엔드포인트는 `/api/v2/monitor/health`, 스케줄러는 `airflow jobs check`로 바뀐다. | 차단 |
| 2 | 설정 | `AIRFLOW__WEBSERVER__BASE_URL`은 `AIRFLOW__API__BASE_URL`로 옮긴다. `AIRFLOW__API__AUTH_BACKEND`(2.x 값)는 무의미해진다. **`AIRFLOW__API_AUTH__JWT_SECRET`, `AIRFLOW__API__SECRET_KEY`, `AIRFLOW__CORE__EXECUTION_API_SERVER_URL`을 모든 컴포넌트에 같은 값으로 고정해야 한다.** | 차단 |
| 3 | 인증 | 기본값이 SimpleAuthManager로 바뀌었다. 기존 사용자(`ab_user`)와 `_AIRFLOW_WWW_USER_CREATE` 흐름을 유지하려면 **FAB provider + FabAuthManager를 권장한다**(공식 compose 템플릿 3.3.2도 FAB를 쓴다). | 높음 |
| 4 | 이미지/패키지 | `apache/airflow:3.3.2-python3.12`로 Python 태그를 고정한다. `apache-airflow-providers-standard`, `-postgres`, `-fab`를 명시하고 `apache-airflow==3.3.2`를 함께 지정해 설치한다. playwright 설치 단계는 그대로 유지할 수 있다(빌드 후 검증 필요). | 중간 |
| 5 | 메타DB | **3.3.1부터 PostgreSQL 13 지원이 빠졌다(14~18만 지원).** 현재 `postgres:13`이므로 PG 메이저 업그레이드(dump/restore)가 선행돼야 한다. 출발 버전은 2.7 이상이면 되므로 2.10.4는 조건을 충족한다. | 차단 |
| 6 | REST API | `/api/v1`이 제거되고 `/api/v2`(JWT)로 대체됐다. 저장소 안에서 v1을 호출하는 곳은 없다. `callbacks.py`의 UI 딥링크(`/dags/{id}/grid?dag_run_id=`)는 3.x 형식(`/dags/{id}/runs/{run_id}`)으로 바꾸고, `AIRFLOW__WEBSERVER__BASE_URL` 환경변수 참조도 수정해야 한다. | 중간 |
| 7 | 부수 발견 | 현행 compose의 스케줄러 헬스체크는 `ENABLE_HEALTH_CHECK`가 꺼져 있어(2.10 기본값 False) 항상 unhealthy일 가능성이 높다. `FERNET_KEY=''`이므로 KIS appkey/secret Variable이 평문으로 저장돼 있다. | 참고 |

---

## 1. 컴포넌트 구조 변화

### 1.1 현재와 3.x 비교

| 역할 | 현재 (2.10.4) | Airflow 3.x | 비고 |
|---|---|---|---|
| UI/REST | `airflow-webserver` (`command: webserver`, Flask/FAB) | `airflow-apiserver` (`command: api-server`, FastAPI, React UI) | UI·`/api/v2`·**Execution API(`/execution/`)**를 한 프로세스가 제공한다. |
| DAG 파싱 | 스케줄러 내장 | **`airflow-dag-processor` (`command: dag-processor`) 필수** | 3.0 릴리스 노트의 "Standalone DAG Processor Required". 로컬 구성에서도 별도로 실행해야 한다. |
| 스케줄링/실행 | `airflow-scheduler` (LocalExecutor, 태스크는 스케줄러 컨테이너 안에서 실행) | 동일하다. 단, 태스크 프로세스가 **메타DB에 직접 붙지 않고 api-server의 Execution API를 HTTP로 호출**한다. | 스케줄러 컨테이너에서 `http://airflow-apiserver:8080/execution/`에 도달할 수 있어야 한다. |
| Deferrable | 없음 | `airflow-triggerer` (`command: triggerer`) | 지금 DAG는 deferrable을 쓰지 않으므로 선택 사항이다. 다만 공식 템플릿에 포함돼 있고, `ExternalTaskSensor(deferrable=True)`로 전환할 여지가 있어 **추가를 권장한다**. |
| 초기화 | `airflow-init` (`command: version`, `_AIRFLOW_DB_UPGRADE`) | `airflow-init` (`_AIRFLOW_DB_MIGRATE`) | `_AIRFLOW_DB_UPGRADE`는 아직 동작하지만 entrypoint가 deprecated 경고를 출력한다. |

> 3.x에서는 **태스크 코드가 메타DB 세션을 쓸 수 없다**(Upgrading 가이드의 "Review custom tasks for direct DB access"). DAG 파일 처리기와 triggerer에서는 여전히 접근할 수 있다. 이 차이는 `callbacks.py`에 직접 영향을 준다(§6.3).

### 1.2 헬스체크 엔드포인트 변경

| 서비스 | 현재 | 3.x (공식 템플릿 3.3.2) |
|---|---|---|
| webserver → apiserver | `curl --fail http://localhost:8080/health` | `curl --fail http://localhost:8080/api/v2/monitor/health` |
| scheduler | `curl --fail http://localhost:8974/health` | `airflow jobs check --job-type SchedulerJob --hostname "$${HOSTNAME}"` |
| dag-processor | (없음) | `airflow jobs check --job-type DagProcessorJob --hostname "$${HOSTNAME}"` |
| triggerer | (없음) | `airflow jobs check --job-type TriggererJob --hostname "$${HOSTNAME}"` |

> **현행 버그로 보이는 점**: 현재 스케줄러 헬스체크는 8974 포트를 쓰는데, 이 포트는 `AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK=True`일 때만 열린다. 2.10.4 기본값은 `False`이고 compose에 이 설정이 없으므로 스케줄러 컨테이너는 계속 unhealthy로 표시될 가능성이 높다. 다만 다른 서비스가 이 상태에 의존하지 않아 지금까지 드러나지 않았다. 공식 3.x 템플릿은 `ENABLE_HEALTH_CHECK: 'true'`를 켜고, 헬스체크 자체는 `jobs check`로 수행한다.

### 1.3 공식 docker-compose 3.x 템플릿(3.3.2)과의 diff 요지

공식 템플릿은 CeleryExecutor 기준이다. 이 저장소는 단일 서버 LocalExecutor이므로 Celery 관련 부분은 가져오지 않는다.

| 항목 | 공식 템플릿 3.3.2 | 현 저장소 | 전환안 |
|---|---|---|---|
| 이미지 | `image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:3.3.2}` (`# build: .`) | `build: .` | `build: .` 유지. Dockerfile의 FROM 태그를 바꾼다. |
| Executor | `CeleryExecutor` + redis + `airflow-worker` + flower | `LocalExecutor` | **LocalExecutor 유지**. redis/worker/flower는 제외한다. |
| auth manager | `AIRFLOW__CORE__AUTH_MANAGER: ...FabAuthManager` | (2.x는 암묵적으로 FAB) | 명시적으로 FAB를 설정한다(§3). |
| DB 연결 키 | `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | `AIRFLOW__CORE__SQL_ALCHEMY_CONN` (2.3부터 deprecated) | `[database]` 키로 바꾼다. |
| Execution API | `AIRFLOW__CORE__EXECUTION_API_SERVER_URL: 'http://airflow-apiserver:8080/execution/'` | 없음 | 추가한다(필수). |
| JWT | `AIRFLOW__API_AUTH__JWT_SECRET`, `AIRFLOW__API_AUTH__JWT_ISSUER` | 없음 | 추가한다(필수, 고정값). |
| 스케줄러 헬스 | `AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK: 'true'` | 없음 | 추가한다. |
| config 파일 | `AIRFLOW_CONFIG: '/opt/airflow/config/airflow.cfg'` | `./config`는 마운트하지만 `AIRFLOW_CONFIG`가 없다. | 추가를 권장한다(`airflow config update/lint` 결과를 파일로 관리하기 위해). |
| env_file | `env_file: ${ENV_FILE_PATH:-.env}` | 없음(`.env`는 gitignore에만 있음) | secret 계열을 `.env`로 분리한다. |
| `FERNET_KEY` | `${FERNET_KEY}` | `''` | 키를 생성해 `.env`에 둔다. 기존 평문 Variable은 `airflow rotate-fernet-key`로 재암호화한다. |
| `LOAD_EXAMPLES` | `'true'` | `'false'` | 현행 유지. |
| postgres | `postgres:16`, healthcheck `start_period: 5s` | `postgres:13` | **`postgres:16`으로 올린다(§5).** |
| 서비스 의존성 | api-server/scheduler/dag-processor/triggerer가 `airflow-init: service_completed_successfully`에 의존 | init 의존 없음 | 의존을 추가해 마이그레이션 완료 전에 기동되지 않도록 한다. |
| airflow-init | `entrypoint: /bin/bash` 스크립트(디렉토리 생성·`airflow config list`·chown) + `_AIRFLOW_DB_MIGRATE` | `command: version` + `_AIRFLOW_DB_UPGRADE` | 최소한 `_AIRFLOW_DB_MIGRATE`로 바꾼다. |
| `version: '3.8'` | 없음 | 있음 | 삭제한다(Compose v2에서 obsolete 경고가 난다). |
| `extra_hosts: host.docker.internal` | 없음 | 있음 | **유지한다**(자산 DB 접속에 필요). |
| `./data` 마운트 | 없음 | 있음 | **유지한다**(토큰 파일 `data/tokens/` 등). dag-processor와 triggerer에도 같은 볼륨을 붙인다. |

---

## 2. 설정 키 변경

### 2.1 저장소에서 쓰는 키의 매핑

| 현재 키 (compose) | 3.x 처리 | 근거 |
|---|---|---|
| `AIRFLOW__CORE__SQL_ALCHEMY_CONN` | → `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` (core 키는 config ref에 Deprecated로 표시) | Config Ref `[core] sql_alchemy_conn (Deprecated)` |
| `AIRFLOW__API__AUTH_BACKEND` = `airflow.api.auth.backend.basic_auth,...session` | **삭제**한다. `airflow.api.auth.backend.*` 모듈은 v1 API(Connexion)와 함께 제거됐다. `/api/v2`는 JWT(`/auth/token`)를 쓴다. FAB 자체 API의 백엔드는 `[fab] auth_backends`(`AIRFLOW__FAB__AUTH_BACKENDS`, 기본값 `airflow.providers.fab.auth_manager.api.auth.backend.session`)로 옮겨졌다. | FAB provider Config Ref, 3.0 Release Notes "REST API v2 replaces v1" |
| `AIRFLOW__WEBSERVER__BASE_URL` | → **`AIRFLOW__API__BASE_URL`**. `[webserver]` 섹션은 core config ref에서 사라졌다. 3.x 코드도 `conf.get("api","base_url")`을 읽는다(`TaskInstance.log_url`). | Config Ref `[api] base_url`, 3.0 Release Notes "All the webserver configurations have also been removed" |
| `AIRFLOW__CORE__EXECUTOR`, `FERNET_KEY`, `DAGS_ARE_PAUSED_AT_CREATION`, `LOAD_EXAMPLES` | 그대로 유지한다. | — |

### 2.2 3.0에서 이동하거나 삭제된 주요 키 (릴리스 노트 발췌)

- `[webserver] web_server_host/port/workers/web_server_worker_timeout/ssl_cert/ssl_key/access_logfile`는 `[api] host/port/workers/worker_timeout/ssl_cert/ssl_key/access_logfile`로 옮겨졌다.
- `[webserver] secret_key`는 `[api] secret_key`로 옮겨졌다(#50839).
- `[scheduler] parsing_processes/min_file_process_interval/stale_dag_threshold/...`와 `[core] dag_file_processor_timeout`은 `[dag_processor] ...`로 옮겨졌다.
- 삭제된 키: `[scheduler] allow_trigger_in_future/use_job_schedule/use_local_tz/processor_poll_interval`, `[logging] dag_processor_manager_log_*` 등.
- 기본값 변경: `catchup_by_default=False`, **`create_cron_data_intervals=False`**. 모든 DAG가 bare cron(`'0 7 * * *'` 등)을 쓰므로 `CronTriggerTimetable`로 해석이 바뀐다. `data_interval_*`에 의존하는지는 DAG 쪽 작업과 교차 확인해야 한다. 2.x 의미를 유지하려면 **업그레이드 전에** `AIRFLOW__SCHEDULER__CREATE_CRON_DATA_INTERVALS=True`를 설정한다. 3.x 실행 이력이 생긴 뒤에 바꾸면 1회차 스케줄이 건너뛰어진다.
- 3.3.0: `[core] execution_api_server_url`이 `[workers] execution_api_server_url`로 옮겨졌고, deprecated된 core 매핑은 복원됐다(#63949). 공식 템플릿은 여전히 `AIRFLOW__CORE__EXECUTION_API_SERVER_URL`을 쓰므로 당분간 core 키를 써도 무방하다.

### 2.3 신규 필수 설정 (Execution API / JWT)

| 키 | 값(권장) | 이유 |
|---|---|---|
| `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` | `http://airflow-apiserver:8080/execution/` | 기본값은 `{BASE_URL}/execution/`이고, BASE_URL이 없으면 `http://localhost:8080`이다. **스케줄러 컨테이너 안에서 localhost:8080은 api-server가 아니므로 태스크가 전부 실패한다.** `BASE_URL`을 외부 고정 IP로 설정하더라도 내부 통신용 URL은 따로 명시하는 편이 안전하다. |
| `AIRFLOW__API_AUTH__JWT_SECRET` | 무작위 고정 문자열(`.env`) | "JWT를 생성하거나 검증하는 모든 컴포넌트(Scheduler, API Server)에서 일관돼야 한다"(Config Ref). 설정하지 않으면 컴포넌트마다 다른 키가 생성돼 태스크 토큰 검증이 실패할 수 있다. |
| `AIRFLOW__API_AUTH__JWT_ISSUER` | `airflow` | 공식 템플릿과 같은 값. |
| `AIRFLOW__API__SECRET_KEY` | 무작위 고정 문자열(`.env`) | 세션/CSRF와 로그 조회 토큰에 쓰인다. 인스턴스 간에 같아야 한다. |
| `AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK` | `'true'` | 공식 템플릿과 같은 값. |

### 2.4 `airflow config update` / `lint` 활용 절차

환경변수 대부분이 compose에 있고 `airflow.cfg`는 없으므로, 두 명령을 아래 순서로 쓴다.

```bash
# (A) 2.10.4 이미지에서 현재 유효 설정을 덤프 (참고용)
docker compose run --rm airflow-scheduler airflow config list > /tmp/cfg_2x.txt

# (B) 3.x 이미지에서 변경·삭제 키 점검 (환경변수 포함)
docker compose run --rm airflow-scheduler airflow config lint
docker compose run --rm airflow-scheduler airflow config update          # dry-run: 변경 제안만 출력
docker compose run --rm airflow-scheduler airflow config update --fix    # airflow.cfg에 반영 (AIRFLOW_CONFIG 지정 시)
```

> 3.3.2에서 "`airflow config lint`가 조건부 삭제 규칙에서 아무것도 출력하지 않던 문제"가 수정됐다(#71651). **3.3.2 이상에서 lint를 돌려야 한다.** 환경변수로 넣은 키는 `--fix`가 고쳐 주지 않으므로 compose를 직접 수정해야 한다.

---

## 3. 인증 — SimpleAuthManager vs FAB provider

### 3.1 변경 사항
- 3.0부터 기본 `auth_manager`는 `airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager`이다.
- FAB를 계속 쓰려면 `apache-airflow-providers-fab`를 설치하고 `auth_manager = airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager`를 지정해야 한다.
- 인증 라우트는 `/auth` 아래로 옮겨졌다. 예: OAuth 콜백 `/oauth-authorized/google`은 `/auth/oauth-authorized/google`이 된다.

### 3.2 비교

| 기준 | SimpleAuthManager | FAB provider (FabAuthManager) |
|---|---|---|
| 사용자 정의 | `[core] simple_auth_manager_users = "admin:admin"` (역할은 viewer/user/op/admin 고정) | 메타DB `ab_user` 테이블. 기존 2.x 사용자를 그대로 이어 쓴다. |
| 비밀번호 | 자동 생성 후 api-server 로그와 `$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`에 저장한다. 컨테이너를 재생성하면 볼륨에 마운트하지 않는 한 **매번 바뀐다.** | `airflow users create`로 지정한다. |
| `_AIRFLOW_WWW_USER_CREATE` | **조용히 건너뛴다.** entrypoint가 `auth_manager`에 `FabAuthManager`가 없으면 "Skipping user creation as auth manager different from Fab is used"를 출력한다. | 동작한다(기존과 같음). 이미 존재하는 사용자면 `|| true`로 넘어간다. |
| 공식 문서 입장 | "개발과 테스트 용도". 운영에 쓰려면 접근 통제를 별도로 해야 한다. | 공식 docker-compose 3.3.2 템플릿의 기본값. |
| 비고 | `simple_auth_manager_all_admins=True`로 설정하면 인증을 끈다. | 커스텀 `webserver_config.py`가 있다면 `FabAirflowSecurityManagerOverride`로 바꿔야 한다(이 저장소에는 없다). |

### 3.3 권장안 — FAB provider

- 이유 1: 운영 서버(고정 IP:8080 노출, CALLBACKS_GUIDE.md 참고)에 개발용 auth manager를 올리지 않는다.
- 이유 2: 기존 `ab_user` 계정과 `_AIRFLOW_WWW_USER_*` 흐름을 바꾸지 않고 유지할 수 있다.
- 이유 3: 공식 템플릿과 같은 구성이므로 앞으로 템플릿 diff를 추적하기 쉽다.

`AIRFLOW__CORE__AUTH_MANAGER`를 **모든 컴포넌트 공통 env**에 넣어야 한다. init 컨테이너에서만 FAB로 설정하면 api-server가 Simple로 기동한다.

---

## 4. Python 버전, 베이스 이미지, constraints, provider, requirements.txt

### 4.1 Python / 이미지 태그
- 3.3.2가 지원하는 Python은 3.10~3.14이다(3.1.0에서 3.9 제거, 3.2.0에서 3.14 추가).
- `apache/airflow:3.3.2`의 기본 Python은 3.12이다. 현행 2.10.4 기본값도 3.12이므로 **런타임 Python 버전은 바뀌지 않는다.** 재현성을 위해 태그를 `apache/airflow:3.3.2-python3.12`로 명시한다(Docker Hub에 `3.3.2-python3.10~3.14`와 `slim-*`이 있다).
- slim 이미지는 provider가 최소 구성이므로, 현재 구조(provider를 기본 이미지에서 가져옴)에서는 **regular 이미지를 유지한다.**

### 4.2 필요한 provider

| 패키지 | 필요 이유 (저장소 코드 근거) |
|---|---|
| `apache-airflow-providers-standard` | `airflow.operators.python.get_current_context`, `airflow.sensors.external_task.ExternalTaskSensor`가 core에서 standard provider로 옮겨졌다(`airflow.providers.standard.sensors.external_task` 등). |
| `apache-airflow-providers-postgres` | `PostgresHook`(자산 DB 적재). 2.x에서도 이미지 기본 포함이었다. |
| `apache-airflow-providers-fab` | §3의 FabAuthManager. |

reference 이미지에도 대부분 포함돼 있지만, `requirements.txt` 설치가 의존성을 해석하는 과정에서 버전이 바뀌는 사고를 막으려면 **명시하는 편이 낫다.** 버전은 3.3.2 constraints 파일의 값을 따른다.

### 4.3 requirements.txt 변경안 (초안 — 저장소에 쓰지 않음)

```text
# Airflow providers (버전은 constraints-3.3.2 기준으로 고정)
apache-airflow-providers-standard
apache-airflow-providers-postgres
apache-airflow-providers-fab

# 애플리케이션 의존성 (현행 유지, 메이저 고정 권장)
psycopg2-binary
requests
pandas
numpy
pyupbit
playwright
```

- 현재는 모든 패키지에 버전 고정이 없다. 이 상태로 재빌드하면 `pandas`/`numpy`/`playwright`가 최신 버전으로 올라가 Airflow 3.3.2와 충돌하거나 동작이 바뀔 수 있다. 전환 시점의 버전을 `pip freeze`로 고정하는 것을 권장한다.
- Docker 공식 가이드 권장 패턴: `pip install "apache-airflow==${AIRFLOW_VERSION}" -r requirements.txt`. Airflow 본체를 함께 지정하면 pip가 Airflow를 업·다운그레이드하지 못한다.
- constraints URL 형식은 `https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt`이다. 이 파일을 통째로 `-c`로 걸면 `pyupbit`/`playwright`와 버전이 충돌할 수 있다. **provider 설치에만 constraints를 적용하고, 앱 의존성은 별도 단계로 설치하는 2단계 방식**이 안전하다(§7 Dockerfile 초안).

### 4.4 playwright 설치 단계 영향
- 현행 3단계 구조(airflow 유저로 pip → root로 `playwright install-deps chromium` → airflow 유저로 `playwright install chromium`)는 3.x 이미지에서도 그대로 유효하다. 베이스는 여전히 Debian 계열 공식 이미지이고 사용자/HOME(`/home/airflow`) 구조도 같다.
- 확인할 점:
  1. `/home/airflow/.local/bin/playwright` 경로. 공식 이미지는 패키지를 `~/.local`(user install)에 설치하므로 현행 절대경로가 유효할 가능성이 높다. 빌드 시 `which playwright`로 확인한다.
  2. 브라우저 캐시 `~/.cache/ms-playwright`는 `/home/airflow` 아래에 생긴다. compose는 `user: "${AIRFLOW_UID:-50000}:0"`으로 실행하므로, UID가 50000이 아니면 HOME 권한 때문에 Chromium을 찾지 못할 수 있다(2.x와 같은 위험).
  3. **크롤러(`fund_crawler.py`)는 LocalExecutor 기준으로 scheduler 컨테이너에서 실행된다.** dag-processor는 DAG import만 하므로 Chromium이 필요 없지만 같은 이미지를 쓰므로 문제없다. 이미지 크기만 늘어난다.
  4. 3.x 베이스 OS가 업데이트되면 `install-deps`가 설치하는 apt 패키지명이 바뀔 수 있다. 빌드 로그를 확인한다.

---

## 5. 메타DB — PostgreSQL 버전과 마이그레이션 경로

### 5.1 PostgreSQL 13 지원 여부

| Airflow | 지원 PostgreSQL (Prerequisites) |
|---|---|
| 2.10.4 (현행) | 12, 13, 14, 15, 16 |
| 3.0.0 / 3.1.0 | 13, 14, 15, 16, 17 |
| 3.2.0 / 3.3.0 | 13, 14, 15, 16, 17, 18 |
| **3.3.1 / 3.3.2** | **14, 15, 16, 17, 18** (13 제외) |

결론: **3.3.1 이상을 목표로 하면 `postgres:13`을 반드시 교체해야 한다.** 3.2.x/3.3.0에 머물면 13을 유지할 수 있지만, PG13 자체가 커뮤니티 EOL(2025-11)이므로 함께 올리는 것을 권장한다. 2.10.4가 PG16을 지원하므로 **Airflow 업그레이드 전에 PG를 먼저 올리면 변경 축을 분리할 수 있다.** 공식 템플릿과 맞춰 `postgres:16`을 권장한다.

PG 메이저 업그레이드는 데이터 디렉토리 호환이 안 되므로 같은 볼륨(`postgres-db-volume`)에 이미지 태그만 바꾸면 **기동에 실패한다.** dump/restore로 새 볼륨에 옮겨야 한다(§7 단계 2).

### 5.2 2.x → 3.x DB 마이그레이션 경로
- 최소 출발 버전: **2.7 이상**(3.0 Release Notes "Minimum Supported Versions"). 2.10.4는 조건을 충족한다. 가이드는 최신 2.x 경유를 권장한다. 현재 최신 2.x는 2.11.2(2026-03-11)이다. 2.11은 3.x 대비 deprecation 경고를 제공하는 브리지 릴리스이므로, **선택적으로 2.11.2를 한 번 거치면** DAG 경고를 미리 볼 수 있다.
- 3.3.1에서 "커스텀 Dag bundle이 설정된 경우 2.x → 3.0+ 업그레이드가 실패하던 문제"가 수정됐다. 목표는 3.3.2 이상이 바람직하다.
- 가이드 순서:
  1. **백업**: 핫백업 수단이 없으면 Airflow를 멈추고 백업한다.
  2. **`airflow db clean`**: XCom 등 오래된 데이터를 정리한다. 스키마 마이그레이션이 대용량 테이블에서 느려지기 때문이다. 이 저장소는 매일 7개 DAG가 1년 이상 쌓였으므로 `--clean-before-timestamp` 90일 정도를 권장한다. 3.3.2에서 `callback` 테이블이 purge되지 않던 문제가 수정됐다(#72899). 단, 업그레이드 **전** clean은 2.10.4 바이너리로 수행한다.
  3. `airflow dags reserialize`가 오류 없이 끝나는지 확인한다.
  4. ruff `AIR301/302` 점검(DAG 쪽 작업 범위).
  5. `airflow config update` → `airflow db migrate`.
  6. 기동 스크립트를 교체한다(api-server, dag-processor).
- 롤백:
  - 1순위는 **백업 복원**이다. 2.10.4 이미지와 PG16에 dump를 복원한다. 3.x 이후 생긴 실행 이력은 사라진다.
  - `airflow db downgrade --to-version 2.10.4`도 있지만, 3.x 마이그레이션이 크고(직렬화 DAG 버전 관리, 자산 등) 3.2.0에서 downgrade 마이그레이션 버그 수정 이력(#63625)도 있으므로 **주 경로로 쓰지 않는다.**
  - 롤백 판단 기준은 첫 정기 사이클(06:50~07:00) 전체가 성공하는지로 한다. 실패하면 즉시 복원한다.

---

## 6. REST API v1 제거 → `/api/v2`와 그 영향

### 6.1 변경 사항
- Connexion 기반 `/api/v1`이 제거되고 FastAPI 기반 `/api/v2`가 도입됐다. 400 대신 422를 반환하는 등 검증이 엄격해졌고, `execution_date` 파라미터가 `logical_date`로 바뀌었다.
- `POST /dags/{dag_id}/dagRuns`에서 `logical_date`를 생략하면 None이 된다.
- 인증: `/auth/token`으로 JWT를 받아 Bearer 헤더로 보낸다. 토큰은 주기적으로 회전해야 한다. 원격 CLI는 `airflowctl`(apache-airflow-client)로 분리됐다.

### 6.2 저장소 영향
- `api/v1`을 호출하는 코드나 스크립트는 저장소 전체 grep 결과 **없다**(외부 cron이나 개인 스크립트가 있다면 별도 확인 필요).
- `AIRFLOW__API__AUTH_BACKEND`의 basic_auth 설정은 v1용이므로 삭제한다(§2.1).

### 6.3 `plugins/callbacks.py` 영향 (인프라 관점과 교차하는 부분)

| 위치 | 현재 | 3.x 문제 | 수정 방향 |
|---|---|---|---|
| `_build_dag_url` (L30-32) | `os.environ["AIRFLOW__WEBSERVER__BASE_URL"]` + `/dags/{dag_id}/grid?dag_run_id={run_id}` | ① 환경변수가 `AIRFLOW__API__BASE_URL`로 바뀐다. 기존 변수를 compose에서 지우면 폴백인 `localhost:8080`이 Slack 버튼에 박힌다. ② 2.x Flask UI 경로(`/grid?dag_run_id=`)는 새 React UI와 맞지 않는다. | `conf.get("api","base_url")`을 쓰고 `{base}/dags/{dag_id}/runs/{quote(run_id)}` 형식으로 바꾼다. 3.3.2 `TaskInstance.log_url`은 `{base_url}/dags/{dag_id}/runs/{run_id}/tasks/{task_id}`이며, run_id는 `urllib.parse.quote`로 인코딩한다. 실패 알림은 태스크 단위 URL이 더 유용하다. |
| `_was_previous_run_failed` (L56-71) | `DagRun.find(...)` ORM 조회 | DAG 레벨 `on_success_callback`은 dag-processor에서 실행되므로 DB 접근이 가능하다(콜백 문서: "Errors in callback functions will show up in dag processor logs"). 하지만 `DagRun.find`는 3.x에서 내부 API이고 `r.execution_date` 속성은 제거됐다. **태스크 레벨에서 재사용하면 Task SDK 환경이라 DB 세션이 없어 실패한다.** | `logical_date`나 `run_after`로 정렬한다. 장기적으로는 `/api/v2/dags/{id}/dagRuns` 조회나 Task SDK 기반 API로 바꾼다(DAG 작업 범위). |
| `slack_failure_callback` (L82), `slack_recovery_callback` (L142) | `context["execution_date"]` | 3.0에서 context 키가 제거돼 **KeyError가 나고, 실패 알림 자체가 발송되지 않는다.** | `context["logical_date"]`를 쓴다. 수동 실행이면 None일 수 있으므로 `run_after`로 폴백한다. |
| `plugins/CALLBACKS_GUIDE.md` L63/69/126 | `AIRFLOW__WEBSERVER__BASE_URL`, `context['execution_date']` 안내 | 문서가 실제와 달라진다. | 함께 갱신한다. |

> 태스크 레벨 `on_failure_callback`(default_args)은 3.x에서 워커(LocalExecutor면 scheduler 컨테이너의 태스크 수퍼바이저) 쪽에서 실행된다. 이 경우 `Variable.get`은 Execution API를 통해 동작하지만 ORM은 쓸 수 없다. 정확한 실행 위치는 DAG/콜백 담당 작업(작업 A 등)에서 3.3.2 소스로 교차 검증하기를 권한다.

---

## 7. 단계별 구성 변경안 (초안 — 저장소에는 쓰지 않음)

### 단계 0. 사전 점검 (2.10.4 유지 상태)
```bash
docker compose exec airflow-scheduler airflow dags reserialize
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler airflow db clean \
  --clean-before-timestamp "$(date -d '-90 days' +%F)" --yes --skip-archive
docker compose exec airflow-scheduler airflow config list > cfg_2x_backup.txt
# 경고: DAG가 bare cron을 쓰므로 2.x 의미를 유지하려면 지금 아래 키를 결정해 둘 것
# AIRFLOW__SCHEDULER__CREATE_CRON_DATA_INTERVALS: 'true'
```

### 단계 1. 메타DB 백업
```bash
docker compose stop airflow-webserver airflow-scheduler
docker compose exec -T postgres pg_dumpall -U airflow > backup_airflow_pg13_$(date +%F).sql
docker run --rm -v oracle-airflow_postgres-db-volume:/v -v "$PWD":/b alpine \
  tar czf /b/pgdata13_$(date +%F).tgz -C /v .     # 볼륨 원본 스냅샷 (롤백용)
```

### 단계 2. PostgreSQL 13 → 16 (Airflow 2.10.4 그대로)
```yaml
# docker-compose.yaml (발췌)
services:
  postgres:
    image: postgres:16
    volumes:
      - postgres-db-volume-16:/var/lib/postgresql/data   # 새 볼륨
volumes:
  postgres-db-volume:      # 롤백용으로 보존
  postgres-db-volume-16:
```
```bash
docker compose up -d postgres
docker compose exec -T postgres psql -U airflow -d postgres < backup_airflow_pg13_YYYY-MM-DD.sql
docker compose up -d   # 2.10.4로 하루 정기 사이클 정상 확인
```

### 단계 3. Dockerfile (3.3.2)
```dockerfile
ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.12
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}
ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

USER airflow
# 1-a. Airflow provider: 공식 constraints 적용 (Airflow 본체 고정)
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      apache-airflow-providers-standard \
      apache-airflow-providers-postgres \
      apache-airflow-providers-fab \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# 1-b. 애플리케이션 의존성 (Airflow 본체가 움직이지 않도록 함께 지정)
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

# 2. Chromium 시스템 의존성 (root)
USER root
RUN apt-get update && \
    /home/airflow/.local/bin/playwright install-deps chromium && \
    rm -rf /var/lib/apt/lists/*

# 3. Chromium 바이너리 (airflow 유저)
USER airflow
RUN playwright install chromium
```

### 단계 4. `.env` (gitignore 대상, 예시)
```dotenv
AIRFLOW_UID=50000
FERNET_KEY=<python -c "from cryptography.fernet import Fernet;print(Fernet.generate_key().decode())">
AIRFLOW__API_AUTH__JWT_SECRET=<openssl rand -hex 32>
AIRFLOW__API__SECRET_KEY=<openssl rand -hex 32>
AIRFLOW__API__BASE_URL=http://<고정IP>:8080
_AIRFLOW_WWW_USER_USERNAME=<관리자ID>
_AIRFLOW_WWW_USER_PASSWORD=<강한 비밀번호>
```
> 기존에 `FERNET_KEY=''`로 평문 저장된 Variable/Connection은 키를 넣은 뒤 `airflow rotate-fernet-key`로 암호화한다. **키를 잃어버리면 복호화할 수 없으므로 별도 보관한다.**

### 단계 5. docker-compose.yaml (3.x, LocalExecutor)
```yaml
x-airflow-common:
  &airflow-common
  build: .
  env_file:
    - .env
  environment:
    &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__CORE__AUTH_MANAGER: airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__FERNET_KEY: ${FERNET_KEY}
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AIRFLOW__CORE__EXECUTION_API_SERVER_URL: 'http://airflow-apiserver:8080/execution/'
    AIRFLOW__API_AUTH__JWT_SECRET: ${AIRFLOW__API_AUTH__JWT_SECRET}
    AIRFLOW__API_AUTH__JWT_ISSUER: airflow
    AIRFLOW__API__SECRET_KEY: ${AIRFLOW__API__SECRET_KEY}
    AIRFLOW__API__BASE_URL: ${AIRFLOW__API__BASE_URL:-http://localhost:8080}
    AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK: 'true'
    # 2.x data interval 의미 유지 여부 — DAG 분석(작업 A) 결론에 따라 결정
    # AIRFLOW__SCHEDULER__CREATE_CRON_DATA_INTERVALS: 'true'
    AIRFLOW_CONFIG: '/opt/airflow/config/airflow.cfg'
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
    - ./config:/opt/airflow/config
    - ./data:/opt/airflow/data
  user: "${AIRFLOW_UID:-50000}:0"
  extra_hosts:
    - "host.docker.internal:host-gateway"
  depends_on:
    &airflow-common-depends-on
    postgres:
      condition: service_healthy

services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-db-volume-16:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 10s
      retries: 5
      start_period: 5s
    restart: always

  airflow-apiserver:
    <<: *airflow-common
    command: api-server
    ports:
      - "8080:8080"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/api/v2/monitor/health"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    healthcheck:
      test: ["CMD-SHELL", 'airflow jobs check --job-type SchedulerJob --hostname "$${HOSTNAME}"']
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully
      airflow-apiserver:              # LocalExecutor 태스크가 Execution API를 호출하므로
        condition: service_healthy

  airflow-dag-processor:
    <<: *airflow-common
    command: dag-processor
    healthcheck:
      test: ["CMD-SHELL", 'airflow jobs check --job-type DagProcessorJob --hostname "$${HOSTNAME}"']
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully

  airflow-triggerer:                  # deferrable 미사용 시 생략 가능하나 권장
    <<: *airflow-common
    command: triggerer
    healthcheck:
      test: ["CMD-SHELL", 'airflow jobs check --job-type TriggererJob --hostname "$${HOSTNAME}"']
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully

  airflow-init:
    <<: *airflow-common
    command: version
    environment:
      <<: *airflow-common-env
      _AIRFLOW_DB_MIGRATE: 'true'
      _AIRFLOW_WWW_USER_CREATE: 'true'          # FabAuthManager일 때만 동작
      _AIRFLOW_WWW_USER_USERNAME: ${_AIRFLOW_WWW_USER_USERNAME:-airflow}
      _AIRFLOW_WWW_USER_PASSWORD: ${_AIRFLOW_WWW_USER_PASSWORD}
    user: "0:0"

volumes:
  postgres-db-volume:        # PG13 원본 — 롤백 기간 동안 보존
  postgres-db-volume-16:
```

### 단계 6. 전환 실행
```bash
docker compose down                       # 2.10.4 정지
# (단계 1 백업을 이 시점에 다시 수행 — 최신 상태 보존)
docker compose build --no-cache
docker compose run --rm airflow-scheduler airflow config lint
docker compose run --rm airflow-scheduler airflow config update
docker compose up airflow-init            # db migrate + 사용자 생성 로그 확인
docker compose up -d
docker compose ps                          # 5개 서비스 healthy 확인
curl -s http://localhost:8080/api/v2/monitor/health
```

### 단계 7. 검증 체크리스트
- [ ] UI 로그인(FAB 기존 계정), DAG 7개 import 오류 0건(dag-processor 로그)
- [ ] `test_slack_alert_dag` 수동 실행 → Slack 버튼 URL이 `{API__BASE_URL}/dags/.../runs/...`로 열림
- [ ] 다음 날 06:50 `make_token` → 06:55 `fetch_exchange_rate`/`fetch_fund_price_daily`(Chromium) → 07:00 `fetch_asset_daily` 체인 성공(ExternalTaskSensor + cron 해석 변화 확인)
- [ ] 스케줄러 태스크 로그에 Execution API 401/403(JWT 불일치)과 connection refused(URL 오설정)가 없는지 확인
- [ ] 롤백 기간(1~2주) 동안 `postgres-db-volume`(PG13)과 백업 dump 보존 후 삭제

### 롤백 절차 (요약)
```bash
docker compose down
git checkout <2.10.4 시점 커밋> -- Dockerfile docker-compose.yaml requirements.txt
# 단계 2 완료 후라면: postgres:16 + 2.10.4 조합으로, 단계 6 직전 dump를 새 볼륨에 복원
docker compose up -d
```

---

## 8. 다른 작업과의 교차 의존 (참고)

| 항목 | 담당 범위 | 인프라 쪽 결정에 미치는 영향 |
|---|---|---|
| `create_cron_data_intervals` 결정 | DAG(`data_interval_*`/ExternalTaskSensor `execution_delta` 사용 여부) | compose env 1줄. **업그레이드 전에 확정해야 한다.** |
| `callbacks.py` 수정 (`execution_date`, ORM, URL) | 플러그인/DAG | `AIRFLOW__API__BASE_URL` 키 이름 확정 |
| `from airflow.decorators/operators/sensors` → `airflow.sdk` / `providers.standard` | DAG | standard provider 설치(§4.2) |
| `token_manager.py`의 `airflow.models.Variable` | 플러그인 | Task SDK에서는 `airflow.sdk.Variable` 권장. 동작 여부를 확인해야 한다. |

---

## 9. 출처 (2026-10-12 접속)

- Upgrading to Airflow 3 (3.3.2): https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html
- Release Notes (3.0.0 "Removed Configuration Keys", "Minimum Supported Versions", "Standalone DAG Processor Required", "REST API v2 replaces v1"; 3.3.0 #63949; 3.3.2 #71651, #72899; 3.3.1 2.x→3 업그레이드 수정): https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html
- 공식 docker-compose 템플릿 3.3.2: https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml
- Prerequisites (버전별 PostgreSQL/Python 지원):
  - https://airflow.apache.org/docs/apache-airflow/stable/installation/prerequisites.html (3.3.2)
  - https://airflow.apache.org/docs/apache-airflow/3.3.0/installation/prerequisites.html
  - https://airflow.apache.org/docs/apache-airflow/3.2.0/installation/prerequisites.html
  - https://airflow.apache.org/docs/apache-airflow/3.0.0/installation/prerequisites.html
  - https://airflow.apache.org/docs/apache-airflow/2.10.4/installation/prerequisites.html
- Configuration Reference (`[api] base_url/secret_key`, `[api_auth] jwt_secret`, `[core] execution_api_server_url/auth_manager/simple_auth_manager_*`): https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html
- 2.10.4 Configuration Reference (`enable_health_check` 기본값 False): https://airflow.apache.org/docs/apache-airflow/2.10.4/configurations-ref.html
- FAB provider Configuration Reference (`[fab] auth_backends`): https://airflow.apache.org/docs/apache-airflow-providers-fab/stable/configurations-ref.html
- Simple auth manager: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/auth-manager/simple/index.html
- Docker image entrypoint (`_AIRFLOW_DB_MIGRATE`, `_AIRFLOW_WWW_USER_CREATE`): https://airflow.apache.org/docs/docker-stack/entrypoint.html
- entrypoint 소스 3.3.2 (FAB가 아니면 사용자 생성을 건너뜀, `_AIRFLOW_DB_UPGRADE` deprecated 경고): https://github.com/apache/airflow/blob/3.3.2/scripts/docker/entrypoint_prod.sh
- Docker image 태그·기본 Python(3.12): https://airflow.apache.org/docs/docker-stack/index.html , https://hub.docker.com/r/apache/airflow/tags
- Callbacks (dag processor 로그 위치): https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/callbacks.html
- `TaskInstance.log_url` UI URL 형식 (3.3.2 소스): https://github.com/apache/airflow/blob/3.3.2/airflow-core/src/airflow/models/taskinstance.py
- UI Overview: https://airflow.apache.org/docs/apache-airflow/stable/ui.html
