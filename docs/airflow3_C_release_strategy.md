# 작업 C: Airflow 3.x 릴리스 동향과 마이그레이션 전략 리서치

- 작성일: 2026-10-12
- 대상 저장소: `oracle-airflow`(읽기 전용으로 분석)
- 현재 버전: `apache/airflow:2.10.4`, LocalExecutor, docker-compose(webserver/scheduler/init, postgres:13), TaskFlow DAG 7개, `plugins/callbacks.py`, `plugins/asset_flow`
- 근거: airflow.apache.org 공식 문서와 릴리스 노트, GitHub 이슈. 각 절에 출처 URL을 적었습니다.

---

## 0. 요약

| 항목 | 결론 |
|---|---|
| 최신 안정 버전 | **Airflow 3.3.2 (2026-09-17)**. 공식 Supported Versions 표에서 3.x는 "Maintenance" 상태입니다. |
| 2.x 지원 | **2026-04-22에 EOL**. 2.11.2가 마지막 패치이고, 현재 2.10.4에는 보안 패치도 나오지 않습니다. |
| 공식 업그레이드 경로 | 2.7 이상이면 3.x로 바로 갈 수 있습니다. 공식 문서는 "최신 2.x를 거쳐 3으로" 가라고 권장합니다. 순서는 Ruff AIR 린트 → standard provider 도입 → DB 백업/clean → `airflow config update` → `airflow db migrate` → api-server/dag-processor 기동입니다. |
| 차단 이슈 | ① **PostgreSQL 13은 3.3.2에서 지원하지 않습니다**(14~18 필요). ② `callbacks.py`가 제거된 컨텍스트 키 `execution_date`를 쓰고, `DagRun` ORM을 직접 조회합니다. ③ compose의 `webserver` 명령과 `AIRFLOW__API__AUTH_BACKEND`가 폐기됐습니다. |
| 권장 전략 | **단계적 전환 + 짧은 병행 검증**. 1단계는 기능 동등(센서 체인 유지), 2단계는 선택적으로 Assets/파티션으로 리팩터링합니다. |
| 작업량(추정) | 순수 작업 **3~5인일**에 병행 관찰 **5~7일**이 붙습니다. Assets 리팩터링은 별도로 1.5~3인일입니다. |

---

## 1. 2026-10 기준 릴리스 현황과 2.x EOL

### 1.1 버전 수명주기(공식 표)

| 메이저 | 최신 패치 | 상태 | 최초 릴리스 | Limited Maintenance | EOL |
|---|---|---|---|---|---|
| 3 | 3.3.2 | Maintenance | 2025-04-22 | TBD | TBD |
| 2 | 2.11.2 | **EOL** | 2020-12-17 | 2025-10-22 | **2026-04-22** |

출처: https://airflow.apache.org/docs/apache-airflow/stable/installation/supported-versions.html

- 2.11.0은 2025-05-20, 2.11.1은 2026-02-20, 2.11.2는 2026-03-14에 나왔습니다. 2.11.0 릴리스 노트에는 "Ease migration to Airflow 3" 섹션과 `DeltaTriggerTimetable`(#47074)이 있어, 3.x로 넘어가기 위한 브리지 성격의 릴리스로 볼 수 있습니다. 출처: https://airflow.apache.org/docs/apache-airflow/2.11.2/release_notes.html
- 업스트림과 별개로 Astronomer(Astro)는 Airflow 2를 2027-04까지 지원합니다. 이는 매니지드 서비스의 정책이라 이 프로젝트처럼 자체 호스팅하는 경우에는 적용되지 않습니다. 출처: https://www.astronomer.io/airflow-2-eol/
- **시사점:** 현재 2.10.4는 EOL 라인에 있는 데다 2.10의 마지막 패치도 아닙니다. 보안 취약점이 나와도 업스트림 패치가 없습니다.

### 1.2 3.x 릴리스 타임라인

| 버전 | 날짜 | 버전 | 날짜 |
|---|---|---|---|
| 3.0.0 | 2025-04-22 | 3.1.6 | 2026-01-13 |
| 3.0.1 | 2025-05-12 | 3.1.7 | 2026-02-04 |
| 3.0.2 | 2025-06-10 | 3.1.8 | 2026-03-11 |
| 3.0.3 | 2025-07-14 | 3.2.0 | 2026-04-07 |
| 3.0.4~3.0.6 | 2025-08-08 ~ 08-29 | 3.2.1 / 3.2.2 | 2026-04-21 / 05-29 |
| 3.1.0 | 2025-09-25 | 3.3.0 | 2026-07-06 |
| 3.1.1~3.1.5 | 2025-10-27 ~ 12-12 (3.1.4는 없음) | 3.3.1 / **3.3.2** | 2026-08-12 / **2026-09-17** |

출처: https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html

### 1.3 마이너 릴리스별 주요 변경

**3.0.0 (2025-04-22): 대규모 아키텍처 변경**
- Task Execution API와 Task SDK(AIP-72)가 도입됐습니다. 공식 문서 표현으로는 "Task code can no longer directly access the metadata database."입니다.
- DAG 버저닝(AIP-66), 스케줄러가 관리하는 Backfill(AIP-78), React 기반 새 UI(AIP-38/84), Asset 기반 스케줄링(Dataset에서 Asset으로 이름 변경, AIP-74/75), Edge Executor(AIP-69)가 추가됐습니다.
- REST API `/api/v1`이 FastAPI 기반 `/api/v2`로 바뀌었고, webserver는 `api-server`로 대체됐습니다. DAG processor는 별도 프로세스로 띄워야 합니다.
- SubDAG, SequentialExecutor, SLA가 제거됐습니다. SLA 자리는 Deadline Alerts가 이어받았습니다.
- 기본값이 바뀌었습니다. `catchup_by_default=False`, `create_cron_data_intervals=False`(cron이 `CronTriggerTimetable`로 해석됨), DAG 기본 schedule이 `@once`에서 `None`으로 바뀌었고, 기본 auth manager는 SimpleAuthManager입니다.
- Asset 트리거나 logical_date 없이 API로 트리거한 run은 `logical_date=None`이며 data interval이 없습니다. 이때 컨텍스트에서 `data_interval_*`나 `logical_date`를 읽으면 KeyError가 납니다.
- 출처: https://airflow.apache.org/docs/apache-airflow/3.0.0/release_notes.html

**3.1.0 (2025-09-25)**
- Human-in-the-Loop(HITL), Deadline Alerts, UI 다국어 지원, React 플러그인 시스템, 동기 실행(inference DAG), 새 트리거 룰 `ALL_DONE_MIN_ONE_SUCCESS`가 추가됐습니다.
- Task SDK가 코어와 분리되어 따로 업그레이드할 수 있습니다. 로깅은 structlog로 통일됐습니다.
- Python 3.13을 지원하고 3.9 지원을 중단했습니다. api-server 기본 워커 수는 1로 줄었습니다.
- `airflow.decorators` 같은 레거시 import는 3.1부터 경고를 내고, 이후 제거될 예정입니다.
- 출처: https://airflow.apache.org/docs/apache-airflow/3.1.1/release_notes.html

**3.2.0 (2026-04-07)**
- **Asset Partitioning(AIP-76)**: 공식 블로그 표현으로 "Downstream Dags trigger only when the specific partition they care about gets updated." `CronPartitionTimetable`, `PartitionedAssetTimetable`, temporal/range partition mapper, 파티션 단위 backfill을 지원합니다.
- Multi-team(실험적), Deadline Alerts의 `SyncCallback`, api-server의 Gunicorn 지원(무중단 워커 재활용), SQLAlchemy 2.0, Python 3.14가 들어왔습니다. 컨테이너 이미지에서는 MySQL client가 빠졌습니다.
- 출처: https://airflow.apache.org/blog/airflow-3.2.0/ , https://airflow.apache.org/docs/apache-airflow/3.3.0/release_notes.html

**3.3.0 (2026-07-06)**
- Task/Asset state store(AIP-103), pluggable retry policy(AIP-105), Java/Go용 Task SDK가 추가됐습니다.
- clear, rerun, backfill 시 **DAG bundle 버전을 적용**합니다. Deadlines 페이지도 생겼습니다.
- 출처: https://airflow.apache.org/docs/apache-airflow/3.3.0/release_notes.html , https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+3.x

**3.3.2 기준 전제조건:** Python 3.10~3.14, **PostgreSQL 14~18**이 필요합니다. 출처: https://airflow.apache.org/docs/apache-airflow/stable/installation/prerequisites.html

---

## 2. 공식 권장 업그레이드 경로와 체크리스트

출처: https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html

> "Make sure that you are on Airflow 2.7 or later. It is recommended to upgrade to latest 2.x and then to Airflow 3."

### 2.1 2.10.4에서 2.11.x를 거쳐야 하는가

- **필수는 아닙니다.** 2.10.4는 2.7 이상이라 3.x DB 마이그레이션을 바로 할 수 있습니다.
- 권장하는 방법은 **스테이징에서만 2.11.2 이미지로 한 번 띄워 보는 것**입니다. 2.11은 3.x 대비 deprecation 경고를 내는 브리지 릴리스이므로 경고를 수집할 수 있습니다. 다만 2.11도 EOL이므로 운영 환경에서 오래 머무를 이유는 없습니다.
- 이 프로젝트는 DAG가 7개뿐이라 Ruff AIR 린트로 대부분을 잡을 수 있습니다. 운영은 2.10.4에서 3.3.x로 한 번에 올리고, 2.11은 스테이징 검증 용도로만 쓰는 방식을 권장합니다.

### 2.2 공식 체크리스트(이 프로젝트에 대입)

| # | 공식 단계 | 이 프로젝트에서 할 일 |
|---|---|---|
| 1 | 전제조건 확인(2.7+, 지원되는 Python) | 2.10.4라 충족합니다. 3.3.2 이미지의 기본 Python 버전과 Playwright/Chromium 의존성(`playwright install-deps`)이 호환되는지 확인해야 합니다. |
| 2 | 메타 DB 백업과 정리(`airflow db clean`, `airflow dags reserialize`) | Airflow를 멈춘 상태에서 `pg_dump`로 백업합니다. **PG 13을 14 이상(16/17 권장)으로 먼저 올리는 작업**도 이 단계에 넣습니다. 2.10은 PG 12~16을 지원하므로 2.10.4 상태에서 PG를 먼저 올리면 리스크를 나눌 수 있습니다. |
| 3 | Ruff AIR 린트(Ruff 0.13.1 이상) | `ruff check dags/ plugins/ --select AIR301,AIR302,AIR311,AIR312`로 검사하고 `--fix`, 이어서 `--fix --unsafe-fixes`를 적용합니다. F401로 남은 import를 정리합니다. |
| 4 | Standard provider 설치 | `apache-airflow-providers-standard`를 설치합니다. 2.x에서도 설치할 수 있어 import를 미리 바꿀 수 있습니다. 대상은 `ExternalTaskSensor`와 `get_current_context`입니다. |
| 5 | 태스크 코드의 직접 DB 접근 점검 | `callbacks.py`의 `DagRun.find`가 해당합니다(아래 2.3 참고). 공식 권고는 Airflow Python Client나 REST API로 바꾸는 것입니다. |
| 6 | `airflow config update [--fix]`, `airflow db migrate` | compose의 `AIRFLOW__API__AUTH_BACKEND`, `AIRFLOW__WEBSERVER__BASE_URL`을 3.x 설정으로 옮깁니다(예: `[api] base_url`). |
| 7 | 기동 스크립트 변경 | `webserver`를 `api-server`로 바꾸고, `dag-processor` 서비스를 추가합니다. Deferrable이나 AssetWatcher를 쓰려면 `triggerer`도 추가합니다. |
| 8 | 업그레이드 후 인증 확인 | 기본값인 SimpleAuthManager를 쓸지, 기존 FAB 사용자 DB를 유지하기 위해 FAB provider를 설치하고 `FabAuthManager`를 지정할지 정합니다. |

### 2.3 저장소 코드에서 찾은 3.x 비호환 지점(근거)

| 위치 | 현재 코드 | 3.x 영향 | 조치 |
|---|---|---|---|
| `plugins/callbacks.py:82`, `:142` | `context["execution_date"]` | **제거된 컨텍스트 키**라 KeyError가 납니다. 실패 알림 콜백이 실패하므로 **알림이 사라집니다.** | `context["logical_date"]` 또는 `dag_run.run_after`를 사용하고, None일 때의 처리를 넣습니다. |
| `plugins/callbacks.py:16`, `:58~73` | `from airflow.models import DagRun` 후 `DagRun.find(...)`, `r.execution_date` | 3.x에서는 태스크 런타임의 ORM 접근이 제한됩니다. 이 함수는 DAG 레벨 `on_success_callback`에서 불리는데, 콜백이 실행되는 위치(dag-processor인지 worker인지)와 DB 접근 허용 여부는 공식 문서에 명시돼 있지 않아 **검증이 필요합니다.** `execution_date` 속성도 제거됐습니다. | REST API v2(`GET /api/v2/dags/{dag_id}/dagRuns`)나 `apache-airflow-client`로 직전 run을 조회하도록 바꿉니다. 더 단순하게는 직전 상태를 Variable에 기록하는 방식도 있습니다. |
| `dags/*.py` 공통 | `from airflow.decorators import task, dag, task_group` | 3.1부터 deprecation 경고가 나고 이후 제거됩니다. | `from airflow.sdk import dag, task, task_group` |
| `dags/*.py` 공통 | `from airflow.operators.python import get_current_context` | 경로가 이동했습니다. | `from airflow.sdk import get_current_context` |
| `dags/*.py`, `token_manager.py`, `callbacks.py` | `from airflow.models import Variable` | 권장 경로가 바뀌었습니다(AIR311). | `from airflow.sdk import Variable` |
| `fetch_asset_daily`, `fetch_exchange_rate` | `from airflow.sensors.external_task import ExternalTaskSensor` | 센서가 standard provider로 이동했습니다. | `from airflow.providers.standard.sensors.external_task import ExternalTaskSensor` |
| `dags/*.py` | `from airflow.models.param import Param` | 권장 경로가 바뀌었습니다. | `from airflow.sdk import Param` |
| 3개 수집 DAG의 `get_standard_date` | `context.get('data_interval_end')` | cron DAG의 정기 run은 3.x(`CronTriggerTimetable`)에서도 data_interval_end가 실제 실행 시각과 같아 **결과가 동일합니다.** 다만 **logical_date 없이 API로 트리거하거나 Asset으로 트리거하면 None**이 됩니다. | `dag_run.run_after`로 폴백하는 로직을 추가합니다. |
| `ExternalTaskSensor(execution_delta=5분)` | 2.x의 logical_date는 전날 같은 시각(데이터 구간 시작) | 3.x 기본값에서는 logical_date가 당일 실행 시각입니다. 모든 DAG가 같은 timetable로 바뀌므로 **5분 차이 매칭은 그대로 유지됩니다.** | 전환 직후 첫 run에서 센서 매칭을 반드시 관찰합니다. 공식 문서는 `create_cron_data_intervals`를 Airflow 3 run이 생긴 뒤에 바꾸면 **스케줄 1회가 건너뛰어진다**고 경고하므로, 설정은 처음부터 고정합니다. |
| `docker-compose` | `command: webserver`, `postgres:13`, `AIRFLOW__API__AUTH_BACKEND=basic_auth,session` | 기동이 불가능하거나 지원되지 않는 구성입니다. | api-server와 dag-processor(필요하면 triggerer)를 추가하고, PG 14 이상으로 올리고, auth 설정을 교체합니다. 외부 스크립트가 `/api/v1`과 basic auth를 쓴다면 `/api/v2`와 JWT(`/auth/token`)로 바꿔야 합니다. |
| `catchup=False` 명시 | — | 영향이 없습니다(3.x 기본값과 같습니다). | — |
| `xcom_pull` | 사용하지 않음(TaskFlow 반환값으로 전달) | 3.x에서 `task_ids` 없는 `xcom_pull()`이 현재 태스크만 읽도록 바뀌었지만, 이 프로젝트와는 무관합니다. | — |

---

## 3. 3.x 신규 기능 중 이 프로젝트에 도입할 가치가 있는 것

현재 구조는 다음과 같습니다.

- 06:50 `make_token`
- 06:55 `fetch_exchange_rate`(make_token을 센서로 대기), 06:55 `fetch_fund_price_daily`
- 07:00 `fetch_asset_daily`(두 DAG를 센서로 대기)

연결 방식은 `ExternalTaskSensor`(poke, timeout 600초, `execution_delta` 5분)입니다. 수동 DAG(`rebuild_derived_assets`, `upsert_manual_position`)는 Param을 받아 UI에서 트리거합니다.

### 3.1 Assets 기반 스케줄링으로 센서 체인 대체 (가치: 중~상, 시점: 2단계)

구상은 이렇습니다.

```python
from airflow.sdk import Asset, dag, task
TOKEN = Asset("kis://token")
FX = Asset("postgres://asset/market/exchange_rate")
FUND = Asset("postgres://asset/market/fund_price_daily")

# make_token: generate_tokens 태스크에 outlets=[TOKEN]
# fetch_exchange_rate: schedule=TOKEN, upload 태스크에 outlets=[FX]
# fetch_asset_daily: schedule=(FX & FUND)
```

| 장점 | 단점과 함정 |
|---|---|
| 센서 슬롯(poke 모드에서 최대 10분간 LocalExecutor 슬롯 점유)과 시각 오프셋(06:50/06:55/07:00)에 의존하던 구조가 사라집니다. 상류가 늦어도 timeout 실패가 나지 않습니다. | Asset 트리거 run은 **`logical_date=None`이고 data interval이 없습니다.** 현재의 `data_interval_end` 기반 T-1 계산을 `dag_run.run_after`나 `triggering_asset_events`의 timestamp 기반으로 고쳐야 합니다. |
| UI에서 Asset 계보(lineage)와 이벤트 이력을 볼 수 있습니다. | `schedule=[A, B]`는 "마지막 run 이후 모두 1회 이상 갱신"이라는 의미일 뿐 **날짜 정렬을 보장하지 않습니다.** 예를 들어 fund_price가 어제 실패하고 오늘 성공하면, 어제 FX 이벤트와 오늘 FUND 이벤트로 run이 1회 생깁니다. |
| 상류를 수동으로 재실행하면 하류가 자동으로 다시 계산됩니다. | 반대로 **상류를 백필하거나 재실행할 때 하류가 의도치 않게 트리거됩니다.** 예를 들어 과거 환율만 다시 받으려 해도 asset_daily가 오늘 날짜로 재적재됩니다. |
| 3.2 이상의 **Asset Partitioning**(`CronPartitionTimetable`과 temporal mapper)을 쓰면 "같은 날짜 파티션이 모두 갱신됐을 때만" 하류를 실행할 수 있어, 날짜 정렬 문제를 해결할 수 있습니다. | 파티셔닝은 3.2에서 새로 들어온 기능이고, 일 단위 매핑 예시는 공식 블로그에 없습니다(시간 단위 예시만 있음). 운영 성숙도를 지켜봐야 합니다. |

**권장:** 1단계(버전 업그레이드)에서는 **센서 체인을 그대로 유지**합니다. 동작 동등성을 확인하기 쉽기 때문입니다. 이때 `mode='reschedule'`로 바꿔 슬롯 점유를 줄이는 정도만 검토합니다. 2단계에서 `make_token → exchange_rate` 구간부터 Asset으로 바꾸고, `asset_daily`의 다중 의존은 3.2 이상의 파티션 기반으로 설계합니다.

출처: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html , https://airflow.apache.org/blog/airflow-3.2.0/

### 3.2 DAG 버저닝 / DAG Bundles (가치: 중)

- DAG 구조 변경(태스크 추가나 이름 변경)은 **어떤 번들을 쓰든 자동으로 버전이 기록**되어 UI에서 과거 구조를 볼 수 있습니다. 최근 `fetch_asset_daily`처럼 계좌 단위 태스크 페어가 자주 바뀌는 DAG에 유용합니다.
- 코드 버전에 맞춰 재실행하려면 `GitDagBundle`이 필요합니다. 기본 `LocalDagBundle`(dags 폴더)은 "do not support versioning of the bundle, meaning tasks always run using the latest code"입니다.
- 3.3부터는 clear, rerun, backfill 때 **번들 버전을 적용**합니다.
- 이 프로젝트는 compose로 dags를 마운트하므로 기본은 Local입니다. GitDagBundle을 도입하면 git pull 배포를 대체할 수 있지만 우선순위는 낮습니다.
- 출처: https://airflow.apache.org/docs/apache-airflow/3.0.6/administration-and-deployment/dag-bundles.html , https://www.astronomer.io/docs/learn/airflow-dag-versioning

### 3.3 새 React UI (가치: 중, 자동으로 적용됨)

- Grid/Graph 통합, Asset 뷰, Backfill 뷰, 다국어(3.1) 지원이 들어갔습니다. 3.2부터는 XCom을 UI에서 편집할 수 있고, 3.3에는 Deadlines 페이지가 생겼습니다.
- Param 기반 트리거 폼도 새 UI로 바뀝니다. `upsert_manual_position`과 `rebuild_derived_assets`의 enum이나 날짜 Param이 새 폼에서 제대로 렌더링되는지 **수동 검증 항목**에 넣어야 합니다.

### 3.4 Task SDK (가치: 중, 강제 적용)

- 태스크가 메타 DB와 분리되면서 보안과 안정성이 좋아집니다. 3.1부터 SDK를 따로 업그레이드할 수 있습니다.
- 이 프로젝트에서는 `callbacks.py`의 ORM 조회만 직접 영향을 받습니다. `PostgresHook`으로 업무 DB(`postgres_asset`)에 접근하는 부분은 메타 DB가 아니므로 영향이 없습니다.

### 3.5 스케줄러 관리 Backfill (가치: 낮~중)

- UI와 API로 backfill을 시작하고 모니터링할 수 있습니다. 2.x처럼 CLI 프로세스를 따로 띄우는 방식이 아닙니다.
- `rebuild_derived_assets`는 한 번의 run에서 `start_date~end_date` 범위를 처리하므로 그대로 두는 편이 효율적입니다. 다만 `fetch_exchange_rate`나 `fetch_fund_price_daily`의 과거 구간을 재수집할 때는 UI backfill을 바로 쓸 수 있습니다. 이 경우 `get_standard_date`가 backfill run의 logical_date 기준으로 맞게 계산되는지 검증해야 합니다.

### 3.6 그 밖의 기능

| 기능 | 버전 | 적용성 |
|---|---|---|
| Deadline Alerts(SLA 후속) | 3.1+, 3.2에 SyncCallback 추가 | "07:30까지 `asset_daily`가 끝나지 않으면 Slack 알림" 같은 지연 감지에 적합합니다. 현재는 실패 알림만 있습니다. 실험적 기능입니다. |
| Human-in-the-Loop | 3.1+ | `upsert_manual_position`(월 1회 수동 입력)을 승인 단계가 있는 워크플로로 만들 수 있습니다. 가치는 낮습니다. |
| Pluggable retry policy | 3.3+ | KIS나 Upbit의 레이트리밋 응답(예: 429)에만 재시도를 거는 정책을 만들 수 있습니다. 현재는 `retries=3`과 지수 백오프를 일괄 적용하고 있습니다. |
| `ALL_DONE_MIN_ONE_SUCCESS` 트리거 룰 | 3.1+ | "계좌 일부가 실패해도 나머지는 적재" 구조(`TriggerRule` 사용 중)를 단순하게 만들 여지가 있습니다. |

---

## 4. 커뮤니티에서 보고된 3.0 초기 버그와 함정, 해결 버전

| 이슈 | 내용 | 해결 버전과 근거 |
|---|---|---|
| ExternalTaskSensor와 logical_date null | logical_date가 null인 run에서 센서가 `KeyError: 'logical_date'`를 냄(#47447) | PR #48651 "Make ExternalTaskSensor work with Task SDK"로 3.0.0 GA 전에 수정됨. https://github.com/apache/airflow/issues/47447 |
| 2→3 업그레이드 시 직렬화 잔재 | `dag_code`/`serialized_dag` 테이블 정리 누락 | 3.0.1(#49563). https://airflow.apache.org/docs/apache-airflow/3.0.1/release_notes.html |
| dag-processor 메모리 누수 | 장시간 운영 시 메모리 증가 | 3.0.2(#50558) |
| backfill 간섭 | 한 DAG의 backfill이 다른 DAG에 영향 | 3.0.2(#50577). https://airflow.apache.org/docs/apache-airflow/3.0.2/release_notes.html |
| inlet/outlet 검증 누락 | 태스크 실행 전 잘못된 inlet/outlet 검사가 빠짐 | 3.0.2(#50773) |
| run_as_user 미지원 | 3.0 초기에 impersonation이 빠짐 | 3.0.3(#51780). https://airflow.apache.org/docs/apache-airflow/3.0.3/release_notes.html |
| 스케줄러 크래시 | heartbeat timeout이나 executor 이벤트 처리 중 `DetachedInstanceError` | 3.0.4(#53838/#53858), 3.0.5(#54334) |
| 마이그레이션 후 triggerer 오류 | 2→3 이후 triggerer 에러 | 3.1.2(#55884). https://airflow.apache.org/docs/apache-airflow/3.1.2/release_notes.html |
| 콜백 미실행 | 존재하지 않는 DAG를 대상으로 한 TriggerDagRunOperator에서 on_failure/on_retry 콜백이 실행되지 않음(3.1.0 보고) | https://github.com/apache/airflow/issues/70683 . 이 프로젝트는 TriggerDagRunOperator를 쓰지 않지만, **콜백 경로는 Task SDK에서 다시 구현됐으므로 Slack 알림 회귀 테스트가 필수**입니다. |
| 콜백 에러 로그 위치 | 콜백 에러가 task 로그가 아니라 scheduler(3.1) 또는 dag-processor(3.3) 로그에 남음 | https://airflow.apache.org/docs/apache-airflow/3.3.0/administration-and-deployment/logging-monitoring/callbacks.html . 운영자가 확인할 위치가 바뀝니다. |
| api-server 워커 수 | 3.1에서 기본 워커가 1로 줄어 UI가 느려질 수 있음 | 설정으로 조정하거나 수평 확장합니다(3.1.0 노트). |
| 그 밖의 함정(설계상 변경) | `execution_date` 등 컨텍스트 키 제거, 수동이나 Asset 트리거 시 `logical_date=None`, 미래 logical_date로 트리거 불가, Auth 경로가 `/auth`로 이동, `/api/v1` 제거 | 3.0.0 노트와 Upgrading 가이드 |

**결론:** 3.0.x 초기 스케줄러와 마이그레이션 안정성 이슈는 3.0.5, 3.1.2 무렵까지 정리됐습니다. 2026-10 현재 타깃은 **3.3.2**(최신이자 유일한 유지보수 라인)를 권장합니다. 업스트림은 사실상 최신 마이너에만 패치를 내므로, 3.1이나 3.2에 머무를 이유가 적습니다. 설치는 `apache-airflow==3.3.2`와 공식 constraints 파일로 고정합니다.

---

## 5. 권장 전략

### 5.1 빅뱅 vs 단계적

| 방식 | 평가 |
|---|---|
| 빅뱅(PG, Airflow, Assets 리팩터링을 한 번에) | DAG 7개짜리 소규모 환경이라 기술적으로는 가능합니다. 하지만 **매일 07시 자산 스냅샷의 연속성**이 핵심 가치이므로 원인 분리가 어려운 동시 변경은 피합니다. |
| **단계적(권장)** | 아래 4단계로 나눕니다. 각 단계에서 롤백 지점이 분명합니다. |

**Phase 0: 2.10.4에서 사전 정리 (운영 영향 없음, 0.5~1인일)**
1. Ruff AIR301/302/311/312 린트를 돌리고 import를 정리합니다. standard provider를 설치하고 센서 import를 바꿉니다.
2. `callbacks.py`에서 `execution_date`를 `logical_date`로 바꾸고, `DagRun.find`를 REST API나 Variable 방식으로 교체합니다. 2.x에서도 동작하는 형태로 작성합니다.
3. `get_standard_date`에 `logical_date`/`data_interval_end`가 None일 때 `dag_run.run_after`(3.x)로 폴백하는 로직을 넣습니다.
4. `/api/v1`을 호출하는 외부 스크립트가 있는지 조사합니다. compose에 basic_auth 설정이 있다는 것은 외부에서 쓰고 있을 가능성을 뜻합니다.

**Phase 1: PostgreSQL 13을 16/17로 업그레이드 (2.10.4 유지, 0.5인일)**
- pg_dump/restore로 진행합니다. 3.3.2가 PG 14 이상을 요구하므로 반드시 필요하고, Airflow 업그레이드와 분리해 리스크를 나눕니다. 업무 데이터 DB가 같은 인스턴스에 있다면 함께 다룹니다.

**Phase 2: 스테이징 병행 검증 (1~1.5인일, 관찰 5~7일)**
- 별도 compose 프로젝트로 띄웁니다. 메타 DB는 운영 백업에서 복원한 사본을 쓰고, 이미지는 `apache/airflow:3.3.2`에 Playwright를 얹습니다. 서비스는 api-server, scheduler, dag-processor, init으로 구성합니다.
- 순서: `airflow config update --fix`, `airflow db migrate`, `airflow dags reserialize`
- **업무 DB 쓰기 대상은 스테이징 스키마나 DB로 분리합니다.** `postgres_asset` Connection을 바꾸면 됩니다. upload 로직이 DELETE 스코프 후 INSERT라 멱등적이긴 하지만, 운영 테이블에 이중으로 쓰는 것은 피합니다.
- **병행 운영 시 주의:** `make_token`이 KIS 토큰을 동시에 재발급하면 운영 쪽 토큰과 경합하거나 레이트리밋에 걸릴 수 있습니다. 이 부분은 KIS 정책 확인이 필요합니다. 스테이징에서는 `make_token`을 멈추고 운영 토큰 Variable을 읽기 전용으로 복사하거나, 발급 시각을 띄워 둡니다.
- **검증 방법:**
  1. DAG import 에러 0건(`airflow dags list-import-errors`)
  2. 3개 수집 DAG의 `standard_date` 계산 결과가 2.x와 같은지(T-1)
  3. 센서 매칭 성공(`execution_delta` 5분)과 소요 시간
  4. `account.asset_daily`, 환율, 펀드 NAV 적재 결과를 운영과 행 단위로 diff(계좌 수, 평가금액 합계)
  5. 일부러 실패를 일으켜 Slack 실패 알림을 받고, 이어서 성공 run에서 복구 알림이 오는지 확인
  6. 새 UI에서 Param 트리거 폼으로 `upsert_manual_position`과 `rebuild_derived_assets`를 수동 실행
  7. 콜백 에러가 dag-processor/scheduler 로그에 있는지 확인

**Phase 3: 운영 전환 (0.5인일)**
- 07시대를 피해 오후에 전환합니다. 2.x를 정지하고 `pg_dump`로 최종 백업한 뒤, 이미지와 compose를 교체하고 `db migrate`를 실행합니다. 이어서 DAG를 unpause합니다(`catchup=False`라 누락 run이 몰려 실행되지 않습니다).
- 다음 날 06:50~07:10 run을 직접 지켜봅니다. 첫 3일은 운영 데이터와 스테이징 결과를 대조합니다.

**Phase 4 (선택): 3.x 기능 도입 (1.5~3인일)**
- Assets 전환(3.1 절), Deadline Alerts(07:30 지연 알림), retry policy를 검토합니다. 버전 업그레이드가 안정된 뒤 별도 PR로 진행합니다.

### 5.2 롤백 계획

| 시점 | 롤백 방법 |
|---|---|
| Phase 1 실패 | PG 13 컨테이너와 볼륨을 그대로 보존해 두고 compose 이미지를 되돌립니다. |
| Phase 3 직후(3.x run 발생 전후) | **1순위:** 3.x 컨테이너를 내리고, 전환 직전 `pg_dump`를 새 DB에 복원한 뒤 `apache/airflow:2.10.4` 이미지와 기존 compose(git 태그)로 기동합니다. **2순위:** `airflow db downgrade`(3.0.1 노트에도 downgrade 관련 수정이 있어 경로는 지원됨). 다만 3.x에서 생긴 run과 버전 데이터가 손실될 수 있으므로 덤프 복원을 우선합니다. |
| 업무 데이터 | upload가 계좌와 날짜 단위로 DELETE 후 INSERT하는 멱등 구조라, 롤백 뒤 2.x에서 해당 날짜를 재실행하면 복구됩니다. `rebuild_derived_assets`로 파생값도 재계산할 수 있습니다. |
| 코드 | Phase 0 변경은 2.x와 호환되게 작성하므로, 코드 롤백 없이 이미지만 되돌려도 됩니다. 3.x 전용 import(`airflow.sdk`)는 Phase 3 직전 커밋으로 분리해 git revert 단위를 명확히 합니다. |

### 5.3 작업량 추정 (1인 기준, 대략)

| 단계 | 작업 | 추정 |
|---|---|---|
| Phase 0 | 린트, import, 콜백과 컨텍스트 수정, 테스트 | 0.5~1인일 |
| Phase 1 | PG 업그레이드와 백업 리허설 | 0.5인일 |
| Phase 2 | Dockerfile/compose 개편(api-server, dag-processor, auth), 스테이징 구축과 검증 | 1~1.5인일, 관찰 5~7일 |
| Phase 3 | 운영 전환, 모니터링, 문서화 | 0.5인일 |
| **소계(버전 업그레이드)** | | **약 3~5인일, 캘린더 기준 약 1.5~2주** |
| Phase 4(선택) | Assets/파티션 전환, Deadline Alerts | 1.5~3인일 |

---

## 6. 확인이 더 필요한 사항(불확실성)

1. **3.x에서 DAG 레벨과 태스크 레벨 콜백이 실행되는 프로세스와 메타 DB 접근 허용 여부**는 공식 콜백 문서에 명시돼 있지 않습니다. 문서에는 "Errors in callback functions will show up in dag processor logs"라고만 되어 있습니다. 그래서 `DagRun.find`는 동작 여부와 상관없이 교체를 권장합니다.
2. 2.11.0의 "Ease migration to Airflow 3" 섹션 본문(구체적인 경고 항목)은 이번 조사에서 본문을 확보하지 못했습니다. 섹션 제목만 확인했습니다.
3. 3.3.x 공식 이미지의 기본 Python과 Debian 베이스에서 Playwright `install-deps chromium`이 호환되는지는 이미지를 빌드해 확인해야 합니다.
4. KIS 토큰을 병행 발급할 때 기존 토큰이 무효화되는지는 KIS Open API 정책 확인이 필요합니다.
5. 일 단위 Asset Partition 매핑(StartOfDay 계열 mapper)의 구체 API는 3.2 블로그에 시간 단위 예시만 있어, 3.3 문서에서 다시 확인해야 합니다.

## 출처

- Supported versions: https://airflow.apache.org/docs/apache-airflow/stable/installation/supported-versions.html
- Upgrading to Airflow 3: https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html
- Release notes(stable, 3.3.2): https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html
- 3.0.0 release notes: https://airflow.apache.org/docs/apache-airflow/3.0.0/release_notes.html
- 3.0.1 / 3.0.2 / 3.0.3 / 3.1.2 release notes: https://airflow.apache.org/docs/apache-airflow/3.0.1/release_notes.html , https://airflow.apache.org/docs/apache-airflow/3.0.2/release_notes.html , https://airflow.apache.org/docs/apache-airflow/3.0.3/release_notes.html , https://airflow.apache.org/docs/apache-airflow/3.1.2/release_notes.html
- 3.3.0 release notes: https://airflow.apache.org/docs/apache-airflow/3.3.0/release_notes.html
- 2.11.2 release notes: https://airflow.apache.org/docs/apache-airflow/2.11.2/release_notes.html
- Prerequisites(3.3.2): https://airflow.apache.org/docs/apache-airflow/stable/installation/prerequisites.html
- Asset scheduling: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html
- Airflow 3.2.0 blog: https://airflow.apache.org/blog/airflow-3.2.0/
- Dag Bundles: https://airflow.apache.org/docs/apache-airflow/3.0.6/administration-and-deployment/dag-bundles.html
- Callbacks(3.3.0): https://airflow.apache.org/docs/apache-airflow/3.3.0/administration-and-deployment/logging-monitoring/callbacks.html
- Announcements: https://airflow.apache.org/announcements/
- Airflow 3.x 로드맵(cwiki): https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+3.x
- GitHub #47447: https://github.com/apache/airflow/issues/47447 , GitHub #70683: https://github.com/apache/airflow/issues/70683
- Astronomer Airflow 2 EOL: https://www.astronomer.io/airflow-2-eol/ , DAG versioning: https://www.astronomer.io/docs/learn/airflow-dag-versioning
