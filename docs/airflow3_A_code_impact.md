# 작업 A — DAG·플러그인 코드의 Airflow 3 영향 분석

- 대상 저장소: `oracle-airflow` (읽기 전용으로 분석, 수정 없음)
- 현재 버전: `apache/airflow:2.10.4`, LocalExecutor, TaskFlow DAG 7개, `plugins/callbacks.py`, `plugins/asset_flow/`
- 기준 버전: **Airflow 3.3.2 (2026-09-17 릴리스, 작성일 2026-10-12 기준 최신)**, `apache-airflow-task-sdk` 1.3.2, `apache-airflow-providers-standard` 1.20.0
- 근거: 공식 문서와 릴리스 노트(맨 아래 출처 참고)를 읽었습니다. 문서에 없는 동작은 PyPI의 3.3.2 wheel 소스를 직접 열어 확인했습니다. 소스로 확인한 항목은 "소스 확인"으로 표시했습니다.

---

## 0. 결론 요약 (TL;DR)

| 우선순위 | 내용 | 결과 |
|---|---|---|
| **P0 (Breaking)** | `callbacks.py`가 `context["execution_date"]`를 읽음 | Airflow 3에서 **KeyError**가 나서 **모든 DAG의 Slack 실패 알림이 발송되지 않음** |
| **P0 (Breaking)** | `callbacks.py`가 `DagRun.find()`로 ORM을 직접 조회 | Airflow 3에서는 `RuntimeError("Direct database access via the ORM is not allowed")`가 남. 그런데 `try/except`가 이를 삼켜 **복구 알림이 조용히 영구 중단됨** |
| **P1 (Breaking, 조건부)** | `context.get('data_interval_end').in_timezone(...)` | `logical_date=None`인 수동/API 트리거 run에서는 값이 `None`이라 **AttributeError**가 남. 정기 스케줄 run은 영향 없음 |
| P2 (Deprecated) | `airflow.decorators`·`airflow.models.Param/Variable`·`airflow.operators.python.get_current_context`·`airflow.sensors.external_task`·`airflow.utils.trigger_rule` | 3.3.2에서도 shim으로 동작하지만 DeprecationWarning이 나고, 이후 버전에서 제거될 예정 |
| P2 (권장) | `Param(default=datetime.now(...))` 2건 | Airflow 3의 DAG 버전 관리에서 **날마다 새 DAG 버전이 생김** (ruff AIR304) |
| P3 (권장) | Slack 링크 URL 형식, `AIRFLOW__WEBSERVER__BASE_URL` | 새 UI(`/dags/{id}/runs/{run_id}`)와 `[api] base_url`에 맞춰 수정 필요 |
| 영향 없음 | `catchup`, `schedule`, plugins 폴더 import, `PostgresHook`, `task_group`, `TriggerRule.ALL_DONE`, XCom `None` 전달 패턴 | 모든 DAG가 `catchup=False`와 `schedule`을 명시하고 있음. plugins 폴더도 계속 `sys.path`에 추가됨 (소스 확인) |

ruff 0.17.0의 `--select AIR --preview` 결과는 **88건**입니다(AIR311 59, AIR301 13, AIR202 11, AIR312 3, AIR304 2). 다만 **P0 두 건과 P1 한 건은 ruff로 검출되지 않으므로** 사람이 직접 고쳐야 합니다(§9).

---

## 1. 파일:라인별 영향 표

심각도 기준
- **Breaking**: Airflow 3.3.2에서 예외가 나거나 기능이 조용히 멈춤
- **Deprecated**: 지금은 동작하지만 경고가 나고 이후 제거될 예정
- **권장**: 동작에는 문제없지만 Airflow 3 관례나 품질 면에서 수정을 권장

### 1.1 `plugins/callbacks.py` (가장 중요)

| 라인 | 현재 코드 | Airflow 3 영향 | 심각도 | ruff |
|---|---|---|---|---|
| 16 | `from airflow.models import DagRun, Variable` | `Variable` → `airflow.sdk.Variable`. `DagRun` ORM 모델은 태스크·콜백 프로세스에서 사용할 수 없음 | Deprecated / Breaking(DagRun) | AIR311(Variable)만 검출 |
| 17 | `from airflow.utils.state import DagRunState` | core에는 남아 있음. SDK에서는 `airflow.sdk.DagRunState`를 export함 | 권장 | 미검출 |
| 21 | `Variable.get(..., default_var="")` | SDK의 `Variable.get` 시그니처는 `default=`임. 레거시 경로는 shim이 변환해 주지만, import를 SDK로 바꾸면 **TypeError**가 남 | Deprecated (import 변경 시 Breaking) | AIR311 (`--fix`가 `default=`로 바꿔 줌) |
| 31–32 | `AIRFLOW__WEBSERVER__BASE_URL`, `/dags/{id}/grid?dag_run_id=` | Airflow 3에서는 webserver가 api-server로 바뀌고 UI가 React로 새로 만들어짐. SDK는 `[api] base_url` 아래 `/dags/{dag_id}/runs/{run_id}`를 씀 (소스 확인: `RuntimeTaskInstance.log_url`) | 권장 (링크 깨짐) | 미검출 |
| 56, 61 | `DagRun.find(...)` | 태스크 및 콜백 하위 프로세스에서 `block_orm_access()`가 호출되어 `RuntimeError: Direct database access via the ORM is not allowed in Airflow 3.0`가 남 (소스 확인). 72행의 `except`가 이를 삼키므로 `_was_previous_run_failed()`는 **항상 False**를 반환하고, 그 결과 복구 알림이 영구히 나가지 않음 | **Breaking (조용한 실패)** | 미검출 |
| 65 | `r.execution_date` | `DagRun.execution_date`는 제거됨 → `logical_date`(None일 수 있음) 또는 `run_after` | Breaking | 미검출 |
| 82 | `context["execution_date"]` (실패 콜백) | `execution_date` 컨텍스트 키가 제거되어 **KeyError**가 나고, 그 결과 실패 알림이 전송되지 않음 | **Breaking** | 미검출 (`context: dict`라 추론 불가) |
| 84 | `ti.max_tries` | SDK `RuntimeTaskInstance`에서도 제공됨 | 영향 없음 | — |
| 142 | `context["execution_date"]` (복구 콜백) | 위와 같이 KeyError가 남. 지금은 56행 때문에 이 줄까지 오지도 않음 | **Breaking** | 미검출 |
| 82, 142 | `.strftime("... KST")` | 원래 값이 UTC라 "KST" 표기가 틀린 상태임(기존 버그). Airflow 3에서 CronTriggerTimetable이 기본이 되면 logical_date가 하루 앞당겨진 "실제 실행 시각"이 되므로, 표시되는 날짜 의미도 바뀜(§4.3) | 권장 | — |

### 1.2 `dags/fetch_asset_daily_dag.py`

| 라인 | 현재 코드 | Airflow 3 영향 | 심각도 | ruff |
|---|---|---|---|---|
| 42 | `from airflow.decorators import task, dag, task_group` | → `from airflow.sdk import dag, task, task_group` | Deprecated | AIR311 (사용처마다 검출) |
| 43 | `from airflow.exceptions import AirflowException` | 3.3.2에서도 동작함. SDK의 정식 위치는 `airflow.sdk.exceptions.AirflowException` | 권장 | 미검출 |
| 44 | `from airflow.models.param import Param` | → `from airflow.sdk import Param` | Deprecated | AIR311 |
| 45 | `from airflow.operators.python import get_current_context` | → `from airflow.sdk import get_current_context` | Deprecated (ruff 분류는 AIR301 "removed") | **AIR301** |
| 46 | `PostgresHook` | 그대로 사용 가능함(postgres provider). 커넥션은 Task SDK API를 거쳐 조회됨 | 영향 없음 | — |
| 47, 125–143 | `from airflow.sensors.external_task import ExternalTaskSensor` | → `airflow.providers.standard.sensors.external_task`. 동작 변화는 §5 참고 | Deprecated | **AIR312** |
| 48, 418 | `from airflow.utils.trigger_rule import TriggerRule` | 3.3.2에서는 `airflow.task.trigger_rule`로 넘겨 주는 shim만 남아 있음(소스 확인) → `from airflow.sdk import TriggerRule` | Deprecated | **미검출** |
| 51 | `from callbacks import ...` | plugins 폴더가 여전히 `sys.path`에 추가되므로 동작함(§7) | 영향 없음 | — |
| 145–161 | `@task def get_standard_date() -> dict` | 반환 타입이 `dict`라서 `multiple_outputs`가 자동으로 추론됨 | 권장 | AIR202 |
| **156–157** | `context.get('data_interval_end').in_timezone(...)` | `logical_date=None`인 run에서는 `data_interval_*` 키가 컨텍스트에 아예 없음(소스 확인). 그래서 `None.in_timezone`이 **AttributeError**를 냄. 정기 스케줄 run은 영향 없음(§4) | **Breaking (수동 run)** | 미검출 |
| 163, 453 | `@task_group` | → `airflow.sdk.task_group`. 반환 dict와 `.override()`도 SDK에서 지원됨 | Deprecated | AIR311 |
| 166·202·220·276·295 | `retry_exponential_backoff=True` | 3.2부터 숫자 배수로 받음. `True`는 2.0으로 변환되어 하위 호환됨(릴리스 노트) | 권장 (`2.0`으로 명시) | — |
| 179·207·239·281·300 | 태스크 내부에서 `from airflow.models import Variable` | → `airflow.sdk.Variable`. 레거시 경로는 SDK로 넘겨 주며 DeprecationWarning을 냄(소스 확인) | Deprecated | AIR311 |
| 244, 437 | `get_current_context()` | 위 45행과 같음 | Deprecated | AIR301 |
| 418–429 | `@task(trigger_rule=ALL_DONE)` + `if not payload` | SDK `PlainXComArg.resolve`는 상류 태스크가 실패해 XCom이 없으면 return_value 키일 때 `None`을 돌려줌(소스 확인). 따라서 기존 "실패한 계좌는 적재 생략" 패턴이 그대로 동작함 | 영향 없음 | — |

### 1.3 `dags/fetch_exchange_rate_dag.py`

| 라인 | 현재 코드 | 영향 | 심각도 | ruff |
|---|---|---|---|---|
| 16 | `airflow.decorators` | → `airflow.sdk` | Deprecated | AIR311 |
| 17, 72 | `airflow.models.param.Param` | → `airflow.sdk.Param` | Deprecated | AIR311 |
| 18, 98, 171 | `airflow.operators.python.get_current_context` | → `airflow.sdk` | Deprecated | AIR301 |
| 20, 80–88 | `ExternalTaskSensor(execution_delta=5min)` | standard provider로 이동. §5 참고 | Deprecated | AIR312 |
| **99–100** | `context.get('data_interval_end').in_timezone(...)` | 수동/API 트리거에서 `logical_date=None`이면 AttributeError | **Breaking (수동 run)** | 미검출 |
| 121, 124 | `from airflow.models import Variable` (태스크 내부) | → `airflow.sdk.Variable` | Deprecated | AIR311 |

### 1.4 `dags/fetch_fund_price_daily_dag.py`

| 라인 | 현재 코드 | 영향 | 심각도 | ruff |
|---|---|---|---|---|
| 16 | `airflow.decorators` | → `airflow.sdk` | Deprecated | AIR311 |
| 17, 71, 75 | `Param` | → `airflow.sdk.Param` | Deprecated | AIR311 |
| 18, 91, 116, 165 | `get_current_context` | → `airflow.sdk` | Deprecated | AIR301 |
| **91–92** | `data_interval_end.in_timezone` | 수동 run에서 AttributeError | **Breaking (수동 run)** | 미검출 |

### 1.5 `dags/make_token_dag.py`, `dags/test_slack_alert_dag.py`

| 파일:라인 | 현재 코드 | 영향 | 심각도 | ruff |
|---|---|---|---|---|
| make_token:16 / test_slack:12 | `airflow.decorators` | → `airflow.sdk` | Deprecated | AIR311 |
| make_token:18 / test_slack:13 | `from callbacks import ...` | 콜백 자체가 Breaking 상태임(§1.1). 특히 `test_slack_alert`는 알림 회귀 테스트에 쓰기 좋음 | (콜백 경유) | — |

### 1.6 `dags/rebuild_derived_assets_dag.py`

| 라인 | 현재 코드 | 영향 | 심각도 | ruff |
|---|---|---|---|---|
| 28 | `airflow.decorators` | → `airflow.sdk` | Deprecated | AIR311 |
| 29 | `airflow.exceptions.AirflowException` | 동작함. `airflow.sdk.exceptions` 사용 권장 | 권장 | 미검출 |
| 30, 89, 93, 99 | `Param` | → `airflow.sdk.Param` | Deprecated | AIR311 |
| 31, 115, 154, 193, 223 | `get_current_context` | → `airflow.sdk` | Deprecated | AIR301 |
| **100** | `Param(datetime.now(kst).strftime(...))` | 파싱할 때마다 기본값이 달라지므로 Airflow 3의 DAG 버전 관리에서 **날짜가 바뀔 때마다 새 DAG 버전이 생김** | 권장 (버전 이력 오염) | **AIR304** |
| 109·148·187·217 | `get_current_context().get('params')` | `params`는 여전히 제공됨. `schedule=None`이고 날짜 컨텍스트를 쓰지 않으므로 logical_date None의 영향이 없음 | 영향 없음 | — |

### 1.7 `dags/upsert_manual_position_dag.py`

| 라인 | 현재 코드 | 영향 | 심각도 | ruff |
|---|---|---|---|---|
| 17 | `airflow.decorators` | → `airflow.sdk` | Deprecated | AIR311 |
| 19, 226 | 모듈 최상단 `from airflow.models import Variable`, 태스크 안의 `Variable.get(...)` | → `airflow.sdk.Variable`. 호출이 태스크 안에 있어 파싱 시점 DB 접근은 없음 | Deprecated | AIR311 |
| 20, 149–176 | `Param(..., type=['null','number'])`, `format='date'`, `enum=` | SDK `Param`도 JSON Schema를 그대로 지원함 | Deprecated (import만) | AIR311 |
| 21, 200 | `get_current_context` | → `airflow.sdk` | Deprecated | AIR301 |
| **161** | `Param(datetime.now(kst).strftime(...))` | 위 §1.6의 100행과 같음 | 권장 | **AIR304** |
| 188 | `@task def validate() -> dict` | multiple_outputs 자동 추론 | 권장 | AIR202 |
| 94 | `_sanity_check` 안의 `datetime.now(kst)` | 런타임에 호출되므로 문제없음 | 영향 없음 | — |

### 1.8 `plugins/asset_flow/**`

| 파일:라인 | 현재 코드 | 영향 | 심각도 | ruff |
|---|---|---|---|---|
| managers/token_manager.py:20, 63, 100 | `from airflow.models import Variable`, `Variable.get(name)` | → `from airflow.sdk import Variable`. 태스크 안에서만 호출되므로 SDK 경로로 동작함 | Deprecated | AIR311 |
| managers/db_manager.py:12, 216–249, 272–286 | `create_engine`, `text()`, `pd.read_sql`, `to_sql` | **Airflow 3.2부터 SQLAlchemy 2.0만 지원함**(3.2.0 릴리스 노트). 현재 코드가 `text()`와 바인드 파라미터, `engine.begin()`을 쓰고 있어 2.0 스타일과 호환됨. 단 `requirements.txt`의 `pandas`가 버전 고정 없이 들어가 있으므로 SQLA 2.0과 맞는 **pandas ≥ 2.2**인지 확인 필요 | 권장 (검증 필요) | — |
| clients/·config/·crawler/·transformers/ | Airflow import 없음 | 영향 없음 | 영향 없음 | — |

---

## 2. (1) import 경로 변경 정리

Airflow 3.0은 DAG 작성용 공개 API를 `airflow.sdk`로 통일했습니다. 레거시 경로는 "deprecated이며 향후 제거된다"고 공지되어 있습니다(3.0.0 릴리스 노트, *New Stable DAG Authoring Interface: airflow.sdk*). 3.3.2 core에서도 `add_deprecated_classes` shim으로 계속 동작하는 것을 소스로 확인했습니다.

| 기존 (2.10) | Airflow 3 권장 | 사용 파일 |
|---|---|---|
| `airflow.decorators.dag / task / task_group` | `airflow.sdk.dag / task / task_group` | DAG 7개 전부 |
| `airflow.models.param.Param` | `airflow.sdk.Param` | 5개 DAG |
| `airflow.models.Variable` | `airflow.sdk.Variable` (`default_var=` → `default=`) | 4개 DAG, callbacks, token_manager |
| `airflow.operators.python.get_current_context` | `airflow.sdk.get_current_context` | 5개 DAG |
| `airflow.sensors.external_task.ExternalTaskSensor` | `airflow.providers.standard.sensors.external_task.ExternalTaskSensor` (`apache-airflow-providers-standard`) | asset_daily, exchange_rate |
| `airflow.utils.trigger_rule.TriggerRule` | `airflow.sdk.TriggerRule` | asset_daily |
| `airflow.utils.state.DagRunState` | `airflow.sdk.DagRunState` | callbacks |
| `airflow.exceptions.AirflowException` | `airflow.sdk.exceptions.AirflowException` (기존 경로도 동작) | 3개 DAG |
| `airflow.providers.postgres.hooks.postgres.PostgresHook` | 그대로 사용 (provider를 Airflow 3 호환 버전으로) | 5개 DAG |

수정 예시 (`fetch_asset_daily_dag.py` 상단):

```python
# Before (2.10)
from airflow.decorators import task, dag, task_group
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule

# After (3.x)
from airflow.sdk import dag, task, task_group, Param, Variable, TriggerRule, get_current_context
from airflow.sdk.exceptions import AirflowException
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
```

> `apache-airflow-providers-standard`는 Airflow 2.x에도 설치할 수 있습니다. 따라서 **2.10.4에 머문 상태에서 import부터 먼저 바꿔 두면** 업그레이드 당일의 변경 폭이 줄어듭니다(Upgrading to Airflow 3 문서).

---

## 3. (2) 제거된 컨텍스트 키 → 대체

Airflow 3.0에서 제거된 컨텍스트 키는 `execution_date`, `next_ds`, `next_ds_nodash`, `prev_ds`, `prev_ds_nodash`, `prev_execution_date`, `prev_execution_date_success`, `next_execution_date`, `tomorrow_ds`, `yesterday_ds`, `conf`, `dag_run.external_trigger`입니다(Upgrading 문서, 3.0.0 릴리스 노트 *Context Cleanup and Parameter Removal*).

저장소 안에서 해당하는 곳은 `callbacks.py`의 82·142행(`context["execution_date"]`)과 65행(`DagRun.execution_date`)뿐입니다. DAG 코드는 이미 `data_interval_end`를 쓰고 있습니다.

```python
# callbacks.py — logical_date는 None일 수 있으므로 run_after로 폴백
def _run_label(context) -> str:
    dag_run = context.get("dag_run")
    dt = context.get("logical_date") or (dag_run.run_after if dag_run else None)
    if dt is None:
        return "-"
    return dt.in_timezone("Asia/Seoul").strftime("%Y-%m-%d %H:%M KST")  # 기존 'KST' 오표기도 함께 수정

execution_date = _run_label(context)   # 82행, 142행 대체
```

---

## 4. (3)·(4) 이전: `logical_date` / `data_interval` 의미 변화

### 4.1 수동·API·Asset 트리거의 `logical_date=None`

3.0.0 릴리스 노트(*Context Behavior for Asset and Manually Triggered DAGs*)의 원문입니다.

> "For DAG runs triggered by an Asset event or through the REST API without specifying a logical_date, Airflow now sets logical_date=None by default. These DAG runs do not have a data interval, and attempting to access data_interval_start, data_interval_end, or logical_date from the task context will raise a KeyError."

3.3.2 Task SDK 소스(`task_runner.py`)도 같습니다. `dag_run.logical_date`가 있을 때만 `logical_date / ds / ts / data_interval_start / data_interval_end`를 컨텍스트에 넣습니다. 그래서 현재 코드의 `context.get('data_interval_end')`는 None을 받고, 바로 다음 `.in_timezone()` 호출에서 AttributeError가 납니다. 해당 위치는 `fetch_asset_daily:156`, `fetch_exchange_rate:99`, `fetch_fund_price_daily:91` 세 곳입니다.

수정 예시 (공통 헬퍼로 빼기를 권장):

```python
from airflow.sdk import get_current_context

def _kst_run_day():
    """정기 run: data_interval_end / 수동·API run(logical_date=None): run_after"""
    ctx = get_current_context()
    end = ctx.get("data_interval_end") or ctx["dag_run"].run_after
    return end.in_timezone("Asia/Seoul")

@task(task_id="get_standard_date")
def get_standard_date() -> dict:
    kst_end = _kst_run_day()
    return {
        "standard_date": kst_end.subtract(days=1).strftime("%Y-%m-%d"),
        "base_date": kst_end.strftime("%Y-%m-%d"),
    }
```

### 4.2 `create_cron_data_intervals` 기본값 변경 → CronTriggerTimetable

Airflow 3.0부터 `[scheduler] create_cron_data_intervals = False`가 기본값입니다. 그래서 cron 문자열 schedule(`'55 6 * * *'` 등, 이 저장소에서는 4개 DAG)은 **CronTriggerTimetable**로 해석됩니다(Upgrading 문서, 3.0.0 릴리스 노트 *Updated Default Configuration*).

| 정기 run (T일 07:00 KST 실행) | 2.10 (CronDataIntervalTimetable) | 3.x 기본 (CronTriggerTimetable) |
|---|---|---|
| `logical_date` | T-1 07:00 | **T 07:00** |
| `data_interval_start` | T-1 07:00 | T 07:00 |
| `data_interval_end` | T 07:00 | T 07:00 |

- 이 저장소의 DAG는 **`data_interval_end`만 기준일 계산에 쓰므로, 정기 run의 `standard_date`/`base_date`는 바뀌지 않습니다.**
- 바뀌는 것은 `logical_date`(그리고 `ds`/`ts`, run_id 날짜, Slack의 "실행 기준일")가 하루 뒤로 밀린다는 점입니다.
- 공식 문서는 "data interval 의미에 의존한다면 업그레이드 **전에** `create_cron_data_intervals=True`를 명시하라. 업그레이드 후에 바꾸면 DAG마다 한 번씩 스케줄 run이 건너뛰어진다"고 경고합니다.
  - **권장**: 이 저장소는 기본값(False)을 그대로 써도 됩니다. 다만 섞어 쓰지 않도록 업그레이드 전에 둘 중 하나로 확정하세요.

### 4.3 콜백 실행 위치 변화

| 콜백 | 2.10 | 3.x |
|---|---|---|
| 태스크 레벨 `on_failure_callback` (`default_args`) | 태스크 프로세스 / 스케줄러(좀비 처리) | **태스크 프로세스(Task SDK runner)**. ORM이 차단됨 |
| DAG 레벨 `on_success_callback` (`slack_recovery_callback`) | 스케줄러 쪽 DagFileProcessor | **standalone dag-processor**의 하위 프로세스에서 실행됨. 이 프로세스도 `WatchedSubprocess` 기반이라 `block_orm_access()`가 적용됨(소스 확인). 에러는 `$AIRFLOW_HOME/logs/dag_processor/`에만 남고 UI에는 표시되지 않음(Callbacks 문서) |

- DAG 콜백의 context는 마지막 TI 정보로 `RuntimeTaskInstance.get_template_context()`를 만들어 넘깁니다. 그래서 `dag`, `run_id`, `dag_run`, `ti`, `reason`, `params`를 쓸 수 있고 `execution_date`는 없습니다(소스 확인).
- dag-processor의 통신 채널(`ToManager`)은 `GetVariable`, `GetConnection`, `GetPreviousDagRun`, `GetPrevSuccessfulDagRun`, `GetTaskStates`, `GetTICount` 요청을 지원합니다(소스 확인).
  - 따라서 콜백 안의 **`Variable.get("SLACK_WEBHOOK_URL")`은 SDK 경유로 동작**하고, **`ti.get_previous_dagrun()`도 사용할 수 있습니다.**
- 공식 문서의 원문: "Task code can no longer directly access the metadata database. Interactions with DAG state, task history, or DAG runs must be performed via the Airflow REST API or exposed context."

### 4.4 `_was_previous_run_failed()` 대안

**대안 A (권장, 추가 의존성 없음): Task SDK 컨텍스트 API**

```python
from airflow.sdk import DagRunState

def _was_previous_run_failed(context) -> bool:
    ti = context.get("ti")
    dag_run = context.get("dag_run")
    if ti is None or dag_run is None or dag_run.logical_date is None:
        return False  # logical_date 없는 수동 run은 비교 대상이 없음
    try:
        # 직전 run(logical_date 기준)을 조회함. state 필터 없이 가져와 '직전 run이 실패였는지'만 판단
        prev = ti.get_previous_dagrun()
    except Exception as e:  # noqa: BLE001 — 콜백은 절대 죽지 않게
        print(f"[callback] 이전 실행 조회 실패: {e}")
        return False
    if prev is None or prev.run_type != dag_run.run_type:
        return False  # 기존 '동일 run_type' 정책 유지(완전 동일 재현은 아님, 아래 주의 참고)
    return prev.state == DagRunState.FAILED

def slack_recovery_callback(context) -> None:
    if not _was_previous_run_failed(context):
        return
    ...
```

- `RuntimeTaskInstance.get_previous_dagrun(state=None)`은 "주어진 logical_date 이전의 직전 DagRun"을 돌려주고, logical_date가 None이면 None을 돌려줍니다(소스 확인, task-sdk 1.3.2).
- **주의**: 기존 코드는 "같은 run_type 중 직전 run"을 찾았지만, 위 API는 run_type 필터가 없습니다.
  - 직전 run이 다른 타입(예: 수동 run이 중간에 끼어든 경우)이면 위 코드는 복구 판단을 생략합니다.
  - 기존 정책을 완전히 재현하려면 대안 B를 쓰세요.

**대안 B: Airflow REST API v2 (`apache-airflow-client` 또는 `requests`)**

```python
# Upgrading 문서 권장: REST API + /auth/token 으로 받은 access token
import requests
from airflow.sdk import Variable

def _prev_same_type_failed(dag_id: str, run_id: str, run_type: str) -> bool:
    base = Variable.get("AIRFLOW_API_BASE", default="http://airflow-apiserver:8080")
    token = requests.post(f"{base}/auth/token",
                          json=Variable.get("AIRFLOW_API_CRED", deserialize_json=True), timeout=5).json()["access_token"]
    r = requests.get(f"{base}/api/v2/dags/{dag_id}/dagRuns",
                     params={"run_type": run_type, "order_by": "-run_after", "limit": 5},
                     headers={"Authorization": f"Bearer {token}"}, timeout=5)
    runs = [x for x in r.json()["dag_runs"] if x["dag_run_id"] != run_id]
    return bool(runs) and runs[0]["state"] == "failed"
```

- 서비스 계정 크리덴셜을 Variable/Connection으로 관리해야 합니다. 또한 auth manager(3.x 기본값은 SimpleAuthManager)에 맞춰 토큰 발급 방식을 확인해야 합니다.
- `run_type` 필터와 `order_by` 파라미터 이름은 배포할 버전의 `/api/v2` OpenAPI 문서로 최종 확인하세요.

**대안 C (비권장)**: `PostgresHook`으로 메타DB를 읽기 전용으로 조회하는 방법입니다. 공식 문서는 "스키마가 공개 API가 아니어서 향후 깨질 수 있다"며 이 방식을 권하지 않습니다.

**추가 권장**: `try/except`가 RuntimeError를 삼켜 "조용한 실패"가 된 것이 핵심 문제입니다. 따라서 예외를 로그에 남길 때 `log.exception`으로 스택까지 남기고, `test_slack_alert` DAG로 실패→복구 시나리오 회귀 테스트를 업그레이드 체크리스트에 넣으세요.

---

## 5. (4) ExternalTaskSensor 동작 변화

standard provider 1.20.0의 `ExternalTaskSensor._get_logical_date()` 소스를 확인했습니다.

```python
# Airflow 3.x: contexts define "logical_date" (or fall back to dag_run.run_after).
logical_date = context.get("logical_date")
if logical_date: return logical_date
if dag_run and dag_run.run_after: return dag_run.run_after
raise ValueError(...)
```

이후 `logical_date - execution_delta`로 상류 run을 찾고, 상태 조회는 ORM 대신 `ti.get_dr_count()`/`get_ti_count()` 같은 SDK API로 합니다.

| 상황 | 영향 |
|---|---|
| **정기 run** (`make_token` 06:50 → `fetch_exchange_rate` 06:55 → `fetch_asset_daily` 07:00, `fetch_fund_price_daily` 06:55) | 모든 cron DAG가 함께 CronTriggerTimetable로 바뀝니다. logical_date가 모두 "당일 실행 시각"으로 같은 만큼 이동하므로 **`execution_delta=5분`이 그대로 맞습니다** (T 07:00 − 5분 = T 06:55). 단 `create_cron_data_intervals`를 DAG마다 다르게 적용하면(예: 일부 DAG만 `timetable=` 명시) 하루가 어긋나 센서가 600초 뒤 timeout됩니다 |
| **업그레이드 전환일** | 업그레이드 뒤 설정을 바꾸면 run이 하나 건너뛰어집니다(§4.2). 상류와 하류가 같은 날 함께 전환되도록 운영 시간(06:50) 밖에 배포하세요 |
| **수동/API 트리거 run** (logical_date=None) | `run_after`(트리거 시각, 초 단위)에서 5분을 뺀 시각으로 상류 run을 찾는데, 그런 run이 없으므로 **600초 뒤 `AirflowSensorTimeout`** → `fetch_asset_daily`의 수동 재실행 시 센서 실패. 2.10에서도 수동 run의 logical_date가 트리거 시각이어서 사실상 같은 문제가 있었지만, 3.x에서는 의미가 더 명확해졌습니다 |
| UI 트리거 | 3.x UI 트리거 폼에서 logical date를 직접 지정하면(예: 당일 07:00 KST) 센서가 상류의 정기 run을 찾을 수 있습니다. 운영 가이드에 이 방법을 명시하세요 |

수정 예시 (수동 재실행 시 센서 우회 + import 변경):

```python
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

wait_for_exchange_rate = ExternalTaskSensor(
    task_id="wait_for_exchange_rate",
    external_dag_id="fetch_exchange_rate",
    external_task_id="upload_exchange_rates",
    execution_delta=timedelta(minutes=5),
    timeout=600, poke_interval=30, mode="reschedule",   # poke → reschedule 권장(LocalExecutor 슬롯 절약)
    soft_fail=False,
)
# 대안: 수동 run에서는 센서를 건너뛰는 short-circuit,
# 또는 장기적으로 Asset 기반 스케줄(outlets=[Asset("market.exchange_rate_daily")])로 센서 체인을 없애는 방안 검토
```

---

## 6. (5) Variable.get / PostgresHook / Param / get_current_context / task_group / TriggerRule

| API | 3.3.2 호환성 | 조치 |
|---|---|---|
| `Variable.get` | `airflow.models.Variable.get`은 실행 컨텍스트(태스크·DAG 파싱·dag-processor)에서 SDK로 넘겨 주며 DeprecationWarning을 냅니다(소스 확인). SDK 시그니처는 `get(key, default=NOTSET, deserialize_json=False)`입니다. **`default_var` 키워드가 없습니다** | `from airflow.sdk import Variable`로 바꾸고 `default_var=` → `default=`. 최상단 레벨에서 `Variable.get`을 호출하는 곳은 없으므로 파싱 부하 문제는 없음 |
| `PostgresHook(postgres_conn_id='postgres_asset').get_sqlalchemy_engine()` | 동작함. 커넥션은 Task SDK의 `GetConnection`으로 API 서버를 거쳐 조회됩니다. **사용자 데이터 DB 접근은 금지 대상이 아닙니다**(금지 대상은 Airflow 메타DB ORM). | Airflow 3 호환 `apache-airflow-providers-postgres`를 고정. 3.2+는 SQLAlchemy 2.0 전용이므로 pandas ≥ 2.2와 함께 `to_sql`/`read_sql` 회귀 테스트 |
| `Param` | `airflow.sdk.Param`으로 같은 JSON Schema 옵션(`type`, `enum`, `format`, `description`)을 지원합니다 | import만 변경. `datetime.now()` 기본값 2건은 AIR304에 따라 수정(아래) |
| `get_current_context` | `airflow.sdk.get_current_context`. 반환 `Context`는 `total=False` TypedDict라서 `logical_date`/`data_interval_*`가 **없을 수 있습니다** | import 변경과 None 방어(§4.1) |
| `task_group` (+ dict 반환, `.override(task_id=...)`) | `airflow.sdk.task_group`에서 그대로 지원됨 | import만 변경 |
| `TriggerRule.ALL_DONE` | `airflow.sdk.TriggerRule`로 의미는 같습니다. 3.0에서는 `ALWAYS`를 teardown과 함께 쓰지 못하도록 제한만 추가됐으며, 이 저장소와는 무관합니다 | import만 변경 (ruff로 검출되지 않으니 수동 수정) |
| `retry_exponential_backoff=True` | 3.2에서 숫자 배수로 확장됐고 `True`는 2.0으로 변환됨 | `2.0`으로 명시하면 의미가 분명해짐 |

`Param` 동적 기본값 수정 예시 (AIR304):

```python
# Before: 파싱 시각마다 기본값 변경 → 매일 새 DAG 버전
'end_date': Param(datetime.now(kst).strftime('%Y-%m-%d'), type='string', format='date'),

# After: 기본값은 고정(null), 값이 비어 있으면 런타임에 계산
'end_date': Param(None, type=['null', 'string'], format='date',
                  description='비우면 실행 시점 기준 어제(KST)'),
...
end = params['end_date'] or pendulum.now('Asia/Seoul').subtract(days=1).to_date_string()
```

> `format='date'` + `null` 조합이 UI 폼에서 기대한 대로 렌더링되는지는 3.x UI에서 한 번 확인하세요.

---

## 7. (6) plugins 폴더 import 방식

- 3.3.2 core의 `airflow/settings.py`에 있는 `prepare_syspath_for_config_and_plugins()`가 여전히 `PLUGINS_FOLDER`(`/opt/airflow/plugins`)와 `config/`를 `sys.path`에 추가합니다(소스 확인). 따라서 **`from callbacks import ...`와 `from asset_flow... import ...`는 그대로 동작합니다.**
- `AirflowPlugin` 클래스, `appbuilder_views`, `flask_blueprints`는 쓰지 않으므로 FastAPI 플러그인 전환(AIP-68) 대상이 아닙니다.
- 주의할 점:
  1. Airflow 3의 standalone **dag-processor, api-server, scheduler, (LocalExecutor) worker** 모두에 `./plugins` 볼륨을 같은 경로로 마운트해야 합니다. DAG 콜백은 dag-processor에서, 태스크 콜백은 worker에서 `callbacks`를 import하기 때문입니다.
  2. 장기적으로는 `asset_flow`를 `dags/` 아래의 공용 패키지(예: `dags/common/`)나 설치형 패키지로 옮기는 것을 권장합니다. DAG 번들(DAG bundle, 버전 관리)을 도입하면 plugins 폴더 코드는 번들 버전과 따로 움직이기 때문입니다.

---

## 8. (7) DAG 파라미터 기본값 변화

| 항목 | 2.10 기본 | 3.x 기본 | 이 저장소 |
|---|---|---|---|
| `catchup` (`catchup_by_default`) | True | **False** | 7개 DAG 모두 `catchup=False`를 명시 → 영향 없음 |
| `schedule` 기본값 | `timedelta(days=1)` / `@once` 계열 | **None** | 7개 DAG 모두 명시(cron 4개, None 3개) → 영향 없음 |
| `schedule_interval`, `timetable` 인자 | 사용 가능 | **제거** (`schedule` 하나로 통일) | 사용하지 않음 |
| cron 문자열 해석 (`create_cron_data_intervals`) | CronDataIntervalTimetable | **CronTriggerTimetable** | §4.2 (data_interval_end 기반이라 기준일은 유지, logical_date 의미는 바뀜) |
| `fail_stop` | — | `fail_fast`로 이름 변경 | 사용하지 않음 |
| 미래 logical_date 트리거 | 허용 | **금지** | 해당 없음 |
| 스킵된 태스크의 `on_success_callback` | 실행 | **실행 안 함** | 태스크 레벨 success 콜백이 없으므로 무관 |

---

## 9. ruff AIR 규칙 자동 검출 결과

실행 환경은 다음과 같습니다. 저장소의 `dags/`와 `plugins/`를 **스크래치패드 임시 디렉터리에 복사한 뒤** 그 복사본에서 실행했습니다. 저장소 파일은 수정하지 않았습니다.

```bash
ruff 0.17.0
ruff check --isolated --preview --select AIR --statistics dags plugins
```

| 규칙 | 건수 | 의미 | 검출된 항목 |
|---|---|---|---|
| AIR311 | 59 | airflow3-suggested-update (아직 동작, 향후 제거) | `airflow.decorators.dag/task/task_group`, `airflow.models.param.Param`, `airflow.models.Variable` (callbacks.py:21, token_manager.py:63·100 포함) |
| AIR301 | 13 | airflow3-removal | `airflow.operators.python.get_current_context` (5개 DAG의 13개 호출부) |
| AIR202 | 11 | `@task` 반환 dict의 multiple_outputs 자동 추론 | fetch_asset_daily의 get_* 10개, upsert의 `validate` |
| AIR312 | 3 | standard provider로 이동 권장 | `ExternalTaskSensor` 3개 |
| AIR304 | 2 | DAG 인자의 동적 값 → 끝없이 DAG 버전 생성 | rebuild_derived_assets:100, upsert_manual_position:161 |
| AIR302 | 0 | 제거되고 provider로 이동한 항목 | — |

- 비 preview 모드(`--select AIR`)에서는 75건이 검출됩니다.
- AIR301/AIR302 검출은 ruff 0.13.1 이상이 필요합니다(Upgrading 문서).
- `--fix --unsafe-fixes`를 쓰면 86건을 자동으로 고칠 수 있습니다. 미리보기(`--diff`)에서 확인한 동작은 다음과 같습니다.
  - `default_var=` → `default=`로 자동 변환합니다.
  - 태스크 안의 지역 `from airflow.models import Variable`을 지우고 **모듈 최상단에 `from airflow.sdk import Variable`을 추가**합니다. 동작은 같지만 import 위치가 바뀝니다.
  - `Param` import를 `airflow.sdk.definitions.param`으로 바꿉니다. 공개 경로인 `airflow.sdk.Param`으로 손봐 두기를 권장합니다.

**ruff로 검출되지 않아 수동으로 고쳐야 하는 항목**

1. `callbacks.py:82,142` — `context["execution_date"]` (P0)
2. `callbacks.py:56–71` — `DagRun.find` ORM 조회와 `r.execution_date` (P0)
3. `fetch_asset_daily:156`, `fetch_exchange_rate:99`, `fetch_fund_price_daily:91` — `data_interval_end` None 방어 (P1)
4. `fetch_asset_daily:48` — `airflow.utils.trigger_rule.TriggerRule`, `callbacks.py:17` — `airflow.utils.state.DagRunState`
5. `callbacks.py:31–32` — UI URL과 base_url 설정 키
6. `airflow.exceptions.AirflowException` (동작은 함, 선택 사항)

---

## 10. 권장 마이그레이션 순서 (코드 측면)

1. **2.10.4에 머문 상태에서 먼저 할 일**
   - `apache-airflow-providers-standard`를 설치하고 import를 바꿉니다. 2.x에서도 동작하는 범위만 바꿔야 합니다. `airflow.sdk`는 2.x에 없으므로 이 단계에서는 provider 경로만 바꿉니다.
   - `callbacks.py`에서 `execution_date`를 `logical_date`/`run_after`로 바꿉니다(2.10에도 `logical_date` 키가 있음).
   - `data_interval_end` None 방어 코드를 넣고, AIR304 수정을 적용합니다.
2. **3.x 전환 PR**
   - `ruff --select AIR3 --fix --unsafe-fixes`를 적용하고 Param 경로를 손봅니다.
   - `_was_previous_run_failed`를 `ti.get_previous_dagrun()` 기반으로 교체합니다(§4.4 대안 A).
   - `create_cron_data_intervals` 정책을 확정하고 docker-compose 설정에 명시합니다.
3. **검증**
   - `test_slack_alert`로 실패 알림 → 성공(복구) 알림이 실제로 오는지 확인합니다.
   - `fetch_asset_daily`를 수동으로 트리거해 센서 동작을 확인합니다.
   - pandas `to_sql`(SQLA 2.0)을 실제 적재로 회귀 테스트합니다.
   - dag-processor 로그(`logs/dag_processor/`)에 콜백 예외가 없는지 확인합니다.

---

## 출처

- Upgrading to Airflow 3 (stable = 3.3.2): https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html
- Airflow Release Notes (3.0.0 *Restricted Metadata Database Access*, *Context Behavior for Asset and Manually Triggered DAGs*, *Context Cleanup and Parameter Removal*, *Updated Default Configuration*, *New Stable DAG Authoring Interface: airflow.sdk*, *Standalone DAG Processor Required* / 3.2.0 *Upgrade SQLAlchemy (SQLA) to 2.0*, *Support numeric multiplier values for retry_exponential_backoff*): https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html
- Callbacks (3.3.2): https://airflow.apache.org/docs/apache-airflow/3.3.2/administration-and-deployment/logging-monitoring/callbacks.html
- DAG Runs (3.3.1): https://airflow.apache.org/docs/apache-airflow/3.3.1/core-concepts/dag-run.html
- ruff Airflow 규칙(AIR): https://docs.astral.sh/ruff/rules/#airflow-air
- 소스 확인에 쓴 PyPI wheel (스크래치패드에서 압축만 풀어 읽음):
  - `apache-airflow-core==3.3.2`: `airflow/dag_processing/processor.py` (`_execute_dag_callbacks`, `ToManager`), `airflow/settings.py` (`prepare_syspath_for_config_and_plugins`), `airflow/models/variable.py`, 레거시 import shim들
  - `apache-airflow-task-sdk==1.3.2`: `execution_time/supervisor.py` (`block_orm_access`, `BlockedDBSession`), `execution_time/task_runner.py` (컨텍스트 구성, `get_previous_dagrun`, `log_url`), `definitions/variable.py`, `definitions/xcom_arg.py`
  - `apache-airflow-providers-standard==1.20.0`: `sensors/external_task.py` (`_get_logical_date`)
