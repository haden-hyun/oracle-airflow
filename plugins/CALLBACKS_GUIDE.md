# Slack 알림 콜백 사용 가이드

`plugins/callbacks.py`에 정의된 두 콜백 함수를 DAG에 연결하면 실패/복구 알림을 Slack으로 받을 수 있다.

---

## 알림 동작 방식

| 상황 | 알림 여부 | 색상 |
|---|---|---|
| Task 최종 실패 (retry 소진) | O | 빨강 |
| 이전 실패 → 현재 성공 (복구) | O | 초록 |
| 매일 정상 성공 | X | — |

> `on_failure_callback`은 retry를 모두 소진한 **최종 실패** 시에만 호출된다.  
> 복구 알림은 직전 DAG Run이 `failed` 상태일 때만 발송되어 노이즈를 차단한다.

---

## 실패 알림 메시지 항목

```
🔴 DAG 실패: fetch_asset_daily
─────────────────────────────
Task        | collect_stock
시도         | 2 / 2
실행 기준일   | 2026-06-05 07:00 KST
Run ID      | scheduled__2026-06-05T07:00:00+00:00

에러 메시지
  File "/opt/airflow/dags/...", line 42, in collect_stock
    raise ValueError("API 응답 없음")
  ValueError: API 응답 없음

[ Airflow UI에서 확인 ]  ← 클릭 시 해당 DAG Run으로 바로 이동
```

---

## 1단계: Airflow Variable 설정

### SLACK_WEBHOOK_URL

Slack Incoming Webhook URL을 Airflow Variable로 등록한다.

1. [api.slack.com/apps](https://api.slack.com/apps) → **Create New App** → **From scratch**
2. 좌측 **Incoming Webhooks** → 활성화 → **Add New Webhook to Workspace**
3. 알림 받을 채널 선택 후 생성된 URL 복사  
   형태: `https://hooks.slack.com/services/T.../B.../xxxxxxxx`
4. Airflow UI → **Admin > Variables** → `SLACK_WEBHOOK_URL` 키로 등록

> ⚠️ Slack 워크스페이스 주소(`xxx.slack.com`)가 아닌 Webhook URL(`hooks.slack.com/...`)을 등록해야 한다.  
> 채널은 Webhook 생성 시 지정되며 URL에 내포되므로 코드에서 별도 지정하지 않는다.

---

## 2단계: Airflow Base URL 설정

Slack 알림의 "Airflow UI에서 확인" 버튼 링크에 사용된다.  
`.env` 파일에 실제 접속 URL을 입력한다.

```dotenv
AIRFLOW__WEBSERVER__BASE_URL=http://<고정IP>:8080
```

`docker-compose.yaml`이 `.env`에서 읽어 컨테이너에 주입하도록 이미 설정되어 있다.

```yaml
AIRFLOW__WEBSERVER__BASE_URL: ${AIRFLOW__WEBSERVER__BASE_URL:-http://localhost:8080}
```

변경 후 컨테이너 재시작:

```bash
docker compose down && docker compose up -d
```

---

## 3단계: DAG에 콜백 연결

```python
from callbacks import slack_failure_callback, slack_recovery_callback
```

### 실패 콜백 — `default_args`에 추가 (task 레벨)

task 단위로 에러 트레이스백을 포함한 알림이 발송된다.

```python
default_args = {
    'owner': 'haejun',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': slack_failure_callback,
}
```

### 복구 콜백 — `@dag` 데코레이터에 추가 (DAG 레벨)

DAG 전체가 성공했을 때만 복구 여부를 판단한다.  
`default_args`(task 레벨)에 넣으면 첫 번째 task 성공 시점에 조기 발송되므로 반드시 DAG 레벨에 지정한다.

```python
@dag(
    dag_id='my_dag',
    default_args=default_args,
    on_success_callback=slack_recovery_callback,
    ...
)
def my_dag():
    ...
```

---

## 콜백 함수 시그니처 참고

Airflow가 콜백 호출 시 넘겨주는 `context` 딕셔너리의 주요 키:

| 키 | 설명 |
|---|---|
| `context['dag']` | DAG 객체 (`dag.dag_id` 등) |
| `context['task_instance']` | TaskInstance 객체 (`task_id`, `try_number`) |
| `context['run_id']` | DAG Run ID |
| `context['execution_date']` | 실행 기준 시각 (pendulum) |
| `context['exception']` | 발생한 예외 객체 |
