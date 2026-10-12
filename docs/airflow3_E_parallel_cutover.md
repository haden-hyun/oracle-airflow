# 작업 E — Airflow 3.3.2 병행 구축 후 기존 스택 제거 전략 평가

- 작성일: 2026-10-12
- 대상 저장소: `oracle-airflow`. 읽기 전용으로 분석했고 이 보고서 외의 파일은 수정하지 않았다.
- 대상 버전: **Airflow 3.3.2**(2026-09-17, 현재 최신 안정판). 요청서의 "3.10"이라는 버전은 존재하지 않으므로 3.3.2 기준으로 작성했다.
- 현재 구성: `apache/airflow:2.10.4` docker-compose(LocalExecutor). 메타DB는 compose 내부 `postgres:13`(볼륨 `postgres-db-volume`)이다. 자산 업무 DB는 호스트의 별도 PostgreSQL(`host.docker.internal`, Connection `postgres_asset`)이고, 실패 알림은 `plugins/callbacks.py`의 Slack webhook(Variable `SLACK_WEBHOOK_URL`)이다.
- 선행 보고서: [A 코드 영향](airflow3_A_code_impact.md), [B 인프라](airflow3_B_infra.md), [C 릴리스·전략](airflow3_C_release_strategy.md). 이 문서는 C의 Phase 2(스테이징 병행 검증)와 Phase 3(운영 전환)을 구체화하고, 사용자가 제안한 "신규 스택 구축 후 기존 스택 제거" 방식으로 어떻게 대체하는지 다룬다.

---

## 0. 결론 요약

| # | 질문 | 결론 |
|---|---|---|
| 1 | 사용자 계획(2.10.4 유지 + 3.3.2 신규 구축 → 전환 후 기존 제거)은 적절한가 | **조건부 적절. 아래 격리 조건을 지키면 in-place 업그레이드보다 낫다.** 롤백이 "기존 스택 다시 기동" 한 줄로 끝나고, C의 Phase 1(기존 스택의 PG13→16 업그레이드)이 필요 없어진다. 다만 두 스택이 **같은 자산 DB에 쓰고 KIS 토큰을 같이 발급하는 상태로 동시에 가동하면 부적절하다.** |
| 2 | 신규 스택의 메타DB는 새로 만드나(clean install), 이관하나(migrate) | **기본은 기존 DB의 덤프를 PG16에 복원한 뒤 `airflow db migrate`로 이관한다.** 실행 이력, Variables, Connections, 사용자, pause 상태가 그대로 넘어오고, 전환 직후 스케줄 중복이나 누락 판단이 쉽다. clean install에 `variables`/`connections`/`users` export·import를 더하는 방식은 **마이그레이션이 실패할 때 쓰는 Plan B**로 둔다. |
| 3 | 같은 서버 충돌 | 포트(8080), compose 프로젝트명과 볼륨명, `./logs`, `./data/tokens`, 이미지 이름이 충돌 지점이다. **신규 스택은 별도 디렉터리에 두고 compose 최상위에 `name: oracle-airflow3`를 명시하며, 섀도 기간에는 `8081` 포트를 쓴다.** 로그는 분리하고, 토큰 디렉터리는 기존 것을 **읽기 전용**으로 마운트한다. |
| 4 | 이중 실행 방지 | ① 신규 스택은 **모든 DAG가 paused인 상태로 기동**한다(이관 DB는 운영의 unpause 상태를 그대로 가져오므로 기동 전에 CLI로 일괄 pause해야 한다). ② `AIRFLOW_CONN_POSTGRES_ASSET` 환경변수로 **스테이징 DB에만 쓰게 한다.** ③ `AIRFLOW_VAR_SLACK_WEBHOOK_URL`로 알림을 테스트 채널로 보낸다. ④ **make_token을 pause하는 것만으로는 토큰 발급을 막을 수 없다.** `GetTokens()`가 파일이 없으면 직접 발급하기 때문이다. 그래서 토큰 디렉터리를 RO로 공유하고, 섀도 실행은 07:30 이후에 수동으로 한다. |
| 5 | 기존 스택 처리 | 전환 당일에는 `docker compose down`(볼륨 유지)까지만 한다. `stop`만 하면 `restart: always` 때문에 **서버가 재부팅될 때 2.x 스케줄러가 되살아난다.** 볼륨과 최종 덤프는 **30일** 보존한 뒤 삭제하고, 덤프 아카이브는 별도 저장소에 **90일** 둔다. |
| 6 | 작업량 | C의 추정(3~5인일, 캘린더 1.5~2주)과 같다. Phase 1(PG 선행 업그레이드)이 빠지는 대신 섀도 격리 설정(약 0.5인일)이 추가된다. |

---

## 1. 두 전략 비교 — in-place vs 블루/그린(병행 구축)

### 1.1 정의

| 전략 | 내용 | 기존 보고서와의 관계 |
|---|---|---|
| **in-place** | 기존 디렉터리와 compose 프로젝트에서 이미지와 compose를 3.x로 교체하고, 같은 메타DB에 `airflow db migrate`를 실행한다. 3.3.2가 PG13을 지원하지 않으므로 PG16 볼륨 이전을 먼저 한다. | B §7, C Phase 1~3의 원안 |
| **블루/그린(사용자안)** | 기존 스택(블루, 2.10.4)은 손대지 않는다. 별도 디렉터리와 compose 프로젝트로 신규 스택(그린, 3.3.2 + PG16)을 만들어 검증한 뒤 운영을 그린으로 옮기고 블루를 제거한다. | C Phase 2의 "별도 compose 프로젝트 스테이징"을 **버리지 않고 운영으로 승격**하는 형태 |

C의 권장안에도 이미 "별도 compose 프로젝트로 띄우는 스테이징"이 들어 있다. 그래서 두 전략의 차이는 **스테이징을 버리고 원래 스택을 고치느냐, 스테이징을 운영으로 승격하느냐**라는 전환 방식에 있다.

### 1.2 비교표

| 기준 | in-place | 블루/그린(격리 조건 충족 시) |
|---|---|---|
| 롤백 | 3.x를 내리고 **덤프를 복원**한 뒤 2.10.4로 기동한다. `db downgrade`는 B §5.2 기준으로 주 경로가 아니다. 덤프와 복원 절차가 맞아야 하므로 수십 분이 걸리고 실수할 여지가 있다. | 그린을 내리고 **블루 디렉터리에서 `docker compose up -d`** 하면 된다. 블루 메타DB(PG13 볼륨)는 전환 이후 한 번도 쓰이지 않았으므로 복원할 필요가 없다. 수 분이면 끝난다. |
| PG13→16 | 기존 스택에서 먼저 해야 한다(C Phase 1). 운영 메타DB 볼륨을 교체하는 작업이 하나 더 생긴다. | 그린이 처음부터 PG16을 쓴다. **블루의 PG13은 건드리지 않는다.** 덤프는 13에서 뜨고 복원은 16에서 한다. |
| 실환경 검증 | 별도 스테이징을 만들어야 하고, 만든 스테이징은 검증 후 버린다. | 검증한 **그 스택이 운영이 된다.** 검증한 구성과 운영 구성의 차이가 `.env` 몇 줄로 줄어든다(§5.4). |
| 다운타임 | 전환 작업 시간(빌드, 마이그레이션, 기동) | 최종 덤프, 복원, migrate에 드는 시간만 걸린다. 빌드는 미리 끝나 있다. 어느 쪽이든 07시 배치를 피해 오후에 하면 업무 영향은 없다. |
| 이중 실행 위험 | 없다(스택이 하나). | **있다.** 섀도 기간 동안 §4의 격리가 필수다. 이 전략의 유일한 큰 비용이다. |
| 서버 자원 | 스택 하나 | 섀도 기간에 두 스택이 동시에 뜬다. 3.x는 api-server, scheduler, dag-processor, triggerer, postgres로 컨테이너가 5개이고, 2.x는 3개다. 이미지도 2개(각각 Chromium 포함)라 **RAM과 디스크를 미리 확인해야 한다**(§3.2). |
| 디렉터리·git | 하나의 작업 트리에서 브랜치를 바꾸면 된다. | 디렉터리가 2개가 되므로 git 전략이 필요하다(§6). |
| 실행 이력 | 그대로 유지된다. | 메타DB를 이관하면 유지되고, clean install이면 사라진다(§2). |

**판단:** 이 프로젝트는 매일 07시 자산 스냅샷이 끊기지 않는 것이 핵심 가치다(C §5.1). 롤백이 쉽다는 블루/그린의 장점이 이중 실행 격리에 드는 비용보다 크다. 다만 서버 메모리가 두 스택을 동시에 띄우기에 부족하면(§3.2) in-place로 돌아가야 한다.

---

## 2. 신규 스택의 메타DB — 이관(migrate) vs 새로 생성(clean install)

### 2.1 무엇이 넘어가고 무엇이 사라지는가

| 항목 | 이관(덤프 복원 + `airflow db migrate`) | clean install(`db migrate` on 빈 DB + export/import) |
|---|---|---|
| DagRun / TaskInstance 실행 이력, 로그 메타 | **유지된다.** Grid 화면에서 2.x 시절 run을 볼 수 있다. 로그 파일 자체는 `./logs`에 있으므로 블루의 로그 디렉터리를 복사하지 않으면 링크만 남는다. | **사라진다.** 필요하면 블루를 보존하는 기간 동안 블루 DB에서 조회한다. |
| Variables(`KIS_*`, `UPBIT`, `SLACK_WEBHOOK_URL`, `MANUAL_ASSET_CONFIG` 등) | 유지된다. | `airflow variables export` 후 `import`로 옮긴다. |
| Connections(`postgres_asset` 등) | 유지된다. | `airflow connections export` 후 `import`로 옮긴다. |
| 사용자(FAB `ab_user`), 역할 | 유지된다. FabAuthManager를 쓸 때만 해당한다(B §3.3). | `airflow users export` 후 `import`로 옮긴다. **비밀번호는 옮겨지지 않는 것으로 보인다**(§2.3). 관리자는 `_AIRFLOW_WWW_USER_*`로 새로 만든다. |
| DAG pause 상태 | **운영 상태(unpaused)가 그대로 복사된다.** 그래서 기동 전에 일괄 pause해야 한다(§4.2). | `DAGS_ARE_PAUSED_AT_CREATION=true`이므로 **모두 paused로 생성된다.** |
| 스케줄 연속성 | 스케줄러가 마지막 run(블루의 오늘 07:00)을 알고 있으므로 **같은 날 같은 run을 다시 만들지 않는다.** | 3.x 기본값인 `CronTriggerTimetable`과 `catchup=False` 조합에서는 오후에 unpause해도 **다음 트리거 시각부터** run을 만든다(공식 timetable 문서의 예: 15시에 활성화하면 다음 날 0시 run부터). 단, `create_cron_data_intervals=True`(2.x 의미)로 바꿨다면 가장 최근 구간 run이 **즉시** 생길 수 있다. 그러면 오후 잔고가 T-1 스냅샷을 덮어쓰므로 주의해야 한다(A §4.2). |
| Fernet | 현재 `FERNET_KEY=''`이므로 값이 평문으로 들어온다. 그린에 키를 설정한 뒤 `airflow rotate-fernet-key`로 암호화한다(B §7 단계 4). | import할 때 그린의 Fernet 키로 암호화돼 저장된다. 정리가 더 깔끔하다. |
| XCom, 오래된 잡 레코드 | 함께 넘어온다. 덤프 전에 `airflow db clean`을 권장한다(B §5.2). | 넘어오지 않는다. |
| 마이그레이션 위험 | 2.10→3.3 스키마 마이그레이션을 실제로 거친다. 이 과정은 **섀도 리허설에서 미리 검증한다.** | 마이그레이션 위험이 없다. |
| 콜백 복구 판단(A §4.4 대안 A `ti.get_previous_dagrun()`) | 첫 run부터 직전 run과 비교할 수 있다. | 첫 run에서는 직전 run이 없으므로 복구 알림이 1회 생략된다(영향은 경미하다). |

### 2.2 권장 — 이관을 기본으로, clean install은 Plan B로

- 공식 업그레이드 가이드는 **기존 메타DB를 마이그레이션하는 경로만** 설명한다. 백업, `db clean`, `db migrate` 순서다. clean install에서 이력을 옮기는 공식 도구는 없다.
- 이관하면 pause 상태와 마지막 run 정보까지 넘어오므로, **전환 당일의 스케줄 공백이나 중복을 판단할 근거가 DB에 남는다.**
- 이관 리허설(§7 단계 2)이 실패하면, 예를 들어 마이그레이션 오류가 3.3.2 패치로도 풀리지 않으면, **그때 clean install로 바꾼다.** 이 프로젝트는 Variable 약 10개, Connection 1~2개, 사용자 1명 수준이라 Plan B의 이관 비용도 작다.

### 2.3 export/import 명령 (Plan B, 그리고 이관 시 사전 백업용)

> 주의: `FERNET_KEY=''` 환경이므로 export 파일에 **KIS appkey/secret, Upbit 키, Slack webhook이 평문으로** 들어간다. 저장소 밖(예: `~/cutover/`, `chmod 600`)에 두고, import가 끝나면 바로 삭제한다. 저장소 루트에 만들면 `.gitignore`에 걸리지 않는다.

**블루(2.10.4)에서 export** — 블루 디렉터리에서 실행한다.

```bash
mkdir -p ~/cutover && chmod 700 ~/cutover
cd ~/oracle-airflow   # 블루 디렉터리

# 1) Variables — '-'는 STDOUT
docker compose exec -T airflow-scheduler airflow variables export - > ~/cutover/variables.json

# 2) Connections — JSON(기본)
docker compose exec -T airflow-scheduler airflow connections export - --file-format json > ~/cutover/connections.json

# 3) 사용자·역할 (2.10.4는 users/roles CLI가 core에 있음)
docker compose exec airflow-scheduler airflow users export /tmp/users.json
docker compose cp airflow-scheduler:/tmp/users.json ~/cutover/users.json
docker compose exec airflow-scheduler airflow roles export -p /tmp/roles.json
docker compose cp airflow-scheduler:/tmp/roles.json ~/cutover/roles.json

# 4) pause 상태 기록 (전환 후 unpause 대상 목록)
docker compose exec -T airflow-scheduler airflow dags list -o json > ~/cutover/dags_2x.json

chmod 600 ~/cutover/*
```

**그린(3.3.2)에서 import** — 그린 디렉터리에서 실행한다. FAB provider가 설치돼 있어야 `users`와 `roles` 명령이 있다.

```bash
cd ~/oracle-airflow3
CUT="-v $HOME/cutover:/cutover:ro"

docker compose run --rm $CUT airflow-scheduler airflow variables import -a overwrite /cutover/variables.json
docker compose run --rm $CUT airflow-scheduler airflow connections import --overwrite /cutover/connections.json
docker compose run --rm $CUT airflow-scheduler airflow roles import /cutover/roles.json
docker compose run --rm $CUT airflow-scheduler airflow users import /cutover/users.json

# 검증(값은 출력하지 않음)
docker compose run --rm airflow-scheduler airflow variables list
docker compose run --rm airflow-scheduler airflow connections list -o plain
```

| 명령 | 3.3.2 공식 문법(요지) | 비고 |
|---|---|---|
| `variables export` | `airflow variables export [-v] file`, `-`면 STDOUT | |
| `variables import` | `airflow variables import [-a {overwrite,fail,skip}] file`, 기본값 overwrite | `.env/.json/.yaml` 지원 |
| `connections export` | `airflow connections export [--file-format {json,yaml,env}] [--serialization-format {json,uri}] file` | `--format`은 deprecated |
| `connections import` | `airflow connections import [--overwrite] file`, 형식은 확장자로 추론 | 기본값은 충돌 시 덮어쓰지 않음 |
| `users export / import` | `airflow users export FILEPATH` / `airflow users import FILEPATH` (**FAB provider CLI**) | import 예시에 password 필드가 없다. 이 때문에 **비밀번호는 옮겨지지 않는 것으로 판단한다(미검증)**. 관리자 비밀번호는 `_AIRFLOW_WWW_USER_PASSWORD`로 다시 만든다. |
| `roles export / import` | `airflow roles export [-p] file` / `import file`: "roles (without permissions)" | 기본 역할만 쓰면 생략해도 된다. |
| `pools export / import` | `airflow pools export FILEPATH` | `default_pool`만 쓰므로 불필요하다. |

출처: https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html , https://airflow.apache.org/docs/apache-airflow-providers-fab/stable/cli-ref.html

### 2.4 이관(덤프 복원) 명령

```bash
# 블루 메타DB 덤프 (PG13). custom 포맷 권장.
cd ~/oracle-airflow
docker compose exec -T postgres pg_dump -U airflow -d airflow -Fc > ~/cutover/meta_2x_$(date +%F_%H%M).dump

# 그린 PG16에 복원
cd ~/oracle-airflow3
docker compose up -d postgres
docker compose exec -T postgres pg_restore -U airflow -d airflow --no-owner --clean --if-exists < ~/cutover/meta_2x_<ts>.dump
```

- 낮은 버전(13)에서 뜬 덤프를 높은 버전(16)에 복원하는 방식은 PostgreSQL이 지원하는 메이저 업그레이드 경로다. 공식 문서는 가능하면 **새 버전의 pg_dump로 덤프**하라고 권장한다. 예: `docker run --rm --network oracle-airflow_default postgres:16 pg_dump -h postgres -U airflow -Fc airflow`. 출처: https://www.postgresql.org/docs/16/upgrading.html
- B §7 단계 1은 `pg_dumpall`(평문 SQL)을 쓴다. 여기서는 단일 DB `airflow`만 옮기면 되므로 `pg_dump -Fc`와 `pg_restore`로 충분하다.

---

## 3. 같은 서버에 두 스택을 띄울 때의 충돌

### 3.1 충돌 지점과 해결

| 자원 | 블루(현행) | 충돌 내용 | 그린 설정(권장) |
|---|---|---|---|
| **compose 프로젝트명** | 최상위 `name:`이 없으므로 **디렉터리 basename**(예: `oracle-airflow`)이 프로젝트명이다. | 그린을 같은 이름의 디렉터리(다른 부모 경로의 `oracle-airflow` 클론 등)에서 띄우면 **같은 프로젝트로 인식된다.** 그 상태에서 `up`을 하면 블루 컨테이너가 재생성되고, `down -v`를 하면 블루 볼륨이 삭제된다. | compose 최상위에 **`name: oracle-airflow3`를 명시**한다. Docker 문서상 우선순위는 `-p` > `COMPOSE_PROJECT_NAME` > `name:` > 디렉터리명이다. 명시해 두면 나중에 디렉터리를 옮기거나 이름을 바꿔도 프로젝트가 유지된다. |
| **볼륨명** | `<프로젝트>_postgres-db-volume`(예: `oracle-airflow_postgres-db-volume`) | 프로젝트명이 같으면 볼륨도 공유된다. **3.x가 PG13 데이터 디렉터리를 PG16 이미지로 열려다 기동에 실패하거나, 같은 메타DB를 두 스케줄러가 쓰게 된다.** | 볼륨 키를 `postgres-db-volume-16`으로 바꾼다. 실제 이름은 `oracle-airflow3_postgres-db-volume-16`이 된다. 블루 볼륨을 `external: true`로 참조하지 **않는다.** |
| **포트 8080** | `airflow-webserver` 8080:8080 | 호스트 포트가 겹쳐 그린 api-server가 기동하지 못한다. | 섀도 기간에는 `"${AIRFLOW_HOST_PORT:-8080}:8080"`에 `.env`로 `8081`을 준다. 전환 때 8080으로 바꾼다. 클라우드 보안그룹이나 방화벽에서 8081을 열어야 한다면 **관리자 IP로만 허용한다.** |
| **BASE_URL(Slack 링크)** | `AIRFLOW__WEBSERVER__BASE_URL` | 섀도 기간 그린 알림의 링크는 8081을 가리켜야 한다. | `.env`의 `AIRFLOW__API__BASE_URL=http://<IP>:8081`을 전환 때 8080으로 바꾼다(B §2.1, A §1.1). |
| **`./logs`** | 블루 디렉터리의 `./logs` | 공유하면 두 스택의 run_id가 같아(`scheduled__2026-10-13T...`) 같은 경로 `dag_id=/run_id=/task_id=`에 로그가 섞이거나 덮어써진다. 3.x의 `dag_processor` 로그도 섞인다. | **공유하지 않는다.** 그린 디렉터리의 `./logs`를 쓴다. 2.x 로그가 필요하면 블루를 보존하는 기간 동안 블루 디렉터리에서 본다. |
| **`./data/tokens`** | `TokenManager`가 `data/tokens/YYYYMMDD_token.json`을 쓰고 과거 파일을 삭제한다. | ① 분리하면 그린의 수집 DAG가 `GetTokens()`에서 파일이 없음을 보고 **KIS 토큰을 직접 발급한다**(token_manager.py `GetTokens` → `TokenGenerator`). ② 읽기·쓰기로 공유하면 두 `make_token`이 06:50에 동시에 실행돼 둘 다 "파일 없음"으로 판단하고 발급하는 경합이 생긴다. | **섀도 기간에는 블루의 `data/tokens`를 그린에 읽기 전용으로 마운트한다**(§4.3). 전환 후에는 그린 자체의 `./data/tokens`로 바꾼다. |
| **토큰 경로(CWD 의존)** | `TOKEN_DIR` 기본값이 상대경로 `data/tokens`라 태스크 프로세스의 작업 디렉터리에 따라 달라진다. | 3.x Task SDK 태스크 프로세스의 CWD가 2.x와 같은지(`/opt/airflow`)는 확인하지 못했다. | 그린 compose 환경변수에 `TOKEN_DIR: /opt/airflow/data/tokens`를 **절대경로로 명시**한다. 코드 변경이 필요 없다. |
| **이미지 이름/태그** | `build: .`만 있으므로 Compose v2가 `<프로젝트>-<서비스>` 이름으로 서비스마다 이미지를 만든다(예: `oracle-airflow-airflow-scheduler`). | 프로젝트명이 다르면 이름은 겹치지 않는다. 다만 서비스마다 같은 내용의 태그가 생겨 정리할 때 헷갈린다. | `build: .`와 함께 **`image: oracle-airflow:3.3.2-py3.12`**를 지정해 단일 태그로 관리한다. 블루 제거 때 `oracle-airflow-*` 태그만 지우면 된다. |
| **Chromium(Playwright)** | 이미지 안에 설치된다(`~/.cache/ms-playwright`). | 이미지별로 따로 설치되므로 **파일 충돌은 없다.** 문제는 동시 실행이다. 06:55 `fetch_fund_price_daily`가 두 스택에서 동시에 돌면 headless Chromium 2개가 메모리를 쓰고, 같은 사이트를 동시에 크롤링한다. | 섀도 기간에는 그린의 정기 DAG를 paused로 두고 **07:30 이후에 수동 실행**한다(§4.4). 빌드 후 `docker compose run --rm airflow-scheduler playwright --version`과 크롤러 단독 실행으로 확인한다(B §4.4). |
| **네트워크** | `<프로젝트>_default` | 프로젝트별로 분리되므로 충돌하지 않는다. 서비스명 `postgres`도 네트워크별로 해석된다. | 그대로 둔다. |
| **`extra_hosts: host.docker.internal`** | 블루와 같다. | 두 스택 모두 호스트 PG에 붙을 수 있다. 그래서 **쓰기 대상을 분리해야 한다**(§4.1). | 유지한다. |
| **`restart: always`** | 블루 전 서비스 | 블루를 `stop`만 해 두면 Docker 데몬이나 서버를 재시작할 때 다시 뜬다(Docker restart policy: 수동 정지된 컨테이너는 데몬 재시작 시 재기동된다). | 전환 후 블루는 **`down`(컨테이너 삭제, 볼륨 유지)**한다(§7 단계 5). |

출처: https://docs.docker.com/compose/how-tos/project-name/ , https://docs.docker.com/reference/compose-file/volumes/ , https://docs.docker.com/engine/containers/start-containers-automatically/

### 3.2 자원 사전 점검

```bash
free -h            # 섀도 기간 동안 2.x(3) + 3.x(5) 컨테이너가 동시에 상주
df -h /var/lib/docker
docker system df   # 이미지(각각 Chromium 포함), 빌드 캐시
docker stats --no-stream
```

- 판단 기준(경험치): 3.x 스택이 유휴 상태에서 쓰는 메모리를 리허설 기동 후 `docker stats`로 측정한다. 여기에 블루 사용량과 Chromium 실행 피크를 더한 값이 가용 메모리의 70%를 넘으면 블루/그린을 포기하고 in-place(B §7)로 간다. 또는 섀도 기간에 그린의 triggerer를 생략해 부담을 줄인다(B §1.1에 따르면 현재 deferrable을 쓰지 않으므로 선택 사항이다).

### 3.3 그린 compose 차이점(초안, B §7 단계 5에 덧붙이는 부분)

B §7 단계 5의 3.x compose를 기반으로 아래 항목만 추가하거나 변경한다. 저장소에는 쓰지 않았다.

```yaml
name: oracle-airflow3                     # ★ 프로젝트명 고정

x-airflow-common:
  &airflow-common
  build: .
  image: oracle-airflow:3.3.2-py3.12      # ★ 단일 태그
  env_file:
    - .env
  environment:
    &airflow-common-env
    # ... B §7 단계 5와 동일 ...
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
    TOKEN_DIR: /opt/airflow/data/tokens   # ★ CWD 의존 제거
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs            # ★ 블루와 분리
    - ./plugins:/opt/airflow/plugins
    - ./config:/opt/airflow/config
    - ./data:/opt/airflow/data
    # ★ 섀도: 블루 토큰 디렉터리를 RO로 덮어 마운트 / 전환 후: 자체 디렉터리 RW
    - ${TOKEN_HOST_DIR:-./data/tokens}:/opt/airflow/data/tokens${TOKEN_MOUNT_MODE:-}

services:
  airflow-apiserver:
    <<: *airflow-common
    command: api-server
    ports:
      - "${AIRFLOW_HOST_PORT:-8080}:8080" # ★ 섀도 8081 → 전환 8080
    # ... 이하 B §7 단계 5와 동일 ...

volumes:
  postgres-db-volume-16:                  # ★ 실제 이름 oracle-airflow3_postgres-db-volume-16
```

---

## 4. 이중 실행 위험과 격리 설계

### 4.1 위험 목록

| 위험 | 발생 조건 | 영향 | 대책 |
|---|---|---|---|
| 두 스케줄러가 같은 자산 DB에 기록 | 그린의 `postgres_asset`이 운영 자산 DB를 가리키고 DAG가 unpaused인 경우 | upload가 "DELETE 범위 후 INSERT"라 최종 결과는 멱등에 가깝다(C §5.1). 하지만 **두 run이 겹쳐 실행되면** 한쪽 DELETE와 다른 쪽 INSERT가 엇갈려 행이 중복되거나 누락될 수 있다. 실행 시각이 달라(06:55 vs 15:00) 평가금액이 다른 스냅샷이 **나중에 실행된 쪽으로 덮어써진다.** | 섀도 기간에는 그린이 **스테이징 DB에만 쓰게 한다**(§4.5). |
| KIS 토큰 중복 발급 | 그린 `make_token`이 실행되거나, 그린 수집 DAG의 `GetTokens()`가 파일을 찾지 못한 경우 | KIS 토큰 발급은 **1분당 1회 제한**이 있고, 6시간 이내에 재신청하면 기존 토큰이 반환되며 발급 때마다 알림이 온다(**제3자 자료이며 공식 포털 원문은 확인하지 못함**). 동시에 발급하면 한쪽이 차단되거나 블루의 06:50 발급이 실패할 수 있다. 재발급이 기존 토큰을 무효화하는지는 C §6-4에서 미확인으로 남아 있다. | ① 블루 토큰 디렉터리를 RO로 마운트한다. ② 그린의 `make_token`을 포함한 모든 DAG를 paused로 둔다. ③ 섀도 실행은 블루 파일이 생긴 뒤(07:30 이후)에만 한다. |
| 그린 DAG가 운영 pause 상태로 기동 | 메타DB를 이관한 경우. 블루에서 unpaused였던 DAG가 그대로 unpaused | 그린 기동 즉시 다음 cron부터 운영 DB와 KIS를 상대로 실행된다. | `airflow dags pause`를 **스케줄러를 기동하기 전에** 실행한다(§4.2). |
| Slack 알림 혼선 | 그린이 운영 webhook을 그대로 쓰는 경우 | 섀도 실패 알림이 운영 채널에 섞여 진짜 장애와 구분할 수 없다. | `AIRFLOW_VAR_SLACK_WEBHOOK_URL`로 테스트 채널 webhook을 쓴다(§4.5). |
| 블루 부활 | 전환 후 블루를 `stop`만 하고 서버가 재부팅된 경우 | 두 스케줄러가 06:50에 함께 동작한다. | 전환 시 블루 DAG를 일괄 pause하고, 이어서 `docker compose down`을 한다(§7 단계 5). |
| 크롤링 동시 접속 | 두 `fetch_fund_price_daily`가 같은 시각에 실행된 경우 | 대상 사이트 차단, 메모리 피크 | 그린은 수동으로 시간을 띄워 실행한다(§4.4). |

### 4.2 신규(그린) 쪽 pause 기본값

- `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION='true'`는 **DAG가 메타DB에 처음 등록될 때만** 적용된다. clean install에서는 7개 DAG가 모두 paused로 생기므로 추가 조치가 필요 없다.
- **이관 DB에서는 이 설정이 소용없다.** `dag.is_paused` 값이 블루에서 복사되기 때문이다. 3.0에서 `[scheduler] use_job_schedule`이 삭제돼(B §2.2) 설정으로 스케줄 전체를 끌 방법도 없다. 따라서 **init(migrate) 다음, scheduler를 기동하기 전에** 아래를 실행한다.

```bash
cd ~/oracle-airflow3
docker compose up airflow-init                       # migrate만 수행 후 종료
docker compose run --rm airflow-scheduler \
  airflow dags pause --treat-dag-id-as-regex -y '.*' # 등록된 모든 DAG pause
docker compose run --rm airflow-scheduler airflow dags list --columns dag_id,is_paused
docker compose up -d                                  # 이후 기동
```

- 3.3.2의 `dags pause`/`unpause`는 `--treat-dag-id-as-regex`와 `-y`를 지원한다(CLI ref).

### 4.3 make_token "비활성화"의 실제 의미

| 방법 | 결과 | 평가 |
|---|---|---|
| 그린 `make_token`만 pause | 그린 `fetch_exchange_rate`의 `ExternalTaskSensor`가 상류 run을 찾지 못해 600초 뒤 timeout된다. 센서를 우회하면 `GetTokens()`가 파일이 없을 때 **직접 발급한다.** | **불충분하다.** |
| `make_token` pause + 그린 `data/tokens`를 별도 빈 디렉터리로 | 수집 DAG를 실행하는 순간 KIS 발급이 일어난다. | **부적절하다.** |
| **블루 `data/tokens`를 RO로 마운트 + 07:30 이후 수동 실행(권장)** | 블루가 06:50에 만든 당일 파일이 있으므로 그린 `make_token`은 `TokenGenerator()`의 "파일이 있으면 즉시 return" 분기로 끝난다. 수집 DAG도 파일만 읽는다. RO라서 그린이 블루 파일을 지우거나 고칠 수 없다. | **적절하다.** 코드 변경이 필요 없다. |
| 코드에 섀도 플래그 추가(예: `TOKEN_ISSUE_ENABLED=false`면 발급 대신 파일 대기) | 정기 스케줄 그대로 섀도를 돌릴 수 있다. | 선택 사항이다. 3.x 브랜치에만 넣는 코드 변경이며, 전환 후 플래그를 제거하거나 기본값을 true로 둔다. |

RO 마운트에서 주의할 점이 있다. 블루 파일에 `REQUIRED_TOKEN_KEYS` 중 일부가 빠져 있으면 `GetTokens()`가 `os.remove`와 재발급을 시도한다. RO 마운트에서는 remove가 실패하므로 **KIS 호출 전에 예외로 끝나며, 안전한 방향으로 실패한다.** 반대로 블루 파일이 아직 없으면(06:50 이전이거나 블루 make_token이 실패한 경우) `TokenGenerator()`가 KIS 발급 API를 **먼저 호출한 뒤** 파일 쓰기에서 실패한다. 그래서 수동 실행 전에 당일 파일이 있는지 반드시 확인한다(§4.4 체크).

### 4.4 섀도 실행 방식

정기 DAG는 그린에서 계속 paused로 둔다. 매일 블루의 07:00 사이클이 성공한 뒤 수동으로 체인을 실행한다.

```bash
# 0) 당일 토큰 파일 존재 확인 (그린 컨테이너 시점)
docker compose run --rm airflow-scheduler ls -l /opt/airflow/data/tokens/$(TZ=Asia/Seoul date +%Y%m%d)_token.json
```

1. UI 트리거 폼에서 **logical date를 당일 정기 시각(KST)으로 지정**해 순서대로 실행한다. `make_token`(06:50) → `fetch_exchange_rate`(06:55), `fetch_fund_price_daily`(06:55) → `fetch_asset_daily`(07:00). logical date를 지정하면 `ExternalTaskSensor`의 `execution_delta=5분` 매칭이 정기 run과 같아진다(A §5). CLI로 할 때는 3.3.2의 `airflow dags trigger --help`로 logical date 옵션 이름을 확인한다.
2. logical date를 **지정하지 않은** 수동 run도 별도로 1회 돌려 본다. A §4.1의 P1(`data_interval_end`가 None이 되는 문제)을 고쳤는지 검증하기 위해서다.
3. Param 기반 수동 DAG(`upsert_manual_position`, `rebuild_derived_assets`)는 스테이징 DB를 대상으로 새 UI 폼에서 실행한다(C §3.3).

### 4.5 스테이징 Connection·Variable 분리 — 환경변수 오버라이드

Airflow는 기본적으로 Connection과 Variable을 **환경변수에서 먼저 찾고, 그다음 메타DB에서 찾는다**(Secrets backend 문서의 기본 탐색 순서). 이 순서를 이용하면 메타DB(이관본)는 운영 값을 그대로 둔 채, `.env` 두 줄로 섀도 기간의 쓰기와 알림 대상을 바꿀 수 있다. 전환할 때는 **두 줄을 지우기만 하면** 운영 값이 쓰인다.

```dotenv
# ~/oracle-airflow3/.env — 섀도 기간 전용 (전환 시 ★ 줄 삭제)
AIRFLOW_HOST_PORT=8081
AIRFLOW__API__BASE_URL=http://<고정IP>:8081
TOKEN_HOST_DIR=../oracle-airflow/data/tokens
TOKEN_MOUNT_MODE=:ro
★AIRFLOW_CONN_POSTGRES_ASSET=postgresql://<user>:<pw>@host.docker.internal:5432/asset_staging
★AIRFLOW_VAR_SLACK_WEBHOOK_URL=https://hooks.slack.com/services/<테스트채널>
```

- 스테이징 DB 준비: 호스트 PG에 `asset_staging` DB를 만들고, 운영 자산 DB를 통째로 복사(`pg_dump asset | psql asset_staging`)해 수동 원장, 기준 테이블, 과거 스냅샷을 맞춘다. 개인 자산 DB라 규모가 작다고 보고 전체 복사를 권장한다.
- 적용 확인: `docker compose run --rm airflow-scheduler airflow connections get postgres_asset -o json`에서 host와 schema가 `asset_staging`인지 확인한다. 이 명령이 환경변수 Connection을 보여 주지 않는 버전이라면, `test_slack_alert`처럼 가벼운 DAG를 하나 실행하거나 `rebuild_derived_assets`의 로그로 접속 DB를 확인한다.
- 환경변수로 정의한 Connection과 Variable은 UI 목록에 나타나지 않는다. 섀도 기간에 UI에서 `postgres_asset`을 수정해도 **효과가 없다**는 점을 기억해 둔다.
- 출처: https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/secrets-backend/index.html , https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html , https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html

---

## 5. 섀도 검증 항목 (C Phase 2 검증 목록 + 병행 고유 항목)

| # | 체크포인트 | 통과 기준 |
|---|---|---|
| S1 | 기동 | `docker compose ps`에서 5개 서비스 healthy, `curl :8081/api/v2/monitor/health` OK, `airflow dags list-import-errors`가 0건 |
| S2 | 격리 | 그린 DAG가 전부 `is_paused=True`이고, `postgres_asset`이 `asset_staging`을 가리키며, Slack 테스트 채널로 수신되고, `data/tokens`가 RO(그린 컨테이너에서 `touch` 실패) |
| S3 | 블루 무영향 | 섀도 기간 동안 블루의 06:50~07:10 체인이 매일 성공하고 KIS 발급 실패나 알림 이상이 없다. **블루가 한 번이라도 영향을 받으면 즉시 그린을 중지한다.** |
| S4 | 데이터 동등성 | 같은 `standard_date`에 대해 운영과 스테이징의 `account.asset_daily`를 비교해 계좌 수가 같고, 평가금액 차이가 실행 시각 차이로 설명되는 범위에 있다. 환율과 펀드 NAV는 일치해야 한다. |
| S5 | 날짜 의미 | 3개 수집 DAG의 `standard_date`가 T-1이고, logical date를 지정하지 않은 수동 run에서도 AttributeError가 없다(A P1 수정 확인). |
| S6 | 알림 | `test_slack_alert`로 실패 알림과 그다음 성공 시 복구 알림이 모두 오고, 링크가 `/dags/{id}/runs/{run_id}` 형식으로 열린다(A P0, B §6.3). |
| S7 | 운영 리허설 | **단계 2의 이관 리허설을 마지막 날 한 번 더** 수행해 소요 시간을 잰다. 이 시간이 전환 당일의 다운타임 추정치가 된다. |

관찰 기간은 C와 같은 **5~7일**이다. 월 1회 실행하는 `upsert_manual_position`은 스테이징에서 수동으로 1회 실행하는 것으로 대신한다.

---

## 6. 디렉터리 구조와 git 전략

### 6.1 대안 비교

| 대안 | 장점 | 단점 | 평가 |
|---|---|---|---|
| A. 같은 디렉터리에서 브랜치 전환 | 디렉터리 하나 | 브랜치를 바꾸는 순간 **블루가 마운트한 `./dags`, `./plugins`가 3.x 코드로 바뀐다.** 2.x 스케줄러가 `airflow.sdk` import에 실패한다. 병행 운영 자체가 불가능하다. | **부적절** |
| B. 같은 디렉터리에 `docker-compose.airflow3.yaml` 추가 | 파일이 한 곳에 있다. | `./dags`와 `./plugins`를 공유하므로 A와 같은 문제가 생긴다. DAG 코드는 2.x와 3.x 양쪽에서 돌아야 하는데, A §10에 따르면 `airflow.sdk` import는 2.x에서 불가능하다. `./logs`와 `./data`도 공유하게 된다. | **부적절** |
| C. `git worktree add ../oracle-airflow3 feat/airflow3` | 저장소를 하나로 유지하고 커밋이 즉시 공유된다. | 전환 후 그린 디렉터리에서 `main`을 체크아웃하려면 원래 작업 트리의 `main` 체크아웃을 먼저 해제해야 한다. 블루 디렉터리를 삭제할 때 worktree 관계(`git worktree prune`)도 정리해야 한다. | 조건부(개발 PC에서는 편리하다) |
| **D. 서버에 별도 clone 디렉터리(`~/oracle-airflow3`, 브랜치 `feat/airflow3`)** | 블루와 **완전히 독립**된다. 전환 후 그대로 `main`을 추적하면 된다. 블루 디렉터리는 통째로 지우기만 하면 된다. | 디스크를 조금 더 쓴다(코드 크기는 작다). | **권장** |

### 6.2 권장 git 흐름

1. **블루 시점 고정**: 전환 전에 `main`에 `git tag v2.10.4-final`을 만들고 push한다. 블루 디렉터리는 이 태그 상태로 둔다(롤백 기준점).
2. **2.x 호환 사전 정리는 `main`에서**: C Phase 0와 A §10-1(standard provider 경로, `execution_date`를 `logical_date`로, `data_interval_end` None 방어, AIR304)은 2.10.4에서도 동작하므로 `main`에 머지하고 **블루에 먼저 배포**한다. 이렇게 하면 두 브랜치의 차이가 "3.x 전용 변경"만으로 줄어든다.
3. **3.x 전용 변경은 `feat/airflow3`에서**: `airflow.sdk` import, Dockerfile, compose(`name:`, 포트, 볼륨), callbacks의 `get_previous_dagrun`, requirements 고정을 담는다. 섀도 기간에 버그를 고치면 이 브랜치에 커밋하고 그린 디렉터리에서 `git pull`한다. `.env`는 커밋하지 않는다(기존 `.gitignore`).
4. **섀도 기간에 `main`에 들어간 DAG 수정**(업무 변경)은 `feat/airflow3`로 merge하거나 rebase해서 따라간다. 섀도 기간에는 **업무 기능 변경을 동결**하는 것을 권장한다.
5. **전환 시점**: `feat/airflow3`를 `main`에 머지하는 PR을 만들고 `v3.3.2-cutover` 태그를 단다. 그린 디렉터리에서 `git checkout main && git pull`을 한다. 이후 운영 디렉터리는 `~/oracle-airflow3`다.
6. **제거 이후**: 블루 디렉터리를 삭제한다. 디렉터리명을 `oracle-airflow`로 되돌리고 싶다면 compose에 `name:`을 고정해 두었으므로 **이름을 바꿔도 프로젝트와 볼륨이 유지된다.** 단, 이름을 바꾸기 전에 블루 디렉터리를 먼저 삭제해 같은 이름이 둘이 되지 않게 한다. README의 인프라 절(버전, PG, 포트, 명령)도 갱신한다.

서버 디렉터리 예시:

```
~/oracle-airflow/        # 블루: tag v2.10.4-final, project=oracle-airflow (디렉터리명)
  ├─ data/tokens/        #  ← 그린이 섀도 기간에 RO 마운트
  └─ logs/
~/oracle-airflow3/       # 그린: branch feat/airflow3 → 전환 후 main, project=oracle-airflow3 (name: 고정)
  ├─ .env                # 섀도용 오버라이드 포함 (§4.5)
  ├─ data/tokens/        # 전환 후 사용
  └─ logs/
~/cutover/               # export·덤프 산출물 (chmod 700, 저장소 밖)
```

---

## 7. 단계별 컷오버 절차와 체크포인트

| 단계 | 시점 | 작업 | 체크포인트(통과해야 다음 단계) |
|---|---|---|---|
| **0. 사전 정리** | D-14~ | C Phase 0(2.x 호환 코드 수정)을 `main`에 머지하고 블루에 배포한다. `v2.10.4-final` 태그를 단다. §3.2 자원을 점검한다. 호스트 PG에 `asset_staging`을 만든다. | 블루의 정기 사이클이 3일 연속 성공하고, 메모리 여유가 기준을 충족한다. |
| **1. 그린 구축** | D-10 | 별도 clone(`feat/airflow3`)을 만들고 Dockerfile(B §7 단계 3), compose(§3.3), `.env`(§4.5)를 작성한다. `docker compose build`를 실행한다. | 이미지가 빌드되고, Chromium이 동작하며, `airflow config lint`에 경고가 없다(3.3.2 이상, B §2.4). |
| **2. 이관 리허설** | D-10 | 블루에서 `db clean`(선택, B §5.2)을 하고 `pg_dump -Fc`로 덤프한다. 그린 PG16에 복원하고 `airflow-init`(migrate)을 실행한다. 이어서 **`dags pause '.*'`**(§4.2)를 하고 `up -d`, `rotate-fernet-key`를 실행한다. | migrate가 오류 없이 끝나고, DAG가 전부 paused이며, S1과 S2를 통과한다. **migrate가 실패하면 clean install(Plan B, §2.3)로 바꾼다.** |
| **3. 섀도 운영** | D-9 ~ D-2 | 매일 07:30 이후 §4.4 방식으로 수동 체인을 실행한다. S3~S6을 기록하고, 버그는 `feat/airflow3`에서 고친다. | S3~S6 5~7일 연속 통과. 마지막 날 S7(리허설 재수행과 시간 측정). |
| **4. 전환** | D-day 14:00 전후(07시 사이클 성공 확인 후, 다음 06:50까지 충분한 여유) | 아래 4-a~4-h | 4-h 통과 |
| **5. 블루 정지** | D-day 전환 직후 | 블루 DAG를 일괄 pause한 뒤 `docker compose down`(**`-v` 금지**)을 한다. | `docker ps`에 블루 컨테이너가 없다. 블루 볼륨은 `docker volume ls`에 남아 있다. |
| **6. 안정화** | D+1 ~ D+14 | D+1 06:50~07:10을 직접 관찰한다. 첫 3일은 C Phase 3처럼 데이터를 대조한다. | D+1~D+3 체인 성공, `asset_daily` 계좌 수와 합계가 정상, Slack 알림이 정상. |
| **7. 블루 제거** | D+30 | §9 | — |

**단계 4 상세(전환 당일)**

```bash
# 4-a. 블루: 현재 pause 상태 기록 후 일괄 pause, 스케줄러/웹서버 정지 (postgres는 덤프 위해 유지)
cd ~/oracle-airflow
docker compose exec -T airflow-scheduler airflow dags list -o json > ~/cutover/dags_2x_final.json
docker compose exec airflow-scheduler airflow dags pause -y \
  make_token fetch_exchange_rate fetch_fund_price_daily fetch_asset_daily   # 2.10.4는 정규식 옵션 유무 확인 후 개별 지정
docker compose stop airflow-scheduler airflow-webserver

# 4-b. (선택) 블루 기준 export — Plan B 대비 백업
#      §2.3 블루 export 명령 (scheduler 정지 상태이므로 'docker compose run --rm airflow-scheduler ...'로 실행)

# 4-c. 최종 덤프
docker compose exec -T postgres pg_dump -U airflow -d airflow -Fc > ~/cutover/meta_2x_final.dump

# 4-d. 그린: 리허설 DB 폐기 후 최종 덤프로 재이관
cd ~/oracle-airflow3
docker compose down                                  # 컨테이너만 (볼륨 유지)
docker volume rm oracle-airflow3_postgres-db-volume-16   # ★ 이름 재확인: 반드시 'oracle-airflow3_' 접두사
docker compose up -d postgres
docker compose exec -T postgres pg_restore -U airflow -d airflow --no-owner < ~/cutover/meta_2x_final.dump
docker compose up airflow-init                       # db migrate
docker compose run --rm airflow-scheduler airflow dags pause --treat-dag-id-as-regex -y '.*'

# 4-e. .env 전환: ★ 오버라이드 2줄 삭제, 포트·BASE_URL 8080, 토큰 디렉터리 자체 RW
#   AIRFLOW_HOST_PORT=8080 / AIRFLOW__API__BASE_URL=http://<IP>:8080
#   TOKEN_HOST_DIR, TOKEN_MOUNT_MODE 삭제(기본값 ./data/tokens RW)
mkdir -p data/tokens && cp ../oracle-airflow/data/tokens/$(TZ=Asia/Seoul date +%Y%m%d)_token.json data/tokens/
# 블루 컨테이너가 8080을 놓았는지 확인 (4-a에서 webserver stop)
docker compose up -d
docker compose run --rm airflow-scheduler airflow rotate-fernet-key   # FERNET_KEY 설정 시

# 4-f. 운영 대상 확인
docker compose run --rm airflow-scheduler airflow connections get postgres_asset -o json   # 운영 DB인지
curl -s http://localhost:8080/api/v2/monitor/health

# 4-g. 블루에서 unpaused였던 DAG만 unpause (dags_2x_final.json의 is_paused=false 목록)
docker compose run --rm airflow-scheduler airflow dags unpause -y make_token
docker compose run --rm airflow-scheduler airflow dags unpause -y fetch_exchange_rate
docker compose run --rm airflow-scheduler airflow dags unpause -y fetch_fund_price_daily
docker compose run --rm airflow-scheduler airflow dags unpause -y fetch_asset_daily

# 4-h. 체크: 오늘 날짜 run이 새로 생기지 않았는지(이관 DB이므로 마지막 run=오늘 07:00),
#      다음 run 예정 시각이 내일 06:50/06:55/07:00 인지 UI에서 확인
#      test_slack_alert 수동 실행 → 운영 채널 수신 + 8080 링크
```

- 4-h에서 오늘 날짜로 run이 새로 생겼다면, 즉시 해당 DAG를 pause하고 run을 취소한다. 그 run이 `asset_daily`에 썼다면 오늘 07:00 결과로 복구하기 위해 블루 시점 값을 기준으로 재적재하거나 `rebuild_derived_assets`를 실행해야 한다. 이 경우는 주로 `create_cron_data_intervals=True`로 설정했을 때 생긴다(§2.1).
- 4-e에서 토큰 파일을 복사하는 이유: 복사하지 않으면 그날 남은 시간 동안 수동 실행할 때 `GetTokens()`가 새로 발급한다. 같은 날이라 KIS가 기존 토큰을 돌려줄 가능성이 크지만(6시간 규칙, 미확인) 불필요한 호출을 피한다.
- 4-a의 블루 일괄 pause 명령: 2.10.4 CLI에 `--treat-dag-id-as-regex`가 있는지는 확인하지 못했으므로 DAG를 하나씩 지정했다. pause 상태는 덤프에도 들어가지만, 4-d에서 그린 쪽을 다시 일괄 pause하고 4-g에서 기록된 목록대로 unpause하므로 결과는 같다.

---

## 8. 롤백

| 시점 | 증상 예 | 롤백 절차 | 데이터 처리 |
|---|---|---|---|
| 단계 2~3(섀도) | migrate 실패, 섀도 검증 실패 | 그린만 `docker compose down`한다. **블루는 원래부터 운영 중이라 할 일이 없다.** | 스테이징 DB만 영향을 받는다. |
| 단계 4 도중 | 4-d~4-f에서 실패 | 그린 `down` → 블루 디렉터리에서 `docker compose start airflow-webserver airflow-scheduler` → 4-a에서 pause한 DAG를 `unpause` | 블루 메타DB는 4-a 이후 바뀐 것이 pause 상태뿐이다. |
| D+1 ~ 블루 down 이후 | 06:50 체인 실패, 알림 누락 | ① 그린 `docker compose down` ② 블루 디렉터리(`v2.10.4-final`)에서 `docker compose up -d`(볼륨이 남아 있으므로 그대로 기동) ③ 블루 DAG unpause ④ 그날 데이터를 위해 `fetch_*`를 수동 재실행하거나 `rebuild_derived_assets`로 복구 | upload가 계좌·날짜 단위로 DELETE 후 INSERT하므로 재실행하면 복구된다(C §5.2). **그린이 만든 run 이력은 블루에 없다**(블루 DB가 전환 시점에 멈춰 있기 때문). 필요하면 그린 DB를 덤프해 보관한다. |
| D+30 이후 | — | 블루 볼륨이 삭제됐으므로 롤백하려면 `~/cutover/meta_2x_final.dump`를 PG13에 복원하고 `v2.10.4-final`로 기동해야 한다. 덤프 보존 기간(90일) 안에서만 가능하다. | 위와 같다. |

**롤백 판단 기준**(B §5.2와 같음): 전환 다음 날 첫 정기 사이클(06:50~07:10)이 한 번이라도 실패하고, 원인을 07:30까지 고칠 수 없으면 롤백한다.

---

## 9. 기존(블루) 스택 제거 절차와 보존 기간

### 9.1 보존 기간

| 대상 | 보존 | 근거 |
|---|---|---|
| 블루 컨테이너 | **D-day에 삭제**(`down`) | `restart: always` 때문에 부활할 위험이 있다(§3.1). 이미지와 볼륨이 있으면 수 분 안에 다시 만들 수 있다. |
| 블루 메타DB 볼륨(`oracle-airflow_postgres-db-volume`, PG13) | **D+30** | 2주 안정화 + 월말·월초 수동 DAG(`upsert_manual_position`) 사이클을 1회 포함하는 기간이다. |
| 블루 이미지(`oracle-airflow-airflow-*`, 2.10.4 + Chromium) | **D+30** | 롤백할 때 재빌드하지 않아도 된다. 재빌드하면 `requirements.txt`가 고정되지 않아(B §4.3) 다른 패키지 버전으로 빌드될 수 있다. |
| 블루 디렉터리(`./logs` 포함) | **D+30** 이후 로그만 압축 보관 | 2.x 시절 태스크 로그를 조회하기 위해서다. |
| 최종 덤프 `meta_2x_final.dump`와 볼륨 tar | **D+90**, 서버 밖(별도 스토리지)에 보관 | 볼륨 삭제 후 롤백이 가능한 마지막 수단이다. 평문 시크릿이 들어 있으므로 암호화해서 보관한다. |
| `~/cutover/*.json`(variables, connections, users export) | **import 검증 직후 삭제**. 늦어도 D+1. | 평문 KIS·Upbit 키와 webhook이 들어 있다. |

### 9.2 제거 절차 (D+30)

```bash
# 0) 그린이 유일한 운영 스택인지 재확인
docker ps --format '{{.Names}}' | sort
docker volume ls | grep -E 'oracle-airflow(3)?_'

# 1) 블루 볼륨 최종 아카이브 (서버 밖으로 이동)
docker run --rm -v oracle-airflow_postgres-db-volume:/v -v "$HOME/cutover":/b alpine \
  tar czf /b/pgdata13_final_$(date +%F).tgz -C /v .
# → 암호화 후 외부 스토리지로 복사, 90일 보존

# 2) 블루 로그 압축
tar czf ~/cutover/logs_2x_$(date +%F).tgz -C ~/oracle-airflow logs

# 3) 블루 compose 리소스 제거 — 블루 디렉터리에서, 프로젝트명 확인 후
cd ~/oracle-airflow
docker compose config | head -1          # name: oracle-airflow 인지 확인 (oracle-airflow3이면 중단!)
docker compose down --rmi local -v       # 블루 컨테이너·로컬 빌드 이미지·블루 볼륨 삭제
#   ※ '-v'는 이 프로젝트의 볼륨만 지운다. 반드시 블루 디렉터리에서, config 확인 후 실행.

# 4) 남은 이미지·빌드 캐시 정리
docker image ls | grep -E 'oracle-airflow-|apache/airflow:2\.10'
docker image rm apache/airflow:2.10.4     # 베이스 이미지 (다른 용도 없으면)
docker builder prune                      # 대화형 확인

# 5) 디렉터리·git 정리
rm -rf ~/oracle-airflow                    # (선택) 이후 그린 디렉터리 이름 변경 가능 — name: 고정 덕분
# 원격: feat/airflow3 브랜치 삭제, 태그 v2.10.4-final은 유지
```

- **가장 위험한 명령은 `down -v`다.** 프로젝트명이 디렉터리명으로 결정되는 블루에서는 **잘못된 디렉터리에서 실행하면 다른 스택의 볼륨이 지워진다.** 그래서 3)의 `docker compose config`로 `name`을 확인하는 단계를 생략하지 않는다. 더 보수적으로 하려면 `down --rmi local`(볼륨 유지) 후 `docker volume rm oracle-airflow_postgres-db-volume`을 이름을 직접 적어 따로 실행한다.
- 3.x 운영 쪽 후속 작업: `docker system df`로 회수한 용량을 확인하고, README의 인프라 절(Airflow 3.3.2, PG16, 서비스 5개)과 `plugins/CALLBACKS_GUIDE.md`(B §6.3)를 갱신한다.

---

## 10. 결론 — 판정과 추천 절차

### 10.1 판정표

| 방안 | 판정 | 이유 |
|---|---|---|
| **블루/그린 병행 구축 + 섀도 격리 + 메타DB 이관(덤프 → PG16 → `db migrate`)** | **적절 (추천)** | 롤백이 블루 재기동만으로 끝난다. PG 선행 업그레이드가 필요 없다. 이력, Variables, Connections, 사용자, pause 상태가 보존된다. 검증한 스택이 그대로 운영이 된다. |
| 블루/그린 + clean install + `variables`/`connections`/`users` export·import | **조건부 적절** | 2.x 실행 이력을 포기해도 되고, 이관 리허설이 실패했을 때 쓴다. 비밀번호는 재설정해야 하고, export 파일의 평문 시크릿을 관리해야 하며, unpause 시점(07시 이후, `create_cron_data_intervals` 기본값 유지)을 지켜야 한다. |
| in-place 업그레이드(B §7, C Phase 1~3 원안) | **조건부 적절** | 서버 메모리나 디스크가 두 스택을 감당하지 못할 때 쓴다. 이때도 C Phase 2의 별도 스테이징 검증은 필요하며, 롤백은 덤프 복원으로 한다. |
| 사용자 원안을 격리 없이 실행(그린을 운영 Connection, webhook, 토큰 디렉터리 그대로 unpause해서 병행) | **부적절** | 두 스케줄러가 같은 자산 DB에 서로 다른 시각의 스냅샷을 쓰고, KIS 토큰을 동시에 발급해 1분 제한에 걸리며, 운영 Slack 채널이 혼선된다. |
| 같은 디렉터리에서 브랜치 전환이나 compose 파일 추가로 병행 | **부적절** | `./dags`와 `./plugins`를 공유하므로 2.x와 3.x 코드를 동시에 만족시킬 수 없다(`airflow.sdk`). `./logs`와 `./data`도 공유된다. |
| 전환 후 블루를 `stop`만 하고 방치 | **부적절** | `restart: always` 때문에 재부팅하면 2.x 스케줄러가 부활해 이중 실행된다. |

### 10.2 추천 절차 (요약)

| 순서 | 단계 | 핵심 작업 | 체크포인트 | 롤백 |
|---|---|---|---|---|
| 1 | 사전 정리 (D-14) | 2.x 호환 코드 수정을 `main`에서 블루에 배포, `v2.10.4-final` 태그, 자원 점검, `asset_staging` 생성 | 블루 3일 연속 성공, 메모리 여유 | 해당 없음 |
| 2 | 그린 구축 (D-10) | 별도 clone `~/oracle-airflow3`(`feat/airflow3`), `name: oracle-airflow3`, 포트 8081, PG16 새 볼륨, 이미지 태그 고정, `TOKEN_DIR` 절대경로 | 빌드 성공, Chromium 동작, `config lint` 깨끗 | 그린 삭제 |
| 3 | 이관 리허설 (D-10) | 블루 `pg_dump -Fc` → 그린 `pg_restore` → `db migrate` → **일괄 pause** → 기동 | migrate 성공, 전 DAG paused, health OK | 실패하면 clean install(Plan B) |
| 4 | 섀도 (D-9~D-2) | 환경변수로 Connection과 Slack 오버라이드, 블루 토큰 RO 마운트, 07:30 이후 logical date를 지정해 수동 체인 실행 | S1~S7, 블루 무영향 | 그린 `down` |
| 5 | 전환 (D-day 오후) | 블루 pause와 정지 → 최종 덤프 → 그린 재이관 → `.env` 오버라이드 제거, 8080 전환 → 기록된 목록대로 unpause | 오늘자 run 미생성, 내일 예정 run 확인, Slack 운영 채널 수신 | 블루 `start`와 unpause |
| 6 | 블루 정지 (D-day) | 블루 `docker compose down`(볼륨 유지), `feat/airflow3`를 `main`에 머지 | 블루 컨테이너 0개, 볼륨 존재 | 블루 `up -d` |
| 7 | 안정화 (D+1~D+14) | D+1 06:50~07:10 관찰, 3일간 데이터 대조 | 체인 성공, `asset_daily` 정상 | 블루 `up -d`, 해당 일자 재적재 |
| 8 | 블루 제거 (D+30) | 볼륨 아카이브 → `down --rmi local -v`(프로젝트명 확인 후) → 이미지와 캐시 정리, 덤프는 외부에 90일 보관 | 회수 용량 확인, README 갱신 | 덤프 복원(90일 이내) |

---

## 11. 확인이 더 필요한 사항

1. **KIS 토큰 정책**(1분 1회, 6시간 이내 재신청 시 기존 토큰 반환, 재발급 시 기존 토큰 무효화 여부)은 제3자 블로그(공식 예제 `kis_auth.py` 인용)로만 확인했다. KIS Developers 공식 공지로 다시 확인해야 한다. C §6-4와 같은 미확인 항목이다.
2. FAB `users export`가 비밀번호 해시를 포함하는지는 공식 CLI 문서에 명시돼 있지 않다. import 예시에 password 필드가 없어 "포함하지 않는다"로 판단했다.
3. 2.10.4 CLI의 `dags pause`가 `--treat-dag-id-as-regex`를 지원하는지는 확인하지 못했다. 3.3.2에서는 지원한다. 그래서 블루 쪽은 개별 지정으로 적었다.
4. `airflow connections get`이 환경변수로 정의한 Connection을 보여 주는지는 버전마다 다를 수 있다. 섀도 기동 직후 실제 접속 DB를 로그로 교차 확인한다.
5. 3.3.2 Task SDK 태스크 프로세스의 작업 디렉터리. `TOKEN_DIR`을 절대경로로 명시하면 이 문제는 회피된다.
6. 서버의 실제 블루 디렉터리명(= compose 프로젝트명, 볼륨 접두사). 이 문서는 로컬 저장소 이름인 `oracle-airflow`를 가정했다. 서버에서 `docker volume ls`로 확인한 뒤 명령을 바꿔야 한다.

---

## 출처 (2026-10-12 접속)

- Upgrading to Airflow 3 (백업, `db clean`, `db migrate` 순서. 신규 설치나 병행 운영에 관한 언급은 없음): https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html
- CLI 레퍼런스 3.3.2 (`variables export/import -a`, `connections export --file-format/import --overwrite`, `dags pause/unpause --treat-dag-id-as-regex -y`, `dags list --columns`, `db export-archived`, "users/roles는 FAB provider 문서 참조"): https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html
- FAB provider CLI (`users export/import/create`, `roles export/import`): https://airflow.apache.org/docs/apache-airflow-providers-fab/stable/cli-ref.html
- Timetables (`CronTriggerTimetable`와 `catchup=False`에서 활성화 시 다음 트리거부터 실행): https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/timetable.html
- Set up a Database Backend (`airflow db migrate`로 스키마 생성): https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html
- Secrets backend (환경변수 → 메타DB 탐색 순서): https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/secrets-backend/index.html
- Managing Connections (`AIRFLOW_CONN_*`): https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html
- Managing Variables (`AIRFLOW_VAR_*`): https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html
- Docker Compose 프로젝트명 우선순위: https://docs.docker.com/compose/how-tos/project-name/
- Docker Compose volumes (`{project_name}_` 접두사, `name`, `external`): https://docs.docker.com/reference/compose-file/volumes/
- Docker restart policy: https://docs.docker.com/engine/containers/start-containers-automatically/
- PostgreSQL 16 업그레이드(pg_dump/restore 메이저 업그레이드): https://www.postgresql.org/docs/16/upgrading.html
- KIS 토큰 정책(제3자, 공식 예제 인용): https://algolab.co.kr/blog/kis-api-access-token-expiry-refresh-2026
- 선행 보고서: `docs/airflow3_A_code_impact.md`, `docs/airflow3_B_infra.md`, `docs/airflow3_C_release_strategy.md`
- 저장소 근거: `docker-compose.yaml`(`restart: always`, `build: .`, 볼륨, 포트), `plugins/asset_flow/managers/token_manager.py`(`TOKEN_DIR` 기본값, `TokenGenerator`의 "파일이 있으면 return", `GetTokens`의 "파일이 없으면 발급"), `dags/make_token_dag.py`
