# Handoff — Airflow 2.10.4 → 3.3.2 전환 리서치 종합

- 작성일: 2026-10-12
- 상태: **리서치 완료 / 구현 착수 전** (이 문서와 `docs/airflow3_*.md`는 설계 문서이며 코드·설정은 아직 바꾸지 않았다)
- 현재: `apache/airflow:2.10.4`, LocalExecutor, docker-compose(webserver/scheduler/init), 메타DB `postgres:13`, 자산 DB는 호스트 PostgreSQL(`postgres_asset`), 실패 알림은 Slack
- 목표: `apache/airflow:3.3.2`(2026-09-17, 최신 안정판). Airflow 2.x는 **2026-04-22 EOL**
- 이전 handoff(연금저축 계좌 분리, 2026-08-27)는 구현이 끝나 삭제했다. 내용은 git 이력 `78a913b`에서 볼 수 있다.

---

## 0. 확정된 결정 (사용자 승인)

| # | 결정 | 근거 문서 |
|---|---|---|
| 1 | 목표 버전은 **3.3.2**로 한다. "3.10"이라는 버전은 존재하지 않는다. | C §1 |
| 2 | 전환 방식은 **블루/그린**이다. 기존 2.10.4는 유지한 채 3.3.2를 새로 구축하고, 전환한 뒤 기존 스택을 제거한다. 섀도 격리 조건을 반드시 지킨다. | E §10 |
| 3 | 메타DB를 PG16으로 이관한다. 이관 전 **메타DB와 자산 DB 백업은 필수**다. | D §1~3 |
| 4 | 실패 알림은 **Slack에서 Discord로** 바꾼다. | D §4~5 |
| 5 | **계좌번호도 secrets 대상**이다. git, config, 직렬화 DAG, XCom에 남기지 않는다. | G §6, I §2 |
| 6 | 키(appkey/secret)마다 **만료일을 저장**하고 만료 전에 Discord로 알린다. 알림은 **단계 방식**(D-14, D-7, D-3부터 매일, 만료 후 매일, 2주 구간의 매주 월요일)으로 보낸다. | H §3 |
| 7 | 만료 점검은 **별도 DAG를 만들지 않는다.** `make_token` 안에 태스크로 두고, 알릴 것이 없으면 skip한다. | H §2 |
| 8 | 최종 목표는 **서버의 secrets YAML 값(계좌번호, 키, 만료일)만 바꾸면** 토큰 발급, 수집, 만료 알림이 모두 따라오는 구조다. | G, I |

## 1. 권장안 (아직 확정 전, 구현 때 확인)

| 영역 | 권장 | 근거 |
|---|---|---|
| 인증 | FAB provider + `FabAuthManager`. 기존 계정과 `_AIRFLOW_WWW_USER_CREATE`를 유지한다. | B §3 |
| secrets 저장 | 독자 스키마 단일 파일 `secrets/asset_flow.secrets.yaml`(gitignore, 디렉터리째 RO 마운트) + pydantic 로더. LocalFilesystemBackend는 쓰지 않는다. | G §4 |
| 스키마 | `credentials[]`(앱키·만료일)와 `accounts[]`(계좌번호·참조)를 분리한다. **list + 필수 `id`** 구조에, 중복 키를 거부하는 로더를 둔다. 숫자는 따옴표로 감싼다. | G §2~3, §7 |
| Discord 알림 | `requests`로 직접 호출하는 콜백. webhook은 `.env`의 `AIRFLOW_CONN_DISCORD_ALERT`에 두어 secrets 파일과 분리한다. | D §5 |
| 수집 DAG | 동적 태스크 매핑으로 바꾼다. `list_accounts`가 별칭만 넘기고 `collect_account.expand()`가 계좌별로 실행한다. 파싱 시점에 secrets를 읽지 않는다. | I §2.3 |
| 토큰 캐시 | 키 지문(해시)을 함께 저장하고, 지문이 다르면 그 키만 재발급한다. 파일 잠금을 걸어 KIS 1분 1회 제한을 지킨다. | I §3.1 |
| 메타DB 이관 | 블루 `pg_dump -Fc` → 그린 PG16 `pg_restore` → `airflow db migrate` → 기동 전 일괄 pause. clean install은 Plan B다. | E §2, D §1 |
| dag_id | `<도메인>_<동사>_<대상테이블>`(예: `fetch_asset_daily` → `account_ingest_asset_daily`). 3.x 전환 배포 때 커밋을 분리해 함께 적용한다. | F §2 |
| `create_cron_data_intervals` | 3.x 기본값(False)을 쓴다. 기준일 계산이 `data_interval_end` 기반이라 영향이 없다. 업그레이드 전에 확정한다. | A §4.2 |

## 2. 반드시 고쳐야 하는 차단 이슈

| # | 위치 | 문제 | 조치 |
|---|---|---|---|
| P0 | `plugins/callbacks.py:82,142` | `context["execution_date"]`가 제거된 키라 KeyError가 나고, **모든 실패 알림이 끊긴다.** | `logical_date`를 쓰고, 없으면 `run_after`로 대체한다(Discord 콜백 교체 때 함께 고침). |
| P0 | `plugins/callbacks.py:56-71` | `DagRun.find()` ORM 직접 조회가 차단되는데, 이 예외를 삼켜 **복구 알림이 조용히 영구 중단된다.** | `ti.get_previous_dagrun()`으로 바꾼다. |
| P1 | `fetch_asset_daily:156`, `fetch_exchange_rate:99`, `fetch_fund_price_daily:91` | 수동·API 실행에서는 `data_interval_end`가 None이라 AttributeError가 난다. | `run_after`로 대체하는 공통 헬퍼를 둔다. |
| 인프라 | `docker-compose.yaml` | `webserver`가 폐지됐고 `dag-processor`가 필수다. JWT, Execution API URL 설정이 새로 필요하고, PG13은 지원되지 않는다. | B §7의 3.x compose 초안을 쓴다. |
| 구조 | `token_manager.py` | 토큰 6개를 한꺼번에 발급해서, **KIS 키 하나만 만료돼도 전체 수집이 실패한다.** 쓰지 않는 `KIS_IRP`도 필수 목록에 들어 있다. | credential별로 발급하고 실패를 격리한다(I §3.1). |

그 밖의 import 경로 변경 88건은 `ruff check --select AIR --preview`로 검출되고, 그중 86건은 자동 수정된다(A §9).

## 3. 실행 로드맵

| 단계 | 시점 | 작업 | 관련 문서 |
|---|---|---|---|
| 0. 사전 정리 (블루, 2.10.4) | D-14~ | standard provider 경로 적용, `execution_date` 수정, None 방어, Discord 콜백, secrets 로더·검증 CLI·토큰 지문, 만료 점검 태스크. 모두 2.x와 호환되게 짜서 블루에 먼저 배포한다. `v2.10.4-final` 태그를 단다. | A §10, D §5, G §7, H §4, I §6.2 |
| 1. 백업 | D-10 | 메타DB와 자산 DB를 `pg_dumpall -g` + `pg_dump -Fc`로 백업하고, 볼륨 tar, 행수·md5 검증, 복원 리허설을 한다. 백업은 저장소 밖에 암호화해 보관한다. | D §1~3 |
| 2. 그린 구축 | D-10 | 별도 clone `~/oracle-airflow3`(`feat/airflow3`), `name: oracle-airflow3`, 포트 8081, PG16 새 볼륨, `airflow.sdk` import, 동적 매핑, dag_id 변경 커밋. | B §7, E §3, F §2, I §2 |
| 3. 이관 리허설 | D-10 | 덤프 복원 → `db migrate` → **기동 전 일괄 pause** → 기동. | E §2, §4.2 |
| 4. 섀도 운영 | D-9~D-2 | 스테이징 DB와 테스트 채널로 바꾸고, 블루 토큰 디렉터리를 RO로 마운트한다. 07:30 이후 logical date를 지정해 수동 체인을 실행한다. 검증 항목은 S1~S7. | E §4~5 |
| 5. 전환 | D-day 오후 | 블루 pause·정지 → 최종 덤프 → 그린 재이관 → 오버라이드 제거, 8080 전환 → unpause. 블루는 `down`(볼륨 유지)한다. | E §7 |
| 6. 안정화 | D+1~D+14 | 06:50~07:10 체인을 관찰하고 데이터를 대조한다. | E §7 |
| 7. 블루 제거 | D+30 | 볼륨 아카이브 후 `down --rmi local -v`(프로젝트명 확인 필수). 덤프는 90일 보관한다. | E §9 |

작업량은 순수 작업 약 3~5인일에 섀도 관찰 5~7일이다(C §5.3). secrets·동적 매핑 재구성은 별도로 약 2~3인일이다(I §6.3).

## 4. 아직 확인되지 않은 사항

1. **KIS 정책**: 개인 appkey의 정확한 만료 규칙, 갱신하면 키가 재발급되는지, 토큰 발급 1분 1회 제한과 재발급 시 기존 토큰이 무효화되는지. KIS Developers 포털 "신청현황"과 공식 공지로 확인한다.
2. **Upbit**: 새 키를 발급할 때 서버 IP를 허용 목록에 다시 등록해야 하는지 실측한다.
3. **git 이력의 계좌번호**: `upsert_manual_position_dag.py`의 `ACCOUNT_CODE_ENUM`과 삭제한 옛 handoff에 계좌번호가 있었다. 파일을 고쳐도 **git 이력에는 남는다.** 지우려면 `git filter-repo` 같은 이력 재작성이 필요하다(원격 저장소 공개 범위에 따라 결정).
4. **운영 서버 정보**: 실제 블루 디렉터리명(compose 프로젝트명, 볼륨 접두사), 메모리와 디스크 여유. 두 스택을 동시에 띄울 수 없으면 in-place로 바꾼다(E §3.2).
5. **실환경 검증 필요**: 3.3.2 이미지에서 Playwright/Chromium 빌드, dag-processor 콜백에서 `get_previous_dagrun` 동작, 새 UI의 Param 폼 렌더링.

## 5. 문서 색인

| 문서 | 내용 |
|---|---|
| [`airflow3_A_code_impact.md`](../airflow3_A_code_impact.md) | DAG·플러그인 코드의 3.x 영향(파일:라인 표, ruff AIR 결과, 수정 예시) |
| [`airflow3_B_infra.md`](../airflow3_B_infra.md) | 컴포넌트·설정 키·인증·이미지·PG·REST API v2 영향, 3.x Dockerfile/compose 초안 |
| [`airflow3_C_release_strategy.md`](../airflow3_C_release_strategy.md) | 릴리스 타임라인, 2.x EOL, 공식 업그레이드 경로, 신규 기능의 도입 가치, 단계적 전략 |
| [`airflow3_D_backup_discord.md`](../airflow3_D_backup_discord.md) | PG13→16 백업·이관 런북(검증 SQL, 롤백), Discord 알림 방식 비교와 콜백 전체 코드 |
| [`airflow3_E_parallel_cutover.md`](../airflow3_E_parallel_cutover.md) | 블루/그린 vs in-place 평가, 같은 서버 충돌, 이중 실행 격리, 컷오버·롤백·제거 절차 |
| [`airflow3_F_variables_naming.md`](../airflow3_F_variables_naming.md) | Variable 현황과 대안 비교, DAG 명명 규칙과 dag_id 변경 영향. 계좌번호를 config에 두는 안은 G로 대체됨 |
| [`airflow3_G_secrets_schema.md`](../airflow3_G_secrets_schema.md) | secrets YAML 스키마(list+id, 정규화), 저장 방식, pydantic 검증, 비밀 경계, 로더 코드 |
| [`airflow3_H_key_expiry_alert.md`](../airflow3_H_key_expiry_alert.md) | KIS·Upbit 키 유효기간, 만료 점검 실행 방식(make_token + skip), 단계 알림 정책, 구현·테스트 |
| [`airflow3_I_yaml_driven_automation.md`](../airflow3_I_yaml_driven_automation.md) | 하드코딩 16곳, 동적 태스크 매핑, 키 교체·계좌 변경 함정, YAML 파손 시 동작, 변경 절차 |

### 문서 간 충돌의 정리

| 항목 | 충돌 | 채택 |
|---|---|---|
| 계좌번호 위치 | F: config에 둠 | **G·I: secrets** (결정 5) |
| secrets 구조 | I: 별칭을 키로 한 dict | **G: list + 필수 `id`** |
| 만료 점검 위치 | G·I: 별도 DAG `ops_check_credentials` | **H: `make_token` 안의 태스크 + skip** (결정 7) |
| 알림 빈도 | I: D-14부터 매일 | **H: 단계 알림** (결정 6) |
| secrets 파일명 | I: `asset_flow.yaml` | **G: `asset_flow.secrets.yaml`** |
| 저장 백엔드 | F: LocalFilesystemBackend | **G·I: 독자 스키마 + 전용 로더** |
| DAG 콜백의 DB 접근 | B: dag-processor에서는 가능 | **A: 3.3.2 소스상 dag-processor도 ORM 차단** → 교체 필수 |
