# 작업 H: API 키 만료일 관리와 만료 2주 전 Discord 알림 (Airflow 3.3.2)

- 작성일: 2026-10-12
- 대상 저장소: `oracle-airflow`. 읽기 전용으로 분석했고, 이 보고서 외의 파일은 수정하지 않았다.
- 기준 버전: **Airflow 3.3.2**, `apache-airflow-task-sdk` 1.3.2, `apache-airflow-providers-standard` 1.20.0
- 선행 보고서와의 관계
  - [F](airflow3_F_variables_naming.md): 비밀은 `secrets/`의 LocalFilesystemBackend에 둔다. 단 **사용자 결정 (1)에 따라 F §1.3의 "계좌번호를 `dags/config/asset_flow.yaml`에 두는 안"은 폐기**한다. 계좌번호·appkey/secret·만료일은 모두 서버의 secrets YAML에 둔다. secrets YAML의 정확한 스키마와 로더는 작업 G가 정한다. 이 문서는 G 로더가 **credential별 `expires_on`을 돌려준다고 가정**하고, 그 인터페이스(§4.1)에 맞춰 작성했다.
  - [D](airflow3_D_backup_discord.md): Discord 전송은 D §5.2의 `_post_discord`, `_field`, `_payload`, `_footer`를 그대로 재사용한다.
  - [E](airflow3_E_parallel_cutover.md): 섀도 기간의 격리 원칙(테스트 webhook, 수동 실행)을 테스트 절차(§5)에 적용한다.
- 근거 표기
  - **소스 확인**: PyPI wheel(`apache-airflow-core==3.3.2`, `apache-airflow-task-sdk==1.3.2`, `apache-airflow-providers-standard==1.20.0`)을 스크래치패드에 풀어 직접 읽은 내용이다.
  - **공식**: 사업자가 직접 운영하는 문서·약관이다.
  - **제3자**: 블로그나 튜토리얼이다. 정책 판단의 근거로는 약하다.

---

## 0. 요약 (TL;DR)

| # | 결론 |
|---|---|
| 1 | **KIS**: 개인이 KIS Developers에서 신청한 appkey/appsecret은 **신청일로부터 1년 단위**다. 공식 약관 문구는 "1년 단위로 재신청"(제휴 사용자 약관 제5조④)이고, 연장 절차의 세부 내용(만료 30일 전부터 갱신, 갱신 시 키 재발급)은 제3자 자료로만 확인했다. **만료일을 조회하는 API는 없다.** 그래서 포털 화면에 보이는 만료일을 secrets YAML에 **사람이 직접 적어야** 한다. |
| 2 | **Upbit**: 공식 API `GET /v1/api_keys`가 키별 **`expire_at`(KST)**을 돌려준다(공식). "유효기간 1년, 연장 불가, 만료되면 새로 발급"은 고객센터 문구를 인용한 2차 자료로만 확인했다. 만료되면 `expired_access_key` 오류가 난다(공식 FAQ). 그래서 **YAML 값을 원본으로 쓰고, API 값과 날마다 대조**해서 어긋나면 알린다. |
| 3 | **영향 범위가 다르다(코드 확인).** `TokenGenerator()`는 6개 토큰을 dict 하나로 한꺼번에 발급한다. 따라서 **KIS 키가 하나라도 만료되면 토큰 파일이 만들어지지 않고 → 환율·자산 수집 전체가 실패한다.** 쓰지 않는 `KIS_IRP`도 여기에 포함된다(F 부수 발견 1). 반면 Upbit JWT는 로컬에서 서명만 하므로 발급은 성공한다. **Upbit 키가 만료되면 `get_upbit_assets`만 401로 실패한다.** 만료 알림에는 이 영향 범위를 함께 적는다. |
| 4 | **실행 방식은 사용자 제안 (ii)를 조건부로 채택한다.** `make_token`(F 권장명 `auth_issue_api_tokens`)에 `check_key_expiry` 태스크를 **`generate_tokens`와 병렬로, 의존성 없이** 추가한다. 알릴 것이 없으면 `AirflowSkipException`으로 skip한다. 키가 만료돼 토큰 발급이 실패하는 날에도 만료 알림이 나가야 하므로, **`generate_tokens` 하류에 두는 배치는 금지**한다. |
| 5 | (ii)를 고른 결정적 이유: 별도 DAG (i)은 `DAGS_ARE_PAUSED_AT_CREATION=true` 때문에 **paused 상태로 생성된다. unpause를 잊으면 알림이 아무 신호 없이 영원히 오지 않는다.** (ii)는 make_token이 멈추면 07시 체인 전체가 멈춰서 바로 드러난다. skip은 DAG run 성공 판정, 복구 콜백, 하류 센서(`external_task_id='generate_tokens'`)에 영향을 주지 않는다(소스 확인, §2.3). |
| 6 | 3.x Deadline Alerts는 "DAG run이 기준 시각까지 끝났는가"를 감시하는 기능이다. 날짜 기반 만료 점검에는 **맞지 않는다**(§2.5). |
| 7 | 알림 정책: **D-14, D-7, D-3부터는 매일(만료 당일과 만료 후 포함), 그리고 D-14 구간의 매주 월요일**에 보낸다. 상태 저장 없이 날짜 규칙만으로 정하므로 **중복 억제 저장소가 필요 없다.** 알릴 키가 여럿이면 **메시지 1건(embed 1개)에 키별 필드로** 묶는다. |
| 8 | 날짜는 반드시 **KST 기준**으로 계산한다. 06:50 KST는 UTC로 전날 21:50이다. 그래서 컨테이너 기본 TZ(UTC)로 `date.today()`를 쓰면 **하루가 어긋난다.** `dag_run.run_after`를 KST로 바꿔 날짜를 얻고, 테스트용으로 `as_of` Param을 둔다. |
| 9 | 테스트: 순수 함수 pytest, 만료일을 임박하게 바꾼 **테스트용 secrets 파일 + `airflow tasks test`**, 그리고 `as_of` Param으로 날짜를 이동시킨 수동 run. 이렇게 세 겹으로 검증한다(§5). |

---

## 1. 키 유효기간 정책 조사

### 1.1 한국투자증권(KIS) Open API appkey/appsecret

| 항목 | 내용 | 근거 | 확인 수준 |
|---|---|---|---|
| 이용 기간 | "사용자는 회사의 오픈 API 서비스 이용을 **1년 단위로 재신청**하여야 한다. 단, 투자자문·일임 계약을 체결한 경우 해당 계약의 유효 기간 내에서 서비스 이용 기간이 자동 연장될 수 있다." | 「오픈 API 서비스 이용 약관(제휴 이용기관_사용자)」 제5조④, 2025-06-04 개정. 원문 PDF를 내려받아 확인했다. | **공식**. 다만 약관 이름이 "제휴 이용기관을 통한 사용자"용이라 KIS Developers에 직접 신청한 개인에게 그대로 적용되는지는 문구상 단정할 수 없다. |
| 이용 기간(구 상품) | "사용기간 : 신청일로부터 1년", "3개월이상 미거래시 자동으로 서비스가 해지될 수 있으니…(해지시 재신청후 사용 가능)" | 한국투자증권 홈페이지 eFriend Expert(Open API) 안내 | **공식**이지만 대상이 OCX 기반 eFriend Expert이고 REST(KIS Developers)가 아니다. 같은 회사의 정책 방향을 보여 주는 보조 근거로만 쓴다. |
| 해지 사유 | "최근 1년간 오픈 API 서비스 이용 실적이 없는 경우" 이용 승인을 해지할 수 있다. | 위 약관 제7조①3 | 공식. 이 프로젝트는 매일 호출하므로 해당하지 않는다. |
| 개인 KIS Developers 키 | "이용기간은 신청일로부터 1년", 문자 설정 시 "서비스 만료 1개월 전 메시지", 갱신은 "만료 30일 전부터 신청 가능", "갱신 시 신청일로부터 1년 연장되며 **APP Key, APP Secret가 재발급**" | 위키독스 『파이썬으로 배우는 오픈API 트레이딩 초급 예제』 01) 오픈API 서비스 신청 (검색 요약으로 확인, 원문 페이지는 403으로 직접 열지 못함) | **제3자**. 포털의 "신청현황" 화면에서 직접 확인해야 한다. |
| appsecret 재조회 | 포털은 SECRET을 다시 평문으로 보여 주지 않는다. 분실하면 삭제 후 재발급해야 한다. | AlgoLab 블로그 | 제3자 |
| 액세스 토큰 | `/oauth2/tokenP`로 발급하며 24시간 유효하다. 저장소 `plugins/asset_flow/config/kis.py` 주석에도 "1일 유효"로 적혀 있다. | 저장소, 제3자 | 이 문서의 대상(키 만료)과는 다른 개념이다. |
| 만료일 조회 API | **없다.** 공식 포털 문서에는 제휴사용 "갱신토큰 만기일 저장 → 만기 1개월 전 푸시/SMS 안내" 같은 운영 권고만 있다. 개인 키의 만료일을 돌려주는 엔드포인트는 찾지 못했다. | KIS Developers provider-doc4(제휴사 토큰 관리), open-trading-api GitHub README | 공식 문서에서 **찾지 못함** |
| 만료되면 | 토큰 발급(`tokenP`)이 거부될 것으로 예상한다. 정확한 HTTP 코드와 메시지는 확인하지 못했다. | — | 미확인. 만료 당일 로그로 확인해 §3.4의 예외 메시지 매핑을 보완한다. |

**결론(KIS)**
- 만료일은 **사람이 secrets YAML에 적는다.** 포털 "신청현황"의 만료일을 그대로 옮기는 것이 가장 정확하다.
- 갱신하면 appkey/secret이 **재발급된다는 제3자 기술**이 있다. 그래서 운영 절차를 "**appkey, secret, expires_on 세 값을 한 번에 교체한다**"로 정한다(§3.5). 셋 중 하나만 바뀌는 실수를 잡는 장치는 §3.4 "stale 만료일 감지"다.
- CMA는 ISA의 키와 토큰을 공유한다(`fetch_asset_daily_dag.py` `get_cma_cash`). 그래서 만료 관리 단위는 **계좌가 아니라 credential(appkey 1개)**이다.

### 1.2 Upbit Open API 키

| 항목 | 내용 | 근거 | 확인 수준 |
|---|---|---|---|
| 만료일 조회 API | `GET https://api.upbit.com/v1/api_keys` → `[{access_key, expire_at}]`. `expire_at`은 "해당 Access Key의 Deprecated일시 (KST)", 형식은 `yyyy-MM-dd'T'HH:mm:ss+09:00`이다. "인증이 필요한 API입니다. 별도 권한은 필요하지 않습니다." | docs.upbit.com API 키 리스트 조회, changelog "API 키 목록 조회 API 추가" | **공식** |
| 만료되면 | 오류 코드 `expired_access_key`: "API 키가 만료되었습니다. Open API 관리 페이지에서 만료된 키를 삭제하고, 신규키를 발급받길 바랍니다." 만료 키가 발급 한도를 채우고 있으면 신규 발급이 막히므로 "만료된 Key" 탭에서 지운다. | docs.upbit.com Open API(api-key) FAQ | **공식** |
| 유효기간 | "Open API Key 토큰의 유효 기간은 **1년**이며 **연장은 불가**합니다. 유효 기간 만료 후 계속 사용하기를 원하시면 추가로 Open API Key를 발급받으셔야 합니다." | 업비트 고객센터 문구로 인용된 2차 자료(검색 요약). 고객센터 원문(support.upbit.com)은 403이라 직접 열지 못했다. | **제3자 인용**. 다만 `expire_at`을 API로 받을 수 있으므로 이 수치에 의존할 필요가 없다. |
| 만료 전 안내 | 개인 블로그에 "만료가 가까워졌다"는 메시지를 받았다는 사례가 있다. 채널(카카오톡/이메일)은 확정하지 못했다. | 제3자 | 미확인 |
| Secret 재조회 | "Secret Key는 발급 시에만 확인 가능합니다." | docs.upbit.com 인증 가이드 | 공식 |

**결론(Upbit)**
- 연장할 수 없으므로 만료되면 **새 access/secret 쌍을 발급**해 YAML의 `appkey`, `secret`, `expires_on`을 교체한다.
- 만료일은 YAML에 적되, 매일 `/v1/api_keys`로 **교차 검증**한다(§3.4). 사람이 날짜를 잘못 적거나 키만 바꾸고 날짜를 안 바꾸는 실수를 자동으로 잡을 수 있다. API 호출이 `expired_access_key`로 실패하면 그 자체를 "만료됨"으로 판정한다.
- IP 허용 목록: 공식 가이드는 "호출지의 IP를 허용 목록에 등록"하라고 안내한다. 새 키를 발급할 때 서버 IP를 다시 등록해야 한다(키당 최대 10개).

### 1.3 저장소 코드로 본 "만료되면 무엇이 깨지나"

| credential | 토큰 발급 경로 | 만료 시 실패 지점 | 영향 범위 |
|---|---|---|---|
| `KIS_STOCK`, `KIS_ISA`, `KIS_PENSION_DEDUCTIBLE`, `KIS_PENSION_NON_DEDUCTIBLE`, `KIS_IRP` | `TokenManager._get_kis_token` → `POST /oauth2/tokenP` | `TokenGenerator()`의 `tokens = {...}` dict 리터럴. **하나라도 예외가 나면 전체가 `Exception("[ERROR] 토큰 발급 실패")`로 끝나고 파일이 생기지 않는다.** | `make_token` 실패 → `fetch_exchange_rate.wait_for_token` 센서가 600초 뒤 timeout → `fetch_asset_daily`의 센서도 연쇄 실패. 수집 태스크의 `GetTokens()`도 같은 `TokenGenerator()`를 다시 호출하므로 같은 이유로 실패한다. 결과적으로 **환율, KIS 5계좌, Upbit까지 전부 실패**한다. `fetch_fund_price_daily`(크롤링)와 원장 기반 4계좌(IRP·DC·적금·청약)는 토큰과 무관하지만, `fetch_asset_daily`가 환율 센서에서 먼저 막히면 함께 멈춘다. |
| `UPBIT` | `TokenManager._get_upbit_token` → 로컬 `jwt.encode` | 발급은 성공한다. `get_upbit_assets`의 `/v1/accounts` 호출이 401(`expired_access_key`)로 실패한다. | 업비트 계좌 1개만 실패한다. 다른 계좌는 계속 적재된다(계좌별 get/upload 페어 불변식). |

- `KIS_IRP`는 쓰는 곳이 없는데도 `REQUIRED_TOKEN_KEYS`에 들어 있다. 그래서 **IRP 키가 만료되면 체인 전체가 멈춘다**(F §1.1 부수 발견 1). 이 문서의 알림은 이 위험을 경고문으로 표시만 한다. 근본 대책(발급 목록에서 제외하거나 credential별 발급 실패를 격리)은 별도 PR로 다룬다.
- 그래서 **KIS 키 만료는 "한 키 = 전체 장애"**이고, 2주 전 알림이 실제로 필요한 이유도 여기에 있다.

---

## 2. 만료 점검 실행 방식 비교

### 2.1 후보

| 후보 | 구성 |
|---|---|
| (i) 별도 일배치 DAG | `ops_check_key_expiry`(F 명명 규칙 기준). 매일 09:00 KST 등 **독립 스케줄**, 태스크 1~2개 |
| (ii-a) make_token에 추가, `generate_tokens` **하류** | `generate_tokens >> check_key_expiry` |
| (ii-b) make_token에 추가, `generate_tokens`와 **병렬(의존성 없음)** + 알릴 것이 없으면 skip | `[generate_tokens, check_key_expiry]` |
| (ii-c) (ii-b)에서 `@task.short_circuit`로 판정과 전송을 분리 | `check_key_expiry(short_circuit) >> notify_key_expiry` |

### 2.2 skip 수단 3가지의 동작 (소스 확인)

| 수단 | 판정 태스크 자신의 상태 | 하류 태스크 | 실행되는 콜백 |
|---|---|---|---|
| `raise AirflowSkipException("...")` | **SKIPPED** | 일반 trigger rule에 따라 전파된다(기본 `all_success`면 하류도 skipped) | 그 태스크의 **`on_skipped_callback`**. `on_success_callback`은 실행되지 않는다. `task_runner.finalize()`가 `state == SKIPPED`일 때 `on_skipped_callback`만 호출한다. |
| `ShortCircuitOperator` / `@task.short_circuit`가 falsy를 반환 | **SUCCESS**. `DownstreamTasksSkipped`를 잡아 `_handle_current_task_success`로 처리하고, 하류 skip은 `SkipDownstreamTasks` 메시지로 supervisor에 위임한다. | `ignore_downstream_trigger_rules=True`(기본값)이면 **모든 하위 후손**(`get_flat_relatives`)을 trigger rule과 상관없이 skip한다. `False`면 **직계 하류만** skip하고, 그 아래는 각자의 trigger rule을 따른다. teardown은 제외된다. | 판정 태스크의 `on_success_callback`. skip된 하류는 **실행되지 않았으므로 콜백이 하나도 없다.** |
| 하류 trigger rule에 의한 skip | — | — | 없음. 공식 Callbacks 문서도 `on_skipped_callback`은 "AirflowSkipException이 raise된 경우"에만 호출되며, 앞선 분기나 trigger rule 때문에 실행이 시작되지 않은 태스크에는 호출되지 않는다고 적고 있다. |

- 근거: `airflow/providers/standard/operators/python.py` `ShortCircuitOperator.execute`, `utils/skipmixin.py` `SkipMixin.skip/_set_state_to_skipped`, `airflow/sdk/execution_time/task_runner.py`(`except DownstreamTasksSkipped` / `except AirflowSkipException` / `finalize`).
- **3.x 변경점**: "skip된 태스크에서는 `on_success_callback`이 실행되지 않는다"(PR #40936, 호환성 표에 breaking change로 기재). 이 저장소에는 태스크 레벨 `on_success_callback`이 없다. 복구 알림은 DAG 레벨이다. 그래서 **직접 영향은 없다.** skip을 "정상"으로 보고 무언가를 하고 싶다면 `on_skipped_callback`을 써야 한다. 이 설계에서는 필요 없다.
- 3.3.2에서 `airflow.exceptions.AirflowSkipException`은 `_DEPRECATED_EXCEPTIONS`에 들어 있다. 새 코드는 **`from airflow.sdk.exceptions import AirflowSkipException`**을 쓴다(소스 확인).

### 2.3 skip이 DAG run 상태, 복구 콜백, 하류에 미치는 영향 (소스 확인)

| 대상 | 영향 |
|---|---|
| DAG run 상태 | `DagRun.update_state`는 미완료 TI가 없고 판정 대상 TI가 모두 `State.success_states = {SUCCESS, SKIPPED}`이면 run을 **SUCCESS**로 만든다(`airflow/models/dagrun.py`, `airflow/utils/state.py`). 그래서 `check_key_expiry`가 skipped여도 make_token run은 **성공**이다. |
| DAG 레벨 `on_success_callback`(복구 알림) | run이 SUCCESS이므로 평소처럼 호출된다. D §5.2의 `_was_previous_run_failed` 판정도 그대로 동작한다. skip 때문에 복구 알림이 잘못 나가는 일은 없다. |
| `check_key_expiry`가 **실패**하는 경우 | make_token run이 FAILED가 된다. 다음 날 성공하면 "복구" 알림이 1건 나간다. 의도된 동작이다(점검 기능 자체의 장애도 알려야 한다). |
| 하류 DAG의 센서 | `fetch_exchange_rate.wait_for_token`은 `external_task_id='generate_tokens'`로 **태스크 단위**로 매칭한다(`dags/fetch_exchange_rate_dag.py:80-88`). 그래서 `check_key_expiry`의 성공, skip, 실패는 07시 체인에 **영향이 없다.** ⚠️ 나중에 센서를 DAG 단위(`external_task_id=None`)로 바꾸면 점검 실패가 체인을 막게 된다. 이 결합 조건을 make_token의 `doc_md`에 적어 둔다. |
| 하류 TriggerRule | (ii-b)는 하류가 없어서 상관없다. (ii-c)에서 `notify` 아래에 다른 태스크를 붙인다면 `ignore_downstream_trigger_rules=False`와 `trigger_rule="none_failed"`를 함께 검토한다. |

### 2.4 장단점 평가

| 기준 | (i) 별도 DAG | (ii-a) 하류 배치 | **(ii-b) 병렬 + skip (권장)** | (ii-c) short_circuit 2태스크 |
|---|---|---|---|---|
| **장애 격리: 토큰 발급이 실패한 날에도 만료 알림이 나가는가** | ✅ | ❌ **결정적 결함.** 키가 만료되면 `generate_tokens`가 실패(재시도 2회, 약 6분)하고, 그러면 점검은 `upstream_failed`가 된다. 가장 알림이 필요한 날에 알림이 사라진다. `trigger_rule="all_done"`으로 우회할 수는 있지만 재시도가 끝날 때까지 지연된다. | ✅ 같은 run 안에서 독립 실행된다. | ✅ |
| 점검이 실패했을 때 07시 체인 영향 | 없음 | 없음(센서가 태스크 단위) | 없음(센서가 태스크 단위). 단 make_token run이 FAILED로 보인다. | (ii-b)와 같음 |
| **"조용한 미동작" 위험** | ❌ **paused로 생성된다**(`DAGS_ARE_PAUSED_AT_CREATION=true`, F §2.6). unpause를 잊으면 run이 아예 생기지 않으므로 실패 콜백도 없고 **아무 신호 없이 알림이 사라진다.** 3.x 전환과 dag_id 변경 때 unpause해야 할 대상이 하나 더 늘어난다. | ✅ make_token이 멈추면 체인 전체가 멈춰서 바로 드러난다. | ✅ 같음 | ✅ 같음 |
| 알림 시각 | 자유롭게 정할 수 있다(예: 09:00) | 06:50 이후 | 06:50(토큰 발급과 같은 시각) | 06:50 |
| UI 가독성 | DAG가 하나 늘어난다 | — | 태스크 색으로 판정이 보인다(skipped = 알릴 것 없음, success = 알림 발송) | 판정은 항상 success라서 하류 notify의 색으로 봐야 한다 |
| 코드 양 | DAG 파일 1개 추가 | 태스크 1개 | 태스크 1개 | 태스크 2개 + XCom |
| 관심사 분리 | 가장 깔끔하다 | 섞인다 | 섞이지만 같은 "auth" 도메인(F의 `auth_issue_api_tokens`)이라 자연스럽다 | (ii-b)와 같음 |
| secrets만 바꾸면 따라오는가 | ✅ | ✅ | ✅ | ✅ |

**결정: (ii-b)를 채택한다.** 사용자 제안 (ii)를 받아들이되, 아래 3가지 조건을 붙인다.

1. `check_key_expiry`는 `generate_tokens`와 **의존성 없이 병렬**로 둔다. (ii-a) 하류 배치는 금지한다.
2. 알릴 것이 없으면 `AirflowSkipException`으로 끝낸다. 하류가 없으므로 ShortCircuit의 하류 skip 기능은 필요 없다. 태스크가 하나면 UI 색(skipped/success/failed)만으로 상태가 드러난다.
3. `retries=1`로 둔다. make_token의 기본값 `retries=2`, `retry_delay=3분`을 따르면 Discord 장애 때 재시도가 불필요하게 길어진다. 실패 콜백은 D의 `discord_failure_callback`(DAG default_args)을 그대로 쓴다.

(i)이 더 나은 경우도 있다. 알림 시각을 06:50이 아니라 업무 시간으로 옮기고 싶거나, 나중에 make_token을 없애고 토큰 발급을 수집 태스크 안으로 흡수할 때다. 그때는 같은 태스크 함수를 새 DAG로 옮기기만 하면 되도록 **판정 로직을 `plugins/asset_flow/ops/key_expiry.py`의 순수 함수로 분리**한다(§4.2).

### 2.5 3.x Deadline Alerts는 이 용도에 맞는가

- `DeadlineAlert(reference, interval, callback)`이 쓰는 기준점은 `DeadlineReference.DAGRUN_LOGICAL_DATE`, `DAGRUN_QUEUED_AT`, `FIXED_DATETIME(dt)`, `AVERAGE_RUNTIME`이다(소스 확인: task-sdk 1.3.2 `definitions/deadline.py`). 이 기능은 **"DAG run이 기준 시각 + interval까지 끝나지 않았다"**는 상황을 감시한다. 즉 SLA를 대체하는 기능이다.
- 키 만료는 "어떤 날짜가 다가온다"는 **달력 이벤트**이고 run의 지연과는 관계가 없다. `FIXED_DATETIME(만료일 - 14일)`로 흉내 낼 수는 있다. 하지만 그러려면 ① 만료일을 **DAG 파싱 시점(top-level)**에 secrets에서 읽어야 하고(Best Practices 위반, F §1.2(a)), ② 키를 교체할 때마다 DAG 정의가 바뀌며, ③ 알림이 run마다 생기는 등 의미가 맞지 않는다.
- **판정: 부적합.** 대신 Deadline Alerts는 별도 용도로 검토할 만하다. 예를 들어 "07:30 KST까지 `fetch_asset_daily`가 끝나지 않으면 알림"(`DAGRUN_LOGICAL_DATE` + interval)이다. 이는 이 작업의 범위 밖이다.

---

## 3. 알림 정책

### 3.1 빈도: 매일 vs 단계

| 정책 | 키 1개 기준 알림 수(D-14~D-0) | 장점 | 단점 |
|---|---|---|---|
| D-14부터 매일 | 15건 | 사용자가 처음 요청한 표현과 그대로 맞는다. 놓칠 일이 없다. | **알림 피로.** KIS 키 5개를 같은 날 신청했다면 만료일도 같아서 묶어도 15일 연속 알림이다. 결국 무시하게 되고, D-1 알림도 함께 묻힌다. |
| 엄격한 단계(D-14/7/3/1/0/만료 후) | 5건 + 만료 후 | 피로가 적다 | **해당 날짜의 run이 실패하거나 pause되면 그 단계를 놓친다.** 상태 없이 따라잡을 수 없다. |
| **권장: 단계 + 따라잡기** | 약 7~8건 | 피로가 적고, 놓친 단계는 다음 규칙이 메운다. 상태 저장이 필요 없다. | 규칙이 조금 복잡하다(순수 함수 1개라 테스트하기 쉽다). |

**권장 규칙** (`days_left = expires_on - 오늘(KST)`)

```
알림 발송 조건 (하나라도 해당하는 credential이 있으면 발송):
  days_left ∈ {14, 7}            # 단계 알림
  days_left ≤ 3                  # D-3, D-2, D-1, D-0(당일), 만료 후(음수) → 매일
  0 ≤ days_left ≤ 14 and 월요일   # 따라잡기: 단계 날짜를 놓쳤거나 새 키의 만료가 이미 임박한 경우
  expires_on 미기재 and 월요일     # 설정 누락은 주 1회 알림
```

- 사용자의 "2주 전부터 알림"은 충족한다. D-14에 첫 알림이 나가고, 그 뒤로 2주 구간 안에서 최소 주 1회, 마지막 3일과 만료 후에는 매일 나간다.
- 매일 알림으로 바꾸고 싶으면 규칙을 `days_left ≤ 14` 한 줄로 바꾸면 된다(§4.2 `should_notify`).
- **만료 후에는 매일** 보낸다. 만료된 KIS 키는 체인 전체 장애이므로 해결될 때까지 계속 상기시키는 것이 맞다. 키를 교체하고 YAML의 `expires_on`을 갱신하면 알림이 자동으로 멈춘다.

### 3.2 심각도와 색

| 등급 | 조건 | embed 색 | 아이콘 |
|---|---|---|---|
| INFO | 8 ≤ days_left ≤ 14 | `0xFDD835`(노랑) | 🟡 |
| WARN | 4 ≤ days_left ≤ 7 | `0xFB8C00`(주황) | 🟠 |
| CRIT | 0 ≤ days_left ≤ 3 | `0xE53935`(빨강) | 🔴 |
| EXPIRED | days_left < 0, 또는 Upbit API가 `expired_access_key` 응답 | `0x6A1B9A`(보라) | ⛔ |
| CONFIG | `expires_on` 미기재, YAML과 API 값 불일치, stale 의심 | `0x757575`(회색) | ⚙️ |

메시지의 색은 **포함된 항목 중 가장 높은 등급**을 따른다. 우선순위는 EXPIRED > CRIT > WARN > INFO > CONFIG다. CONFIG는 날짜 등급보다 낮지만, 불일치처럼 즉시 알려야 하는 경우는 `force` 플래그로 날짜 규칙을 우회한다. YAML과 API가 불일치하면 둘 중 더 임박한 날짜의 등급도 함께 적용한다.

### 3.3 중복 억제 상태 저장이 필요한가

- **필요 없다.** 규칙이 "그날의 날짜"만으로 결정되고(멱등), DAG는 하루 1회 스케줄로 돈다. 그래서 같은 날 정기 run은 1번뿐이다.
- 같은 날 **수동으로 다시 실행하면 알림이 다시 나간다.** 테스트와 재확인 용도이므로 의도된 동작이다.
- 상태를 두면(Variable, XCom, DB 테이블) 오히려 손해가 생긴다. "어제 보냈다고 기록됐지만 실제 전송은 실패한" 경우에 알림이 사라진다. 저장소가 새로운 장애 지점이 되며, 메타DB에 쓰면 E의 블루/그린 전환 때 이관 대상이 하나 늘어난다.
- 예외: 나중에 "확인(ack) 버튼을 누르면 그날까지 알림 중지" 같은 기능이 필요해지면, 그때 secrets YAML이 아닌 일반 설정에 `snooze_until`을 두는 방식을 검토한다. 지금은 범위 밖이다.

### 3.4 만료 후 수집 실패와의 연결, 그리고 자동 교차 검증

| 장치 | 내용 | 위치 |
|---|---|---|
| ① 만료 알림에 **영향 범위** 표시 | KIS credential은 "만료 시 토큰 일괄 발급 실패 → 환율·KIS·Upbit 수집 전체 중단"으로, `KIS_IRP`에는 "미사용인데 발급 필수 목록에 포함"을 덧붙이고, Upbit는 "업비트 계좌 1개만 실패"로 적는다(§1.3). | `check_key_expiry` |
| ② **실패 알림에 만료 정보 첨부** | `TokenManager._get_kis_token`이 HTTP 오류로 끝날 때 해당 credential의 `expires_on`을 예외 메시지에 붙인다. 예: `"KIS_ISA 토큰 발급 실패 (HTTP 403) — secrets 기준 만료일 2026-10-10 (D+2, 만료됨)"`. 그러면 D의 실패 알림 embed 본문만 보고도 원인을 바로 알 수 있다. Upbit는 `get_upbit_assets`에서 401 응답 본문의 `error.name == "expired_access_key"`를 같은 형식으로 감싼다. | G 로더를 쓰는 token_manager 수정 PR(§4.4) |
| ③ **Upbit API 대조** | 매일 `/v1/api_keys`를 호출해 YAML의 `appkey`와 같은 `access_key`의 `expire_at` 날짜를 YAML `expires_on`과 비교한다. 다르면 CONFIG 항목으로 알린다(이때는 날짜 규칙과 상관없이 **즉시** 알림). 응답에 해당 키가 없거나 `expired_access_key`이면 EXPIRED로 판정한다. 비교와 판정에는 **API 값을 우선**한다. | `check_key_expiry` |
| ④ **stale 만료일 감지(KIS)** | "YAML상 이미 만료됐는데 오늘 KIS 토큰 발급이 성공했다"면 `expires_on` 갱신을 빠뜨린 것이다(키는 연장됐는데 날짜만 그대로). `generate_tokens` **하류**(기본 `all_success`)에 작은 태스크 `verify_expiry_metadata`를 두어, 발급이 성공한 날에만 `days_left < 0`인 KIS credential을 CONFIG 경고로 보낸다. 이 태스크는 판정 정확도를 보완할 뿐이고 장애 격리 대상은 아니라서 하류에 둬도 된다. | make_token(선택) |

### 3.5 운영 절차 (사용자 목표 (3): "secrets YAML만 바꾸면 모든 과정이 따라온다")

1. 알림을 받으면 KIS Developers의 "신청현황"에서 갱신하거나, Upbit Open API 관리에서 새 키를 발급한다(Upbit는 서버 IP 등록 필수).
2. 서버의 secrets YAML에서 해당 credential의 **`appkey`, `secret`, `expires_on`을 한 번에** 바꾼다. 계좌번호는 바뀌지 않는다.
3. 아무것도 재시작하지 않는다. LocalFilesystemBackend와 G 로더는 조회할 때마다 파일을 다시 읽는다(F §1.2(c), 소스 확인).
4. 그날 이미 토큰 파일이 있다면 옛 키로 발급된 토큰이다. 필요하면 `data/tokens/<오늘>_token.json`을 지우고 make_token을 수동 실행해 새 키로 즉시 검증한다. 다음 날 06:50에는 자동으로 새 키를 쓴다.
5. 다음 날 알림이 오지 않으면 끝이다. Upbit는 ③의 API 대조로 새 키의 날짜까지 자동 검증된다.

---

## 4. 구현 예시 (Airflow 3 SDK, 저장소에는 쓰지 않음)

### 4.1 G 로더에 대한 가정(인터페이스 계약)

G 작업의 로더가 아래 형태를 제공한다고 가정한다. 이름이 다르면 `key_expiry.py`의 import 한 줄과 어댑터만 고치면 된다.

```python
# (가정) plugins/asset_flow/config/secrets_loader.py — 작업 G 산출물
@dataclass(frozen=True)
class Credential:
    name: str                 # credential ID. 예: "KIS_ISA" (토큰 dict 키 규약과 동일)
    provider: str             # "kis" | "upbit"
    appkey: str               # mask_secret 처리된 값
    secret: str
    expires_on: date | None   # KST 기준 만료일(포털/API에 표시된 날짜). 미기재 시 None
    used_by: tuple[str, ...]  # 이 credential을 쓰는 계좌 키. 예: ("KIS_ISA", "KIS_CMA")

def credentials() -> dict[str, Credential]: ...
```

secrets YAML 예시(형식의 최종안은 G를 따른다. 여기서는 `expires_on` 위치만 보인다):

```yaml
# /opt/airflow/secrets/asset_flow.secrets.yaml  (gitignore, 서버에만 존재, chmod 640)
credentials:
  KIS_ISA:
    provider: kis
    appkey: "<APPKEY>"
    secret: "<APPSECRET>"
    expires_on: 2027-03-14        # KIS Developers > 신청현황의 만료일. 따옴표 없이 쓰면 YAML이 date로 파싱
  UPBIT:
    provider: upbit
    appkey: "<ACCESS_KEY>"
    secret: "<SECRET_KEY>"
    expires_on: 2027-01-05        # /v1/api_keys expire_at의 날짜 부분(KST)
accounts:
  KIS_CMA: {credential: KIS_ISA, account: "<CANO>", product_code: "<CODE>", type: "한국투자 CMA"}
  # ... 사용자 결정 (1): 계좌번호도 여기(secrets)에 둔다
```

> `expires_on`의 의미: **"이 날짜부터 키를 쓸 수 없다고 본다"**(보수적 해석). KIS는 "신청일로부터 1년"의 마지막 날이 포함되는지 공식 근거가 없다. Upbit `expire_at`은 시각까지 있지만 날짜만 취한다. 두 경우 모두 하루 일찍 알리는 쪽으로 오차를 둔다.

### 4.2 판정과 메시지: `plugins/asset_flow/ops/key_expiry.py`

Airflow import가 없는 순수 모듈이라 pytest로 바로 테스트할 수 있다.

```python
"""
API 키 만료 점검 — 판정(순수 함수)과 Discord embed 생성

호출부: make_token DAG의 check_key_expiry 태스크 (작업 H §4.3)
입력:   secrets 로더(작업 G)의 Credential 목록 + 기준일(KST)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import IntEnum

from callbacks import _field, _footer, _payload  # 작업 D §5.2 재사용 (plugins/ 루트 모듈)

STAGE_DAYS = frozenset({14, 7})
DAILY_FROM = 3          # D-3부터 매일(만료 당일·만료 후 포함)
WINDOW = 14             # 알림 구간
MONDAY = 0

# 영향 범위 문구 (§1.3) — 토큰 일괄 발급 구조가 바뀌면 함께 수정
KIS_BULK_ISSUE_KEYS = frozenset({
    "KIS_STOCK", "KIS_ISA", "KIS_PENSION_DEDUCTIBLE", "KIS_PENSION_NON_DEDUCTIBLE", "KIS_IRP",
})  # = TokenManager.REQUIRED_TOKEN_KEYS - {"UPBIT"}
RENEW_GUIDE = {
    "kis": "KIS Developers > 신청현황에서 갱신(만료 30일 전부터) → appkey/secret/expires_on 교체",
    "upbit": "업비트 Open API 관리에서 신규 발급(연장 불가, 서버 IP 등록) → appkey/secret/expires_on 교체",
}


class Level(IntEnum):
    OK = 0
    CONFIG = 1   # 미기재·불일치·stale — 날짜와 무관한 설정 문제(즉시성은 force로 표현)
    INFO = 2
    WARN = 3
    CRIT = 4
    EXPIRED = 5


COLOR = {Level.INFO: 0xFDD835, Level.WARN: 0xFB8C00, Level.CRIT: 0xE53935,
         Level.EXPIRED: 0x6A1B9A, Level.CONFIG: 0x757575}
ICON = {Level.INFO: "🟡", Level.WARN: "🟠", Level.CRIT: "🔴", Level.EXPIRED: "⛔", Level.CONFIG: "⚙️"}


@dataclass
class ExpiryItem:
    name: str
    provider: str
    expires_on: date | None
    used_by: tuple[str, ...] = ()
    days_left: int | None = None
    level: Level = Level.OK
    notes: list[str] = field(default_factory=list)
    force: bool = False      # 날짜 규칙과 무관하게 즉시 알림 (API 불일치 등)


def level_of(days_left: int | None) -> Level:
    if days_left is None:
        return Level.CONFIG
    if days_left < 0:
        return Level.EXPIRED
    if days_left <= DAILY_FROM:
        return Level.CRIT
    if days_left <= 7:
        return Level.WARN
    if days_left <= WINDOW:
        return Level.INFO
    return Level.OK


def should_notify(item: ExpiryItem, as_of: date) -> bool:
    """§3.1 규칙. 매일 알림으로 바꾸려면 본문을 `return item.force or item.days_left is None or item.days_left <= WINDOW`로."""
    if item.force:
        return True
    d = item.days_left
    if d is None:
        return as_of.weekday() == MONDAY
    return d in STAGE_DAYS or d <= DAILY_FROM or (0 <= d <= WINDOW and as_of.weekday() == MONDAY)


def evaluate(creds, as_of: date) -> list[ExpiryItem]:
    """creds: Iterable[Credential]  →  만료일 기준 판정 결과(전체)"""
    items = []
    for c in creds:
        d = (c.expires_on - as_of).days if c.expires_on else None
        it = ExpiryItem(c.name, c.provider, c.expires_on, tuple(c.used_by), d, level_of(d))
        if c.expires_on is None:
            it.notes.append("secrets YAML에 expires_on 미기재")
        if c.provider == "kis" and c.name in KIS_BULK_ISSUE_KEYS:
            it.notes.append("만료 시 토큰 일괄 발급 실패 → 환율·KIS·Upbit 수집 전체 중단")
            if c.name == "KIS_IRP":
                it.notes.append("KIS_IRP는 미사용인데 발급 필수 목록에 포함(F §1.1) — 정리 권장")
        elif c.provider == "upbit":
            it.notes.append("만료 시 업비트 계좌 1개만 실패")
        items.append(it)
    return items


def select(items: list[ExpiryItem], as_of: date) -> list[ExpiryItem]:
    """알림 트리거가 하나라도 있으면, 구간 안의 모든 항목을 한 메시지에 묶어 보여 준다."""
    if not any(should_notify(i, as_of) for i in items):
        return []
    shown = [i for i in items if i.force or i.level != Level.OK]
    return sorted(shown, key=lambda i: (-int(i.level), i.days_left if i.days_left is not None else 9999))


def _d_label(d: int | None) -> str:
    if d is None:
        return "만료일 미상"
    return f"D-{d}" if d > 0 else ("오늘 만료(D-day)" if d == 0 else f"만료 {-d}일 경과")


def build_embed(items: list[ExpiryItem], as_of: date) -> dict:
    worst = max(i.level for i in items)
    fields = []
    for i in items:
        lines = [
            f"만료일 **{i.expires_on.isoformat() if i.expires_on else '-'}** ({_d_label(i.days_left)})",
            f"사용 계좌: {', '.join(i.used_by) or '-'}",
            *[f"• {n}" for n in i.notes],
            f"조치: {RENEW_GUIDE.get(i.provider, '-')}",
        ]
        fields.append(_field(f"{ICON.get(i.level, '')} {i.name} ({i.provider.upper()})", "\n".join(lines), inline=False))
    return {
        "title": f"🔑 API 키 만료 점검 — {len(items)}건 확인 필요",
        "color": COLOR[worst],
        "description": f"기준일 {as_of.isoformat()} (KST) · secrets YAML의 `expires_on` 기준",
        "fields": fields,
        "footer": _footer(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def build_payload(items: list[ExpiryItem], as_of: date) -> dict:
    return _payload(build_embed(items, as_of))   # D의 _finalize(한도 6000자/25필드) + allowed_mentions 차단
```

### 4.3 Upbit 만료일 교차 검증

```python
# plugins/asset_flow/ops/key_expiry.py (계속)
import uuid

import jwt
import requests

UPBIT_API_KEYS_URL = "https://api.upbit.com/v1/api_keys"


def upbit_expire_on(access_key: str, secret: str) -> tuple[date | None, str | None]:
    """(만료일 KST, 오류명). 쿼리 파라미터가 없는 GET이라 JWT에 query_hash가 필요 없다."""
    token = jwt.encode({"access_key": access_key, "nonce": str(uuid.uuid4())}, secret)
    resp = requests.get(UPBIT_API_KEYS_URL, headers={"Authorization": f"Bearer {token}"}, timeout=(3, 5))
    if resp.status_code == 401:
        try:
            return None, resp.json().get("error", {}).get("name") or "unauthorized"
        except ValueError:
            return None, "unauthorized"
    resp.raise_for_status()
    for k in resp.json():
        if k.get("access_key") == access_key:
            # "2027-01-05T15:30:00+09:00" → KST 날짜
            return datetime.fromisoformat(k["expire_at"]).date(), None
    return None, "access_key_not_listed"


def apply_upbit_crosscheck(item: ExpiryItem, api_date: date | None, err: str | None, as_of: date) -> None:
    if err == "expired_access_key":
        item.level, item.force = Level.EXPIRED, True
        item.notes.append("Upbit API 응답: expired_access_key")
    elif err:
        item.notes.append(f"Upbit 만료일 조회 실패({err}) — YAML 값으로만 판정")
    elif api_date and api_date != item.expires_on:
        # 판정은 API 값 우선: 더 임박한 쪽 등급을 쓰고, 최소 CONFIG로 즉시 알림
        api_days = (api_date - as_of).days
        item.force = True
        item.level = max(item.level, level_of(api_days), Level.CONFIG)
        item.notes.append(f"YAML({item.expires_on}) ≠ API({api_date}, {_d_label(api_days)}) — YAML 갱신 필요")
```

- 응답 본문에는 키 목록만 있고 secret은 없다. 그래도 로그에 응답 전체를 출력하지는 않는다.
- 401의 원인은 만료 말고도 JWT 서명 오류, IP 미등록 등일 수 있다. `expired_access_key`만 만료로 판정하고, 나머지는 "조회 실패" 메모로 남긴다.

### 4.4 DAG 변경: `dags/make_token_dag.py` (3.x 전환 후 형태, 발췌)

```python
from datetime import date, datetime, timedelta

import pendulum
from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException   # 3.3: airflow.exceptions 경로는 deprecated

from callbacks import discord_failure_callback, discord_recovery_callback

KST = pendulum.timezone("Asia/Seoul")

default_args = {
    "owner": "haejun",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": discord_failure_callback,
}


def _as_of_kst() -> date:
    """기준일(KST). Param as_of가 있으면 그 값, 없으면 run_after(3.x 필수 필드)를 KST로 변환.
    date.today()는 컨테이너 TZ(UTC) 기준이라 06:50 KST(=전날 21:50 UTC)에 하루가 어긋난다."""
    ctx = get_current_context()
    override = (ctx["params"] or {}).get("as_of")
    if override:
        return date.fromisoformat(override)
    return pendulum.instance(ctx["dag_run"].run_after).in_timezone(KST).date()


@dag(
    dag_id="make_token",                       # F 권장: auth_issue_api_tokens (3.x 전환 시 일괄 변경)
    default_args=default_args,
    schedule="50 6 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=KST),
    catchup=False,
    on_success_callback=discord_recovery_callback,
    tags=["auth", "token", "setup", "daily"],
    params={
        "as_of": Param(None, type=["null", "string"], format="date",
                       description="테스트용 기준일(YYYY-MM-DD, KST). 비우면 실행일"),
    },
    doc_md="""
    ⚠️ fetch_exchange_rate.wait_for_token은 external_task_id='generate_tokens'(태스크 단위) 매칭이어야 한다.
    DAG 단위로 바꾸면 check_key_expiry 실패가 07시 체인을 막는다(작업 H §2.3).
    """,
)
def make_token_dag():

    @task(task_id="generate_tokens")
    def generate_tokens() -> None:
        from asset_flow.managers.token_manager import TokenManager
        TokenManager().TokenGenerator()

    @task(task_id="check_key_expiry", retries=1, retry_delay=timedelta(minutes=2))
    def check_key_expiry() -> dict:
        from asset_flow.config.secrets_loader import credentials          # 작업 G (가정)
        from asset_flow.ops import key_expiry as ke
        from callbacks import _post_discord                               # 작업 D §5.2

        as_of = _as_of_kst()
        creds = credentials()
        items = ke.evaluate(creds.values(), as_of)

        for it in items:
            if it.provider == "upbit":
                c = creds[it.name]
                try:
                    api_date, err = ke.upbit_expire_on(c.appkey, c.secret)
                except Exception as e:                                   # 네트워크 등 — 점검 자체는 계속
                    api_date, err = None, type(e).__name__
                ke.apply_upbit_crosscheck(it, api_date, err, as_of)

        selected = ke.select(items, as_of)
        summary = {i.name: i.days_left for i in items}
        print(f"[key_expiry] 기준일={as_of} 판정={summary} 알림대상={[i.name for i in selected]}")
        if not selected:
            raise AirflowSkipException(f"알릴 키 없음 (기준일 {as_of})")   # → TI skipped, DAG run은 success

        if not _post_discord(ke.build_payload(selected, as_of)):
            raise RuntimeError("Discord 전송 실패 — 재시도")               # retries=1 후 실패 콜백
        return {"as_of": as_of.isoformat(), "notified": [i.name for i in selected]}

    @task(task_id="verify_expiry_metadata", retries=0)
    def verify_expiry_metadata() -> None:
        """(선택, §3.4-④) 토큰 발급이 '성공한' 날에만 실행: YAML상 만료된 KIS 키가 실제로는 살아 있으면 stale."""
        from asset_flow.config.secrets_loader import credentials
        from asset_flow.ops import key_expiry as ke
        from callbacks import _post_discord

        as_of = _as_of_kst()
        stale = [i for i in ke.evaluate(credentials().values(), as_of)
                 if i.provider == "kis" and i.days_left is not None and i.days_left < 0]
        if not stale:
            raise AirflowSkipException("stale 만료일 없음")
        for i in stale:
            i.level, i.force = ke.Level.CONFIG, True
            i.notes.insert(0, "YAML상 만료됐지만 오늘 토큰 발급 성공 → expires_on 갱신 누락 의심")
        _post_discord(ke.build_payload(stale, as_of))

    tokens = generate_tokens()
    tokens >> verify_expiry_metadata()   # 하류 OK: 발급 성공을 전제로 하는 '정확도 보완' 태스크
    check_key_expiry()                   # ★ 의존성 없음 — 토큰 발급이 실패해도 실행 (§2.4)


make_token_dag()
```

설계 메모

| 요구 | 반영 |
|---|---|
| 장애 격리 | `check_key_expiry`에는 upstream이 없다. `generate_tokens`가 실패(키 만료)해도 같은 run에서 실행된다. |
| skip 의미 | 알릴 것이 없으면 `AirflowSkipException` → TI는 SKIPPED, DAG run은 SUCCESS(§2.3). `on_skipped_callback`은 등록하지 않는다(알릴 것이 없다는 뜻이므로 조용히 둔다). |
| 실패 의미 | secrets 파싱 오류, Discord 전송 실패 등은 failed → D의 실패 알림 → 다음 날 성공 시 복구 알림. `_post_discord`는 예외를 던지지 않고 bool을 반환하므로 명시적으로 raise한다. |
| `verify_expiry_metadata`의 위치 | 기본 trigger rule `all_success`라서 발급이 실패한 날에는 `upstream_failed`가 되어 실행되지 않는다. 그날은 `check_key_expiry`가 EXPIRED로 알리므로 정보 손실이 없다. |
| 2.10.4 호환 | 이 코드는 3.x 전용이다(`airflow.sdk`, `run_after`). 2.x에서 먼저 쓰려면 D처럼 import 폴백을 두고, `run_after` 대신 `data_interval_end`를 쓴다. G 로더가 3.x 전환 PR에 들어가므로(F §1.5 단계 2) **H도 같은 PR이나 그 직후 PR**에 넣는 것이 자연스럽다. |
| 마스킹 | G 로더가 `mask_secret(appkey)`를 호출한다고 가정한다(F §1.4). 이 태스크는 키 값을 출력하지 않고 credential 이름과 날짜만 남긴다. |

**token_manager의 실패 메시지 보강(§3.4-②), 발췌**

```python
# TokenManager._get_kis_token 의 HTTPError 분기
except requests.exceptions.HTTPError as e:
    hint = _expiry_hint(variable_name)          # "secrets 기준 만료일 2026-10-10 (만료 2일 경과)" 또는 ""
    raise RuntimeError(f"{variable_name} 토큰 발급 실패 (HTTP {response.status_code}) {hint}".strip()) from e


def _expiry_hint(name: str) -> str:
    try:
        from asset_flow.config.secrets_loader import credentials
        from asset_flow.ops.key_expiry import _d_label
        exp = credentials()[name].expires_on
        if exp is None:
            return ""
        d = (exp - pendulum.now("Asia/Seoul").date()).days
        return f"— secrets 기준 만료일 {exp} ({_d_label(d)})"
    except Exception:
        return ""                               # 힌트 생성 실패가 원래 예외를 가리지 않게
```

### 4.5 Discord embed 예시 (실제 전송 JSON)

기준일 2026-10-12(월), KIS 4개 키는 2026-10-15 만료, `KIS_IRP`는 2026-10-26, Upbit는 YAML과 API가 불일치하는 경우:

```json
{
  "username": "Airflow",
  "allowed_mentions": {"parse": []},
  "embeds": [{
    "title": "🔑 API 키 만료 점검 — 6건 확인 필요",
    "color": 15022389,
    "description": "기준일 2026-10-12 (KST) · secrets YAML의 `expires_on` 기준",
    "fields": [
      {"name": "🔴 KIS_ISA (KIS)", "inline": false,
       "value": "만료일 **2026-10-15** (D-3)\n사용 계좌: KIS_ISA, KIS_CMA\n• 만료 시 토큰 일괄 발급 실패 → 환율·KIS·Upbit 수집 전체 중단\n조치: KIS Developers > 신청현황에서 갱신(만료 30일 전부터) → appkey/secret/expires_on 교체"},
      {"name": "🔴 KIS_STOCK (KIS)", "inline": false, "value": "만료일 **2026-10-15** (D-3)\n사용 계좌: KIS_STOCK\n• …"},
      {"name": "🔴 KIS_PENSION_DEDUCTIBLE (KIS)", "inline": false, "value": "…"},
      {"name": "🔴 KIS_PENSION_NON_DEDUCTIBLE (KIS)", "inline": false, "value": "…"},
      {"name": "🟡 KIS_IRP (KIS)", "inline": false,
       "value": "만료일 **2026-10-26** (D-14)\n사용 계좌: -\n• 만료 시 토큰 일괄 발급 실패 → 환율·KIS·Upbit 수집 전체 중단\n• KIS_IRP는 미사용인데 발급 필수 목록에 포함(F §1.1) — 정리 권장\n조치: …"},
      {"name": "⚙️ UPBIT (UPBIT)", "inline": false,
       "value": "만료일 **2026-12-30** (D-79)\n사용 계좌: UPBIT\n• 만료 시 업비트 계좌 1개만 실패\n• YAML(2026-12-30) ≠ API(2026-12-01, D-50) — YAML 갱신 필요\n조치: 업비트 Open API 관리에서 신규 발급(연장 불가, 서버 IP 등록) → appkey/secret/expires_on 교체"}
    ],
    "footer": {"text": "oracle-airflow · <hostname>"},
    "timestamp": "2026-10-11T21:50:12+00:00"
  }]
}
```

- 키가 최대 7개(KIS 5개 + Upbit + 여유)라서 필드 25개 한도와 6000자 한도 안에 들어간다. 넘치더라도 D의 `_finalize`가 뒤쪽 필드부터 잘라 낸다. 정렬이 심각도 내림차순이므로 **덜 급한 항목부터 잘린다.**
- 특정 사용자 멘션을 원하면 `content="<@사용자ID>"`와 `allowed_mentions={"users": ["<ID>"]}`를 EXPIRED/CRIT일 때만 넣는다. D의 `_payload`는 기본으로 멘션을 모두 차단하므로 이 경우에만 덮어쓴다.

---

## 5. 테스트 방법

### 5.1 단위 테스트 (Airflow 불필요)

`tests/ops/test_key_expiry.py` 예시. `callbacks` import는 `sys.path`에 `plugins/`를 넣어 해결한다.

```python
from datetime import date, timedelta
import pytest
from asset_flow.ops import key_expiry as ke

MON = date(2026, 10, 12)     # 월요일
TUE = date(2026, 10, 13)

@pytest.mark.parametrize("d,as_of,expected", [
    (15, TUE, False), (14, TUE, True), (13, TUE, False), (10, MON, True),   # 월요일 따라잡기
    (7, TUE, True), (5, TUE, False), (3, TUE, True), (1, TUE, True),
    (0, TUE, True), (-1, TUE, True), (-30, TUE, True), (20, MON, False),
])
def test_should_notify(d, as_of, expected):
    item = ke.ExpiryItem("KIS_ISA", "kis", as_of + timedelta(days=d), days_left=d, level=ke.level_of(d))
    assert ke.should_notify(item, as_of) is expected

def test_level_boundaries():
    assert [ke.level_of(x) for x in (15, 14, 8, 7, 4, 3, 0, -1, None)] == [
        ke.Level.OK, ke.Level.INFO, ke.Level.INFO, ke.Level.WARN, ke.Level.WARN,
        ke.Level.CRIT, ke.Level.CRIT, ke.Level.EXPIRED, ke.Level.CONFIG]

def test_embed_within_discord_limits():
    items = [ke.ExpiryItem(f"K{i}", "kis", MON, days_left=0, level=ke.Level.CRIT, notes=["x" * 900]) for i in range(30)]
    emb = ke.build_payload(items, MON)["embeds"][0]
    assert len(emb["fields"]) <= 25
    assert len(emb["title"]) + len(emb["description"]) + sum(len(f["name"]) + len(f["value"]) for f in emb["fields"]) <= 6000
```

### 5.2 테스트용 secrets로 실제 경로 검증 (스테이징이나 E의 그린 스택)

1. 테스트 파일을 만든다. 운영 secrets를 복사한 뒤 `expires_on`만 바꾼다. **appkey/secret은 운영 값을 그대로 둔다.** Upbit API 대조까지 검증하기 위해서다. 이 파일도 gitignore 대상이고 `chmod 640`으로 둔다.
   ```yaml
   # /opt/airflow/secrets/test/asset_flow.secrets.yaml
   credentials:
     KIS_STOCK:   {provider: kis,   ..., expires_on: 2026-10-26}   # 기준일+14 → INFO, 단계 알림
     KIS_ISA:     {provider: kis,   ..., expires_on: 2026-10-15}   # +3  → CRIT
     KIS_PENSION_DEDUCTIBLE:     {..., expires_on: 2026-10-12}     # 0   → 오늘 만료
     KIS_PENSION_NON_DEDUCTIBLE: {..., expires_on: 2026-10-11}     # -1  → EXPIRED
     KIS_IRP:     {provider: kis,   ...}                           # 미기재 → CONFIG(월요일만)
     UPBIT:       {provider: upbit, ..., expires_on: 2099-01-01}   # API와 불일치 → 즉시 CONFIG
   ```
2. **알림은 테스트 채널로** 보낸다(E §4.5 원칙). 컨테이너 환경변수 `AIRFLOW_CONN_DISCORD_ALERT`를 테스트 webhook으로 덮어쓴다.
3. 기록을 남기지 않는 단독 실행으로 확인한다. G 로더의 파일 경로 환경변수 이름은 G를 따른다. 여기서는 `ASSET_FLOW_SECRETS_FILE`로 가정한다.
   ```bash
   docker compose run --rm \
     -e ASSET_FLOW_SECRETS_FILE=/opt/airflow/secrets/test/asset_flow.secrets.yaml \
     -e AIRFLOW_CONN_DISCORD_ALERT='{"conn_type":"http","password":"https://discord.com/api/webhooks/<테스트>"}' \
     airflow-scheduler airflow tasks test make_token check_key_expiry 2026-10-12
   ```
   `airflow tasks test`는 의존성을 무시하고 TI 상태를 DB에 기록하지 않는다. 그래서 `generate_tokens`를 건드리지 않는다(KIS 토큰을 발급하지 않는다).
4. 날짜를 이동시키는 시나리오는 운영 secrets를 그대로 두고 **`as_of` Param**으로 확인한다. UI의 Trigger 폼에서 `as_of`를 입력한다.

| # | 조건 | 기대 결과 |
|---|---|---|
| H1 | 위 테스트 파일, as_of=2026-10-12(월) | 메시지 1건. 6개 필드가 심각도 순서로 나온다(⛔ → 🔴 → 🟡 → ⚙️). 보라색이다. 태스크는 success다. |
| H2 | as_of=2026-10-13(화) | KIS_IRP(미기재)는 빠진다. 나머지는 같다. |
| H3 | 모든 `expires_on`을 2027년 이후로 설정, as_of=화요일 | 메시지가 없다. 태스크는 **skipped**, DAG run은 **success**다. 복구 알림도 없다. |
| H4 | Discord Connection을 잘못된 URL로 설정 | `_post_discord`가 False를 반환한다 → 재시도 1회 → failed → 실패 콜백(이것도 실패하며 로그만 남음). DAG run은 failed. 다음 정상 run에서 복구 알림이 나간다. |
| H5 | secrets YAML 문법 오류 | 로더 예외로 failed가 되고 실패 알림이 나간다. `generate_tokens`도 같은 로더를 쓰므로 함께 실패한다. 이는 정상이다(설정 장애). |
| H6 | 정기 run에서 `generate_tokens`가 실패하도록 함(테스트 파일에서 KIS_ISA의 secret을 고의로 틀리게) | `generate_tokens`는 failed, **`check_key_expiry`는 독립적으로 실행돼 알림을 보낸다.** 실패 알림 본문에 §3.4-②의 만료일 힌트가 붙는다. `verify_expiry_metadata`는 `upstream_failed`다. |
| H7 | KIS_PENSION_NON_DEDUCTIBLE의 `expires_on`이 과거인데 키가 실제로 유효 | `verify_expiry_metadata`가 "갱신 누락 의심" CONFIG 알림을 보낸다. |

5. 회귀 확인 항목: `fetch_exchange_rate.wait_for_token`이 H3~H5 어떤 경우에도 `generate_tokens`의 성공만 보고 통과하는지 확인한다(§2.3).

### 5.3 타임존 검증 포인트

- 정기 run의 `run_after`는 `2026-10-11T21:50:00+00:00`(UTC)이고, `_as_of_kst()`는 **2026-10-12**를 반환해야 한다. 태스크 로그의 `[key_expiry] 기준일=` 줄로 확인한다.
- `date.today()`나 `datetime.now()`(naive)를 쓰면 이 run에서 2026-10-11이 나온다. 그러면 D-14 알림이 하루 늦어지고, "오늘 만료"가 "D-1"로 표시된다. 코드 리뷰 체크리스트에 "만료 계산에 naive 날짜 금지"를 넣는다.
- YAML의 `expires_on: 2027-03-14`(따옴표 없음)는 `yaml.safe_load`가 `datetime.date`로 파싱한다. 따옴표를 붙이면 문자열이 되므로, G 로더에서 `date.fromisoformat`으로 정규화하는 것을 G에 요청 사항으로 남긴다.

---

## 6. 남은 불확실성과 G에 대한 요청

1. **KIS 개인 키의 정확한 만료 규칙**(마지막 날 포함 여부, 갱신 시 appkey 재발급 여부, 만료 후 `tokenP` 오류 코드)은 공식 문서에서 찾지 못했다. 포털 "신청현황" 화면과 첫 만료 사이클의 로그로 확정한다.
2. **Upbit "1년, 연장 불가"**는 고객센터 문구를 인용한 2차 자료다. 다만 설계가 `/v1/api_keys`의 `expire_at`에 의존하므로 이 수치가 틀려도 영향은 없다.
3. Upbit 조회 전용 키에서 `/v1/api_keys` 호출에 IP 허용 목록 등록이 필요한지 확인해야 한다. 공식 가이드는 "호출지의 IP를 허용 목록에 등록"하라고만 적고 있다. 첫 실행에서 401(`no_authorization_ip`류)이 나오면 서버 IP를 등록한다.
4. **G에 대한 요청**: ① credential 단위의 `expires_on: date | None`, ② credential → 계좌(`used_by`) 역참조, ③ `appkey`의 `mask_secret` 처리, ④ YAML 파일 경로를 환경변수로 덮어쓸 수 있게 해서(§5.2 테스트) 날짜 문자열을 `date`로 정규화.
5. 근본 대책(이 문서 범위 밖): `TokenGenerator`가 credential 하나의 실패로 전체 토큰 파일을 만들지 못하는 구조와 `KIS_IRP` 데드 의존성. 고치면 "KIS 키 하나 만료 = 전체 장애"가 "해당 계좌만 실패"로 줄어든다. 그때는 §4.2의 `KIS_BULK_ISSUE_KEYS` 영향 문구도 함께 고친다.

---

## 출처 (2026-10-12 접속)

**한국투자증권**
- 오픈 API 서비스 이용 약관(제휴 이용기관_사용자), 2025-06-04 개정, 제5조④(1년 단위 재신청), 제7조①3(1년 미이용 해지) — **공식**: https://file.koreainvestment.com/updata/namo/09101613%EC%98%A4%ED%94%88API%EC%84%9C%EB%B9%84%EC%8A%A4%EC%9D%B4%EC%9A%A9%EC%95%BD%EA%B4%80_%EC%A0%84%EB%AC%B8.pdf
- eFriend Expert(Open API) 안내, "사용기간 : 신청일로부터 1년", 3개월 미거래 해지 — **공식(다른 상품)**: https://securities.koreainvestment.com/main/customer/systemdown/OpenAPI.jsp
- KIS Developers 제휴사 토큰 관리(만기일 저장, 1개월 전 안내 권고) — **공식(제휴사 대상)**: https://apiportal.koreainvestment.com/provider-doc4 , 기존계좌 API 신청: https://apiportal.koreainvestment.com/provider-doc2
- KIS 공식 GitHub 예제(만료일 조회 API 언급 없음): https://github.com/koreainvestment/open-trading-api
- 위키독스 「01) 오픈API 서비스 신청」(1년, 만료 1개월 전 문자, 30일 전 갱신, 갱신 시 키 재발급) — **제3자**: https://wikidocs.net/159301
- AlgoLab, KIS API 발급 가이드(SECRET 재조회 불가, 토큰 24시간) — **제3자**: https://algolab.co.kr/blog/kis-api-key-guide-30min , https://algolab.co.kr/blog/kis-api-access-token-expiry-refresh-2026

**Upbit**
- API 키 리스트 조회 `GET /v1/api_keys`(`expire_at` KST) — **공식**: https://docs.upbit.com/kr/reference/list-api-keys
- Changelog: API 키 목록 조회 API 추가 — **공식**: https://docs.upbit.com/kr/changelog/open_api_keys
- Open API 안내·FAQ(`expired_access_key`, 만료된 Key 삭제) — **공식**: https://docs.upbit.com/kr/docs/api-key
- 인증 요청 가이드(IP 허용 목록, Secret 1회 표시, JWT) — **공식**: https://docs.upbit.com/kr/docs/create-authorization-request
- 고객센터 "Open API를 사용하고 싶어요"(1년·연장 불가 문구의 원출처로 인용됨, 직접 열람 실패 403): https://support.upbit.com/hc/ko/articles/4403180359705
- 키 유효기간 갱신 사례 — **제3자**: https://mg.jnomy.com/%EC%97%85%EB%B9%84%ED%8A%B8-open-api-%ED%82%A4-%EC%9C%A0%ED%9A%A8%EA%B8%B0%EA%B0%84-%EA%B0%B1%EC%8B%A0%ED%95%98%EB%8A%94-%EB%B0%A9%EB%B2%95

**Airflow**
- Callbacks(`on_skipped_callback`은 AirflowSkipException일 때만, 분기·trigger rule로 skip되면 호출 안 됨): https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/callbacks.html
- ShortCircuitOperator / `@task.short_circuit`(`ignore_downstream_trigger_rules`): https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/operators/python.html
- Deadline Alerts: https://airflow.apache.org/docs/apache-airflow/stable/howto/deadline-alerts.html
- 3.0 호환성 표(PR #40936 "skip 시 on_success_callback 미실행", breaking): https://cwiki-test.apache.org/confluence/x/BpGMEw
- CLI `airflow tasks test`: https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html
- 소스 확인(스크래치패드에 wheel만 풀어 읽음):
  - `apache-airflow-providers-standard==1.20.0`: `operators/python.py`(`ShortCircuitOperator.execute`, `get_flat_relatives`/`get_direct_relatives`, teardown 제외), `utils/skipmixin.py`(`SkipMixin.skip`, `DownstreamTasksSkipped`)
  - `apache-airflow-task-sdk==1.3.2`: `execution_time/task_runner.py`(`except DownstreamTasksSkipped` → success, `except AirflowSkipException` → SKIPPED, `finalize()`의 상태별 콜백, `_run_task_state_change_callbacks` 예외 삼킴), `exceptions.py`(`AirflowSkipException`), `definitions/deadline.py`(`DeadlineAlert`, `DeadlineReference`)
  - `apache-airflow-core==3.3.2`: `models/dagrun.py`(`update_state`: success_states면 run SUCCESS 후 DAG on_success_callback), `utils/state.py`(`success_states = {SUCCESS, SKIPPED}`), `exceptions.py`(`_DEPRECATED_EXCEPTIONS`에 `AirflowSkipException`)

**저장소 근거**
- `plugins/asset_flow/managers/token_manager.py`(`REQUIRED_TOKEN_KEYS`, `TokenGenerator`의 일괄 dict 발급, `GetTokens`의 재발급, KST 날짜)
- `dags/make_token_dag.py`(06:50, `retries=2`, `generate_tokens`), `dags/fetch_exchange_rate_dag.py:80-88`(`external_task_id='generate_tokens'`)
- `dags/fetch_asset_daily_dag.py`(`get_cma_cash`의 ISA 토큰 공유, 계좌별 get/upload 불변식, `get_upbit_assets`)
- `plugins/asset_flow/clients/kis_client.py`(헤더 `appKey`/`appSecret`), `plugins/asset_flow/clients/upbit_client.py`, `plugins/asset_flow/config/kis.py`(토큰 1일 유효 주석)
- 선행 보고서: `docs/airflow3_D_backup_discord.md` §5.2, `docs/airflow3_E_parallel_cutover.md` §4.5, `docs/airflow3_F_variables_naming.md` §1.1·§1.3·§2.6
