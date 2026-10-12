# secrets — 자산 수집 자격증명·계좌 설정

이 디렉터리는 **서버에만 존재하는 비밀 파일**을 두는 곳이다. git에는 이 README와 `*.example.yaml` 템플릿만 올라간다.

| 파일 | git | 내용 |
|---|---|---|
| `asset_flow.secrets.yaml` | ❌ (gitignore) | 실제 값 — KIS/Upbit 앱키·시크릿, 만료일, 계좌번호, 계좌→키 참조. **단일 진실 공급원** |
| `asset_flow.secrets.example.yaml` | ✅ | 형식 템플릿. 더미 값으로 검증을 통과한다 |
| `README.md` | ✅ | 이 문서 |

설계 근거: [`docs/airflow3_G_secrets_schema.md`](../docs/airflow3_G_secrets_schema.md), [`docs/airflow3_H_key_expiry_alert.md`](../docs/airflow3_H_key_expiry_alert.md), [`docs/airflow3_I_yaml_driven_automation.md`](../docs/airflow3_I_yaml_driven_automation.md)

> **현재 상태**: 템플릿만 있다. 이 파일을 읽는 로더(`plugins/asset_flow/secrets_config.py`)와 DAG 연동은 아직 구현되지 않았다. 지금 운영 중인 2.10.4는 여전히 Airflow Variable(`KIS_*`, `UPBIT` 등)을 읽는다.

---

## 1. 무엇을 어디에 두나

| 값 | 위치 |
|---|---|
| KIS appkey/appsecret, Upbit access/secret, 키 만료일 | **이 파일** `credentials` |
| 계좌번호(CANO·은행 계좌번호), 상품코드, 계좌 표시명, 계좌→키 참조, 활성 여부 | **이 파일** `accounts` |
| 테이블명, 펀드코드, 계좌 유형별 multiplier, 만료 경고 일수 | `dags/config/asset_flow.yaml` (커밋, 비밀 아님) |
| Discord webhook, Fernet/JWT 키, DB 비밀번호 | `.env` (gitignore) — **이 파일에 넣지 않는다.** 이 파일이 깨졌을 때도 알림은 나가야 하기 때문 |

---

## 2. 처음 만들기

```bash
cd ~/oracle-airflow3                       # 운영 디렉터리
cp secrets/asset_flow.secrets.example.yaml secrets/asset_flow.secrets.yaml
vi secrets/asset_flow.secrets.yaml         # 더미 값을 실제 값으로 교체

# 권한: 컨테이너가 user ${AIRFLOW_UID}:0 으로 실행되므로
sudo chown ${AIRFLOW_UID:-50000}:0 secrets/asset_flow.secrets.yaml
chmod 640 secrets/asset_flow.secrets.yaml

git check-ignore -v secrets/asset_flow.secrets.yaml   # 반드시 ignore 대상으로 나와야 한다
```

docker-compose에서는 **디렉터리 단위로 읽기 전용 마운트**한다. 파일 단위로 마운트하면 편집기가 새 파일로 저장(inode 변경)했을 때 컨테이너가 옛 파일을 계속 본다.

```yaml
# x-airflow-common 또는 airflow-scheduler (LocalExecutor면 태스크가 scheduler에서 돈다)
environment:
  ASSET_FLOW_SECRETS: /opt/airflow/secrets/asset_flow.secrets.yaml
volumes:
  - ./secrets:/opt/airflow/secrets:ro
```

---

## 3. 값 바꾸기 (계좌 추가, 키 교체, 만료일 갱신)

**이 파일만 고치면 된다. 코드 수정·재시작은 필요 없다.** 다음 태스크 실행부터 반영된다.

```bash
cd ~/oracle-airflow3/secrets
cp asset_flow.secrets.yaml asset_flow.secrets.yaml.new
cp asset_flow.secrets.yaml backup/asset_flow.secrets.$(date +%Y%m%d_%H%M).yaml   # 백업 (backup/도 gitignore)
vi asset_flow.secrets.yaml.new

# ① 검증 — 통과하면 "OK: credentials=N accounts=M"과 임박 만료 목록이 나온다
docker compose exec airflow-scheduler \
  python -m asset_flow.secrets_config /opt/airflow/secrets/asset_flow.secrets.yaml.new

# ② 통과했을 때만 같은 디렉터리 안에서 교체 (mv는 원자적)
mv asset_flow.secrets.yaml.new asset_flow.secrets.yaml
```

| 변경 | 할 일 | 결과 |
|---|---|---|
| **키 갱신** (KIS 1년 재신청, Upbit 재발급) | 해당 credential의 `app_key`·`app_secret`·`expires_on` **세 값을 한 번에** 교체 | 토큰 파일의 키 지문이 달라져 그 키만 자동 재발급. 만료 알림이 멈춘다 |
| **계좌 추가** | `accounts`에 블록 추가. 새 키면 `credentials`에도 추가 | 다음 run부터 수집 대상에 포함 (동적 태스크 매핑) |
| **계좌 해지** | 항목을 지우지 말고 `enabled: false` | 수집만 멈추고 과거 데이터는 그대로 |
| **계좌번호 변경** | `cano`/`account_no` 수정 | ⚠ `account_code`가 바뀌어 **새 계좌로 취급되고 시계열이 끊긴다.** 같은 계좌의 번호 정정이면 업무 DB 마이그레이션이 필요하다 (I §3.3) |
| **CMA처럼 키 공유** | 계좌의 `credential`에 다른 credential id 지정 | 공유 관계가 파일에 명시된다 |

키 갱신 시 참고:
- **KIS**: KIS Developers '신청현황'에서 갱신. 갱신하면 appkey/secret이 재발급된다는 자료가 있으므로 세 값을 모두 확인한다. 만료일 조회 API가 없어 `expires_on`은 직접 옮겨 적는다.
- **Upbit**: 연장이 안 되므로 새 키를 발급한다. **서버 IP를 허용 목록에 다시 등록**하고, 만료된 키는 'Open API 관리'에서 삭제한다.
- 당일 즉시 검증하려면 `data/tokens/<오늘>_token.json`을 지우고 `make_token`을 수동 실행한다.

---

## 4. 검증이 거부하는 실수

| 실수 | 오류 예 |
|---|---|
| 같은 id 두 번 (블록 복사 후 이름 안 바꿈) | `accounts.id 중복: ['IRP']` |
| 한 블록 안에 같은 필드 두 번 | `중복 키 'display_name': 72행 (처음 71행)` |
| 따옴표 없는 숫자 | `accounts.0.product_code: Input should be a valid string` |
| 계좌번호 자릿수 틀림 | `accounts.1.cano: String should match pattern '^\d{8}$'` |
| 없는 credential 참조 (오타) | `accounts[KIS_CMA].credential: 정의되지 않은 'KIS_IAS'` |
| `expires_on` 누락 | `credentials.4.expires_on: Field required` |
| 존재하지 않는 날짜 | `credentials.0.expires_on: ... day is out of range for month` |
| 필드명 오타 | `credentials.0.expire_on: Extra inputs are not permitted` |
| kind에 맞지 않는 필드 | `accounts.6: kind=manual_fund: 필수 누락 [], 허용 안 됨 ['cano']` |
| 같은 계좌번호 두 번 | `accounts: 같은 계좌번호(account_code)가 두 번 이상 등장` |

오류 메시지에는 **위치만 나오고 값은 나오지 않는다.** 검증에 실패한 파일로 수집이 돌면 태스크가 실패하고 Discord로 알림이 간다(예전 값으로 몰래 대체하지 않는다).

---

## 5. 하지 말 것

- 이 파일을 git에 올리거나, 채팅·이슈·스크린샷에 붙이지 않는다.
- Discord webhook을 이 파일에 넣지 않는다 (`.env`의 `AIRFLOW_CONN_DISCORD_ALERT`).
- 블루/그린 섀도 기간에 운영 파일을 그린에 그대로 복사하지 않는다. 스테이징 전용 파일을 따로 둔다 (KIS 토큰 경합, E §4.3).
- 백업 사본을 저장소 밖으로 옮길 때는 암호화한다 (`gpg -c` 등).
