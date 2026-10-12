# secrets — 자산 수집 자격증명·계좌 설정

서버에만 두는 비밀 파일 디렉터리. git 에는 이 README 와 `*.example.yaml` 만 올라간다.

| 파일 | git | 내용 |
|---|---|---|
| `asset_flow.secrets.yaml` | ❌ | KIS/Upbit/공공데이터 키·만료일, 계좌번호, 계좌→키 참조 (**단일 진실 공급원**) |
| `asset_flow.secrets.example.yaml` | ✅ | 형식 템플릿 (더미 값으로 검증 통과) |

- 로더: `plugins/asset_flow/secrets_config.py` (태스크 실행 시마다 다시 읽음 → 수정 즉시 반영, 재시작 불필요)
- 마운트: scheduler 에만 디렉터리 단위 읽기 전용 (`./secrets:/opt/airflow/secrets:ro`)

## 1. 무엇을 어디에 두나

| 값 | 위치 |
|---|---|
| 앱키·시크릿·만료일 | 이 파일 `credentials` |
| 계좌번호·상품코드·표시명·kind·활성 여부·계좌→키 참조 | 이 파일 `accounts` |
| 환율·휴장일 API 가 쓸 키 | 이 파일 `services` |
| 테이블명·펀드코드·수기 유형 multiplier·만료 알림 규칙 | `dags/config/asset_flow.yaml` (커밋) |
| Discord webhook·Fernet/JWT·DB 비밀번호 | `.env` — 이 파일이 깨져도 알림은 나가야 하므로 분리 |

## 2. 처음 만들기

```bash
cd ~/airflow3
cp secrets/asset_flow.secrets.example.yaml secrets/asset_flow.secrets.yaml
vi secrets/asset_flow.secrets.yaml           # 더미 값을 실제 값으로
chmod 700 secrets && chmod 640 secrets/asset_flow.secrets.yaml
```

## 3. 검증

```bash
cd ~/airflow3
docker compose exec airflow-scheduler bash -c \
  'cd /opt/airflow/plugins && python -m asset_flow.secrets_config /opt/airflow/secrets/asset_flow.secrets.yaml'
# OK: credentials=N accounts=M  (+ 14일 내 만료 키 WARN)
# 종료코드 0 통과 / 1 검증 실패 / 2 경고
```

- `--previous <변경 전 파일>`: 계좌번호(account_code) 변경 경고, 키 지문 변경 안내
- 오류 메시지에는 위치만 나오고 값은 나오지 않는다. 검증 실패 파일로는 수집이 실패하고 Discord 알림이 간다 (옛 값으로 대체하지 않음).

## 4. 값 바꾸기

```bash
cd ~/airflow3/secrets
cp -p asset_flow.secrets.yaml asset_flow.secrets.yaml.new && vi asset_flow.secrets.yaml.new
# 3번 검증을 .new 파일로 실행 → 통과하면
mv asset_flow.secrets.yaml.new asset_flow.secrets.yaml      # 같은 디렉터리 안 mv = 원자적 교체
```

| 변경 | 할 일 | 결과 |
|---|---|---|
| 키 갱신 (KIS 1년 재신청, Upbit 재발급) | `app_key`·`app_secret`·`expires_on` 함께 교체 | 키 지문 변경 → 그 키만 자동 재발급, 만료 알림 중지 |
| 계좌 추가 | `accounts` 에 블록 추가 (새 키면 `credentials` 도) | 다음 run 부터 수집 (동적 매핑) |
| 계좌 해지 | 삭제 대신 `enabled: false` | 수집만 중지, 과거 데이터 유지 |
| 계좌번호 변경 | `cano`/`account_no` 수정 | ⚠ `account_code` 가 바뀌어 새 계좌로 취급 (시계열 단절) |
| 키 공유 (CMA → ISA) | 계좌 `credential` 에 다른 credential id | 공유 관계가 파일에 명시됨 |

- KIS: KIS Developers '신청현황'에서 갱신 (만료일 조회 API 없음 → `expires_on` 직접 기재)
- Upbit: 연장 불가 → 새 키 발급, **서버 IP 허용 목록 재등록**, 만료 키 삭제
- 당일 바로 확인: `auth_issue_api_tokens` 를 수동 실행 (지문 바뀐 키만 재발급)

## 5. 검증이 거부하는 실수

| 실수 | 오류 예 |
|---|---|
| 같은 id 두 번 | `accounts.id 중복: ['IRP']` |
| 한 블록에 같은 필드 두 번 | `중복 키 'display_name': 72행 (처음 71행)` |
| 따옴표 없는 숫자 | `accounts.0.product_code: Input should be a valid string` |
| 계좌번호 자릿수 | `accounts.1.cano: String should match pattern '^\d{8}$'` |
| 없는 credential 참조 | `accounts[KIS_CMA].credential: 정의되지 않은 'KIS_IAS'` |
| `expires_on` 누락 | `credentials.4.expires_on: Field required` |
| 필드명 오타 | `credentials.0.expire_on: Extra inputs are not permitted` |
| kind 에 맞지 않는 필드 | `accounts.6: kind=manual_fund: 필수 누락 [], 허용 안 됨 ['cano']` |

## 6. 하지 말 것

- 이 파일을 git·채팅·이슈·스크린샷에 올리지 않는다.
- Discord webhook 을 이 파일에 넣지 않는다 (`.env`).
- 사본을 서버 밖으로 옮길 때는 암호화한다 (`gpg -c`).
