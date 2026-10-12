# 작업 D — PostgreSQL 16 이관 백업 런북 + Discord 실패 알림 (Airflow 3.3.2)

- 작성일: 2026-10-12
- 대상 저장소: `oracle-airflow`. 읽기 전용으로 분석했고, 이 보고서 외의 파일은 수정하지 않았다.
- 현재 구성: `apache/airflow:2.10.4` + docker-compose(LocalExecutor), 메타DB는 compose 내부 `postgres:13`(볼륨 `postgres-db-volume`), 자산 업무 DB는 호스트 서버의 별도 PostgreSQL(`host.docker.internal` 경유, Connection `postgres_asset`), 실패 알림은 `plugins/callbacks.py`의 Slack webhook(Variable `SLACK_WEBHOOK_URL`)
- 기준 버전: **Airflow 3.3.2**(2026-09-17, 최신 안정판). 요청서의 "3.10"은 존재하지 않는 버전이라 3.3.2로 작성했다. 3.1.x와 다른 부분은 `> 3.1.x 주석`으로 따로 표시했다.
- 선행 문서와의 관계: [A 코드 영향](airflow3_A_code_impact.md) §4.3·§4.4(콜백 실행 위치, `ti.get_previous_dagrun`), [B 인프라](airflow3_B_infra.md) §5·§7(PG16 필요성, 단계 1~2), [C 전략](airflow3_C_release_strategy.md) §5(Phase 0~3)의 결론을 그대로 따른다. 이 문서는 **B §7 단계 1~2를 실행 가능한 런북으로 구체화**하고, **A §4.4 콜백 수정을 Discord로 이식**한다.
- 근거 표기: "소스 확인"은 PyPI wheel을 스크래치패드에 풀어서 직접 읽은 항목이다(`apache-airflow-providers-discord==3.12.3`, `apache-airflow-providers-apprise==2.4.0`, `apache-airflow-task-sdk==1.3.2`).

---

## 0. 요약 (TL;DR)

| 영역 | 결론 |
|---|---|
| 메타DB 백업 형식 | **`pg_dumpall --globals-only`(역할) + `pg_dump -Fc`(airflow DB)** 조합을 쓴다. B §7의 `pg_dumpall` 평문 SQL 한 벌보다 복원 검증(`pg_restore -l`)과 부분·병렬 복원이 쉽고, 압축된다. 추가로 **컨테이너를 멈춘 상태에서 볼륨 tar 스냅샷**을 떠서 PG13 롤백용으로 쓴다. |
| 덤프 도구 버전 | PostgreSQL 공식 권고에 따라 **PG16 클라이언트(`postgres:16` 이미지)로 PG13 서버를 덤프**한다. |
| 함정 1 | PG13 이미지는 비밀번호를 **md5**로 저장하고, PG16 이미지의 `pg_hba`는 **scram-sha-256**을 쓴다. globals를 복원하면 `airflow` 역할의 비밀번호가 md5 해시로 덮여 **Airflow가 접속하지 못할 수 있다.** 복원 직후 `ALTER ROLE ... PASSWORD`로 다시 해시한다(§1.6). |
| 함정 2 | B §7은 덤프를 저장소 디렉터리(`$PWD`)에 쓴다. `.gitignore`에 `*.sql`/`*.dump`가 없어서 **KIS 키가 평문으로 들어 있는 덤프**(`FERNET_KEY=''`, B §0 #7)가 커밋될 위험이 있다. **저장소 밖**(`/srv/backup/oracle-airflow/`)에 저장한다. |
| 검증 | 파일 수준: `sha256sum`, `pg_restore -l`. 데이터 수준: 테이블별 **정확 행수**와 **정렬된 행 텍스트의 md5**를 원본과 복원본에서 각각 뽑아 `diff`한다(§1.4 SQL). Airflow 수준: `alembic_version`, `airflow db check`. |
| 자산 업무 DB | Airflow 3.3.2는 업무 DB의 PG 버전을 제약하지 않는다. 따라서 **업무 DB는 "백업 필수, PG16 이관은 선택"**이다. 이관할 경우 **같은 호스트에 PG16을 다른 포트(5433)로 나란히 띄우고 `postgres_asset` Connection의 포트만 바꾸는** 방식을 권장한다. 롤백은 Connection을 원래대로 돌리면 된다(§2). |
| 보관 | 덤프는 **90일**, tar 스냅샷과 PG13 볼륨은 **3.x 첫 정기 사이클 성공 후 14일**(B §7 "1~2주"와 일치) 동안 보관한다. 2차 사본은 암호화해 서버 밖에 둔다. |
| Discord 권장안 | **(a) `requests`로 Discord webhook을 직접 호출하는 콜백**. 2.10.4와 3.3.2에서 같은 코드로 동작하므로 C의 Phase 0(업그레이드 전 선반영)에 넣을 수 있다. 또 embed 한도에 맞춘 자르기, 429 처리, 예외 방어를 직접 통제할 수 있다. |
| (b) discord provider | `DiscordNotifier`는 **있다**(3.12.3). 하지만 **`apache-airflow>=2.11.0`을 요구해 2.10.4에는 설치할 수 없다.** embed는 1개만 보낼 수 있고, 한도를 넘으면 자르지 않고 `ValueError`를 낸다. 복구 판정 로직도 없어서 결국 래퍼 콜백이 필요하다(§4.2). |
| (c) Apprise | 멀티 채널(Slack과 Discord 동시 발송)에는 유리하다. 하지만 embed 필드 구조를 통제할 수 없고 의존성이 늘어난다. |
| (d) Slack 호환 `/slack` | 현재 payload는 **Block Kit `blocks`**이다. Discord 문서에는 Block Kit 지원이 명시돼 있지 않고 `mrkdwn`은 미지원으로 적혀 있다. 그래서 payload를 legacy attachment로 다시 써야 해서 "재사용"이 되지 않는다. 비상용 임시 수단으로만 쓴다. |
| 테스트 | 기존 `test_slack_alert`는 태스크가 항상 실패하므로 **복구 알림을 검증할 수 없다.** Param으로 성공/실패를 고르는 테스트 DAG와, logical_date를 지정하는 트리거 절차를 제시한다(§5.5). |

---

## 1. 메타DB(PG13 컨테이너) 백업 → PG16 이관 런북

B §7의 단계 1(백업)과 단계 2(PG13→16, Airflow 2.10.4는 그대로)를 구체화한다. C §5.1의 **Phase 1**에 해당한다. **Airflow 버전은 바꾸지 않고 PG만 올린다**(2.10.4는 PG 12~16을 지원한다. B §5.1).

### 1.0 형식 선택 근거

| 후보 | 장점 | 단점 | 이 런북에서의 역할 |
|---|---|---|---|
| `pg_dumpall` (평문 SQL) | 역할·DB·권한이 한 파일에 들어간다. | 압축되지 않는다. 내용 목록을 볼 수 없고 `psql`로 처음부터 끝까지 재생해야 한다. 병렬 복원이나 일부 테이블만 복원할 수 없다. | **`--globals-only`로 역할·테이블스페이스만** 받는다. |
| `pg_dump -Fc` (custom) | 압축된다. `pg_restore -l`로 TOC를 검증할 수 있다. `-j` 병렬 복원, 일부 객체만 복원, `--single-transaction`을 쓸 수 있다. 공식 문서도 "most flexible output format"이라고 한다. | DB 하나만 담고 역할은 담지 않는다. | **airflow DB 본체** |
| 볼륨 tar 스냅샷 | 바이트 단위로 원본을 보존한다. PG13으로 즉시 롤백할 수 있다. | 같은 메이저 버전(13)에서만 쓸 수 있다. 서버가 정지된 상태에서만 일관성이 보장된다. | **롤백 보험** |
| `pg_upgrade` | 대용량 DB에서 빠르다. | 공식 `postgres` 이미지 두 개의 바이너리를 한 컨테이너에 모아야 해서 compose 환경에서는 번거롭다. 메타DB는 작아서(수백 MB 이하로 추정) 얻는 이득이 없다. | 쓰지 않음 |

- **PG16 클라이언트로 덤프한다.** PostgreSQL 공식 업그레이드 문서는 "It is recommended that you use the pg_dump and pg_dumpall programs from the newer version of PostgreSQL"이라고 권고한다. 새 버전의 pg_dump가 구버전 서버를 덤프하는 것은 지원되지만, 그 반대는 지원되지 않는다.
- **덤프 전에 Airflow를 정지한다.** 덤프 자체는 MVCC 스냅샷이라 실행 중에도 일관성이 있다. 하지만 덤프 시점 이후의 run 이력이 유실되므로, 이관 창에서는 쓰기를 막는다.

### 1.1 사전 변수와 사전 점검

> 아래 명령은 **Airflow가 도는 리눅스 서버**에서 저장소 루트(`docker-compose.yaml`이 있는 곳)를 기준으로 bash로 실행한다.

```bash
# ── 공통 변수 ─────────────────────────────────────────────
set -euo pipefail
export TS=$(date +%Y%m%d_%H%M)
export BK=/srv/backup/oracle-airflow/$TS          # 저장소 밖! (.gitignore에 *.dump 없음)
sudo mkdir -p "$BK" && sudo chown "$USER" "$BK" && chmod 700 "$BK"

# compose 프로젝트명/볼륨/네트워크 확인 (디렉터리명 기반이 기본: oracle-airflow)
docker compose ls
docker volume ls --format '{{.Name}}' | grep postgres-db-volume
docker network ls --format '{{.Name}}' | grep -E '_default$'
export PROJ=oracle-airflow                         # 위 출력으로 확인 후 수정
export VOL13=${PROJ}_postgres-db-volume
export NET=${PROJ}_default

# 디스크 여유 (DB 크기 × 3 이상 권장: dump + tar + 새 볼륨)
docker compose exec -T postgres psql -U airflow -d airflow -Atc \
  "select pg_size_pretty(pg_database_size('airflow'));"
df -h /srv/backup /var/lib/docker
```

```bash
# ── 2.10.4에서 할 사전 정리 (B §7 단계 0과 동일, 선택) ──
docker compose exec airflow-scheduler airflow dags reserialize
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler airflow db clean \
  --clean-before-timestamp "$(date -d '-90 days' +%F)" --skip-archive --yes
```

### 1.2 정지와 백업 (서비스 영향: 약 5~10분, 06:50~07:10은 피한다)

```bash
# 1) 쓰기 차단: Airflow 컴포넌트만 정지 (postgres는 유지)
docker compose stop airflow-webserver airflow-scheduler

# 2) 원본 서버 버전 기록
docker compose exec -T postgres psql -U airflow -d airflow -Atc "select version();" | tee "$BK/server_version.txt"
docker compose exec -T postgres psql -U airflow -d airflow -Atc "select version_num from alembic_version;" | tee "$BK/alembic_version.txt"

# 3) 덤프 — PG16 클라이언트로, compose 네트워크 안에서 실행
PGRUN="docker run --rm --network $NET -e PGPASSWORD=airflow -v $BK:/bk postgres:16"
$PGRUN pg_dumpall -h postgres -U airflow --globals-only -f /bk/globals.sql
$PGRUN pg_dump    -h postgres -U airflow -d airflow -Fc -Z 6 -f /bk/airflow.dump
$PGRUN pg_dump    -h postgres -U airflow -d airflow --schema-only -f /bk/airflow_schema.sql   # 사람이 읽는 참고용

# 4) 검증용 지표 추출 (§1.4 SQL을 파일로 저장해 두고 실행)
docker compose exec -T postgres psql -U airflow -d airflow -At -v ON_ERROR_STOP=1 \
  < verify_counts.sql > "$BK/src_counts.tsv"
docker compose exec -T postgres psql -U airflow -d airflow -At -v ON_ERROR_STOP=1 \
  < verify_md5.sql    > "$BK/src_md5.tsv"

# 5) PG13 볼륨 tar 스냅샷 — 반드시 postgres까지 멈춘 뒤 (파일 일관성)
docker compose stop postgres
docker run --rm -v $VOL13:/v:ro -v $BK:/bk alpine \
  sh -c 'cd /v && tar czf /bk/pgdata13_volume.tgz .'

# 6) 체크섬 + 아카이브 TOC 검증
( cd "$BK" && sha256sum globals.sql airflow.dump airflow_schema.sql pgdata13_volume.tgz > SHA256SUMS )
$PGRUN pg_restore -l /bk/airflow.dump > "$BK/airflow.toc" && wc -l "$BK/airflow.toc"

# 7) 비밀값 동봉 메모: 현재 FERNET_KEY=''(평문). 이후 Fernet 키를 도입했다면 키도 함께 보관해야 복원 후 복호화 가능
echo "FERNET_KEY at backup time: <empty>" > "$BK/README.txt"
```

> `verify_counts.sql`과 `verify_md5.sql`은 §1.4의 SQL을 저장한 파일이다. 저장소에 두지 말고 `$BK` 옆의 작업 디렉터리(예: `/srv/backup/oracle-airflow/scripts/`)에 둔다.

### 1.3 복원 리허설 (운영 볼륨을 건드리지 않음)

본 이관 전에 **임시 PG16 컨테이너에 복원해서 검증 지표가 같은지 먼저 확인한다.** 운영 compose와 상관없는 컨테이너라서 실패해도 영향이 없다.

```bash
docker run -d --name pg16-rehearsal -e POSTGRES_USER=airflow -e POSTGRES_PASSWORD=airflow \
  -e POSTGRES_DB=airflow -v $BK:/bk postgres:16
until docker exec pg16-rehearsal pg_isready -U airflow; do sleep 1; done

# globals → 본체 순서. globals의 "role airflow already exists" 에러 1건은 정상
docker exec pg16-rehearsal psql -U airflow -d postgres -f /bk/globals.sql
docker exec pg16-rehearsal pg_restore -U airflow -d airflow --no-owner --role=airflow \
  --exit-on-error --single-transaction /bk/airflow.dump

docker exec -i pg16-rehearsal psql -U airflow -d airflow -At < verify_counts.sql > "$BK/rehearsal_counts.tsv"
docker exec -i pg16-rehearsal psql -U airflow -d airflow -At < verify_md5.sql    > "$BK/rehearsal_md5.tsv"
diff "$BK/src_counts.tsv" "$BK/rehearsal_counts.tsv" && echo "COUNTS OK"
diff "$BK/src_md5.tsv"    "$BK/rehearsal_md5.tsv"    && echo "MD5 OK"

docker rm -f pg16-rehearsal
```

- `--exit-on-error`로 실패하면 옵션을 빼고 다시 실행해 에러 목록을 확인한다. `COMMENT ON SCHEMA public` 같은 무해한 에러인지 판단한 뒤 본 이관에서는 같은 옵션 조합을 쓴다. PG15부터 `public` 스키마의 기본 소유자와 권한이 바뀌었으므로 이런 에러가 나올 수 있다.
- **리허설에 걸린 시간을 기록**해 둔다. 본 이관의 다운타임 추정치로 쓴다.

### 1.4 검증 SQL (원본·복원본 양쪽에서 실행해 `diff`)

**(1) 테이블별 정확 행수** — `verify_counts.sql`

```sql
-- n_live_tup(추정치)가 아니라 count(*)로 정확히 셈. 메타DB/업무DB 공용
SELECT table_schema || '.' || table_name AS tbl,
       (xpath('/row/c/text()',
              query_to_xml(format('SELECT count(*) AS c FROM %I.%I', table_schema, table_name),
                           false, true, '')))[1]::text::bigint AS row_count
FROM information_schema.tables
WHERE table_type = 'BASE TABLE'
  AND table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY 1;
```

**(2) 테이블 내용 체크섬** — `verify_md5.sql`

```sql
-- 행 텍스트를 바이트 순(COLLATE "C")으로 정렬해 md5. 버전/로케일이 달라도 정렬이 흔들리지 않게 함
SET TimeZone = 'UTC';            -- timestamptz 텍스트 표현 고정
SET extra_float_digits = 1;      -- float 표현 고정 (PG12+ 기본값과 동일, 명시만)
SELECT table_schema || '.' || table_name AS tbl,
       (xpath('/row/h/text()',
              query_to_xml(format(
                'SELECT md5(coalesce(string_agg(t::text, E''\n'' ORDER BY t::text COLLATE "C"), '''')) AS h FROM %I.%I t',
                table_schema, table_name), false, true, '')))[1]::text AS md5
FROM information_schema.tables
WHERE table_type = 'BASE TABLE'
  AND table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY 1;
```

**(3) 객체 수와 시퀀스** — 원본과 복원본의 결과가 같아야 한다

```sql
SELECT 'tables',    count(*) FROM pg_tables     WHERE schemaname NOT IN ('pg_catalog','information_schema')
UNION ALL SELECT 'indexes',   count(*) FROM pg_indexes    WHERE schemaname NOT IN ('pg_catalog','information_schema')
UNION ALL SELECT 'sequences', count(*) FROM pg_sequences
UNION ALL SELECT 'views',     count(*) FROM pg_views      WHERE schemaname NOT IN ('pg_catalog','information_schema')
UNION ALL SELECT 'extensions',count(*) FROM pg_extension;
SELECT schemaname||'.'||sequencename, last_value FROM pg_sequences ORDER BY 1;
```

**(4) Airflow 의미 검증(메타DB 전용)**

```sql
SELECT version_num FROM alembic_version;                                  -- 원본과 동일해야 함
SELECT dag_id, state, count(*) FROM dag_run GROUP BY 1,2 ORDER BY 1,2;    -- DAG별 run 상태 분포
SELECT max(start_date) FROM dag_run;                                      -- 마지막 run 시각 = 정지 직전
SELECT key FROM variable ORDER BY 1;                                      -- KIS 토큰/키, SLACK_WEBHOOK_URL 존재
SELECT conn_id, conn_type FROM connection ORDER BY 1;                     -- postgres_asset 존재
```

> 메타DB가 커서 md5가 오래 걸리면 `log`, `xcom`, `task_instance_history`, `rendered_task_instance_fields`처럼 큰 테이블은 행수만 비교하고, 나머지는 md5까지 비교한다. 1.1의 `db clean`을 하면 대부분 해결된다.

### 1.5 본 이관: PG16 새 볼륨으로 복원

`docker-compose.yaml`은 B §7 단계 2처럼 바꾼다. **새 볼륨을 쓰고 PG13 볼륨은 그대로 남긴다.**

```yaml
# docker-compose.yaml (발췌 — 적용은 별도 PR)
services:
  postgres:
    image: postgres:16
    volumes:
      - postgres-db-volume-16:/var/lib/postgresql/data
volumes:
  postgres-db-volume:        # PG13 원본 — 보존 기간(§3) 동안 삭제 금지
  postgres-db-volume-16:
```

```bash
docker compose up -d postgres
until docker compose exec -T postgres pg_isready -U airflow; do sleep 1; done
docker compose exec -T postgres psql -U airflow -Atc "select version();"     # 16.x 확인

# globals → 본체 (리허설과 동일한 옵션)
docker compose exec -T postgres psql -U airflow -d postgres < "$BK/globals.sql"
docker compose exec -T postgres pg_restore -U airflow -d airflow --no-owner --role=airflow \
  --exit-on-error --single-transaction < "$BK/airflow.dump"
```

> `pg_restore`는 표준입력으로 받은 custom 아카이브도 처리한다. `-j`(병렬)는 표준입력이나 `--single-transaction`과 함께 쓸 수 없다. 메타DB는 작아서 병렬 복원이 필요 없다.

### 1.6 복원 후 필수 조치와 검증

```bash
# (1) 비밀번호 해시 재생성 — PG13(md5) → PG16(scram-sha-256) 함정 해소
docker compose exec -T postgres psql -U airflow -d postgres -Atc \
  "select rolname, left(rolpassword, 14) from pg_authid where rolcanlogin;"
#   'md5…'로 시작하면 ↓ 실행 (password_encryption 기본값이 scram-sha-256이라 재해시됨)
docker compose exec -T postgres psql -U airflow -d postgres -c "ALTER ROLE airflow PASSWORD 'airflow';"

# (2) 통계 갱신 (dump/restore는 통계를 옮기지 않음)
docker compose exec -T postgres vacuumdb -U airflow -d airflow --analyze-in-stages

# (3) 데이터 검증 — 원본 지표와 diff
docker compose exec -T postgres psql -U airflow -d airflow -At < verify_counts.sql > "$BK/dst_counts.tsv"
docker compose exec -T postgres psql -U airflow -d airflow -At < verify_md5.sql    > "$BK/dst_md5.tsv"
diff "$BK/src_counts.tsv" "$BK/dst_counts.tsv" && diff "$BK/src_md5.tsv" "$BK/dst_md5.tsv" && echo "DATA OK"

# (4) Airflow 2.10.4로 기동해 접속/스키마 확인
docker compose up -d
docker compose exec airflow-scheduler airflow db check
docker compose exec airflow-scheduler airflow db check-migrations
docker compose exec airflow-scheduler airflow dags list | head
```

- **수용 기준**: `DATA OK`가 나오고 `airflow db check`가 통과하며, UI에 과거 run 이력이 보이면 된다. 다음 날 **06:50~07:10 정기 체인 전체가 성공**하면 Phase 1을 완료한 것으로 본다(B §5.2의 롤백 판단 기준과 같다).
- PG13 이미지와 PG16 이미지의 glibc 버전이 다르면 collation 버전 경고가 나올 수 있다. dump/restore는 인덱스를 새로 만들므로 별도 조치는 필요 없다. 경고가 남으면 `ALTER DATABASE airflow REFRESH COLLATION VERSION;`을 실행한다.

### 1.7 롤백

| 실패 시점 | 조치 | 예상 소요 |
|---|---|---|
| 복원·검증 단계(§1.5~1.6) | compose를 `postgres:13` + `postgres-db-volume`으로 되돌리고 `docker compose up -d`를 실행한다. PG13 볼륨은 정지된 채 그대로이므로 **데이터 손실이 없다.** | 2분 |
| PG16으로 운영을 시작한 뒤 | 위와 같다. 단 **PG16에서 생긴 run 이력은 사라진다.** 업무 데이터는 업로드가 멱등적이므로 해당 날짜를 2.10.4에서 재실행하면 복구된다(C §5.2). | 5분 + 재실행 |
| PG13 볼륨이 손상된 경우 | tar 스냅샷으로 새 볼륨을 만든다(아래). | 5분 |

```bash
docker compose down
sha256sum -c "$BK/SHA256SUMS"
docker volume create ${PROJ}_postgres-db-volume-restored
docker run --rm -v ${PROJ}_postgres-db-volume-restored:/v -v $BK:/bk alpine \
  sh -c 'cd /v && tar xzf /bk/pgdata13_volume.tgz'
# compose의 postgres를 image: postgres:13, 볼륨을 postgres-db-volume-restored로 지정(external: true) 후
docker compose up -d
```

> PG16 dump를 PG13에 거꾸로 복원하는 경로는 **지원되지 않는다.** 롤백 수단은 PG13 볼륨과 tar뿐이므로 보존 기간 동안 절대 지우지 않는다.

### 1.8 Airflow 3.3.2 전환 당일(C Phase 3)의 2차 백업

B §7 단계 6의 "단계 1 백업을 이 시점에 다시 수행"을 같은 방식으로 반복한다. 이때는 이미 PG16이므로 `PGRUN`과 서버 버전이 같다. 볼륨 tar는 `postgres-db-volume-16`을 대상으로 뜬다. **3.x `db migrate` 직전의 이 덤프가 3.x 롤백의 1순위 수단이다**(B §5.2, C §5.2). 복원 대상은 `postgres:16` + `apache/airflow:2.10.4` 조합이다.

---

## 2. 자산 업무 DB(호스트 PostgreSQL) 백업 → (선택) PG16 이관 런북

### 2.0 결론부터

- Airflow 3.3.2의 PostgreSQL 14~18 요구 사항은 **메타DB에만 적용된다.** 업무 DB는 `PostgresHook`과 psycopg2가 붙을 수 있는 버전이면 된다. 따라서 업무 DB의 PG16 이관은 **필수가 아니다.**
- 그래도 **Airflow 전환 직전 백업은 필수**다. 3.x 첫 run에서 DAG 로직 회귀(예: A §4의 기준일 계산 변화)가 생기면 업무 테이블이 잘못 적재될 수 있기 때문이다.
- 호스트 PG가 **EOL 버전**(13 이하. PG13은 2025-11 EOL)이면 이번 기회에 함께 올리는 것을 권장한다. 다만 **Airflow 전환과 같은 날 하지 않는다**(C §5.1의 변경 축 분리 원칙).

### 2.1 접속 정보와 서버 버전 확인

```bash
# (1) Airflow가 보는 접속 정보 (비밀번호 포함 출력 — 화면 공유 주의)
docker compose exec airflow-scheduler airflow connections get postgres_asset -o json

export AHOST=localhost        # 호스트에서 실행하므로 host.docker.internal 대신 localhost/127.0.0.1
export APORT=5432 AUSER=<user> ADB=<db>      # 위 출력으로 채움
export PGPASSWORD='<password>'

# (2) 서버 버전 — 아래 중 가능한 것
psql -h $AHOST -p $APORT -U $AUSER -d $ADB -Atc "select version();"
psql -h $AHOST -p $APORT -U $AUSER -d $ADB -Atc "show server_version_num;"
pg_lsclusters 2>/dev/null                       # Debian/Ubuntu: 클러스터·포트·데이터 경로
systemctl list-units 'postgresql*' --no-pager   # 서비스명으로 버전 추정
ls /usr/lib/postgresql/ 2>/dev/null; ls /usr/pgsql-* -d 2>/dev/null   # 설치된 바이너리
# 호스트에 psql이 없으면 컨테이너로:
docker run --rm --add-host host.docker.internal:host-gateway -e PGPASSWORD postgres:16 \
  psql -h host.docker.internal -p $APORT -U $AUSER -d $ADB -Atc "select version();"

# (3) 이관 영향 요소
psql -h $AHOST -p $APORT -U $AUSER -d $ADB -c '\dx'                  # 확장(버전 호환 확인)
psql -h $AHOST -p $APORT -U $AUSER -d $ADB -Atc "select datname, pg_size_pretty(pg_database_size(datname)), datcollate from pg_database where not datistemplate;"
psql -h $AHOST -p $APORT -U $AUSER -d postgres -Atc "show password_encryption;"
psql -h $AHOST -p $APORT -U $AUSER -d $ADB -Atc "select rolname, rolsuper from pg_roles where rolcanlogin;"
```

> 역할을 덤프(`pg_dumpall -g`)하려면 슈퍼유저 권한이 필요하다. `postgres_asset`의 계정이 슈퍼유저가 아니면 서버 관리자 계정(예: `sudo -u postgres`)으로 실행한다.

### 2.2 백업 (Airflow 쓰기 중지 상태에서)

```bash
# 업무 DB에 쓰는 DAG 실행이 없을 때(07:10 이후 ~ 다음 날 06:50 전). 확실히 하려면 Airflow 일시정지
docker compose stop airflow-scheduler      # 3.x 전환 당일이면 이미 정지 상태

export ABK=/srv/backup/oracle-airflow/$TS/asset
mkdir -p "$ABK" && chmod 700 "$ABK"
# 신버전(16) 클라이언트 사용 권장 — 호스트 psql이 구버전이면 컨테이너 사용
APG="docker run --rm --add-host host.docker.internal:host-gateway -e PGPASSWORD -v $ABK:/bk postgres:16"
$APG psql -h host.docker.internal -p $APORT -U $AUSER -d $ADB -Atc "select version();" > "$ABK/server_version.txt"
$APG pg_dumpall -h host.docker.internal -p $APORT -U <superuser> --globals-only -f /bk/globals.sql
$APG pg_dump    -h host.docker.internal -p $APORT -U $AUSER -d $ADB -Fc -Z 6 -f /bk/$ADB.dump
$APG pg_restore -l /bk/$ADB.dump > "$ABK/$ADB.toc"

psql -h $AHOST -p $APORT -U $AUSER -d $ADB -At < verify_counts.sql > "$ABK/src_counts.tsv"
psql -h $AHOST -p $APORT -U $AUSER -d $ADB -At < verify_md5.sql    > "$ABK/src_md5.tsv"
# 업무 의미 지표 (스키마는 저장소 기준: account.asset_daily 등)
psql -h $AHOST -p $APORT -U $AUSER -d $ADB -At -c \
  "select base_date, count(*) from account.asset_daily group by 1 order by 1 desc limit 14;" > "$ABK/src_asset_daily_tail.tsv"
( cd "$ABK" && sha256sum * > SHA256SUMS )
```

> 업무 DB에 다른 DB가 더 있으면 DB마다 `pg_dump -Fc`를 반복한다(`pg_database` 목록 참고). `account.asset_daily`의 날짜 컬럼명은 실제 스키마에 맞게 고친다.

### 2.3 (선택) PG16 나란히 설치 → 복원 → 전환

호스트 OS에 맞는 공식 패키지로 PG16을 설치한다(Debian/Ubuntu는 PGDG apt, `pg_createcluster 16 main --port 5433`). 기존 클러스터는 **멈추지 않고 그대로 둔다.**

```bash
# 복원 (PG16 클러스터, 포트 5433)
export NPORT=5433
psql -h localhost -p $NPORT -U postgres -d postgres -f "$ABK/globals.sql"          # 역할
createdb -h localhost -p $NPORT -U postgres -O $AUSER $ADB
pg_restore -h localhost -p $NPORT -U postgres -d $ADB -j 4 --exit-on-error "$ABK/$ADB.dump"
vacuumdb   -h localhost -p $NPORT -U postgres -d $ADB --analyze-in-stages

# 검증
psql -h localhost -p $NPORT -U $AUSER -d $ADB -At < verify_counts.sql > "$ABK/dst_counts.tsv"
psql -h localhost -p $NPORT -U $AUSER -d $ADB -At < verify_md5.sql    > "$ABK/dst_md5.tsv"
diff "$ABK/src_counts.tsv" "$ABK/dst_counts.tsv" && diff "$ABK/src_md5.tsv" "$ABK/dst_md5.tsv" && echo "ASSET DATA OK"
```

- **접속 허용**: 컨테이너는 `host-gateway`(대개 docker0의 172.17.0.1)를 거쳐 들어온다. 그래서 새 클러스터에도 기존 클러스터와 같은 `listen_addresses`와 `pg_hba.conf`(도커 대역 허용)를 옮겨 둔다. 비밀번호가 md5 해시로 넘어왔다면 §1.6과 같은 방법으로 `ALTER ROLE ... PASSWORD`를 실행한다.
- **전환**: Airflow 쪽에서는 **Connection의 포트만** 바꾼다. 코드는 바뀌지 않는다.

```bash
docker compose exec airflow-scheduler airflow connections get postgres_asset -o json > "$ABK/conn_postgres_asset_before.json"
# UI(Admin > Connections)에서 port 5432 → 5433 수정이 가장 간단.
# CLI로 하려면 delete 후 add (기존 값 전체를 그대로 다시 지정해야 함)
```

- 전환한 뒤 `rebuild_derived_assets`를 짧은 범위로 수동 실행해 읽기와 쓰기를 확인한다. 다음 날 정기 run의 `account.asset_daily` 건수가 `src_asset_daily_tail.tsv`의 패턴과 같은지도 확인한다.

### 2.4 롤백

| 상황 | 조치 |
|---|---|
| 업무 DB PG16 전환 후 문제 | `postgres_asset` 포트를 5432로 되돌린다. 구 클러스터는 계속 살아 있다. **그사이 PG16에 적재된 날짜**는 구 클러스터에서 해당 DAG를 재실행해 맞춘다(DELETE 후 INSERT 구조라 멱등적이다). |
| 업무 데이터 오염(3.x 회귀 등) | 대상 테이블만 골라 복원한다: `pg_restore -l` → 목록 편집 → `pg_restore -L list.txt --data-only --table=...`. 또는 해당 날짜를 2.x나 수정된 코드로 재실행한다. |

---

## 3. 보관 위치와 보존 기간

| 산출물 | 1차 위치 | 2차 사본 | 보존 기간 | 근거 |
|---|---|---|---|---|
| 메타DB `globals.sql`, `airflow.dump`, 검증 TSV | `/srv/backup/oracle-airflow/<TS>/` (서버 로컬, 저장소 밖, `chmod 700`) | 서버 밖(NAS나 오브젝트 스토리지). **`gpg -c` 또는 `age`로 암호화해서 업로드** | **90일** | 3.x 안정화(C: 관찰 5~7일 + 여유)와 분기 감사 주기를 고려했다. |
| PG13 볼륨 tar, `postgres-db-volume`(PG13) | 서버 로컬 | tar만 2차 사본 | **3.x 첫 정기 사이클 성공 + 14일** | B §7 체크리스트의 "롤백 기간 1~2주"와 같다. PG13으로 돌아갈 수 있는 유일한 수단이다. |
| 3.x 전환 직전 dump(§1.8) | 같은 위치 | 같은 방식 | **90일** | 3.x 롤백의 1순위 수단이다. |
| 업무 DB dump | `/srv/backup/oracle-airflow/<TS>/asset/` | 같은 방식 | **90일**. 이와 별개로 상시 백업(일 7개, 주 4개)이 없다면 신설을 권장한다. | 업무 데이터는 메타DB보다 가치가 크다. |
| Fernet 키(도입 후) | 비밀번호 관리자 | — | 키를 쓰는 동안 계속 | 키를 잃으면 덤프 안의 Variable과 Connection을 복호화할 수 없다(B §7 단계 4). |

- **암호화가 필요한 이유**: 현재 `FERNET_KEY=''`이므로 메타DB 덤프에 **KIS appkey/secret, Slack/Discord webhook URL, `postgres_asset` 비밀번호가 평문으로 들어 있다**(B §0 #7).
- 만료된 백업 삭제: `find /srv/backup/oracle-airflow -maxdepth 1 -type d -mtime +90 -print` 로 대상을 먼저 확인한 뒤 수동으로 삭제한다. 자동 삭제는 롤백 기간이 끝난 후에 켠다.

---

## 4. Discord 실패 알림 — 방법 비교

### 4.0 전제: 현재 알림 구조와 Airflow 3에서 바뀌는 점(A·B 요약)

| 항목 | 현재(2.10.4) | Airflow 3.3.2에서 |
|---|---|---|
| 실패 알림 | `default_args.on_failure_callback = slack_failure_callback` (태스크 레벨) | **Task SDK 태스크 프로세스**에서 실행된다(LocalExecutor면 scheduler 컨테이너). ORM은 차단된다. 콜백이 던진 예외는 `_run_task_state_change_callbacks`가 `log.exception("Failed to run task callback")`으로 삼키므로 **태스크 상태에는 영향이 없지만 알림은 조용히 사라진다**(소스 확인, task-sdk 1.3.2). |
| 복구 알림 | DAG 레벨 `on_success_callback = slack_recovery_callback` | **dag-processor** 하위 프로세스에서 실행된다. 에러는 `logs/dag_processor/`에만 남는다(A §4.3). |
| 깨지는 곳 | — | `context["execution_date"]` KeyError(P0), `DagRun.find` ORM 차단(P0), `AIRFLOW__WEBSERVER__BASE_URL`과 2.x UI 경로(P3) |

> 3.1.x 주석: 콜백 에러 로그 위치가 3.1은 scheduler 로그, 3.3은 dag-processor 로그로 다르다(C §4). `BaseNotifier`에 context를 넘기는 기능은 3.1.0부터 지원된다(discord provider 소스의 `AIRFLOW_V_3_1_PLUS` 분기).

### 4.1 비교표

| 기준 | (a) `requests` 직접 호출 콜백 | (b) discord provider `DiscordNotifier`/`DiscordWebhookHook` | (c) Apprise provider `AppriseNotifier` | (d) Slack 호환 `/slack` 엔드포인트 |
|---|---|---|---|---|
| 2.10.4 설치 가능 | ✅ (추가 의존성 없음, `requests` 기존 사용) | ❌ **`apache-airflow>=2.11.0` 요구**(PyPI 3.12.3 메타데이터) | ❌ `>=2.11.0` 요구(2.4.0) | ✅ |
| 3.3.2 호환 | ✅ (SDK import + 2.x 폴백) | ✅ 3.12.3(2026-05-11), `common-compat`/`http` provider 의존 | ✅ 2.4.0(2026-09-29), `apprise>=1.8.0` | ✅ |
| C Phase 0(업그레이드 전 선반영) | **가능** | 불가 | 불가 | 가능 |
| embed 표현력 | 완전 통제(색, 필드, 링크, timestamp, footer) | embed **1개**(`payload["embeds"] = [embed]`), TypedDict | markdown 헤더를 embed로 변환. **필드/색 직접 지정 불가** | legacy attachment만. Block Kit `blocks`와 `mrkdwn`은 문서상 미지원 |
| 한도 초과 처리 | 직접 잘라서 보냄(설계 가능) | **잘라내지 않고 `ValueError`**(validate_embed). 총 6000자 검사도 없음 | 2000자 단위 분할(apprise 동작) | Discord가 400 반환 |
| 429/레이트리밋 | `retry_after` 기반 재시도 직접 구현 | `HttpHook.run` → 비 2xx이면 예외. 기본 재시도 없음 | apprise 내부 처리(제한적) | 직접 구현 |
| 실패 시 동작 | 콜백 안에서 삼키고 로그 | `BaseNotifier.__call__`이 로그 후 **re-raise** → 태스크 러너가 다시 삼킴(알림 유실) | 같음 | (a)와 같음 |
| 복구(이전 run 실패) 판정 | 콜백 안에서 구현 | **없음** → 래퍼 콜백이 필요하므로 결국 (a)와 비슷한 코드가 생김 | 없음 | (a)와 같음 |
| 비밀 관리 | Connection password(마스킹) / Variable 폴백 | HTTP Connection, `extra.webhook_endpoint`(토큰이 extra에 노출됨, 마스킹 키 아님) | Connection extra `config.path`(URL 노출) | (a)와 같음 |
| 멀티 채널(Slack+Discord 병행) | 함수 두 개 호출 | Discord만 | **강점**: 한 번에 여러 서비스 | Discord만 |
| 유지보수 | 자체 코드 약 200줄 | provider 업그레이드를 따라감. 소스상 문서/검증 불일치가 있음(아래) | provider와 apprise 두 겹 | 기존 payload를 다시 써야 함 |

**(b) 소스에서 확인한 사항** (`apache-airflow-providers-discord==3.12.3`)
- `airflow.providers.discord.notifications.discord.DiscordNotifier(discord_conn_id="discord_webhook_default", text="", username="Airflow", avatar_url=ICON_URL, tts=False, embed=None)`가 있다. `template_fields`에 `embed`가 들어 있어 Jinja 렌더링이 된다.
- Connection 형식: host `https://discord.com/api/`, extra `{"webhook_endpoint": "webhooks/{id}/{token}"}`. 엔드포인트는 정규식 `webhooks/[0-9]+/[a-zA-Z0-9_-]+`로 검증하므로 `?wait=true`나 `thread_id`를 붙일 수 없다.
- `Embed` TypedDict의 docstring은 description이 2048자라고 적고 있지만 검증 코드는 4096자로 검사한다. author name도 메시지는 256자인데 실제 검사는 2048자로 한다. 이런 사소한 불일치가 있다.
- `DiscordNotifier`의 기본 conn id(`discord_webhook_default`)가 Hook의 기본값(`discord_default`)과 다르다.

**(c) Apprise**: Connection `apprise_default`에 `{"path": "discord://{WebhookID}/{WebhookToken}/", "tag": "alert"}`를 넣는다. 전환 기간에 **Slack과 Discord로 동시에 발송**해야 할 때 가장 간단하다. 하지만 이 프로젝트는 채널을 하나로 바꾸는 것이 목적이고 2.10.4에 설치할 수 없으므로 우선순위가 낮다.

**(d) Slack 호환**: `POST /webhooks/{id}/{token}/slack`. Discord 문서에는 "We do not support Slack's `channel`, `icon_emoji`, `mrkdwn`, or `mrkdwn_in` properties"라고만 적혀 있다. 현재 `callbacks.py` payload는 `attachments[].blocks`(Block Kit)만 담고 있는데, Block Kit 지원 여부는 **문서에 없다**. 그래서 빈 메시지 400이나 내용 누락이 예상된다(실측 필요: §5.5-0의 curl). 쓰려면 `{"text": ..., "attachments": [{"color", "title", "title_link", "text", "fields": [{"title", "value", "short"}]}]}` 형식으로 다시 써야 하므로 재사용의 이점이 없다. **URL만 바꿔 당장 알림을 살리는 비상 수단**으로만 남겨 둔다.

### 4.2 권장안: (a) 직접 호출 콜백

1. **C 전략과 맞는다.** Phase 0(2.10.4)에 먼저 배포해 Slack에서 Discord로 바꾸는 작업과 Airflow 업그레이드 작업을 **분리**할 수 있다. 업그레이드 당일에는 알림 채널이 이미 검증된 상태가 된다.
2. A §1.1의 P0 두 건(`execution_date`, `DagRun.find`)과 P3(URL)을 **같은 PR에서** 함께 고친다.
3. 실패 콜백이 다시 실패하지 않도록 방어하고, 한도에 맞춰 자르고, 429를 처리하는 일을 **알림 경로 안에서 직접 통제**할 수 있다. (b)는 한도를 넘으면 예외를 내고 알림을 잃는다.
4. **향후 (b)로 옮기기 쉽게** embed dict 구조를 provider의 `Embed` TypedDict 키(title/description/url/timestamp/color/footer/fields)와 똑같이 맞춘다.

---

## 5. 권장안 상세

### 5.1 Webhook URL 관리 (Connection 우선, Variable 폴백)

| 방식 | 값 | 마스킹 | 비고 |
|---|---|---|---|
| **Connection `discord_alert` (권장)** | conn_type `http`, **password = 전체 webhook URL** | ✅ Connection password는 UI와 로그에서 마스킹된다. | `.env`의 `AIRFLOW_CONN_DISCORD_ALERT`로 넣으면 메타DB에도 남지 않는다. |
| Variable `DISCORD_WEBHOOK_URL` (폴백) | 전체 URL | ❌ 키 이름에 `secret`/`password`/`token` 같은 민감 키워드가 없어 평문으로 노출된다. | 빠르게 시험할 때만 쓴다. |

```bash
# (A) UI: Admin > Connections > + : Conn Id=discord_alert, Type=HTTP, Password=<webhook URL>
# (B) CLI
docker compose exec airflow-scheduler airflow connections add discord_alert \
  --conn-type http --conn-password 'https://discord.com/api/webhooks/<id>/<token>'
# (C) .env (메타DB에 저장 안 함, compose env_file로 주입 — B §7 단계 4·5 구성)
AIRFLOW_CONN_DISCORD_ALERT='{"conn_type": "http", "password": "https://discord.com/api/webhooks/<id>/<token>"}'
```

- Discord 채널 설정 → 연동 → 웹후크에서 URL을 만든다. **URL 자체가 인증 수단**이므로 유출되면 웹후크를 삭제하고 다시 만든다.
- Discord 문서에 따르면 404를 받은 웹후크로는 다시 요청하지 말아야 한다. 반복하면 IP가 일시 제한될 수 있다. 콜백은 404/401을 받으면 재시도하지 않는다.

### 5.2 `plugins/callbacks.py` 교체안 (전체 예시 — 저장소에는 쓰지 않음)

```python
"""
Airflow DAG 알림 콜백 모듈 (Discord)

사용법:
    from callbacks import discord_failure_callback, discord_recovery_callback

    default_args = {'on_failure_callback': discord_failure_callback}       # 태스크 레벨
    @dag(..., on_success_callback=discord_recovery_callback)               # DAG 레벨(이전 실패 시에만)

Webhook URL: Connection 'discord_alert'(password=전체 URL) → 없으면 Variable 'DISCORD_WEBHOOK_URL'
Airflow 2.10.4와 3.3.2 양쪽에서 동작하도록 import 폴백을 둔다.
"""

from __future__ import annotations

import logging
import socket
import time
import traceback
from datetime import datetime, timezone
from urllib.parse import quote, urlencode

import pendulum
import requests

try:  # Airflow 3.x (Task SDK)
    from airflow.sdk import BaseHook, Variable, conf
    _AF3 = True
except ImportError:  # Airflow 2.x
    from airflow.configuration import conf
    from airflow.hooks.base import BaseHook
    from airflow.models import Variable
    _AF3 = False

log = logging.getLogger(__name__)

CONN_ID = "discord_alert"
VAR_KEY = "DISCORD_WEBHOOK_URL"
KST = pendulum.timezone("Asia/Seoul")
COLOR_FAIL = 0xE53935
COLOR_OK = 0x43A047

# Discord embed 한도 (docs.discord.com/developers/resources/message — Embed Limits)
LIM_TITLE, LIM_DESC, LIM_FIELDS = 256, 4096, 25
LIM_FIELD_NAME, LIM_FIELD_VALUE, LIM_FOOTER, LIM_TOTAL = 256, 1024, 2048, 6000
ERROR_BUDGET = 1500          # 에러 본문은 여유 있게 짧게 (모바일 가독성 + 총 6000자 보호)
HTTP_TIMEOUT = (3, 5)        # (connect, read) 초
MAX_ATTEMPTS = 3
MAX_TOTAL_WAIT = 10.0        # 429 대기 총합 상한(초) — 콜백이 워커 슬롯을 오래 잡지 않게


# ──────────────────────────────────────────────────────────────
# 전송
# ──────────────────────────────────────────────────────────────
def _webhook_url() -> str | None:
    try:
        url = BaseHook.get_connection(CONN_ID).password
        if url:
            return url
    except Exception:  # 연결 없음(AirflowNotFoundException 등)
        pass
    try:
        url = Variable.get(VAR_KEY, default=None) if _AF3 else Variable.get(VAR_KEY, default_var=None)
        return url or None
    except Exception:
        return None


def _post_discord(payload: dict) -> bool:
    """Discord webhook 전송. 실패해도 예외를 밖으로 던지지 않는다."""
    url = _webhook_url()
    if not url:
        log.warning("[callback] Connection '%s'/Variable '%s' 미설정 → Discord 알림 생략", CONN_ID, VAR_KEY)
        return False
    waited = 0.0
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            # wait=true: 저장 성공 시 200 + 메시지 본문, 실패 시 에러 본문을 받음 (기본 false면 204)
            resp = requests.post(url, params={"wait": "true"}, json=payload, timeout=HTTP_TIMEOUT)
        except requests.RequestException as e:
            log.warning("[callback] Discord 전송 예외(%s/%s): %s", attempt, MAX_ATTEMPTS, type(e).__name__)
            time.sleep(min(2 ** attempt, 4))
            continue
        if resp.status_code in (200, 204):
            return True
        if resp.status_code == 429:
            try:
                retry_after = float(resp.json().get("retry_after", 1.0))
            except ValueError:
                retry_after = float(resp.headers.get("Retry-After", 1.0))
            if waited + retry_after > MAX_TOTAL_WAIT:
                break
            waited += retry_after
            time.sleep(retry_after)
            continue
        if resp.status_code in (401, 403, 404):
            # 웹후크 삭제/토큰 오류 — 재시도 금지(Discord 권고). URL은 로그에 남기지 않는다
            log.error("[callback] Discord 웹후크 무효(%s). Connection '%s' 갱신 필요", resp.status_code, CONN_ID)
            return False
        if 500 <= resp.status_code < 600:
            time.sleep(min(2 ** attempt, 4))
            continue
        log.error("[callback] Discord 전송 실패 %s: %s", resp.status_code, resp.text[:300])  # 400: 한도/형식 오류
        return False
    log.error("[callback] Discord 전송 포기(재시도 소진)")
    return False


# ──────────────────────────────────────────────────────────────
# 컨텍스트 → 표시값 (모두 None 안전)
# ──────────────────────────────────────────────────────────────
def _trunc(s: str, n: int) -> str:
    s = s or ""
    return s if len(s) <= n else s[: n - 1] + "…"


def _run_dt(context: dict):
    """logical_date 우선, 없으면(3.x 수동/API/Asset run) dag_run.run_after. (A §3)"""
    dr = context.get("dag_run")
    dt = context.get("logical_date") or (getattr(dr, "logical_date", None) if dr else None)
    if dt is None and dr is not None:
        dt = getattr(dr, "run_after", None) or getattr(dr, "start_date", None)
    return dt


def _kst_label(dt) -> str:
    if dt is None:
        return "-"
    return pendulum.instance(dt).in_timezone(KST).strftime("%Y-%m-%d %H:%M KST")  # 기존 UTC를 'KST'로 오표기하던 버그 수정


def _base_url() -> str:
    try:
        if _AF3:
            return conf.get("api", "base_url", fallback="http://localhost:8080").rstrip("/")
        return conf.get("webserver", "base_url", fallback="http://localhost:8080").rstrip("/")
    except Exception:
        return "http://localhost:8080"


def _run_url(dag_id: str, run_id: str, task_id: str | None = None) -> str:
    base = _base_url()
    if _AF3:  # 새 React UI: /dags/{dag_id}/runs/{run_id}[/tasks/{task_id}]
        url = f"{base}/dags/{quote(dag_id, safe='')}/runs/{quote(run_id, safe='')}"
        return f"{url}/tasks/{quote(task_id, safe='')}" if task_id else url
    q = {"dag_run_id": run_id, **({"task_id": task_id} if task_id else {})}
    return f"{base}/dags/{dag_id}/grid?{urlencode(q)}"


def _error_text(context: dict) -> str:
    exc = context.get("exception")
    if exc is None:
        return str(context.get("reason") or "에러 정보 없음")  # DAG 레벨 콜백은 reason만 있을 수 있음
    if isinstance(exc, BaseException):
        lines = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)).strip().splitlines()
        text = "\n".join(lines[-8:])
    else:
        text = str(exc)
    text = text.replace("```", "'''")  # 코드블록 깨짐 방지
    return _trunc(text, ERROR_BUDGET)


def _field(name: str, value, inline: bool = True) -> dict:
    return {"name": _trunc(str(name), LIM_FIELD_NAME), "value": _trunc(str(value) or "-", LIM_FIELD_VALUE), "inline": inline}


def _finalize(embed: dict) -> dict:
    """embed 한도 강제: 필드 25개, 총 6000자."""
    embed["title"] = _trunc(embed.get("title", ""), LIM_TITLE)
    embed["description"] = _trunc(embed.get("description", ""), LIM_DESC)
    embed["fields"] = embed.get("fields", [])[:LIM_FIELDS]
    total = lambda e: len(e["title"]) + len(e["description"]) + len(e.get("footer", {}).get("text", "")) + sum(
        len(f["name"]) + len(f["value"]) for f in e["fields"])
    over = total(embed) - LIM_TOTAL
    if over > 0:  # 1순위: description(에러 본문)을 줄인다
        embed["description"] = _trunc(embed["description"], max(len(embed["description"]) - over, 50))
    while total(embed) > LIM_TOTAL and embed["fields"]:  # 2순위: 뒤쪽 필드부터 버린다
        embed["fields"].pop()
    return embed


def _payload(embed: dict) -> dict:
    return {
        "username": "Airflow",
        "embeds": [_finalize(embed)],
        "allowed_mentions": {"parse": []},  # 에러 메시지 속 @everyone 등이 실제 멘션되지 않게
    }


def _footer() -> dict:
    return {"text": _trunc(f"oracle-airflow · {socket.gethostname()}", LIM_FOOTER)}


# ──────────────────────────────────────────────────────────────
# 콜백
# ──────────────────────────────────────────────────────────────
def discord_failure_callback(context: dict) -> None:
    """태스크 최종 실패(재시도 소진) 시 알림. 절대 예외를 던지지 않는다."""
    try:
        ti = context.get("ti") or context.get("task_instance")
        dag = context.get("dag")
        dag_id = getattr(dag, "dag_id", None) or getattr(ti, "dag_id", "?")
        task_id = getattr(ti, "task_id", "-")
        run_id = context.get("run_id") or getattr(ti, "run_id", "-")
        dr = context.get("dag_run")
        try_no = getattr(ti, "try_number", "?")
        max_tries = (getattr(ti, "max_tries", 0) or 0) + 1
        dt = _run_dt(context)

        embed = {
            "title": f"🔴 DAG 실패: {dag_id}",
            "url": _run_url(dag_id, run_id, task_id),           # 제목 클릭 → 태스크 화면
            "color": COLOR_FAIL,
            "description": f"**에러 메시지**\n```\n{_error_text(context)}\n```",
            "fields": [
                _field("Task", f"`{task_id}`"),
                _field("시도", f"{try_no} / {max_tries}"),
                _field("실행 기준", _kst_label(dt)),
                _field("Run 유형", getattr(dr, "run_type", "-")),
                _field("Run ID", f"`{run_id}`", inline=False),
            ],
            "footer": _footer(),
            "timestamp": datetime.now(timezone.utc).isoformat(),   # Discord가 보는 사람의 시간대로 표시
        }
        _post_discord(_payload(embed))
    except Exception:
        log.exception("[callback] discord_failure_callback 내부 오류(알림 생략)")


def _was_previous_run_failed(context: dict) -> bool:
    """직전 run(logical_date 기준)이 같은 run_type이고 FAILED였는지. ORM 대신 Task SDK API 사용(A §4.4 대안 A)."""
    ti = context.get("ti") or context.get("task_instance")
    dr = context.get("dag_run")
    if ti is None or dr is None or getattr(dr, "logical_date", None) is None:
        log.info("[callback] logical_date 없는 run → 복구 판정 생략")
        return False
    try:
        prev = ti.get_previous_dagrun()      # 3.x: SDK → API/dag-processor 경유, 2.x: ORM
    except Exception:
        log.exception("[callback] 이전 DagRun 조회 실패")
        return False
    if prev is None or str(getattr(prev, "run_type", "")) != str(getattr(dr, "run_type", "")):
        return False
    return str(getattr(prev, "state", "")).lower().endswith("failed")   # enum/str 모두 처리


def discord_recovery_callback(context: dict) -> None:
    """DAG 전체 성공 시, 직전 동일 유형 run이 실패였을 때만 복구 알림."""
    try:
        if not _was_previous_run_failed(context):
            return
        dag = context.get("dag")
        dr = context.get("dag_run")
        dag_id = getattr(dag, "dag_id", None) or getattr(dr, "dag_id", "?")
        run_id = context.get("run_id") or getattr(dr, "run_id", "-")
        embed = {
            "title": f"🟢 DAG 복구: {dag_id}",
            "url": _run_url(dag_id, run_id),
            "color": COLOR_OK,
            "description": "이전 실패 → 현재 성공 ✅",
            "fields": [
                _field("실행 기준", _kst_label(_run_dt(context))),
                _field("Run 유형", getattr(dr, "run_type", "-")),
                _field("Run ID", f"`{run_id}`", inline=False),
            ],
            "footer": _footer(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        _post_discord(_payload(embed))
    except Exception:
        log.exception("[callback] discord_recovery_callback 내부 오류(알림 생략)")


# 전환 기간 호환: DAG 7개의 import를 바꾸기 전까지 기존 이름으로도 Discord를 보냄
slack_failure_callback = discord_failure_callback
slack_recovery_callback = discord_recovery_callback
```

**코드 설계 메모**

| 요구 사항 | 반영 위치 |
|---|---|
| Airflow 3 SDK import | `from airflow.sdk import BaseHook, Variable, conf`. 3.3.2 `airflow.sdk.__all__`에 세 이름이 모두 있다(소스 확인). 2.x에서는 폴백한다. `Variable.get`의 키워드가 `default`(SDK)와 `default_var`(2.x)로 다른 점도 분기했다(A §1.1 21행). |
| logical_date / run_after 폴백 | `_run_dt()`. 3.x 수동/API run은 `logical_date=None`이므로 `run_after`를 쓴다(A §3, task-sdk `DagRun.run_after`는 필수 필드). |
| 복구 판정 | `ti.get_previous_dagrun()`. task-sdk 1.3.2 소스에서는 `logical_date`가 None이면 None을 반환하고, 그 외에는 `GetPreviousDagRun` 메시지로 조회한다. **ORM을 쓰지 않는다.** |
| 새 UI URL | `[api] base_url` + `/dags/{dag_id}/runs/{run_id}/tasks/{task_id}`. SDK `RuntimeTaskInstance.log_url`과 같은 경로 형식이고 run_id는 `quote`로 인코딩한다(소스 확인). |
| 실패 콜백 재실패 방지 | 콜백 본문 전체를 `try/except Exception` → `log.exception`으로 감쌌다. 네트워크 timeout은 (3, 5)초로 두고, 429 대기 총합은 10초로 제한했다. 401/403/404는 재시도하지 않고, URL은 로그에 남기지 않는다. |
| 한도 | `_finalize()`가 필드 25개와 총 6000자를 강제하고, `_trunc()`이 항목별 한도를 맞춘다. 에러 본문은 1500자로 제한한다. |

> **3.x에서 `print` 대신 `logging`을 쓰는 이유**: 3.x 태스크 로그는 structlog로 수집되고, DAG 콜백 로그는 dag-processor 로그 파일에 남는다. 표준 `logging` 메시지는 두 경로 모두에 레벨과 함께 기록되므로 조용한 실패를 찾기 쉽다(A §4.4의 "log.exception으로 스택까지 남겨라"와 같은 결론).

### 5.3 등록 방법

**태스크 레벨(실패) — `default_args`**: 재시도가 모두 소진된 최종 실패에서만 호출된다. 재시도 중간에는 `on_retry_callback`이 호출된다.

```python
from airflow.sdk import dag            # 2.10.4에서는 from airflow.decorators import dag
from callbacks import discord_failure_callback, discord_recovery_callback

default_args = {
    "owner": "haejun",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": discord_failure_callback,
}

@dag(
    dag_id="fetch_asset_daily",
    default_args=default_args,
    schedule="0 7 * * *",
    catchup=False,
    on_success_callback=discord_recovery_callback,     # DAG 레벨: 직전 run 실패 시에만 복구 알림
    # on_failure_callback=discord_failure_callback,    # (선택) DAG 레벨 실패: dagrun_timeout 등 태스크 밖 실패도 잡고 싶을 때
)
def fetch_asset_daily(): ...
```

- DAG 레벨 `on_failure_callback`까지 걸면 태스크가 실패할 때 **알림이 두 번** 간다(태스크 1번 + DAG 1번). 현재 구조처럼 태스크 레벨만 쓰고, `dagrun_timeout`을 도입할 때 DAG 레벨을 추가한다.
- `fetch_asset_daily`처럼 계좌 태스크가 여러 개 동시에 실패하면 **알림이 태스크 수만큼** 간다. 레이트리밋은 §5.4에서 다룬다.
- 콜백 인자에는 리스트도 넣을 수 있다. 예: `on_failure_callback=[discord_failure_callback, slack_failure_callback_legacy]`. Slack과 병행 발송하는 기간에 이렇게 쓴다.
- **배포 순서(C Phase 0)**: ① `callbacks.py`를 교체한다(별칭 덕분에 DAG는 수정하지 않아도 된다). ② `discord_alert` Connection을 등록한다. ③ §5.5 테스트를 한다. ④ DAG 7개의 import를 `discord_*`로 바꾼다. ⑤ 별칭과 Variable `SLACK_WEBHOOK_URL`을 정리한다. ⑥ `plugins/CALLBACKS_GUIDE.md`를 갱신한다.

> 참고로 (b)를 쓸 경우 등록 방법은 다음과 같다. `on_failure_callback=DiscordNotifier(discord_conn_id="discord_alert", text="{{ dag.dag_id }}.{{ ti.task_id }} 실패", embed={...})`. 이때 Connection은 host `https://discord.com/api/`, extra `{"webhook_endpoint":"webhooks/<id>/<token>"}` 형식이어야 한다. 이 형식은 §5.1과 호환되지 않는다.

### 5.4 Discord 제한 사항 정리

| 항목 | 한도 | 출처 | 이 구현의 처리 |
|---|---|---|---|
| `content` | 2000자 | Execute Webhook | `content`를 쓰지 않고 embed만 보낸다. |
| embeds/메시지 | 최대 10개 | Execute Webhook | 1개만 보낸다. |
| title | 256자 | Embed Limits | 자른다. |
| description | 4096자 | Embed Limits | 에러 본문 1500자 + 자르기 |
| fields | 25개 | Embed Limits | 5개 이하, `[:25]` |
| field.name / value | 256 / 1024자 | Embed Limits | 자른다. |
| footer.text | 2048자 | Embed Limits | 자른다. |
| author.name | 256자 | Embed Limits | 쓰지 않는다. |
| **총합** | 모든 embed의 title+description+fields+footer+author **합계 6000자**. 넘으면 400 Bad Request | Embed Limits | `_finalize()`가 description부터 줄인다. |
| 필수 | `content`, `embeds`, `file`, `poll` 등 중 **최소 하나** | Execute Webhook | embed 1개 |
| `color` | 정수(0xRRGGBB) | Embed Object | `0xE53935` |
| `timestamp` | ISO8601 | Embed Object | UTC ISO 문자열 |
| 레이트리밋 | **수치는 문서에 고정돼 있지 않다.** "should not be hard coded". 웹후크는 `webhook_id`(+token) 단위로 버킷이 따로 계산된다. 429 응답에는 `retry_after`(초)와 `global`이 들어 있고 `Retry-After` 헤더도 온다. | Rate Limits | `retry_after`만큼 기다렸다가 재시도한다. 대기 총합은 10초로 제한한다. |
| 무효 요청 | IP당 **10분에 401/403/429 응답 10,000건**을 넘으면 Cloudflare가 일시 차단한다. 404를 받은 웹후크는 다시 쓰지 않는다. | Rate Limits | 401/403/404는 재시도하지 않는다. |

- 커뮤니티에서 흔히 말하는 "웹후크당 약 5건/2초, 채널당 30건/분"은 **공식 문서에 없는 경험치**다. 설계 근거로 쓰지 않고, 응답 헤더(`X-RateLimit-Remaining`, `X-RateLimit-Reset-After`)와 429 처리에 맡긴다.
- 폭주 시나리오: KIS 장애 등으로 `fetch_asset_daily`의 계좌 태스크 약 10개가 동시에 최종 실패하면 짧은 시간에 알림이 약 10건 나간다. 429가 나도 `retry_after`가 보통 1초 안팎이라 대부분 재시도로 전달된다. 이 부분은 실측이 필요하다(§5.5-4). 그래도 부족하면 2단계로 "DAG 레벨 실패 콜백 1건으로 요약"하는 방식으로 바꾼다.

### 5.5 테스트 방법 (`test_slack_alert` 대응)

**문제점**: 현재 `test_slack_alert`는 `task_will_fail`이 **항상** 실패한다. 그래서 DAG 레벨 `on_success_callback`(복구)이 절대 호출되지 않아 **복구 알림을 검증할 수 없다.** 또 3.x에서 logical_date 없이 수동 트리거하면 복구 판정 자체가 생략된다(§5.2 `_was_previous_run_failed`).

**0) 웹후크 단독 확인 (Airflow 밖, 1분)** — embed 형식과 Slack 호환 엔드포인트 실측

```bash
WH='https://discord.com/api/webhooks/<id>/<token>'
curl -sS -X POST "$WH?wait=true" -H 'Content-Type: application/json' \
  -d '{"username":"Airflow","allowed_mentions":{"parse":[]},"embeds":[{"title":"🔴 테스트","color":15022389,"description":"```\nhello\n```","fields":[{"name":"Task","value":"`t1`","inline":true}]}]}' | head -c 300; echo
# (d) 검증: 현행 Slack Block Kit payload가 /slack 에서 어떻게 보이는지
curl -sS -o /dev/null -w '%{http_code}\n' -X POST "$WH/slack" -H 'Content-Type: application/json' \
  -d '{"attachments":[{"color":"#E53935","blocks":[{"type":"header","text":{"type":"plain_text","text":"DAG 실패"}}]}]}'
```

**1) 테스트 DAG 교체안** — `dags/test_discord_alert_dag.py` (예시, 저장소에는 쓰지 않음)

```python
"""
Discord 실패/복구 알림 테스트 DAG
- conf/Param 'fail'=True  → task_maybe_fail 실패 → 실패 알림(재시도 0회)
- conf/Param 'fail'=False → DAG 성공 → 직전 run이 실패였으면 복구 알림
복구 판정은 logical_date가 있어야 동작하므로 트리거 시 logical_date를 지정한다.
"""
from datetime import datetime

import pendulum
from airflow.sdk import Param, dag, get_current_context, task   # 2.10.4: airflow.decorators / airflow.models.param / airflow.operators.python

from callbacks import discord_failure_callback, discord_recovery_callback

kst = pendulum.timezone("Asia/Seoul")


@dag(
    dag_id="test_discord_alert",
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=kst),
    catchup=False,
    tags=["test", "discord", "alert"],
    default_args={"retries": 0, "on_failure_callback": discord_failure_callback},
    on_success_callback=discord_recovery_callback,
    params={"fail": Param(True, type="boolean", description="True면 실패 알림, False면 성공(복구 판정)")},
)
def test_discord_alert():
    @task
    def task_will_succeed():
        print("항상 성공")

    @task
    def task_maybe_fail():
        if get_current_context()["params"]["fail"]:
            raise ValueError("의도된 실패 — Discord 실패 알림 확인용\n두 번째 줄 @everyone (멘션 차단 확인)")

    task_will_succeed() >> task_maybe_fail()


test_discord_alert()
```

**2) 시나리오와 기대 결과**

| # | 트리거 | 기대 결과 | 확인 위치 |
|---|---|---|---|
| T1 | `fail=true`, logical_date=지금 | 🔴 실패 알림 1건. 제목 링크를 누르면 `/dags/test_discord_alert/runs/<run_id>/tasks/task_maybe_fail`이 열린다. 시도 `1 / 1`, 실행 기준이 KST로 표시되고 `@everyone`은 멘션되지 않는다. | Discord, 태스크 로그 |
| T2 | `fail=false`, logical_date=T1보다 뒤 | 🟢 복구 알림 1건 | Discord, **dag-processor 로그**(3.x) |
| T3 | `fail=false`, logical_date=T2보다 뒤 | 알림 없음(연속 성공) | dag-processor 로그에 예외가 없음 |
| T4 | `fail=false`, **logical_date 없이** API 트리거(3.x) | 알림 없음. 로그에 "logical_date 없는 run → 복구 판정 생략"이 남는다. | dag-processor 로그 |
| T5 | Connection `discord_alert`를 지운 뒤 `fail=true` | 알림 없음. 태스크 로그에 "미설정 → Discord 알림 생략" 경고가 남는다. **태스크 상태는 failed 그대로이고 콜백 예외는 없다.** | 태스크 로그 |
| T6 | Connection password를 잘못된 토큰으로 바꾼 뒤 `fail=true` | 401/404 에러 로그가 1회만 남고 재시도는 없다. URL은 로그에 노출되지 않는다. | 태스크 로그 |

**3) 트리거 명령 (Airflow 3.3.2)**

```bash
BASE=http://localhost:8080
TOKEN=$(curl -s -X POST $BASE/auth/token -H 'Content-Type: application/json' \
  -d '{"username":"<admin>","password":"<pw>"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')
trigger() {  # $1=fail(true|false)  $2=logical_date(ISO, 생략 시 null)
  LD=${2:+\"$2\"}; LD=${LD:-null}
  curl -s -X POST "$BASE/api/v2/dags/test_discord_alert/dagRuns" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    -d "{\"logical_date\": $LD, \"conf\": {\"fail\": $1}}" | head -c 300; echo
}
docker compose exec airflow-scheduler airflow dags unpause test_discord_alert
trigger true  "$(date -u +%FT%TZ)"; sleep 90      # T1
trigger false "$(date -u +%FT%TZ)"; sleep 60      # T2 (복구)
trigger false "$(date -u +%FT%TZ)"; sleep 60      # T3
trigger false                                      # T4 (logical_date=null)
docker compose exec airflow-dag-processor sh -c 'grep -rn "\[callback\]" /opt/airflow/logs/dag_processor | tail -20'
```

- **미래 시각의 logical_date는 3.x에서 거부된다**(A §8). 그래서 "지금"으로 지정하고 앞 run이 끝난 뒤에 다음 run을 트리거한다.
- UI로 할 때는 Trigger 폼에서 **Logical date를 직접 입력**하고 Params의 `fail`을 바꾼다.
- 2.10.4(Phase 0)에서는 `airflow dags trigger test_discord_alert -c '{"fail": true}'`를 쓴다. 2.x 수동 run은 logical_date가 트리거 시각이므로 T4 시나리오는 해당되지 않는다.

**4) (선택) 폭주와 429 확인**: `task_maybe_fail`을 `.expand(i=list(range(12)))`로 매핑한 변형 DAG을 한 번 실행한다. 알림 12건이 모두 도착하는지, 태스크 로그에 429 재시도가 있었는지 확인한다.

**5) 회귀 체크리스트에 추가할 항목** (B §7 단계 7, C Phase 2와 연결)
- [ ] T1~T6 통과(2.10.4 Phase 0에서 한 번, 3.3.2 스테이징에서 한 번)
- [ ] 3.x 정기 run의 실패/복구 알림에 표시되는 "실행 기준" 시각이 CronTriggerTimetable 의미(당일 실행 시각, A §4.2)와 맞는지 확인
- [ ] `logs/dag_processor/`와 태스크 로그에 `Failed to run task callback`이나 `[callback] ... 내부 오류`가 0건인지 확인
- [ ] 기존 `test_slack_alert_dag.py`는 `test_discord_alert`로 대체한 뒤 삭제

---

## 6. 남은 불확실성 (실측 필요)

1. **3.3.2 DAG 레벨 콜백의 `ti.get_previous_dagrun()`**: A §4.3은 소스를 근거로 dag-processor가 `GetPreviousDagRun`을 지원한다고 확인했다. 실제 반환값과 run_type 비교 결과는 T2·T3로 검증한다.
2. **(d) Slack 호환 엔드포인트의 Block Kit 처리**: 공식 문서에는 미지원 속성만 나열돼 있다. 현행 payload가 실제로 어떻게 보이는지는 §5.5-0의 curl로 확인한다.
3. **호스트 업무 DB 버전, 로그인 계정 권한, `pg_hba`**: §2.1 명령의 결과로 확정한다.
4. **PG15+ `public` 스키마 관련 `pg_restore` 에러 여부**: §1.3 리허설로 확정한다.
5. Discord 웹후크의 실제 레이트리밋 수치: 문서상 고정되지 않으므로 §5.5-4로 관찰한다.

---

## 출처 (2026-10-12 접속)

**PostgreSQL**
- Upgrading a PostgreSQL Cluster (새 버전 pg_dump 사용 권고): https://www.postgresql.org/docs/16/upgrading.html
- pg_dump (custom 형식, `-Fc`, `-Z`): https://www.postgresql.org/docs/16/app-pgdump.html
- pg_dumpall (`--globals-only`): https://www.postgresql.org/docs/16/app-pg-dumpall.html
- pg_restore (`-l`, `-L`, `-j`, `--single-transaction`, `--exit-on-error`): https://www.postgresql.org/docs/16/app-pgrestore.html
- SQL Dump / 대용량 처리: https://www.postgresql.org/docs/16/backup-dump.html
- File System Level Backup(서버 정지 필요): https://www.postgresql.org/docs/16/backup-file.html
- `password_encryption`(기본 scram-sha-256, PG14+): https://www.postgresql.org/docs/16/runtime-config-connection.html
- PG15 릴리스 노트(public 스키마 권한 변경): https://www.postgresql.org/docs/release/15.0/
- 버전 정책(PG13 EOL 2025-11): https://www.postgresql.org/support/versioning/
- Docker 공식 postgres 이미지(`POSTGRES_USER/DB`, initdb): https://hub.docker.com/_/postgres

**Airflow**
- Prerequisites 3.3.2(PG 14~18): https://airflow.apache.org/docs/apache-airflow/stable/installation/prerequisites.html
- Upgrading to Airflow 3(백업, ORM 차단, `logical_date`): https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html
- Callbacks(콜백 실행 위치, 로그): https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/callbacks.html
- Notifications(BaseNotifier 사용법): https://airflow.apache.org/docs/apache-airflow/stable/howto/notifications.html
- CLI `db check`, `db check-migrations`, `connections add`: https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html
- REST API v2(`POST /api/v2/dags/{dag_id}/dagRuns`, `/auth/token`): https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html
- Connection 환경변수(`AIRFLOW_CONN_*` JSON): https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html
- Secrets masking(민감 키워드): https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/mask-sensitive-values.html
- Discord provider 3.12.3: https://airflow.apache.org/docs/apache-airflow-providers-discord/stable/index.html , `DiscordNotifier` API: https://airflow.apache.org/docs/apache-airflow-providers-discord/stable/_api/airflow/providers/discord/notifications/discord/index.html
- Discord provider PyPI(요구 `apache-airflow>=2.11.0`): https://pypi.org/project/apache-airflow-providers-discord/
- Apprise provider(Connection 형식): https://airflow.apache.org/docs/apache-airflow-providers-apprise/stable/connections.html , PyPI: https://pypi.org/project/apache-airflow-providers-apprise/
- 소스 확인에 쓴 wheel: `apache-airflow-providers-discord==3.12.3`(`notifications/discord.py`, `notifications/embed.py`, `hooks/discord_webhook.py`), `apache-airflow-providers-apprise==2.4.0`(`notifications/apprise.py`), `apache-airflow-task-sdk==1.3.2`(`bases/notifier.py` `__call__` re-raise, `execution_time/task_runner.py` `_run_task_state_change_callbacks`·`get_previous_dagrun`·`log_url`, `api/datamodels/_generated.py` `DagRun`, `__init__.py` export 목록)

**Discord**
- Message 리소스(Embed 구조와 Embed Limits): https://docs.discord.com/developers/resources/message
- Webhook 리소스(Execute Webhook, Execute Slack-Compatible Webhook): https://docs.discord.com/developers/resources/webhook
- Rate Limits(429, `retry_after`, 무효 요청 한도, 웹후크 버킷): https://docs.discord.com/developers/topics/rate-limits
- Apprise Discord 문법: https://github.com/caronc/apprise/wiki/Notify_discord
