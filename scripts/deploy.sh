#!/usr/bin/env bash
# 배포: main fast-forward → 변경 범위 판별 → 재시작/재빌드 → 검증
#
#   scripts/deploy.sh                 # git fetch + merge --ff-only 후 반영 (기본)
#   scripts/deploy.sh --range A B     # 이미 받은 변경 A..B 반영 (git 훅이 사용)
#   scripts/deploy.sh --check         # 반영 없이 검증·드리프트 점검만
#   옵션: --dry-run (할 일만 출력) · --force (금지 시간대·실행 중 run 무시)
#
# 변경 → 조치
#   Dockerfile · requirements.txt · docker-compose.yaml → build + up -d (설정 같은 postgres 는 재생성 안 됨)
#   plugins/**                                       → restart scheduler·dag-processor·apiserver
#   그 외 (dags/ · config · docs)                    → 없음 (자동 재파싱 / 매 실행 다시 읽음)
#
# 05:50~08:00 KST (정기 DAG 06:00~07:50) 에는 재시작·재빌드하지 않고 경고만 (--force 로 무시)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

APP_SERVICES=(airflow-apiserver airflow-scheduler airflow-dag-processor)
RESTART_SERVICES=(airflow-scheduler airflow-dag-processor airflow-apiserver)
BLOCK_START=0550   # KST HHMM
BLOCK_END=0800

mode=pull; from=""; to=""; dry=0; force=0
while (($#)); do
  case "$1" in
    --range) mode=range; from="$2"; to="$3"; shift 3 ;;
    --check) mode=check; shift ;;
    --dry-run) dry=1; shift ;;
    --force) force=1; shift ;;
    -h|--help) sed -n '2,13p' "$0"; exit 0 ;;
    *) echo "알 수 없는 인자: $1" >&2; exit 2 ;;
  esac
done

red()  { printf '\033[1;31m%s\033[0m\n' "$*"; }
yel()  { printf '\033[1;33m%s\033[0m\n' "$*"; }
grn()  { printf '\033[1;32m%s\033[0m\n' "$*"; }
run()  { echo "+ $*"; ((dry)) || "$@"; }

in_block_window() {
  local now=$((10#$(TZ=Asia/Seoul date +%H%M)))
  ((now >= 10#$BLOCK_START && now < 10#$BLOCK_END))
}

# 변경 파일 → 조치 수준 (rebuild > restart > none)
classify() {
  local files="$1" level=none
  while IFS= read -r f; do
    case "$f" in
      Dockerfile|requirements.txt|docker-compose.yaml) level=rebuild ;;
      plugins/*) [[ $level == rebuild ]] || level=restart ;;
    esac
  done <<<"$files"
  echo "$level"
}

# 실행 중 dag_run 수 (메타 DB 읽기 전용)
active_runs() {
  docker compose exec -T postgres psql -U airflow -d airflow -tAc \
    "select count(*) from dag_run where state in ('running','queued')" 2>/dev/null | tr -d '[:space:]'
}

wait_healthy() {
  local deadline=$((SECONDS + 300)) s id st
  for s in "${APP_SERVICES[@]}"; do
    while :; do
      id=$(docker compose ps -q "$s")
      st=$(docker inspect -f '{{.State.Health.Status}}' "$id" 2>/dev/null || echo missing)
      [[ $st == healthy ]] && break
      ((SECONDS < deadline)) || { red "  $s: $st (5분 초과)"; return 1; }
      sleep 5
    done
  done
}

# plugins/ · 이미지 입력 변경이 컨테이너 기동 이후면 옛 모듈로 실행 중
drift_check() {
  local started started_ts changed_ts
  started=$(docker inspect -f '{{.State.StartedAt}}' "$(docker compose ps -q airflow-scheduler)")
  started_ts=$(date -d "$started" +%s)
  changed_ts=$( { git log -1 --format=%ct -- plugins Dockerfile requirements.txt docker-compose.yaml
                  find plugins -name '*.py' -newermt "@$started_ts" -printf '%T@\n' 2>/dev/null | cut -d. -f1; } | sort -n | tail -1)
  echo "  scheduler 기동 $(TZ=Asia/Seoul date -d "$started" '+%F %T KST')"
  if ((changed_ts > started_ts)); then
    red "  ✗ plugins/·이미지 입력이 기동 이후 변경됨 ($(TZ=Asia/Seoul date -d "@$changed_ts" '+%F %T KST')) → 재시작 필요"
    return 1
  fi
  echo "  ✓ 기동 이후 plugins/·이미지 입력 변경 없음"
}

verify() {
  local ok=0
  echo "== 검증"
  wait_healthy && echo "  ✓ ${APP_SERVICES[*]} healthy" || ok=1
  drift_check || ok=1

  local errs
  errs=$(docker compose exec -T airflow-scheduler airflow dags list-import-errors -o plain 2>&1 || true)
  if [[ -z ${errs//[[:space:]]/} || $(tr -d '\r' <<<"$errs" | sed '/^[[:space:]]*$/d') == "No data found" ]]; then echo "  ✓ list-import-errors 없음"
  else red "  ✗ import 오류"; sed 's/^/    /' <<<"$errs"; ok=1; fi

  # 새 프로세스로 plugins 로드 + 설정 검증 (secrets 내용은 출력하지 않음)
  if docker compose exec -T -e PYTHONPATH=/opt/airflow/plugins airflow-scheduler python -c \
      'from asset_flow.app_config import load_app_config; load_app_config(); print("ok")' >/dev/null 2>&1; then
    echo "  ✓ load_app_config() 통과"
  else
    red "  ✗ load_app_config() 실패:"
    docker compose exec -T -e PYTHONPATH=/opt/airflow/plugins airflow-scheduler python -c \
      'from asset_flow.app_config import load_app_config; load_app_config()' 2>&1 | tail -3 | sed 's/^/    /'
    ok=1
  fi
  ((ok == 0)) && grn "== 정상" || red "== 검증 실패 — 위 항목 확인"
  return $ok
}

apply() {
  local level="$1"
  [[ $level == none ]] && { echo "== 재시작 불필요 (dags/·config·문서만)"; return 0; }

  if ((!force)) && in_block_window; then
    red "== 05:50~08:00 KST 정기 DAG 시간대 — $level 하지 않음"
    red "   08:00 이후 scripts/deploy.sh --check 로 확인 후 반영: scripts/deploy.sh --range <이전> HEAD"
    return 3
  fi
  local n; n=$(active_runs || echo "?")
  if ((!force)) && [[ $n != 0 ]]; then
    [[ $n == "?" || -z $n ]] && red "== 메타 DB 조회 실패 — 실행 중 run 확인 불가 (확인 후 --force)" \
                            || red "== 실행 중·대기 dag_run ${n}건 — 끝난 뒤 다시 실행 (또는 --force)"
    return 4
  fi

  case "$level" in
    rebuild)
      echo "== 이미지 재빌드 + 재생성 (postgres 제외)"
      run docker compose build
      run docker compose up -d   # airflow-init(migrate) 포함, 설정 같은 postgres 는 그대로
      ;;
    restart)
      echo "== plugins/ 변경 → 재시작"
      run docker compose restart "${RESTART_SERVICES[@]}"
      ;;
  esac
}

case "$mode" in
  check)
    verify ;;
  pull|range)
    if [[ $mode == pull ]]; then
      [[ $(git rev-parse --abbrev-ref HEAD) == main ]] || { red "main 브랜치가 아님 — 중단"; exit 1; }
      git fetch --quiet
      from=$(git rev-parse HEAD); to=$(git rev-parse '@{u}')
      # 훅이 같은 반영을 두 번 하지 않도록
      export AIRFLOW_DEPLOY_RUNNING=1
      run git merge --ff-only '@{u}'
    fi
    files=$(git diff --name-only "$from" "$to")
    if [[ -z $files ]]; then echo "== 변경 없음 ($(git rev-parse --short "$to"))"
    else echo "== 변경 $(git rev-parse --short "$from")..$(git rev-parse --short "$to")"; sed 's/^/  /' <<<"$files"; fi
    level=$(classify "$files")
    rc=0; apply "$level" || rc=$?
    ((dry)) && exit $rc
    ((rc == 0)) || exit $rc
    verify ;;
esac
