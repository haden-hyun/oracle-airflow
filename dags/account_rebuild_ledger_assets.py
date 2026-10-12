"""
수기 원장 계좌(IRP·DC·적금·청약) asset_daily 재계산 백필 DAG (수동 트리거)

- 대상: secrets 활성 manual_* 계좌 → rebuild_account 동적 매핑
- 날짜 루프는 ledger_rebuild.rebuild_range (upsert DAG 와 공용, 정기 DAG 와 같은 핸들러)
- 원장 없는 날은 건너뜀(정상), NAV 결측일은 건너뛰고 끝에 실패 처리 (KIS·Upbit 은 과거 API 없어 대상 아님)
- 원장 입력 직후 재계산은 upsert DAG 가 자동 수행 → 이 DAG 는 NAV 결측 복구·전 계좌 일괄 재계산용
"""

from datetime import date, datetime

from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException

from alerts import discord_failure_callback, discord_recovery_callback
from asset_flow.assets import ASSET_DAILY, mark
from asset_flow.finalize import finalize
from asset_flow.runtime import KST, kst_today

DAG_DOC = """
### 목적
수기 원장 계좌만 `start_date`~`end_date` 범위로 `asset_daily` 를 재계산·재적재한다.
원장 입력에 따른 재계산은 `account_upsert_manual_position_ledger` 가 자동으로 한다.
이 DAG 는 `fund_price_daily` 결측 복구 후, 또는 수기 계좌 전체를 기간으로 다시 계산할 때 쓴다.

| Task | 내용 |
|---|---|
| `list_manual_accounts` | secrets 활성 `manual_fund`·`manual_cash` 계좌 id |
| `rebuild_account` (매핑) | 날짜 루프: 원장 → 평가 → 재적재. NAV 결측일은 모아서 마지막에 실패. 범위가 비면 skip |
| `refresh_dashboard_views` / `fail_if_upstream_failed` | 대시보드 MV 갱신 / 앞 단계 실패 시 run 실패 처리 |
"""


@dag(
    dag_id="account_rebuild_ledger_assets",
    description="수기 원장 계좌 asset_daily 백필/재계산 (날짜 범위 수동 트리거)",
    doc_md=DAG_DOC,
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=KST),
    catchup=False,
    default_args={"owner": "haejun", "retries": 0, "on_failure_callback": discord_failure_callback},
    on_success_callback=discord_recovery_callback,
    tags=["account", "manual", "ledger", "backfill"],
    params={
        "start_date": Param("2026-01-28", type="string", format="date",
                            description="백필 시작일 (asset_daily 최소 standard_date)"),
        "end_date": Param(None, type=["null", "string"], format="date",
                          description="백필 종료일(포함). 비우면 정기 DAG 최종 계산일 (asset_daily 최대 기준일, 어제 이하)"),
    },
)
def account_rebuild_ledger_assets():
    """
    Purpose: DAG 구성 (수기 계좌 목록 → 계좌별 매핑 백필 → 대시보드 갱신)
    Parameter: 없음
    Input: 없음
    Output: 없음
    """

    @task(task_id="list_manual_accounts")
    def list_manual_accounts() -> list[str]:
        """
        Purpose: 백필 대상 수기 계좌 id 목록
        Parameter: 없음
        Input: secrets 파일
        Output: 정렬된 계좌 id 리스트
        """
        from asset_flow.secrets_config import MANUAL_KINDS, load_secrets

        return sorted(a.id for a in load_secrets().active_accounts(*MANUAL_KINDS))

    @task(task_id="rebuild_account", map_index_template="{{ account_id }}", outlets=[ASSET_DAILY])
    def rebuild_account(account_id: str) -> int:
        """
        Purpose: 수기 계좌 1개 날짜 범위 재계산·재적재
        Parameter:
            - account_id: secrets 수기 계좌 id
        Input: Params(start_date, end_date), 원장, asset_daily, fund_price_daily
        Output: 적재한 날짜 수 (+ asset_daily 쓰기). NAV 결측일이 있으면 나머지 적재 후 RuntimeError
        """
        from asset_flow import db_manager as db
        from asset_flow.pipeline.dispatch import Deps
        from asset_flow.pipeline.ledger_rebuild import rebuild_end, rebuild_range

        ctx = get_current_context()
        ctx["account_id"] = account_id                       # map_index_template 표시명
        params, deps = ctx["params"], Deps.load()
        start = date.fromisoformat(params["start_date"])
        if params.get("end_date"):
            end = date.fromisoformat(params["end_date"])
        else:                                                # 기본: 정기 DAG 최종 계산일
            table = deps.app.tables.asset_daily
            end = rebuild_end(db.get_asset_date_bounds(deps.engine, table.db_schema, table.table_name)[1],
                              kst_today())
        if end is None or start > end:
            raise AirflowSkipException(f"재계산 범위 없음 ({start} ~ {end})")
        result = rebuild_range(account_id, start, end, deps)
        if result.nav_missing:
            raise RuntimeError(f"[{account_id}] NAV 결측 {len(result.nav_missing)}일: {result.nav_missing}")
        mark(ASSET_DAILY, rebuild=f"{start}~{end}", account_id=account_id, days=result.processed)
        return result.processed

    finalize(rebuild_account.expand(account_id=list_manual_accounts()))


account_rebuild_ledger_assets()
