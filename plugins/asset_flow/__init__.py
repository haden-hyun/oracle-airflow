"""
자산 수집 애플리케이션 패키지 (Airflow 3.x 전용)

- 설정: secrets_config(비밀) · app_config(비밀 아님) · runtime(날짜)
- 하위 계층: clients · transformers · crawler · db_manager · tokens · holiday
- 상위 계층: pipeline.dispatch(수집 조합) · ops.key_expiry(점검) — 의존 방향은 상위 → 하위 한 방향
"""
