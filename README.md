# Airflow 개요

개인 데이터 수집·적재 파이프라인을 관리하는 Apache Airflow 레포지토리입니다.

금융 자산, 시장 데이터 등 다양한 도메인의 수집 파이프라인을 **프로젝트 단위**로 구성하며,
각 프로젝트는 독립된 `plugins/<project>/` 라이브러리와 `dags/` 내 DAG 파일 세트로 이루어집니다.

### 레포 구성 원칙

- **DAG**: `dags/` 하위에 프로젝트별 DAG 파일을 배치. 파일명은 `{동사}_{대상}_{주기}_dag.py` 형태를 권장합니다.
- **라이브러리**: `plugins/<project_name>/` 하위에 비즈니스 로직을 패키지로 구성합니다.  
  Airflow는 `plugins/` 디렉토리를 Python 경로에 자동으로 추가하므로, DAG에서 `from <project_name>.<module> import ...` 형태로 별도 설치 없이 사용할 수 있습니다.
- **격리**: 프로젝트 간 라이브러리는 서로 의존하지 않습니다. 공통 유틸리티가 필요한 경우 `plugins/shared/`에 위치시킵니다.

## 인프라

- **Airflow 버전**: 2.10.4
- **Executor**: LocalExecutor (단일 서버)
- **메타 DB**: PostgreSQL 13 (Docker 내부)
- **자산 DB**: PostgreSQL (호스트 서버, `host.docker.internal` 경유)
- **UI**: `http://localhost:8080`

```bash
# 초기 실행
docker-compose up -d

# 종료
docker-compose down
```

# 프로젝트

> 각 프로젝트의 상세 사양(API, 스키마, 함수 명세 등)은 해당 프로젝트의 `plugins/<project>/README.md`를 참고하세요.

## 1. asset-flow

> 개인 자산 데이터를 수집·적재하는 Airflow 파이프라인. KIS(한국투자증권), Upbit API를 통해 매일 자산 현황을 수집하고 PostgreSQL에 저장

### 전체 프로젝트 구조

```
airflow/
├── dags/                                   # DAG 정의 파일
│   ├── make_token_dag.py
│   ├── fetch_exchange_rate_dag.py
│   ├── fetch_fund_price_daily_dag.py
│   └── fetch_asset_daily_dag.py
├── plugins/                                # 공유 라이브러리
│   └── asset_flow/                         # 핵심 비즈니스 로직
│       ├── clients/                        # API 클라이언트 (KIS, Upbit)
│       ├── config/                         # 설정 스키마
│       ├── crawler/                        # 펀드 기준가 크롤러
│       ├── managers/                       # 토큰·DB 매니저
│       └── transformers/                   # 데이터 변환
├── docker-compose.yaml
├── Dockerfile
└── requirements.txt
```

### asset-flow 파이프라인

> 개인 금융 자산(해외주식, 국내주식, ISA, 연금저축, CMA, 암호화폐)을 매일 수집해 DB에 적재하는 파이프라인입니다.

**DAG 종속성 및 스케줄**

```
06:50  make_token
         │  (ExternalTaskSensor, delta=5m)
         ▼
06:55  fetch_exchange_rate      fetch_fund_price_daily
         │                              │
         └──────────(ExternalTaskSensor, delta=5m)──────────┐
                                                             ▼
07:00                                           fetch_asset_daily
```

---

### plugins/asset_flow 라이브러리

> DAG에서 공통으로 사용하는 비즈니스 로직 모음입니다. `plugins/` 하위에 위치하므로 별도 설치 없이 DAG에서 직접 import할 수 있습니다.

세부 사양은 [plugins/asset_flow/README.md](plugins/asset_flow/README.md)를 참고하세요.
