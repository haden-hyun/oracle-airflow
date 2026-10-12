ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.12
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}
ARG AIRFLOW_VERSION
ARG PYTHON_VERSION
# 공식 constraints (빌드 재현성). 베이스 이미지에 Airflow 본체·provider 는 이미 같은 버전으로 설치돼 있음
ARG CONSTRAINTS_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

# 1. 애플리케이션 의존성 (Airflow 본체가 움직이지 않도록 함께 지정)
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt -c "${CONSTRAINTS_URL}" \
 && pip check \
 && pip freeze > /home/airflow/pip-freeze.txt

# 2. Chromium 시스템 의존성 (root)
#    3.x 이미지는 venv(/home/airflow/.local)를 쓰므로 venv의 python으로 실행해야 모듈을 찾는다
USER root
RUN apt-get update \
 && /home/airflow/.local/bin/python -m playwright install-deps chromium \
 && rm -rf /var/lib/apt/lists/*

# 3. Chromium 바이너리 (headless만 사용하므로 headless shell만 설치)
USER airflow
RUN python -m playwright install --only-shell chromium
