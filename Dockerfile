ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.12
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}
ARG AIRFLOW_VERSION
ARG PYTHON_VERSION
# 공식 constraints (빌드 재현성). 베이스 이미지에 Airflow 본체·provider 는 이미 같은 버전으로 설치돼 있음
ARG CONSTRAINTS_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

# 애플리케이션 의존성 (Airflow 본체가 움직이지 않도록 함께 지정)
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt -c "${CONSTRAINTS_URL}" \
 && pip check \
 && pip freeze > /home/airflow/pip-freeze.txt
