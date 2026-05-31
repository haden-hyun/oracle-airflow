FROM apache/airflow:2.10.4

# 1. airflow 유저로 Python 패키지 설치
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# 2. root로 Chromium 시스템 의존성 설치 (apt 필요)
USER root
RUN apt-get update && \
    /home/airflow/.local/bin/playwright install-deps chromium && \
    rm -rf /var/lib/apt/lists/*

# 3. airflow 유저로 Chromium 바이너리 다운로드
USER airflow
RUN playwright install chromium
