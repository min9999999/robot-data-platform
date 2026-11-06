FROM apache/airflow:2.10.1

# dbt는 Airflow와 패키지 버전이 충돌하지 않도록 별도 가상환경에 설치
RUN python -m venv /home/airflow/dbt_venv \
 && /home/airflow/dbt_venv/bin/pip install --no-cache-dir \
      "dbt-core~=1.12.0" "dbt-snowflake~=1.12.0"