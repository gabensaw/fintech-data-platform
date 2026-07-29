FROM apache/airflow:3.0.2

COPY requirements.txt /
COPY --chmod=755 wait-for-postgres.sh /opt/airflow/wait-for-postgres.sh
COPY --chmod=755 entrypoint.sh /opt/airflow/entrypoint.sh

RUN pip install --no-cache-dir -r /requirements.txt