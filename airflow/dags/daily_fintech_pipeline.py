from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="daily_fintech_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["fintech", "portfolio"],
) as dag:

    spark_job = BashOperator(
        task_id="spark_job",
        bash_command="""
        docker exec spark /opt/spark/bin/spark-submit \
        --packages org.postgresql:postgresql:42.7.7 \
        /opt/spark-apps/app/load_gold_to_postgres.py
        """
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
        docker exec dbt dbt run
        """
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
        docker exec dbt dbt test
        """
    )

    spark_job >> dbt_run >> dbt_test