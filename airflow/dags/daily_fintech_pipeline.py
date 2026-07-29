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

    build_silver_layer = BashOperator(
        task_id="build_silver_layer",
        bash_command="docker exec spark /opt/spark/bin/spark-submit /opt/spark-apps/app/silver_job.py",
    )

    build_gold_layer = BashOperator(
        task_id="build_gold_layer",
        bash_command="docker exec spark /opt/spark/bin/spark-submit /opt/spark-apps/app/gold_job.py",
    )

    load_gold_to_postgres = BashOperator(
        task_id="load_gold_to_postgres",
        bash_command="docker exec spark /opt/spark/bin/spark-submit /opt/spark-apps/app/load_gold_to_postgres.py",
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

    (
        build_silver_layer
        >> build_gold_layer
        >> load_gold_to_postgres
        >> dbt_run
        >> dbt_test
    )
