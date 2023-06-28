import pandas as pd
import json
import pymongo
from pendulum import datetime, from_format
from airflow import DAG
from airflow.operators.bash import BashOperator



with DAG(
        dag_id='000_clean_log_dir',
        start_date=datetime(2022, 12, 2, 15, tz="Europe/Moscow"),
        catchup=False,
        schedule_interval='0 0 * * *',
) as dag:
    clean_dir = BashOperator(
        task_id="clean_log_dir",
        bash_command="find /opt/airflow/logs -mtime +7 -delete",
    )

    clean_dir
