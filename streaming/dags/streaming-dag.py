import os
from datetime import datetime, timedelta
import requests

from airflow import DAG
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 10),
    'email': ['test@test.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='streaming-dag',
          default_args=default_args,
          catchup=False,
          schedule_interval="@once")

pyspark_app_home = "/usr/local/spark/app"
pyspark_resource = "/usr/local/spark/resources"

vin_check = BashOperator(
    task_id='vin_check',
    bash_command=f'spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 ' +
    f'{pyspark_app_home}/vin_check.py', dag=dag)


vin_check