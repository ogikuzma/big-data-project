import os
from datetime import datetime, timedelta
import requests

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator


def check_if_preprocessed_file_exists():
    r = requests.get("http://namenode:9870/webhdfs/v1/user/root/data-lake/transformation/batch-preprocessed.csv?op=GETFILESTATUS")
    print(r.status_code)
    if r.status_code == 200:
        return "proceed_to_batch_task"
    else:
        return "preprocessing"

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

dag = DAG(dag_id='batch-dag',
          default_args=default_args,
          catchup=False,
          schedule_interval="@once")

pyspark_app_home = "/usr/local/spark/app"
pyspark_resource = "/usr/local/spark/resources"

preprocessing_task = DummyOperator(task_id='preprocessing_task', dag=dag)
proceed_to_batch_task = DummyOperator(task_id='proceed_to_batch_task', dag=dag)

branch_task = BranchPythonOperator(
    task_id='branching',
    python_callable=check_if_preprocessed_file_exists,
    dag=dag,
)

preprocessing = BashOperator(
    task_id='preprocessing',
    bash_command=f'spark-submit --master spark://spark-master:7077 ' +
    f'{pyspark_app_home}/preprocessing.py', dag=dag)

batch = BashOperator(
    task_id='batch',
    bash_command=f'spark-submit --master spark://spark-master:7077 --jars {pyspark_resource}/postgresql-42.3.0.jar ' +
    f'{pyspark_app_home}/processing.py', trigger_rule='none_failed', dag=dag)

branch_task >> [preprocessing, proceed_to_batch_task] >> batch