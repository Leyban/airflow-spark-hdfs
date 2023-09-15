from airflow import DAG

from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator

import pkg_resources

default_args = {
    'owner': 'leyban',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

def greet():
    print([p.project_name for p in pkg_resources.working_set])

with DAG (
    default_args=default_args,
    dag_id='inspect_dag',
    description='Our first DAG with python operator',
    start_date=datetime(2023, 7, 24),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
    )
    task1
    