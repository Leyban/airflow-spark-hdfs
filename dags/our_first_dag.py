from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args= {
    'owner': 'leyban',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

def context_test(**context):
    print(context)
    print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
    print(context['ds_nodash'])

with DAG(
    dag_id='context_test',
    default_args=default_args,
    description='just testing the context thingy',
    start_date=datetime(2021, 7, 29, 2,),
    schedule_interval=None
) as dag:
    task1 = PythonOperator(
        task_id='first_task',
        python_callable=context_test
    )
    task1
