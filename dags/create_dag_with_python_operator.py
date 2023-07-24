from airflow import DAG


from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'leyban',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

def greet(ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')
    print(f"Hello World! My name is {first_name} {last_name} and I am {age} years old")

def get_name(ti):
    ti.xcom_push(key='first_name', value='Tom')
    ti.xcom_push(key='last_name', value='Cat')

def get_age(ti):
    ti.xcom_push(key='age', value=7)

with DAG (
    default_args=default_args,
    dag_id='our_dag_with_python_operator_v10',
    description='Our first DAG with python operator',
    start_date=datetime(2023, 7, 24),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
    )
    
    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )
    
    task3 = PythonOperator(
        task_id='get_age',
        python_callable=get_age
    )
    
    [task2, task3] >> task1
    