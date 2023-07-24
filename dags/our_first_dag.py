from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args= {
    'owner': 'leyban',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='our_first_dag_v6',
    default_args=default_args,
    description='This is our first dag that we write',
    start_date=datetime(2021, 7, 29, 2,),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo hello world this is the first task"
    )
    
    task2 = BashOperator(
        task_id='second_task',
        bash_command="echo I am the second task that will run after task1"
    )    
    
    task3 = BashOperator(
        task_id='third_task',
        bash_command="echo I am the third task that will run after task3"
    )
    
    # Task dependency method 1
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)
    
    # Task dependency method 2
    # task1 >> task2
    # task1 >> task3
    
    # Task dependency method 2
    task1 >> [task2, task3]
    