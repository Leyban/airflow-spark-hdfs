from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from datetime import datetime

@dag(
        start_date=datetime(2023,1,1),
        catchup=False
        )
def call_go_bin():
    bash_it = BashOperator(
            task_id = "go_bash",
            bash_command= "/opt/bin/test"
    )
    bash_it

call_go_bin()
