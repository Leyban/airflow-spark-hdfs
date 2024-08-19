from airflow.decorators import dag, task_group, task
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule
from datetime import  datetime,timedelta
from pandas import DataFrame, Series
import pandas as pd


@dag(
    dag_id='hadooooooooooooop',
    description='hadoop test',
    schedule="0 16 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=3,
    )
def monthly_commission():
    @task
    def save_to_hdfs():
        df = pd.DataFrame({"a":[1,2,3], "b":[4,5,6]})
        df.to_csv("hdfs://namenode:50070")

        # I don't know UwU

