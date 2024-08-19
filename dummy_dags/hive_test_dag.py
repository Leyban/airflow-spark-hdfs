import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.hive.operators.hive import HiveOperator
# from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    #'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    # 'email': ['airflow@example.com'],
    # 'email_on_failure': False,
    #'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    #'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag_hive = DAG(
        dag_id = "hiveoperator_demo",
        default_args=default_args,
        # schedule_interval='0 0 * * *',
        schedule_interval='@once',
        dagrun_timeout=timedelta(minutes=60),
        description='use case of hive operator in airflow',
        start_date = airflow.utils.dates.days_ago(1)
)

create_table_hql_query = """
create table if not exists dezyre.employee (id int, name string, dept string);
"""

insert_data_hql_query = """
insert into dezyre.employee values(1, 'vamshi','bigdata'),(2, 'divya','bigdata'),(3, 'binny','projectmanager'),
(4, 'omair','projectmanager') ;"""

create_table = HiveOperator(
    hql = create_table_hql_query,
    task_id = "create_table_task",
    hive_cli_conn_id = "hive_cli",
    dag = dag_hive
)

insert_data = HiveOperator(
    hql = insert_data_hql_query,
    task_id = "insert_data_task",
    hive_cli_conn_id = "hive_cli",
    dag = dag_hive
)