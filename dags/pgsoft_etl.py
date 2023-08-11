from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum

PGSOFT_OLD_VERSION_TABLE ='pgsoft_old_version'
        
def get_pgversion():
    conn_collector_pg_hook = PostgresHook(postgres_conn_id='collector_conn_id')
    query = """
        SELECT row_version FROM {0} LIMIT 1
    """.format(PGSOFT_OLD_VERSION_TABLE)

    df = conn_collector_pg_hook.get_pandas_df(query)
    if not df.empty:
        latest_row_version = df['row_version'].iloc[0]
        return latest_row_version
    else:
        return None
    
dag_spark = DAG(
    'spark_demo_dag',
    description='DAG',
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False
)

get_version = PythonOperator(
    task_id='get_pgsoft_version', 
    python_callable=get_pgversion, 
    dag=dag_spark
)
    
extract = SparkSubmitOperator(
    task_id='extract_task',
    application ='/opt/airflow/files/extract.py', # TODO: Change path
    conn_id= 'spark_conn_id',
    application_args=["{{ti.xcom_pull(task_ids='get_pgsoft_version')}}"], # TODO: Check if passed correctly
    dag=dag_spark
)

transform = SparkSubmitOperator(
    task_id='transform_task',
    application ='/opt/airflow/files/transform.py' , # TODO: Change path
    conn_id= 'spark_conn_id',
    dag=dag_spark
)

extract >> transform