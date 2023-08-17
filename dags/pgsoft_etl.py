from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable

from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum

# Parameters
PGSOFT_VERSION_TABLE ='pgsoft_version'

HDFS_DATALAKE = Variable.get("HDFS_DATALAKE")
SPARK_MASTER = Variable.get("SPARK_MASTER")
SPARK_APP_DIR = Variable.get("SPARK_APP_DIR")

PGSOFT_URL = Variable.get("PGSOFT_URL")
PGSOFT_KEY = Variable.get("PGSOFT_KEY")
PGSOFT_OPERATOR = Variable.get("PGSOFT_OPERATOR")

JDBC_POSTGRES_COLLECTOR_CONN = Variable.get("JDBC_POSTGRES_COLLECTOR_CONN")
PGSOFT_URL = Variable.get("PGSOFT_URL")
POSTGRES_PASSWORD = Variable.get("POSTGRES_PW")
POSTGRES_USER = Variable.get("POSTGRES_USER")

COLLECTOR_DB_CONN_STR = Variable.get("COLLECTOR_DB_CONN_STR")


# DAG Definition
dag_spark = DAG(
    'pgsoft_etl',
    description='DAG',
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False
)

extract = SparkSubmitOperator(
    task_id='extract_task',
    application =f'{SPARK_APP_DIR}/extract.py',
    conn_id= 'spark_conn_id',
    application_args=[
        COLLECTOR_DB_CONN_STR, 
        HDFS_DATALAKE, 
        SPARK_MASTER,
        PGSOFT_URL, 
        PGSOFT_KEY,
        PGSOFT_OPERATOR,
        POSTGRES_PASSWORD
    ], 
    dag=dag_spark
)

transform = SparkSubmitOperator(
    task_id='transform_task',
    application =f'{SPARK_APP_DIR}/transform.py',
    conn_id= 'spark_conn_id',
    application_args=[
        HDFS_DATALAKE, 
        SPARK_MASTER,
        JDBC_POSTGRES_COLLECTOR_CONN, 
        POSTGRES_PASSWORD, 
        POSTGRES_USER
    ], 
    dag=dag_spark
)

extract >> transform