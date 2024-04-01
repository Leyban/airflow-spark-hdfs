from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable

from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum

# PGSOFT_VERSION_TABLE ='pgsoft_version'

# HDFS_DATALAKE = Variable.get("HDFS_DATALAKE")
# JDBC_POSTGRES_COLLECTOR_CONN = Variable.get("JDBC_POSTGRES_COLLECTOR_CONN")
# PGSOFT_URL = Variable.get("PGSOFT_URL")
# # POSTGRES_SECRET_PASSWORD = Variable.get("POSTGRES_SECRET_PASSWORD")
# POSTGRES_USER = Variable.get("POSTGRES_USER")

# vardict = {
#     "HDFS_DATALAKE" : Variable.get("HDFS_DATALAKE"),
#     "JDBC_POSTGRES_COLLECTOR_CONN" : Variable.get("JDBC_POSTGRES_COLLECTOR_CONN"),
#     "PGSOFT_URL" : Variable.get("PGSOFT_URL"),
#     # "POSTGRES_SECRET_PASSWORD" : Variable.get("POSTGRES_SECRET_PASSWORD"),
#     "POSTGRES_USER" : Variable.get("POSTGRES_USER")
# }

# def get_pgversion():
#     conn_collector_pg_hook = PostgresHook(postgres_conn_id='collector_conn_id')
#     query = """
#         SELECT row_version FROM {0} LIMIT 1
#     """.format(PGSOFT_VERSION_TABLE)

#     df = conn_collector_pg_hook.get_pandas_df(query)
#     if not df.empty:
#         latest_row_version = df['row_version'].iloc[0]
#         return latest_row_version
#     else:
#         return None

    
dag_spark = DAG(
    'arg_test',
    description='DAG',
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False
)

# argpass = PythonOperator(
#     task_id='get_pgsoft_version', 
#     python_callable=get_pgversion, 
#     dag=dag_spark
# )
    
# argprint = SparkSubmitOperator(
#     task_id='argprint',
#     application ='/opt/spork/test.py',
#     conn_id= 'spark_conn_id',
#     application_args=["{{ti.xcom_pull(task_ids='get_pgsoft_version')}}", HDFS_DATALAKE, JDBC_POSTGRES_COLLECTOR_CONN, PGSOFT_URL,POSTGRES_USER], # TODO: Check if passed correctly
#     dag=dag_spark
# )

# argpass >> argprint
