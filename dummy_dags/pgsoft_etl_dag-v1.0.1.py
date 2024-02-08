from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import pendulum

# DAG Definition
dag_spark = DAG(
    'pgsoft_etl-v1.0.1',
    description='DAG',
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False
)

extract = SparkSubmitOperator(
    task_id='extract',
    application ="{{ var.value.get('SPARK_APP_DIR') }}/extract.py",
    conn_id= 'spark_conn_id',
    env_vars={
        'COLLECTOR_DB_CONN_STR': "{{ var.value.get('COLLECTOR_DB_CONN_STR')}}",
        'HDFS_DATALAKE': "{{ var.value.get('HDFS_DATALAKE')}}", 
        'SPARK_MASTER': "{{ var.value.get('SPARK_MASTER')}}",
        'PGSOFT_URL': "{{ var.value.get('PGSOFT_URL')}}", 
        'PGSOFT_KEY': "{{ var.value.get('PGSOFT_KEY')}}",
        'PGSOFT_OPERATOR': "{{ var.value.get('PGSOFT_OPERATOR')}}",
        'POSTGRES_PASSWORD': "{{ var.value.get('POSTGRES_PW')}}"
    },
    dag=dag_spark
)

transform = SparkSubmitOperator(
    task_id='transform',
    application ="{{ var.value.get('SPARK_APP_DIR') }}/transform.py",
    conn_id= 'spark_conn_id',
    env_vars={
        'HDFS_DATALAKE': "{{ var.value.get('HDFS_DATALAKE')}}", 
        'SPARK_MASTER': "{{ var.value.get('SPARK_MASTER')}}",
        'JDBC_POSTGRES_COLLECTOR_CONN': "{{ var.value.get('JDBC_POSTGRES_COLLECTOR_CONN')}}", 
        'POSTGRES_PW': "{{ var.value.get('POSTGRES_PW')}}", 
        'POSTGRES_USER': "{{ var.value.get('POSTGRES_USER')}}",
        'SPARK_DRIVERS_DIR': "{{ var.value.get('SPARK_DRIVERS_DIR')}}",
    },
    dag=dag_spark
)

extract >> transform