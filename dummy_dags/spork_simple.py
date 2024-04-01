from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

from pyspark.sql import SparkSession

def sparktest():
    # Spark session & context
    spark = SparkSession.builder.master("spark://172.17.0.1:7077").getOrCreate()
    sc = spark.sparkContext

    # Sum of the first 100 whole numbers
    rdd = sc.parallelize(range(100 + 1))
    rdd.sum()
    # 5050
    
default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'owner': 'Leyban'
}

with DAG(
    default_args=default_args,
    dag_id='test_simple_spork',
    start_date=datetime(2023, 7, 24),
    schedule_interval="0 10 * * *",
    catchup=False
) as dag:
    test = PythonOperator(
        task_id='sparktest',
        python_callable=sparktest
    )

    test