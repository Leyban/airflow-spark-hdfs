from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import Row
from datetime import date
from pyspark.sql.functions import lit

def sparktest():
    spark = SparkSession.builder \
        .appName("HadoopSparkTest") \
        .master("local[1]") \
        .enableHiveSupport() \
        .getOrCreate()

    today = date.today()
    year = today.year
    month = today.month
    day = today.day

    record = Row("key", "value")
    df = spark.createDataFrame([record(i, "val" + str(i)) for i in range(1, 10)])
    df.createOrReplaceTempView("records")

    out_df = df.withColumn('year', lit(year)).withColumn('month', lit(month)).withColumn('day', lit(day))
    print(out_df.show())

    tblLocation = 'hdfs://namenode:8020/user/hive/datalake/records.parquet'
    out_df.write.partitionBy('year', 'month', 'day').mode('append').parquet(tblLocation)
    
    
default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'owner': 'Leyban'
}

with DAG(
    default_args=default_args,
    dag_id='test',
    start_date=datetime(2023, 7, 24),
    schedule_interval="0 10 * * *",
    catchup=False
) as dag:
    test = PythonOperator(
        task_id='sparktest',
        python_callable=sparktest
    )

    test