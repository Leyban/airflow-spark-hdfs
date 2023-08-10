from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import Row
from datetime import date
from pyspark.sql.functions import lit
import pyspark
import sys
import socket
import getpass as gt

print(sys.version)

print("PySpark version:", pyspark.version)

print("User login:", gt.getuser())

#hostname = "spark-master"

#ipaddress = socket.gethostbyname(hostname)

spark = SparkSession.builder \
    .appName("HadoopSparkTest") \
    .master("spark://spark-master:7077") \
    .config("spark.submit.deployMode","cluster") \
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
out_df.show()

# tblLocation = 'hdfs://172.16.199.46:8020/user/hive/datalake/records'
# out_df.write.partitionBy('year', 'month', 'day').mode('append').parquet(tblLocation)

# sqlDF = spark.sql("SELECT * FROM records")
# sqlDF.show()

# #textFile = spark.sparkContext.textFile("hdfs://172.16.199.45:8020/user/hive/test01.txt")
# #textFile.collect()
# textFile = spark.read.text("hdfs://172.16.199.46:8020/user/hive/test01.txt")
# textFile.show()

spark.stop()
# spark_demo_dag.py

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum


dag_spark = DAG(
    'spark_demo_dag',
    description='DAG',
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
)

extract = SparkSubmitOperator(
    task_id='extract_task',
    application ='/opt/airflow/files/extract.py' ,
    conn_id= 'spark_conn_id',
    dag=dag_spark
)

transform = SparkSubmitOperator(
    task_id='transform_task',
    application ='/opt/airflow/files/transform.py' ,
    conn_id= 'spark_conn_id',
    dag=dag_spark
)


extract >> transform
transform
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import date


spark = SparkSession.builder.master("spark://spark-master:7077") \
                .appName('Transfrom data') \
                .config('spark.sql.warehouse.dir', 'hdfs://172.16.199.46:8020/user/hive/warehouse') \
                .config('hive.metastore.uris', 'thrift://172.16.199.46:9083') \
                .config('hive.exec.dynamic.partition', 'true') \
                .config('hive.exec.dynamic.partition.mode', 'nonstrict') \
                .enableHiveSupport() \
                .getOrCreate()

df = spark.read.parquet('hdfs://172.16.199.46:8020/user/hive/datalake/records')

#spark.sql("CREATE database IF NOT EXISTS reports")
#spark.sql("USE reports")
#spark.sql("CREATE TABLE IF NOT EXISTS daily_rebate (key BIGINT, value STRING, year INT, month INT, day INT, calculated STRING) USING HIVE")

df.show()

#result_df = df.withColumn('year', lit(year)).withColumn('month', lit(month)).withColumn('day', lit(day)).withColumn('calculated', lit('true'))
#result_df.show()
#result_df.write.format('hive').mode('append').saveAsTable('reports.daily_rebate')

spark.stop() 