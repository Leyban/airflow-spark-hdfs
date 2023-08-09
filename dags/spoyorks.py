from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
import pendulum

import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkFiles
from pyspark.sql import types
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import functions as F

import pandas as pd
import datetime


default_args = {
    'owner': 'Leyban'
}

dag_spark = DAG(
    'spark_demo_dag',
    default_args=default_args,
    description='DAG',
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
)

test = SparkSubmitOperator(
    task_id='sporkitest',
    application ='/opt/spork/test.py' ,
    conn_id= 'spark_conn_id',
    verbose=True,
    dag=dag_spark
)

test 