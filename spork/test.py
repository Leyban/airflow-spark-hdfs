import sys
import socket
import getpass as gt
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, date_format, col

print('==============================================')
print('Starting App')
print('==============================================')
print('User login:', gt.getuser())

hostname = socket.gethostname() 
ipaddress = socket.gethostbyname(hostname)
print(ipaddress)


print(sys.version)

print('User login:', gt.getuser())

print('======================= ALL ARGS ==========================')
print(sys.argv)

print('======================= SPARK TEST START ==========================')
spark = SparkSession.builder \
    .appName("Fetch_PGSoft_to_HDFS") \
    .enableHiveSupport() \
    .getOrCreate()



response = requests.get('http://172.17.0.1:8800/pg_soft/v2/Bet/GetHistory')
response.raise_for_status() 

if response.status_code == 404:
    print(" Error 404: Not Found ")
else:
    print(" Response contains ", len(response.json()), " rows ")

# Create DF
print(" Creating Spark Dataframe ")
json_data = response.json()
df = spark.createDataFrame(json_data) 

# Partitioning
df = df \
    .withColumn("betTime",to_timestamp(df["betTime"])) \
    .withColumn("year", date_format(col("betTime"), "yyyy")) \
    .withColumn("quarter", date_format(col("betTime"), "Q")) 

# Save to HDFS
print(" Saving to HDFS ")
df.write.partitionBy('year', 'quarter').mode('append').parquet('hdfs://namenode:9000/user/hive/test/datalake/wagers/pgsoft')

