from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import Row
from datetime import date
from pyspark.sql.functions import lit
import pyspark
import sys
import socket
import getpass as gt

spark = SparkSession.builder \
    .appName("HadoopSparkTest") \
    .master("spark://spark-master:7077") \
    .config("spark.submit.deployMode","cluster") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .enableHiveSupport() \
    .getOrCreate()


print(sys.version)

print("PySpark version:", pyspark.version)

print("User login:", gt.getuser())

print("======================= ALL ARGS ==========================")
print(sys.argv)

print("======================= INDIVIDUAL ARGS START ==========================")

for v in sys.argv:
    print(v)

print("======================= INDIVIDUAL ARGS START ==========================")


# #hostname = "spark-master"

# #ipaddress = socket.gethostbyname(hostname)

# spark = SparkSession.builder \
#     .appName("HadoopSparkTest") \
#     .master("spark://spark-master:7077") \
#     .config("spark.submit.deployMode","cluster") \
#     .enableHiveSupport() \
#     .getOrCreate()
    
# today = date.today()
# year = today.year
# month = today.month
# day = today.day

# record = Row("key", "value")
# df = spark.createDataFrame([record(i, "val" + str(i)) for i in range(1, 10)])
# df.createOrReplaceTempView("records")

# out_df = df.withColumn('year', lit(year)).withColumn('month', lit(month)).withColumn('day', lit(day))
# out_df.show()

# tblLocation = 'hdfs://namenode:8020/user/hive/datalake/records'
# out_df.write.partitionBy('year', 'month', 'day').mode('append').parquet(tblLocation)

# # sqlDF = spark.sql("SELECT * FROM records")
# # sqlDF.show()

# # #textFile = spark.sparkContext.textFile("hdfs://172.16.199.45:8020/user/hive/test01.txt")
# # #textFile.collect()
# # textFile = spark.read.text("hdfs://172.16.199.46:8020/user/hive/test01.txt")
# # textFile.show()

# spark.stop()