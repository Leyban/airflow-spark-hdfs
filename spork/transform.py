from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import sys

# Extract Variables
HDFS_DATALAKE = sys.argv[1]
SPARK_MASTER = sys.argv[2]
JDBC_POSTGRES_COLLECTOR_CONN = sys.argv[3]
PGSOFT_URL = sys.argv[4]
POSTGRES_PASSWORD = sys.argv[5]
POSTGRES_USER = sys.argv[6]

postgres_table = 'pgsoft_wager'

hdfs_location = f'{HDFS_DATALAKE}/wagers/pgsoft'

postgres_jar_location = "../postgresql-42.6.0.jar" # TODO: Use correct path

# Start Session
spark = SparkSession.builder \
    .appName("Clean_PGSoft_Save_to_HDFS") \
    .master(SPARK_MASTER) \
    .enableHiveSupport() \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .config("spark.jars", postgres_jar_location) \
    .getOrCreate()


# Load Data into DF
df=spark.read.parquet(hdfs_location)

# Drop Duplicates
df = df.dropDuplicates(["betId"])

# Drop Null Values
df = df.na.drop(subset=["betId"]) 

# Reformat for Postgres
df = df \
    .withColumnRenamed("balanceAfter","balance_after") \
    .withColumnRenamed("balanceBefore","balance_before") \
    .withColumnRenamed("betAmount","bet_amount") \
    .withColumnRenamed("betId","bet_id") \
    .withColumnRenamed("betType","bet_type") \
    .withColumnRenamed("betTime","bet_time") \
    .withColumnRenamed("gameId","game_id") \
    .withColumnRenamed("gameId","game_id") \
    .withColumnRenamed("jackpotRtpContributionAmount","jackpot_rtp_contribution_amount") \
    .withColumnRenamed("jackpotWinAmount","jackpot_win_amount") \
    .withColumnRenamed("parentBetId","parent_bet_id") \
    .withColumnRenamed("playerName","player_name") \
    .withColumnRenamed("rowVersion","row_version") \
    .withColumnRenamed("transactionType","transaction_type") \
    .withColumnRenamed("winAmount","win_amount") \
    .drop("id") \
    .drop("year") \
    .drop("quarter")

# Add create and update timestamps
df = df.withColumn("update_at", current_timestamp()).withColumn("create_at", current_timestamp())

# Write to Postgres
df.write.format('jdbc').options(
    url=JDBC_POSTGRES_COLLECTOR_CONN,
    driver='org.postgresql.Driver',
    dbtable=postgres_table,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD
).mode('Append').save()


