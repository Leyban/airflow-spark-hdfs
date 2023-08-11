from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# Define Constants
# TODO: Use correct params
postgres_conn_string = 'jdbc:postgresql://172.17.0.1:5432/collectorDB' 
postgres_user = 'postgres'
postgres_password = 'secret'
postgres_table = 'pgsoft_wager'

hdfs_location = './user/hive/datalake/wagers/pgsoft' # TODO: Use correct path

postgres_jar_location = "../postgresql-42.6.0.jar" # TODO: Use correct path

# Start Session
spark = SparkSession.builder \
    .appName("Clean_PGSoft_Save_to_HDFS") \
    .master("local") \
    .enableHiveSupport() \
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
    url=postgres_conn_string,
    driver='org.postgresql.Driver',
    dbtable=postgres_table,
    user=postgres_user,
    password=postgres_password
).mode('Append').save()


