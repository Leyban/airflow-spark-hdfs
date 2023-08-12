from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, date_format, col
import logging
import requests
import sys

# Initialize spark session
spark = SparkSession.builder \
    .appName("Fetch_PGSoft_to_HDFS") \
    .master("local") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .enableHiveSupport() \
    .getOrCreate()

# Extract Spark Session from arguments
# TODO: Inspect Arguments
latest_row_version = sys.argv[1]

# Define constants
secret_key = ""
operator_token = ""
pg_history_url = ""

history_api = '/v2/Bet/GetHistory'

form_data = {
    "secret_key":     secret_key,
    "operator_token": operator_token,
    "bet_type":        "1",
    "row_version":  latest_row_version,
    "count":          "5000"
}

url = "http://172.17.0.1:8800/pg_soft" # TODO: Use Correct API
# url = f"{pg_history_url}{history_api}" 

tblLocation = 'hdfs://172.18.0.1:9010/user/hive/datalake/wagers/pgsoft' # TODO: Use correct file path

try:
    # Fetch From API
    print(f"Start download pg: row_version {latest_row_version}")
    response = requests.post(url, data=form_data)
    response.raise_for_status() 
    
    if response.status_code == 404:
        print("Error 404: Not Found")
    else:
        json_content = response.json()
        print(json_content)
    
    # Create DF
    print(f"response contains {len(response.json())} rows")
    json_data = response.json()
    df = spark.createDataFrame(json_data) 

    # Partitioning
    df = df \
        .withColumn("betTime",to_timestamp(df["betTime"])) \
        .withColumn("year", date_format(col("betTime"), "yyyy")) \
        .withColumn("quarter", date_format(col("betTime"), "Q")) 

    # Save to HDFS
    df.write.partitionBy('year', 'quarter').mode('append').parquet(tblLocation)

except requests.exceptions.RequestException as err:
    print("Request error:", err)

except Exception as Argument:
    logging.exception(f"Error occurred: {Argument}")