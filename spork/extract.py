from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, date_format, col
import logging
import requests
import sys

import psycopg2

# Extract Variables
psycopg2_conn_string = sys.argv[1]
HDFS_DATALAKE = sys.argv[2]
SPARK_MASTER = sys.argv[3]
pg_history_url = sys.argv[4]
secret_key = sys.argv[5]
operator_token = sys.argv[6]
postgres_password = sys.argv[7]

history_api = '/v2/Bet/GetHistory'

url = f"{pg_history_url}{history_api}" 

tblLocation = f'{HDFS_DATALAKE}/wagers/pgsoft'


def get_simpleplay_version():    
    try:
        # Define connection
        conn = psycopg2.connect(psycopg2_conn_string, password=postgres_password)
        cursor = conn.cursor()

        # Execute Query
        print("executing")
        cursor.execute("SELECT row_version FROM pgsoft_version LIMIT 1")

        # Get all results
        print("fetching")
        pgsoft_version = cursor.fetchone()

        print(f" PG_SOFT Version {pgsoft_version[0]} ")
        cursor.close()
        conn.close()
            
        return pgsoft_version[0]
    
    except:
        logging.fatal("Unable to fetch data") 


def main():
    # Initialize spark session
    print(" Initializing Spark Session ")
    spark = SparkSession.builder \
        .appName("Fetch_PGSoft_to_HDFS") \
        .master(SPARK_MASTER) \
        .enableHiveSupport() \
        .getOrCreate()

    try:
        # Get Version
        pgsoft_version = get_simpleplay_version()
        
        # Fetch From API
        form_data = {
            "secret_key":     secret_key,
            "operator_token": operator_token,
            "bet_type":        "1",
            "row_version":  pgsoft_version,
            "count":          "5000"
        }
        
        print(f"Start download pg: row_version {pgsoft_version}")
        response = requests.post(url, data=form_data)
        response.raise_for_status() 

        if response.status_code == 404:
            print(" Error 404: Not Found ")
        else:
            print(f" Response contains {len(response.json())} rows ")

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
        df.write.partitionBy('year', 'quarter').mode('append').parquet(tblLocation)

    except requests.exceptions.RequestException as err:
        logging.fatal("Request error:", err)

    except Exception as Argument:
        logging.fatal(f"Error occurred: {Argument}")

if __name__=="__main__":
    main()