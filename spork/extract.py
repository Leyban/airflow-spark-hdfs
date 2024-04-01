from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, date_format, col
import logging
import requests
import sys
import os

import psycopg2

# Extract Variables
psycopg2_conn_string = os.getenv('COLLECTOR_DB_CONN_STR')
HDFS_DATALAKE = os.getenv( 'HDFS_DATALAKE' ) 
SPARK_MASTER = os.getenv( 'SPARK_MASTER' ) 
pg_history_url = os.getenv( 'PGSOFT_URL' ) 
secret_key = os.getenv( 'PGSOFT_KEY' ) 
operator_token = os.getenv( 'PGSOFT_OPERATOR' ) 
postgres_password = os.getenv( 'POSTGRES_PASSWORD' ) 

history_api = '/v2/Bet/GetHistory'
url = f"{pg_history_url}{history_api}" 

tblLocation = f'{HDFS_DATALAKE}/wagers/pgsoft'

def get_simpleplay_version():    
    try:
        # Define connection
        conn = psycopg2.connect(psycopg2_conn_string[1:-1], password=postgres_password)
        cursor = conn.cursor()

        # Execute Query
        cursor.execute("SELECT row_version FROM pgsoft_version LIMIT 1")

        # Get all results
        pgsoft_version = cursor.fetchone()

        print(f" PG_SOFT Version {pgsoft_version[0]} ")
        cursor.close()
        conn.close()
            
        return pgsoft_version[0]
    
    except:
        logging.exception("message") 


def main():
    # Initialize spark session
    print(" Initializing Spark Session ")
    spark = SparkSession.builder \
        .appName("Fetch_PGSoft_to_HDFS") \
        .remote(SPARK_MASTER) \
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
        sys.exit(1)

    except Exception as Argument:
        logging.fatal(f"Error occurred: {Argument}")
        sys.exit(1)

if __name__=="__main__":
    main()