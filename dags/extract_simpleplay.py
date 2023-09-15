from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import psycopg2
import pandas as pd

import os

def extract_simpleplay():    
    # Define connection
    conn = psycopg2.connect(
        database="collectorDB",
        host='172.17.0.1',
        user='airflow',
        password='airflow',
        port='5432'
    )
    cursor = conn.cursor()

    # Execute Query
    cursor.execute("SELECT * FROM simpleplay_wager")

    # Get all results
    records = cursor.fetchall()

    # Create dataframe from query
    df = pd.DataFrame(records, columns=[desc[0] for desc in cursor.description])

    # Dropping rows with null and duplicated bet_id 
    df.drop_duplicates(subset = 'bet_id', inplace=True)
    df.dropna(subset = ['bet_id'], inplace=True)
    
    # Create filepath
    outname = 'simpleplay.csv'
    outdir = './data'
    if not os.path.exists(outdir):
        os.mkdir(outdir)
        
    filepath = os.path.join(outdir, outname)  

    # Save as csv
    df.to_csv(filepath)


default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    default_args=default_args,
    dag_id='simpleplay_record_extraction_v01',
    start_date=datetime(2023, 7, 24),
    schedule_interval="0 10 * * *"
) as dag:
    extract = PythonOperator(
        task_id='extract_data_from_simpleplay_table',
        python_callable=extract_simpleplay
    )

    extract