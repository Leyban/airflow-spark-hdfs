from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import psycopg2
import pandas as pd

def print_mean_gross():    
    conn = psycopg2.connect(
        database="paymentDB",
        host='172.17.0.1',
        user='airflow',
        password='airflow',
        port='5432'
    )

    cursor = conn.cursor()

    cursor.execute("SELECT * FROM deposit ORDER BY update_at LIMIT 100")

    deposits = cursor.fetchmany(size=50)

    df = pd.DataFrame(deposits, columns=[desc[0] for desc in cursor.description])

    mean = df['gross_amount'].mean()

    print(f"The mean value for gross amount is {mean}")



default_args = {
    'owner': 'leyban',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    default_args=default_args,
    dag_id='dag_for_paymentDB_v01',
    start_date=datetime(2023, 7, 24),
    schedule_interval="@daily"
) as dag:
    task1 = PythonOperator(
        task_id='fetch_data_from_payment_db',
        python_callable=print_mean_gross
    )
    task1