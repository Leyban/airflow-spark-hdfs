from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta
import requests
import logging


PGSOFT_OLD_VERSION_TABLE ='pgsoft_old_version'

def download_pgsoft() :
    secret_key = Variable.get("PG_SECRECT_KEY")
    operator_token = Variable.get("PG_OPERATOR_TOKEN")
    pg_history_url = Variable.get("PG_HISTORY_URL")

    history_api = '/v2/Bet/GetHistory'

    # url = f"{pg_history_url}{history_api}" 
    
    url = "htttp://localhost:8800/pg_soft" # MockAPI
    
    latest_row_version = get_pgversion()
    latest_row_version = int(latest_row_version)
    try:
        form_data = {
            "secret_key":     secret_key,
            "operator_token": operator_token,
            "bet_type":        "1",
            "row_version":  latest_row_version,
            "count":          "5000"
        }

        print(f"Start download pg: row_version {latest_row_version}")
        response = requests.post(url, data=form_data)
        response.raise_for_status() 
        print(f"reposonse {response}")
        if response.status_code == 404:
            print("Error 404: Not Found")
        else:
            pass

    except requests.exceptions.RequestException as err:
        print("Request error:", err)
        
def get_pgversion():
    conn_collector_pg_hook = PostgresHook(postgres_conn_id='collector_conn_id')
    query = """
        SELECT row_version FROM {0} LIMIT 1
    """.format(PGSOFT_OLD_VERSION_TABLE)

    df = conn_collector_pg_hook.get_pandas_df(query)
    if not df.empty:
        latest_row_version = df['row_version'].iloc[0]
        return latest_row_version
    else:
        return None