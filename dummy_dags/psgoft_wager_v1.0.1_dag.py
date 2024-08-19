from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.hive.hooks.hive import AirflowException
import pendulum

dag = DAG(
    'pgsoft_wager-v1.0.0',
    description='DAG',
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False
)


def get_month_year(x):
    year = x.strftime("%Y")
    month = x.strftime("%m")

    return f"{month}_{year}"


def fetch_pgsoft_wager(**context):
    import requests
    import pandas as pd
    from datetime import datetime, timedelta
    from airflow.models import Variable
    from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook

    # Extract Variables
    pg_url = Variable.get( 'PG_URL'  )
    pg_secret_key = Variable.get( 'PG_SECRECT_KEY'  )
    pg_operator_token = Variable.get( 'PG_OPERATOR_TOKEN'  )

    # Calculate pgsoft_version
    day_begin = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=60)
    epoch = datetime.utcfromtimestamp(0)
    pgsoft_version = int( (day_begin - epoch).total_seconds() * 1000 )

    # Taking Optional Date Parameters
    if 'pgsoft_version' in context['params']:
        pgsoft_version = context['params']['pgsoft_version']

    history_api = '/v2/Bet/GetHistory'
    url = f"{pg_url}{history_api}" 
    
    # Fetch From API
    form_data = {
        "secret_key": pg_secret_key,
        "operator_token": pg_operator_token,
        "bet_type": "1",
        "row_version": pgsoft_version,
        "count": "5000"
    }
        
    try:
        print(f"Start download pg: row_version {pgsoft_version}")
        print(url)
        response = requests.post(url, data=form_data)
        response.raise_for_status() 

        # Create DF
        print(" Creating Pandas Dataframe ")
        res_obj = response.json().get('data',[])
        total_data_length = len(res_obj)
        print(f"Total {total_data_length}")

        if total_data_length == 0:
            print("No data Received ")
            print(response.json().get('error'))

        df = pd.DataFrame(res_obj)

        # Drop empty rows
        df = df.dropna()
        df = df.reset_index(drop=True)

        # Partitioning
        df['betTime'] = pd.to_datetime(df['betTime'], unit='ms')
        df['monthYear'] = df['betTime'].apply(lambda x: get_month_year(x))

        # Type Corrections
        df['betTime'] = df['betTime'].dt.date
        df['betId'] = df['betId'].astype(int)
        df['parentBetId'] = df['parentBetId'].astype(int)
        df['gameId'] = df['gameId'].astype(int)
        df['platform'] = df['platform'].astype(int)
        df['betType'] = df['betType'].astype(int)
        df['transactionType'] = df['transactionType'].astype(int)
        df['rowVersion'] = df['rowVersion'].astype(int)
        
        print(" Saving to Cassandra ")

        # Create a Cassandra Hook
        cassandra_hook = CassandraHook( cassandra_conn_id='cassandra_conn_id' )
        conn = cassandra_hook.get_conn()

        now = datetime.now()

        for _, row in df.iterrows():

            query = f"""INSERT INTO wagers.pgsoft_by_id (
                bet_id,   
                player_name, 
                bet_time,
                parent_bet_id,
                currency,
                game_id,
                platform,
                bet_type,
                transaction_type,
                bet_amount,
                win_amount,
                jackpot_rtp_contribution_amount,
                jackpot_win_amount,
                balance_before,
                balance_after,
                row_version,
                create_at,
                update_at
            )  VALUES (?{",?" * 17}) IF NOT EXISTS;"""

            parameters = [
               row['betId'],
               row['playerName'],
               row['betTime'],
               row['parentBetId'],
               row['currency'],
               row['gameId'],
               row['platform'],
               row['betType'],
               row['transactionType'],
               row['betAmount'],
               row['winAmount'],
               row['jackpotRtpContributionAmount'],
               row['jackpotWinAmount'],
               row['balanceBefore'],
               row['balanceAfter'],
               row['rowVersion'],
               now,
               now,
               ]

            prepared_query = conn.prepare(query)
            conn.execute(prepared_query, parameters)


    except requests.exceptions.RequestException as err:
        print("Request error:", err)
        raise AirflowException

    except Exception as Argument:
        print(f"Error occurred: {Argument}")
        raise AirflowException

def get_migrated_partition():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    conn_collector_pg_hook = PostgresHook(postgres_conn_id='collector_conn_id')

    rawsql = f"""
        SELECT 
            partition
        FROM migrated_partition
        WHERE vendor='pgsoft'
    """

    df = conn_collector_pg_hook.get_pandas_df(rawsql)

    return df 


def migrate_pgsoft_wager():
    from datetime import datetime
    from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook

    

    try:
        cassandra_hook = CassandraHook( cassandra_conn_id='cassandra_conn_id' )
        conn = cassandra_hook.get_conn()

        now = datetime.now()

        for _, row in df.iterrows():

            query = f"""INSERT INTO wagers.pgsoft_by_date (
                month_year,
                bet_time,
                bet_id,   
                player_name, 
                parent_bet_id,
                currency,
                game_id,
                platform,
                bet_type,
                transaction_type,
                bet_amount,
                win_amount,
                jackpot_rtp_contribution_amount,
                jackpot_win_amount,
                balance_before,
                balance_after,
                row_version,
                create_at,
                update_at
            )  VALUES (?{",?" * 18}) IF NOT EXISTS;"""

            parameters = [
               row['monthYear'],
               row['betTime'],
               row['betId'],
               row['playerName'],
               row['parentBetId'],
               row['currency'],
               row['gameId'],
               row['platform'],
               row['betType'],
               row['transactionType'],
               row['betAmount'],
               row['winAmount'],
               row['jackpotRtpContributionAmount'],
               row['jackpotWinAmount'],
               row['balanceBefore'],
               row['balanceAfter'],
               row['rowVersion'],
               now,
               now,
               ]

            prepared_query = conn.prepare(query)
            conn.execute(prepared_query, parameters)

    except Exception as Argument:
        print(f"Error occurred: {Argument}")
        raise AirflowException


download_pgsoft = PythonOperator(
    task_id='download_pgsoft',
    python_callable=fetch_pgsoft_wager,
    dag=dag
)

migrate_pgsoft = PythonOperator(
    task_id='migrate_pgsoft',
    python_callable=migrate_pgsoft_wager,
    dag=dag
)

download_pgsoft >> migrate_pgsoft
