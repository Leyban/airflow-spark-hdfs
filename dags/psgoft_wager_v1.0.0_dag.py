from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

dag = DAG(
    'pgsoft_wager-v1.0.0_dag',
    description='DAG',
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False
)

# Define a Python function to initialize wagers db and pgsoft table
def init_pgsoft_wager_table():
    from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
    from airflow.models import Variable

    HIVE_WAREHOUSE_LOCATION = Variable.get('HIVE_WAREHOUSE_LOCATION')

    # Create a HiveServer2Hook
    hive_hook = HiveServer2Hook(
        hive_cli_conn_id='hiveserver2_default',  
    )
    
    # Your Hive SQL script
    create_wagers_db_sql = f"CREATE DATABASE IF NOT EXISTS wagers LOCATION '{HIVE_WAREHOUSE_LOCATION}'"
    create_pgsoft_table_sql = """CREATE TABLE IF NOT EXISTS wagers.pgsoft(
            bet_id BIGINT,
            parent_bet_id BIGINT,
            player_name STRING,
            currency STRING,
            game_id INT,
            platform INT,
            bet_type INT,
            transaction_type INT,
            bet_amount FLOAT,
            win_amount FLOAT,
            jackpot_rtp_contribution_amount FLOAT,
            jackpot_win_amount FLOAT,
            balance_before FLOAT,
            balance_after FLOAT,
            row_version INT,
            bet_time TIMESTAMP,
            create_at TIMESTAMP,
            update_at TIMESTAMP
        ) 
        PARTITIONED BY ( year INT, quarter INT )
        STORED AS PARQUET
        """

    # Execute the Hive SQL
    hive_hook.run(sql=create_wagers_db_sql)
    hive_hook.run(sql=create_pgsoft_table_sql)


def fetch_pgsoft_wager(pgsoft_version, date_from, date_to):
    import logging
    import requests
    import sys
    import pandas as pd
    from datetime import datetime 
    from airflow.models import Variable
    from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook

    # Extract Variables
    pg_history_url = Variable.get( 'PGSOFT_URL'  )
    secret_key = Variable.get( 'PGSOFT_KEY'  )
    operator_token = Variable.get( 'PGSOFT_OPERATOR'  )

    history_api = '/v2/Bet/GetHistory'
    url = f"{pg_history_url}{history_api}" 
    
    # Fetch From API
    form_data = {
        "secret_key": secret_key,
        "operator_token": operator_token,
        "bet_type": "1",
        "row_version": pgsoft_version,
        "date_from": date_from,
        "date_to": date_to,
        "count": "5000"
    }
        
    try:
        print(f"Start download pg: row_version {pgsoft_version}")
        response = requests.post(url, data=form_data)
        response.raise_for_status() 

        # Create DF
        print(" Creating Pandas Dataframe ")
        res_obj = response.json().get('data',[])
        total_data_length = len(res_obj)
        print(f"Total {total_data_length}")
        df = pd.DataFrame(res_obj)

        # Partitioning
        df.betTime = pd.to_datetime(df.betTime) 
        df[ 'year' ] = df['betTime'].dt.year
        df[ 'quarter' ] = df['betTime'].dt.quarter

        # Save to HDFS
        print(" Saving to HDFS ")

        # Create a HiveServer2Hook
        hive_hook = HiveServer2Hook(
            hive_cli_conn_id='hiveserver2_default', schema='wagers' 
        )
        
        for row in df.itertuples(index=False):
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            rd = row._asdict()
            
            query = f"""INSERT INTO wagers.pgsoft PARTITION (
                year={rd['year']}, 
                quarter={rd['quarter']} 
            ) (
                bet_id,   
                parent_bet_id,
                player_name, 
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
                bet_time,
                create_at,
                update_at
            )  VALUES (
                {rd['betId']},
                {rd['parentBetId']},
                '{rd['playerName']}',
                '{rd['currency']}',
                {rd['gameId']},
                {rd['platform']},
                {rd['betType']},
                {rd['transactionType']},
                {rd['betAmount']},
                {rd['winAmount']},
                {rd['jackpotRtpContributionAmount']},
                {rd['jackpotWinAmount']},
                {rd['balanceBefore']},
                {rd['balanceAfter']},
                {rd['rowVersion']},
                '{rd['betTime']}',
                '{now}',
                '{now}'
            )"""
            
            hive_hook.run(sql=query)
        
    except requests.exceptions.RequestException as err:
        logging.fatal("Request error:", err)
        sys.exit(1)

    except Exception as Argument:
        logging.fatal(f"Error occurred: {Argument}")
        sys.exit(1)


init_hive_pgsoft = PythonOperator(
    task_id='initialize_table',
    python_callable=init_pgsoft_wager_table,
    dag=dag,
)


download_pgsoft = PythonOperator(
    task_id='download_pgsoft',
    python_callable=fetch_pgsoft_wager,
    op_kwargs={
        "pgsoft_version": "{{ dag_run.conf['pgsoft_version']}}",
        "date_from": "{{ dag_run.conf['date_from']}}",
        "date_to": "{{ dag_run.conf['date_to']}}"
    },
    dag=dag
)


init_hive_pgsoft >> download_pgsoft