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

# Initialize wagers db and pgsoft table
def init_pgsoft_wager_table():
    from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
    from airflow.models import Variable

    HIVE_WAREHOUSE_DIR = Variable.get('HIVE_WAREHOUSE_DIR')

    # Create a HiveServer2Hook
    hive_hook = HiveServer2Hook(
        hiveserver2_conn_id='hive_servicer2_conn_id',  
    )
    
    # Your Hive SQL script
    create_wagers_db_sql = f"CREATE DATABASE IF NOT EXISTS wagers LOCATION '{HIVE_WAREHOUSE_DIR}'"
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
            row_version BIGINT,
            bet_time TIMESTAMP,
            create_at TIMESTAMP,
            update_at TIMESTAMP
        ) 
        PARTITIONED BY ( year INT, month INT )
        STORED AS PARQUET
        """

    # Execute the Hive SQL
    hive_hook.run(sql=create_wagers_db_sql)
    hive_hook.run(sql=create_pgsoft_table_sql)


def fetch_pgsoft_wager(**context):
    import logging
    import requests
    import pandas as pd
    from datetime import datetime, timedelta
    from airflow.models import Variable
    from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook

    # Extract Variables
    pg_url = Variable.get( 'PG_URL'  )
    pg_secret_key = Variable.get( 'PG_SECRECT_KEY'  )
    pg_operator_token = Variable.get( 'PG_OPERATOR_TOKEN'  )

    day_begin = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=60)
    epoch = datetime.utcfromtimestamp(0)
    pgsoft_version = int( (day_begin - epoch).total_seconds() * 1000 )

    date_format = "%Y-%m-%d %H:%M:%S" 

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

        # Partitioning
        df['betTime'] = pd.to_datetime(df['betTime'], unit='ms')
        df['year'] = df['betTime'].dt.year
        df['month'] = df['betTime'].dt.month

        # Save to HDFS
        print(" Saving to HDFS ")

        # Create a HiveServer2Hook
        hive_hook = HiveServer2Hook(
            hiveserver2_conn_id='hive_servicer2_conn_id', schema='wagers' 
        )

        months = df['month'].unique()

        for month in months:
            now = datetime.now().strftime(date_format)

            month_df = df[df['month'] == month]
            month_df = month_df.dropna()
            month_df = month_df.reset_index(drop=True)

            if month_df.shape[0] == 0:
                continue
            
            query = f"""INSERT INTO wagers.pgsoft PARTITION (
                year={month_df['year'].iloc[0]}, 
                 month={month} 
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
            )  VALUES """

            for i, row in month_df.iterrows():
                query += f"""(
                    {row['betId']},
                    {row['parentBetId']},
                    '{row['playerName']}',
                    '{row['currency']}',
                    {row['gameId']},
                    {row['platform']},
                    {row['betType']},
                    {row['transactionType']},
                    {row['betAmount']},
                    {row['winAmount']},
                    {row['jackpotRtpContributionAmount']},
                    {row['jackpotWinAmount']},
                    {row['balanceBefore']},
                    {row['balanceAfter']},
                    {row['rowVersion']},
                    '{row['betTime']}',
                    '{now}',
                    '{now}'
                )"""

                if i != month_df.shape[0] - 1:
                    query += """,
                    """

            hive_hook.run(sql=query)

    except requests.exceptions.RequestException as err:
        logging.fatal("Request error:", err)
        raise AirflowException

    except Exception as Argument:
        logging.fatal(f"Error occurred: {Argument}")
        raise AirflowException


init_hive_pgsoft = PythonOperator(
    task_id='initialize_table',
    python_callable=init_pgsoft_wager_table,
    dag=dag,
)

download_pgsoft = PythonOperator(
    task_id='download_pgsoft',
    python_callable=fetch_pgsoft_wager,
    dag=dag
)

init_hive_pgsoft >> download_pgsoft
