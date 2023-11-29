from airflow.decorators import dag, task_group
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from datetime import  datetime,timedelta
from pandas import DataFrame
import time


ALL_BET_WAGER_TABLE = "allbet_wager"
ASIAGAMING_WAGER_TABLE = "asiagaming_wager"
SAGAMING_WAGER_TABLE = "sagaming_wager"
SIMPLEPLAY_WAGER_TABLE = "simpleplay_wager"
PGSOFT_WAGER_TABLE = "pgsoft_wager"
EBET_WAGER_TABLE = "ebet_wager"
BTI_WAGER_TABLE = "bti_wager"
SABACV_WAGER_TABLE = "sabacv_wager"
SABA_WAGER_TABLE = "saba_wager"
SABA_NUMBER_TABLE = "saba_number"
SABA_VIRTUAL_TABLE = "saba_virtual"
TFGAMING_TABLE = "tfgaming_wager"
EVOLUTION_TABLE = "evolution_wager"
GENESIS_TABLE = "genesis_wager"
WEWORLD_TABLE = "weworld_wager"

PRODUCT_CODE_ALLBET = "allbet"
PRODUCT_CODE_ASIAGAMING = "asiagaming"
PRODUCT_CODE_AGSLOT = "agslot"
PRODUCT_CODE_AGYOPLAY = "agyoplay"
PRODUCT_CODE_SAGAMING = "sagaming"
PRODUCT_CODE_SPSLOT = "simpleplay"
PRODUCT_CODE_SPFISH = "simpleplayfisher"
PRODUCT_CODE_SABACV = "sabacv"
PRODUCT_CODE_PGSOFT = "pgsoft" 
PRODUCT_CODE_EBETGAMING = "ebet"
PRODUCT_CODE_BTISPORTS = "bti"
PRODUCT_CODE_TFGAMING = "tfgaming"
PRODUCT_CODE_EVOLUTION = "evolution"
PRODUCT_CODE_GENESIS = "genesis"
PRODUCT_CODE_SABA = "saba"
PRODUCT_CODE_SABANUMBERGAME = "sabanumbergames"
PRODUCT_CODE_SABAVIRTUAL = "sabavirtualsport"
PRODUCT_CODE_DIGITAIN = "digitain"
PRODUCT_CODE_WEWORLD = "weworld"

    ########
    # TEMP #
##################
# INIT CASSANDRA #
##################

def init_keyspace():
    from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook

    rawcql = """
        CREATE KEYSPACE IF NOT EXISTS wagers 
        WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor' : 3} 
        AND durable_writes = true;
    """

    cassandra_hook = CassandraHook( cassandra_conn_id='cassandra_conn_id' )
    conn = cassandra_hook.get_conn()

    prepared_query = conn.prepare(rawcql)
    conn.execute(prepared_query)


def init_tables():
    from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook

    cassandra_hook = CassandraHook( cassandra_conn_id='cassandra_conn_id' )
    conn = cassandra_hook.get_conn()

    table_cqls = []

    table_cqls.append("""
        CREATE TABLE IF NOT EXISTS wagers.pgsoft_by_id (
            bet_id_range BIGINT,
            bet_id BIGINT,
            parent_bet_id BIGINT,
            player_name TEXT,
            currency TEXT,
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
            update_at TIMESTAMP,
            PRIMARY KEY (bet_id_range, bet_id)
        );

        CREATE TABLE IF NOT EXISTS wagers.pgsoft_by_member (
            player_name TEXT,
            bet_time TIMESTAMP,
            bet_id BIGINT,
            parent_bet_id BIGINT,
            currency TEXT,
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
            create_at TIMESTAMP,
            update_at TIMESTAMP,
            PRIMARY KEY (player_name, bet_time, bet_id)
        );

        CREATE TABLE IF NOT EXISTS wagers.pgsoft_by_date (
            bet_id BIGINT,
            parent_bet_id BIGINT,
            player_name TEXT,
            currency TEXT,
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
            update_at TIMESTAMP,
            year_month INT,
            PRIMARY KEY (year_month, bet_time, bet_id)
        );
    """)

    for cql in table_cqls:
        prepared_query = conn.prepare(cql)
        conn.execute(prepared_query)

################
# Helper Funcs #
################

def get_year_month(x):
    year = x.strftime("%Y")
    month = x.strftime("%m")

    return int(f"{year}{month}")

def get_inserted_data(df, conn):
    import pandas as pd

    inserted_df = pd.DataFrame()

    ranges = df['bet_id_range'].unique()

    for range in ranges:

        range_df = df[df['bet_id_range'] == range]
        range_df = range_df.reset_index(drop=True)

        rawCql = f"""
            SELECT *
            FROM wagers.pgsoft_by_id
            WHERE bet_id_range = ?
            AND bet_id IN (?{",?" * int(range_df.shape[0] - 1)})
        """

        parameters = [range]
        for _, row in range_df.iterrows():
            parameters.append(row['bet_id'])

        prepared_query = conn.prepare(rawCql)
        result = conn.execute(prepared_query, parameters)

        result_df = pd.DataFrame(result)

        inserted_df = pd.concat([inserted_df, result_df])

    return inserted_df 
###############
# Fetch Funcs #
###############

def fetch_pgsoft_wager(**context):
    import math
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

        # Renaming for Consistency
        df = df.rename(columns={
            "betId":"bet_id",
            "parentBetId":"parent_bet_id",
            "playerName":"player_name",
            "gameId":"game_id",
            "betType":"bet_type",
            "transactionType":"transaction_type",
            "betAmount":"bet_amount",
            "winAmount":"win_amount",
            "jackpotRtpContributionAmount":"jackpot_rtp_contribution_amount",
            "jackpotWinAmount":"jackpot_win_amount",
            "balanceBefore":"balance_before",
            "balanceAfter":"balance_after",
            "rowVersion":"row_version",
            "betTime":"bet_time",
            "createAt":"create_at",
            "updateAt":"update_at",
            })

        # Partitioning
        df['bet_time'] = pd.to_datetime(df['bet_time'], unit='ms')
        df['year_month'] = df['bet_time'].apply(lambda x: get_year_month(x))
        df['bet_id_range'] = df['bet_id'].apply(lambda x: math.ceil(x * 1e-13))

        # Type Corrections
        df['bet_time'] = df['bet_time'].apply(lambda x: x.to_pydatetime())
        df['bet_id'] = df['bet_id'].astype(int)
        df['parent_bet_id'] = df['parent_bet_id'].astype(int)
        df['game_id'] = df['game_id'].astype(int)
        df['platform'] = df['platform'].astype(int)
        df['bet_type'] = df['bet_type'].astype(int)
        df['transaction_type'] = df['transaction_type'].astype(int)
        df['row_version'] = df['row_version'].astype(int)
        
        print(" Saving to Cassandra ")

        # Create a Cassandra Hook
        cassandra_hook = CassandraHook( cassandra_conn_id='cassandra_conn_id' )
        conn = cassandra_hook.get_conn()

        tic = time.perf_counter()
        exists_df = get_existing_data(df, conn)
        toc = time.perf_counter()
        print(f"Time for fetching existing data: {toc - tic:0.4f} seconds")

        # Same bet_id different player
        if not exists_df.empty:
            df['duplicate'] = df.apply(lambda row: (row['bet_id'] in exists_df['bet_id'].values) and (row['player_name'] not in exists_df.loc[exists_df['bet_id'] == row['bet_id'], 'player_name'].values), axis=1)
            df = df[~df['duplicate']]

        if df.shape[0] == 0:
            print("No New Data Found")
            return

        # Insert new data
        tic = time.perf_counter()
        for _, row in df.iterrows():

            insert_into_pgsoft_by_id(row, conn)

        toc = time.perf_counter()
        print(f"Time for inserting data to pgsoft_by_id: {toc - tic:0.4f} seconds")

        tic = time.perf_counter()
        inserted_df = get_inserted_data(df, conn)
        inserted_df['year_month'] = inserted_df['bet_time'].apply(lambda x: get_year_month(x))
        toc = time.perf_counter()
        print(f"Time for retrieving inserted data: {toc - tic:0.4f} seconds")

        tic = time.perf_counter()
        for _, row in inserted_df.iterrows():
            insert_into_pgsoft_by_member(row, conn)
            insert_into_pgsoft_by_date(row, conn)
        toc = time.perf_counter()
        print(f"Time for inserting to pgsoft_by_member and pgsoft_by_date: {toc - tic:0.4f} seconds")
        conn.shutdown()

    except requests.exceptions.RequestException as err:
        print("Request error:", err)
        raise AirflowException

    except Exception as Argument:
        print(f"Error occurred: {Argument}")
        raise AirflowException
    

################
# INSERT FUNCS #
################

def insert_into_pgsoft_by_id(row, conn):
    from datetime import datetime

    now = datetime.now()

    rawCql = f"""INSERT INTO wagers.pgsoft_by_id (
            bet_id_range,
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
        ) VALUES (? {",?" * 18})
       """

    parameters = [
       row['bet_id_range'],
       row['bet_id'],
       row['player_name'],
       row['bet_time'],
       row['parent_bet_id'],
       row['currency'],
       row['game_id'],
       row['platform'],
       row['bet_type'],
       row['transaction_type'],
       row['bet_amount'],
       row['win_amount'],
       row['jackpot_rtp_contribution_amount'],
       row['jackpot_win_amount'],
       row['balance_before'],
       row['balance_after'],
       row['row_version'],
       now,
       now,
       ]

    prepared_query = conn.prepare(rawCql)
    conn.execute(prepared_query, parameters)


def insert_into_pgsoft_by_member(row, conn):
    from datetime import datetime

    now = datetime.now()

    rawCql = f"""INSERT INTO wagers.pgsoft_by_member (
            player_name,
            bet_time,
            bet_id,
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
        ) VALUES (? {",?" * 17})
       """

    parameters = [
       row['player_name'],
       row['bet_time'],
       row['bet_id'],
       row['parent_bet_id'],
       row['currency'],
       row['game_id'],
       row['platform'],
       row['bet_type'],
       row['transaction_type'],
       row['bet_amount'],
       row['win_amount'],
       row['jackpot_rtp_contribution_amount'],
       row['jackpot_win_amount'],
       row['balance_before'],
       row['balance_after'],
       row['row_version'],
       now,
       now,
       ]

    prepared_query = conn.prepare(rawCql)
    conn.execute(prepared_query, parameters)


def insert_into_pgsoft_by_date(row, conn):
    from datetime import datetime

    now = datetime.now()

    rawCql = f"""INSERT INTO wagers.pgsoft_by_date (
            year_month,
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
        ) VALUES (? {",?" * 18})
       """

    parameters = [
       row['year_month'],
       row['bet_time'],
       row['bet_id'],
       row['player_name'],
       row['parent_bet_id'],
       row['currency'],
       row['game_id'],
       row['platform'],
       row['bet_type'],
       row['transaction_type'],
       row['bet_amount'],
       row['win_amount'],
       row['jackpot_rtp_contribution_amount'],
       row['jackpot_win_amount'],
       row['balance_before'],
       row['balance_after'],
       row['row_version'],
       now,
       now,
       ]

    prepared_query = conn.prepare(rawCql)
    conn.execute(prepared_query, parameters)

@dag(
    'download_wager-v1.0.0',
    description='DAG',
    schedule="*/5 * * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=5,
)
def download_wager():
    @task_group
    def init_cassandra():
        create_keyspace = PythonOperator(
                task_id="create_keyspace",
                python_callable=init_keyspace,
                )
        create_tables = PythonOperator(
                task_id="create_tables",
                python_callable=init_tables,
                )
                
        create_keyspace >> create_tables


    init_cassandra


download_wager
