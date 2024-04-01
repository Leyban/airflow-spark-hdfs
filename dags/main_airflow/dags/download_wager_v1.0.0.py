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
    from main_airflow.modules.download_wager import cqls

    rawcql = cqls.create_keyspace

    cassandra_hook = CassandraHook( cassandra_conn_id='cassandra_conn_id' )
    conn = cassandra_hook.get_conn()

    prepared_query = conn.prepare(rawcql)
    conn.execute(prepared_query)


def init_tables():
    from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook
    from main_airflow.modules.download_wager import cqls

    cassandra_hook = CassandraHook( cassandra_conn_id='cassandra_conn_id' )
    conn = cassandra_hook.get_conn()

    table_cqls = [
            cqls.create_table_pgsoft_by_id,
            cqls.create_table_pgosft_by_member,
            cqls.create_table_pgosft_by_date,
            ]

    for cql in table_cqls:
        prepared_query = conn.prepare(cql)
        conn.execute(prepared_query)

################
# INSERT FUNCS #
################

def insert_into_pgsoft_by_id(row, conn):
    from datetime import datetime

    now = datetime.now()

    rawcql = f"""INSERT INTO wagers.pgsoft_by_id (
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
            ) 
       VALUES (? {",?" * 18})
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

    prepared_query = conn.prepare(rawcql)
    conn.execute(prepared_query, parameters)


def insert_into_pgsoft_by_member(row, conn):
    from datetime import datetime

    now = datetime.now()

    rawcql = f"""INSERT INTO wagers.pgsoft_by_member (
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

    prepared_query = conn.prepare(rawcql)
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
    from main_airflow.modules.download_wager.pgsoft import fetch_pgsoft_wager

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

    def fetch_api_task(product, fetch_api_func, get_member_data_func):
        fetch_product_api = PythonOperator(
                task_id=f"fetch_{product}_api",
                python_callable=fetch_api_func,
                )
        get_member_data = PythonOperator(
                task_id=f"get_{product}_member_data",
                python_callable=get_member_data_func
                )
        fetch_product_api > get_member_data

    fetch_postgres = fetch_api_task(PRODUCT_CODE_PGSOFT, fetch_pgsoft_wager, todo_mate_good_luck)


    init_cassandra


download_wager
