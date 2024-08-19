from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from pandas import DataFrame


def get_filepath(ds):
    """Retruns filepath and directory for previous day"""

    exec_date = datetime.strptime(ds, "%Y-%m-%d")
    date = exec_date - timedelta(days=1)

    datestamp = date.strftime("%Y%m%d")

    filename = f"pgsoft_{datestamp}"
    dir = f"./data/pgsoft_monthly_summary/{date.strftime('%Y%m')}"
    filepath = f"{dir}/{filename}.db"

    return filepath, dir


@task
def create_pgsoft_monthly_summary_table(**kwargs):
    """Creates Directory and Sqlite file"""
    import sqlite3
    import os

    ds = kwargs['ds']
    if 'custom_ds' in kwargs['params']:
        ds = kwargs['params']['custom_ds']

    filepath, dir = get_filepath(ds)

    if not os.path.exists(dir):
        os.makedirs(dir)

    if os.path.exists(filepath):
        print("File already exists")
        return
    
    conn = sqlite3.connect(filepath)
    curs = conn.cursor()

    get_table_list = "SELECT name FROM sqlite_master WHERE type='table' AND name='pgsoft_monthly_summary'"

    res = curs.execute(get_table_list)
    tables = res.fetchall()

    if len( tables ) == 0:
        print("Creating table 'pgsoft_monthly_summary'")
        create_member_table_sql = """
            CREATE TABLE pgsoft_monthly_summary(
                player_name text,
                currency text,
                bet_amount real,
                win_amount real,
                last_bet_time text
                ) 
        """

        curs.execute(create_member_table_sql)

    curs.close()


@task
def summarize_daily(**kwargs):
    """Takes pgsoft data for yesterday then save it to a sqlite file"""
    import pandas as pd
    import numpy as np
    import sqlite3

    ds = kwargs['ds']
    if 'custom_ds' in kwargs['params']:
        ds = kwargs['params']['custom_ds']

    filepath, _  = get_filepath(ds)
    conn = sqlite3.connect(filepath)

    yesterday_df = pd.read_sql(f"SELECT * FROM pgsoft_monthly_summary", conn)
    if yesterday_df.shape[0] != 0:
        print("File Already Exists")
        return

    exec_date = datetime.strptime(ds, "%Y-%m-%d")
    summarize_date = exec_date - timedelta(days=1)

    date_from = summarize_date.strftime('%Y-%m-%d')
    date_to = exec_date.strftime('%Y-%m-%d')

    print("date from: ", date_from)
    print("date to: ", date_to)

    raw_sql = f"""
        SELECT
            player_name,
            currency,
            bet_amount,
            win_amount,
            bet_time
        FROM pgsoft_wager
        WHERE bet_time >= '{date_from}'
        AND bet_time < '{date_to}'
    """

    conn_collector_pg_hook = PostgresHook(postgres_conn_id='collector_conn_id')

    df: DataFrame = conn_collector_pg_hook.get_pandas_df(raw_sql)

    print(f"Found {df.shape[0]} rows of data")
    if df.empty:
        print("No records found")
        return

    df['bet_time'] = pd.to_datetime(df['bet_time'])

    df = df.groupby(['player_name', 'currency']).agg({
        'bet_amount': np.sum, 
        'win_amount': np.sum, 
        'bet_time': np.max, 
        }).reset_index()

    df = df.rename(columns={'bet_time': 'last_bet_time'}).reset_index()


    df['last_bet_time'] = df['last_bet_time'].dt.strftime("%Y-%m-%d %H:%M:%S.%f")

    print(f"Inserting {df.shape[0]} to {filepath}")
    df.to_sql("pgsoft_monthly_summary", conn, if_exists='replace', index=False)


@task(trigger_rule='all_done')
def save_to_postgres(**kwargs):
    """Reads summarized data from sqlite yesterday then save it to postgres"""
    import pandas as pd
    import sqlite3

    ds = kwargs['ds']
    if 'custom_ds' in kwargs['params']:
        ds = kwargs['params']['custom_ds']

    exec_date = datetime.strptime(ds, "%Y-%m-%d")

    filepath, _ = get_filepath(ds)

    conn = sqlite3.connect(filepath)
    yesterday_df = pd.read_sql(f"SELECT * FROM pgsoft_monthly_summary", conn)

    conn_collector_pg_hook = PostgresHook(postgres_conn_id='collector_conn_id')
    engine_collector = conn_collector_pg_hook.get_sqlalchemy_engine()

    # Clear any previous summary for the month
    exec_date = datetime.strptime(ds, "%Y-%m-%d")
    date_from = exec_date - timedelta(days=1)
    date_to = exec_date

    delete_sql = f"""
        DELETE FROM pgsoft_daily_summary
        WHERE last_bet_time >= '{date_from}'
        AND last_bet_time < '{date_to}'
    """
    print("Deleting date from:", date_from, "\ndate to:", date_to)
    conn_collector_pg_hook.run(delete_sql)

    # Save to Postgres
    print("Inserting ", yesterday_df.shape[0], " to Postgres")
    yesterday_df = yesterday_df[[ 'player_name', 'currency', 'bet_amount', 'win_amount', 'last_bet_time' ]]
    yesterday_df.to_sql("pgsoft_daily_summary", engine_collector, if_exists='append', index=False)


@dag(
    dag_id='pgsoft_summarize_daily-v1.0.0',
    description='Summarizes the player info for each month',
    schedule_interval="0 17 * * *",
    start_date=datetime(2022, 12, 31),
    catchup=False,
    max_active_runs=1,
    )
def daily_summary():
    
    init = create_pgsoft_monthly_summary_table()
    summarize_days = summarize_daily()
    save_pg = save_to_postgres()

    init >> summarize_days >> save_pg

_ = daily_summary()
