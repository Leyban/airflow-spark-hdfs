from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
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


def get_filepaths(ds):
    """Returns an array of [ filepath, datetime ] for every day of the month"""

    exec_date = datetime.strptime(ds, "%Y-%m-%d")
    summarize_date = exec_date - timedelta(days=1)

    filepaths = []

    date_iter = summarize_date.replace(day=2)
    last_date = summarize_date.replace(day=1) + relativedelta(months=1)

    while date_iter <= last_date:
        ds_iter = date_iter.strftime("%Y-%m-%d")

        filepath, _ = get_filepath(ds_iter)
        filepaths.append([ filepath, date_iter ])

        date_iter += timedelta(days=1)

    return filepaths


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
        if os.path.exists(filepath):
            os.remove(filepath)
        os.makedirs(dir)
    
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

    filepath, _  = get_filepath(ds)
    conn = sqlite3.connect(filepath)

    df['last_bet_time'] = df['last_bet_time'].dt.strftime("%Y-%m-%d %H:%M:%S.%f")

    print(f"Inserting {df.shape[0]} to {filepath}")
    df.to_sql("pgsoft_monthly_summary", conn, if_exists='replace', index=False)


@task
def save_to_postgres(**kwargs):
    """Combines all available data for the month from sqlite then save it to postgres"""
    import pandas as pd
    import numpy as np
    import sqlite3
    from shutil import rmtree
    import os

    ds = kwargs['ds']
    if 'custom_ds' in kwargs['params']:
        ds = kwargs['params']['custom_ds']

    exec_date = datetime.strptime(ds, "%Y-%m-%d")

    filepaths = get_filepaths(ds)
    month_df = pd.DataFrame()

    # Combining available sqlite files
    for filepath, date_iter in filepaths:
        print("Reading file: ", filepath)
        if not os.path.exists(filepath):
            if date_iter < exec_date:
                print("Missing File: ", filepath, " Consider running DAG at ", date_iter.strftime("%Y-%m-%d"))
            continue

        conn = sqlite3.connect(filepath)
        iter_df = pd.read_sql(f"SELECT * FROM pgsoft_monthly_summary", conn)
        if iter_df.empty:
            continue

        iter_df['last_bet_time'] = pd.to_datetime(iter_df['last_bet_time'])
        month_df = pd.concat([month_df, iter_df])
        month_df = month_df.groupby(['player_name', 'currency']).agg({
            'bet_amount': np.sum,
            'win_amount': np.sum,
            'last_bet_time': np.max,
            }).reset_index()

    conn_collector_pg_hook = PostgresHook(postgres_conn_id='collector_conn_id')
    engine_collector = conn_collector_pg_hook.get_sqlalchemy_engine()

    # Clear any previous summary for the month
    exec_date = datetime.strptime(ds, "%Y-%m-%d")
    date_from = exec_date.replace(day=1)
    date_to = exec_date.replace(day=1) + relativedelta(months=1)
    if exec_date.day == 1:
        date_from = (exec_date - timedelta(days=1)).replace(day=1)
        date_to = exec_date

    delete_sql = f"""
        DELETE FROM pgsoft_monthly_summary
        WHERE last_bet_time >= '{date_from}'
        AND last_bet_time < '{date_to}'
    """
    print("Deleting date from:", date_from, "\ndate to:", date_to)
    conn_collector_pg_hook.run(delete_sql)

    # Save to Postgres
    print("Inserting ", month_df.shape[0], " to Postgres")
    month_df.to_sql("pgsoft_monthly_summary", engine_collector, if_exists='append', index=False)

    # Delete SQLite File from 2 months ago
    if exec_date.day == 1:
        old_ds = (exec_date - relativedelta(months=1)).strftime('%Y-%m-%d')
        _, old_dir = get_filepath(old_ds)

        print("Checking for old files", old_dir)
        if os.path.exists(old_dir):
            print("Deleting File: ", old_dir)
            rmtree(old_dir)

@dag(
    dag_id='pgsoft_daily_summary-v1.0.0',
    description='Summarizes the player info for each month',
    schedule_interval="0 16 * * *",
    start_date=datetime(2022, 12, 31),
    catchup=False,
    max_active_runs=1,
    default_args={
        'retries': 3
        }
    )
def daily_summary():
    
    init = create_pgsoft_monthly_summary_table()
    summarize_days = summarize_daily()
    save_pg = save_to_postgres()

    init >> summarize_days >> save_pg

_ = daily_summary()
