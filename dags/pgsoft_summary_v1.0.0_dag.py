from airflow.decorators import dag, task
from airflow.exceptions import  AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from pandas import DataFrame


@task
def create_pgsoft_summary_table(**kwargs):
    import sqlite3
    import os

    ds = kwargs['ds']
    if 'custom_ds' in kwargs['params']:
        ds = kwargs['params']['custom_ds']

    filepath, dir = get_filepath(ds)

    if not os.path.exists(dir):
        os.remove(filepath)
        os.makedirs(dir)
    
    conn = sqlite3.connect(filepath)
    curs = conn.cursor()

    get_table_list = "SELECT name FROM sqlite_master WHERE type='table' AND name='pgsoft_summary'"

    res = curs.execute(get_table_list)
    tables = res.fetchall()

    if len( tables ) == 0:
        print("Creating table 'pgsoft_summary'")
        create_member_table_sql = """
            CREATE TABLE pgsoft_summary(
                player_name text,
                currency text,
                bet_amount real,
                win_amount real,
                last_bet_time text
                ) 
        """

        curs.execute(create_member_table_sql)

    curs.close()


def get_filepath(ds):
    exec_date = datetime.strptime(ds, "%Y-%m-%d")
    date = exec_date.replace(day=1) - timedelta(days=1)
    month = date.month
    year = date.year

    filename = f"pgsoft_{year}{month}"
    dir = f"./data/pgsoft_summary/{date.strftime('%Y%m')}"
    filepath = f"{dir}/{filename}.db"

    return filepath, dir


def get_pgsoft_day(date: datetime, **kwargs):
    import pandas as pd
    import numpy as np
    import sqlite3
    import os

    next_day = date + timedelta(days=1) 

    ds = kwargs['ds']
    if 'custom_ds' in kwargs['params']:
        ds = kwargs['params']['custom_ds']

    date_from = date.strftime('%Y-%m-%d')
    date_to = next_day.strftime('%Y-%m-%d')

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

    filepath, _, = get_filepath(ds)

    conn = sqlite3.connect(filepath)

    if os.path.exists(filepath):
        prev_df = pd.read_sql(f"SELECT * FROM pgsoft_summary", conn)
        prev_df['last_bet_time'] = pd.to_datetime(prev_df['last_bet_time'])
        df = pd.concat([df, prev_df])

        df = df.groupby(['player_name', 'currency']).agg({
            'bet_amount': np.sum, 
            'win_amount': np.sum, 
            'last_bet_time': np.max, 
            }).reset_index()

    df['last_bet_time'] = df['last_bet_time'].dt.strftime("%Y-%m-%d %H:%M:%S.%f")

    print(f"Inserting {df.shape[0]} to {filepath}")
    df.to_sql("pgsoft_summary", conn, if_exists='replace')


def get_num_days(**kwargs):
    import calendar

    ds = kwargs['ds']
    if 'custom_ds' in kwargs['params']:
        ds = kwargs['params']['custom_ds']

    exec_date = datetime.strptime(ds, "%Y-%m-%d")

    last_month = exec_date.replace(day=1) - timedelta(days=1)
    year = last_month.year
    month = last_month.month

    num_days = calendar.monthrange(year, month)[1]

    return num_days


@task
def summarize_daily(**kwargs):
    ds = kwargs['ds']
    if 'custom_ds' in kwargs['params']:
        ds = kwargs['params']['custom_ds']

    exec_date = datetime.strptime(ds, "%Y-%m-%d")
    date_iter = ( exec_date.replace(day=1) - timedelta(days=1) ).replace(day=1)
    month = date_iter.month

    while date_iter.month == month:
        get_pgsoft_day(date_iter,  **kwargs)
        date_iter += timedelta(days=1)


@task
def summarize_month(**kwargs):
    import pandas as pd
    import numpy as np
    import sqlite3

    ds = kwargs['ds']
    if 'custom_ds' in kwargs['params']:
        ds = kwargs['params']['custom_ds']

    filepath, _  = get_filepath(ds)

    exec_date = datetime.strptime(ds, "%Y-%m-%d")
    date_from = (exec_date.replace(day=1) - timedelta(days=1)).replace(day=1)
    date_to = exec_date.replace(day=1)

    conn = sqlite3.connect(filepath)

    df = pd.read_sql(f"SELECT * FROM pgsoft_summary", conn)
    df['last_bet_time'] = pd.to_datetime(df['last_bet_time'])

    if df.shape[0] == 0:
        print("No Data Found")
        raise AirflowSkipException

    df = df.groupby(['player_name', 'currency']).agg({
        'bet_amount': np.sum,
        'win_amount': np.sum,
        'last_bet_time': np.max,
        }).reset_index()

    conn_collector_pg_hook = PostgresHook(postgres_conn_id='collector_conn_id')
    engine_collector = conn_collector_pg_hook.get_sqlalchemy_engine()

    # Clear any previous summary for the month
    raw_sql = f"""
        DELETE FROM pgsoft_summary
        WHERE last_bet_time >= '{date_from}'
        AND last_bet_time < '{date_to}'
    """
    print("Deleting date from:", date_from, "\ndate to:", date_to)
    conn_collector_pg_hook.run(raw_sql)

    df.to_sql("pgsoft_summary", engine_collector, if_exists='append', index=False)


@dag(
    dag_id='pgsoft_summary-v1.0.0',
    description='Summarizes the player info for each month',
    schedule_interval="@monthly",
    start_date=datetime(2022, 12, 31),
    catchup=False,
    max_active_runs=1,
    )
def monthly_summary():
    
    init = create_pgsoft_summary_table()
    summarize_days = summarize_daily()
    summarize_final = summarize_month()

    init >> summarize_days >> summarize_final

monthly_summary()
