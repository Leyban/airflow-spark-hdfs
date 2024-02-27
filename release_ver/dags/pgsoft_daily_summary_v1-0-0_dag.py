from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pandas import DataFrame


@task
def summarize_daily(**kwargs):
    """Takes pgsoft data for yesterday then aggregate then save back to postgres_daily_summary"""
    import pandas as pd
    import numpy as np

    ds = kwargs['ds']

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

    conn_collector_pg_hook = PostgresHook(postgres_conn_id='collector_conn_id')
    engine_collector = conn_collector_pg_hook.get_sqlalchemy_engine()

    delete_sql = f"""
        DELETE FROM pgsoft_daily_summary
        WHERE last_bet_time >= '{date_from}'
        AND last_bet_time < '{date_to}'
    """
    print("Deleting date from:", date_from, "\ndate to:", date_to)
    conn_collector_pg_hook.run(delete_sql)

    print(f"Found {df.shape[0]} rows of data")
    if df.empty:
        print("No records found")
        return

    df['bet_time'] = pd.to_datetime(df['bet_time'])
    df['wager_count'] = 1

    df = df.groupby(['player_name', 'currency']).agg({
        'bet_amount': np.sum, 
        'win_amount': np.sum, 
        'bet_time': np.max, 
        'wager_count': np.sum,
        }).reset_index()

    df = df.rename(columns={'bet_time': 'last_bet_time'}).reset_index()

    df['last_bet_time'] = df['last_bet_time'].dt.strftime("%Y-%m-%d %H:%M:%S.%f")

    df = df.filter(['player_name', 'currency', 'bet_amount', 'win_amount', 'last_bet_time', 'wager_count'], axis=1)
    print("Inserting ", df.shape[0], " to Postgres")
    df.to_sql("pgsoft_daily_summary", engine_collector, if_exists='append', index=False)


@dag(
    dag_id='pgsoft_daily_summary-v1.0.0',
    description='Summarizes the player info for each day',
    schedule_interval="0 16 * * *",
    start_date=datetime(2022, 12, 31),
    catchup=False,
    max_active_runs=1,
    default_args={
        'retries': 3
        }
    )
def daily_summary():
    
    summarize_days = summarize_daily()
    summarize_days

_ = daily_summary()
