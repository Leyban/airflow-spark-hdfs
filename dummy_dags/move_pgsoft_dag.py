from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import datetime

dag = DAG('start_move_data_pgsoft_wager', description='move data new pgsoft wager proccessing',
           schedule_interval=None, start_date=datetime.datetime(2023, 8, 1), catchup=False)

conn_collector_pg_hook = PostgresHook(postgres_conn_id='collector_conn_id')
PGSOFT_WAGER_TABLE ='pgsoft_wager'
NEW_PGSOFT_WAGER_TABLE ='new_pgsoft_wager'
PGSOFT_VERSION_TABLE='pgsoft_version'

def start_end_day(bettime):
    start_time = datetime.datetime(bettime.year, bettime.month, bettime.day, 0, 0, 0, 0, tzinfo=datetime.timezone.utc)
    end_time = start_time + datetime.timedelta(days=1) - datetime.timedelta(seconds=1)
    return start_time,end_time

def get_new_pgsoft_wager():
    query ="""
        SELECT * FROM {0}
    """.format(NEW_PGSOFT_WAGER_TABLE)

    df = conn_collector_pg_hook.get_pandas_df(query)
    return df

def move_data_pgsoft_wager():
    utc_now = datetime.datetime.utcnow()
    df = get_new_pgsoft_wager()
    if not df.empty:
        try:
            connection = conn_collector_pg_hook.get_conn()
            cursor = connection.cursor()
            cursor.execute('BEGIN;')
            for w in df.itertuples(index=False):
                start_time,end_time =start_end_day(w.bet_time)
                save_pgwagers ="""
                INSERT INTO {0} 
                (
                    bet_id, parent_bet_id, player_name, currency, game_id, platform, bet_type,
                    transaction_type, bet_amount, win_amount, jackpot_rtp_contribution_amount,
                    jackpot_win_amount, balance_before, balance_after, row_version, bet_time,
                    create_at, update_at
                )
                SELECT
                '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}',
                '{11}', '{12}', '{13}', '{14}', '{15}', '{16}', '{17}', '{18}'
                WHERE NOT EXISTS (SELECT 1 FROM {19} WHERE bet_id = {20} AND bet_time >= '{21}' AND bet_time <='{22}');
                        """.format(PGSOFT_WAGER_TABLE, w.bet_id, w.parent_bet_id, w.player_name,
                    w.currency, w.game_id, w.platform, w.bet_type, w.transaction_type, w.bet_amount, w.win_amount,
                    w.jackpot_rtp_contribution_amount, w.jackpot_win_amount, w.balance_before, w.balance_after,
                    w.row_version, w.bet_time, utc_now, utc_now, PGSOFT_WAGER_TABLE,
                    w.bet_id,start_time,end_time)

                cursor.execute(save_pgwagers)

            connection.commit()
        except Exception as e:
            print(f"Error occurred: {e}")
            connection.rollback() 
        finally:
            cursor.close()
            connection.close()


move_data_pgsoft_wager_operator = PythonOperator(
    task_id='start_move_data_pgsoft_wager', python_callable=move_data_pgsoft_wager, dag=dag)

move_data_pgsoft_wager_operator