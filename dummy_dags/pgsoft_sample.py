from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta, timezone
import requests
import json
import pandas as pd

dag = DAG('extract_pgsoft_wager', description='download pgsoft proccessing',
        schedule_interval='*/2 * * * *', start_date=datetime(2023, 8, 2), catchup=False)

PGSOFT_WAGER_TABLE ='pgsoft_wager'
PGSOFT_VERSION_TABLE='pgsoft_version'
PGSOFT_OLD_VERSION_TABLE ='pgsoft_old_version'

def download_pgsoft() :
    secret_key = Variable.get("PG_SECRECT_KEY")
    operator_token = Variable.get("PG_OPERATOR_TOKEN")
    pg_history_url = Variable.get("PG_HISTORY_URL")
    history_api   = '/v2/Bet/GetHistory'
    url = f"{pg_history_url}{history_api}"
    latest_row_version = get_pgversion()
    latest_row_version = int(latest_row_version)
    try:
        form_data = {
            "secret_key":     secret_key,
            "operator_token": operator_token,
            "bet_type":        "1",
            "row_version":  latest_row_version,
            "count":          "5000"
        }

        print(f"Start download pg: row_version {latest_row_version}")
        response = requests.post(url, data=form_data)
        response.raise_for_status()
        print(f"reposonse {response}")
        if response.status_code == 404:
            print("Error 404: Not Found")
        else:
            try:
                res_obj = response.json().get('data',[])
                total_data_length = len(res_obj)
                print(f"Total {total_data_length}")
                create_pgwager(res_obj)
            except json.JSONDecodeError as err:
                print("JSON parsing error:", err)

    except requests.exceptions.RequestException as err:
        print("Request error:", err)

def get_pgversion():
    conn_collector_pg_hook = PostgresHook(postgres_conn_id='collector_conn_id')
    query = """
        SELECT row_version FROM {0} LIMIT 1
    """.format(PGSOFT_OLD_VERSION_TABLE)

    df = conn_collector_pg_hook.get_pandas_df(query)
    if not df.empty:
        latest_row_version = df['row_version'].iloc[0]
        return latest_row_version
    else:
        return None

def to_utc_time(datetime_in_milliseconds):
    seconds = datetime_in_milliseconds / 1000
    utc_time = datetime.utcfromtimestamp(seconds)
    return utc_time

def date_range(bet_time):
    start_time = datetime(bet_time.year, bet_time.month, bet_time.day, 0, 0, 0, 0, tzinfo=timezone.utc)
    end_time = start_time + timedelta(days=1) - timedelta(seconds=1)
    return start_time, end_time

def create_pgwager(json_data):
    df = pd.DataFrame(json_data)
    utc_now = datetime.utcnow()
    if not df.empty:
        try:
            conn_collector_pg_hook = PostgresHook(postgres_conn_id='collector_conn_id')
            connection = conn_collector_pg_hook.get_conn()
            cursor = connection.cursor()
            cursor.execute('BEGIN;')
            for w in df.itertuples(index=False):
                w_dict = w._asdict()
                bet_time = to_utc_time(w.betTime)
                start_time, end_time = date_range(bet_time)

                w_dict['betTime'] = bet_time
                w_dict["start_time"] = start_time.strftime('%Y-%m-%d %H:%M:%S')
                w_dict["end_time"] = end_time.strftime('%Y-%m-%d %H:%M:%S')
                w_dict['create_at']= utc_now
                w_dict['update_at'] = utc_now

                save_pgwagers ="""
                INSERT INTO {table} 
                (
                    bet_id, parent_bet_id, player_name, currency, game_id, platform, bet_type,
                    transaction_type, bet_amount, win_amount, jackpot_rtp_contribution_amount,
                    jackpot_win_amount, balance_before, balance_after, row_version, bet_time,
                    create_at, update_at
                )
                SELECT
                    %(betId)s, %(parentBetId)s, %(playerName)s, %(currency)s, %(gameId)s,
                    %(platform)s, %(betType)s, %(transactionType)s, %(betAmount)s, %(winAmount)s,
                    %(jackpotRtpContributionAmount)s, %(jackpotWinAmount)s, %(balanceBefore)s,
                    %(balanceAfter)s, %(rowVersion)s,%(betTime)s,%(create_at)s,%(update_at)s
                WHERE NOT EXISTS (SELECT 1 FROM {table} WHERE bet_id = %(betId)s AND bet_time >= %(start_time)s AND bet_time <= %(end_time)s);
                        """.format(table=PGSOFT_WAGER_TABLE)

                cursor.execute(save_pgwagers, w_dict)

            rowVersion = df['rowVersion'].iloc[-1]  
            update_pg_version(cursor, rowVersion)
            connection.commit()
        except Exception as e:
            print(f"Error occurred: {e}")
            connection.rollback() 
        finally:
            cursor.close()
            connection.close()

def update_pg_version(cursor, row_version):
    delete_pgsoft_sql = """
        DELETE FROM {0}
    """.format(PGSOFT_OLD_VERSION_TABLE)

    insert_pgsoft_sql = """
        INSERT INTO {0}(row_version) VALUES({1})
    """.format(PGSOFT_OLD_VERSION_TABLE, row_version)

    cursor.execute(delete_pgsoft_sql)
    cursor.execute(insert_pgsoft_sql)	  



download_pgsoft_operator = PythonOperator(
    task_id='extract_pgsoft_wager', python_callable=download_pgsoft, dag=dag)

download_pgsoft_operator