from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import pendulum

dag = DAG(
    'simpleplay_wager-v1.0.0',
    description='DAG',
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False
)

def DESEnCode(mystr: str, deskey: str):
    import base64
    from Crypto.Cipher import DES
    from Crypto.Util.Padding import pad

    mytext = mystr.encode()
    key = deskey.encode()
    iv = deskey.encode()
    
    cipher = DES.new(key, DES.MODE_CBC, iv)

    padded_data = pad(mytext, DES.block_size)

    msg = cipher.encrypt(padded_data)
    return base64.b64encode(msg).decode()


def BuildMD5(mystr):
    import hashlib

    h = hashlib.md5()
    h.update(mystr.encode())

    data = h.hexdigest()
    return data


def get_simpleplay_df(**context):
    import time 
    import requests
    import pandas as pd
    import urllib.parse
    import xml.etree.ElementTree as ET
    from io import StringIO
    from airflow.models import Variable

    secret_key = Variable.get( 'SIMPLEPLAY_SECRET_KEY' )
    md5_key = Variable.get( 'SIMPLEPLAY_MD5_KEY' )
    encrypt_key = Variable.get( 'SIMPLEPLAY_ENCRYPT_KEY' )
    url = Variable.get( 'SIMPLEPLAY_URL' )

    date_to = datetime.utcnow()
    date_from = date_to - timedelta(hours=3)

    # Taking Optional Date Parameters
    date_format = '%Y-%m-%d %H:%M:%S'
    if 'date_from' in context['params']:
        date_from = datetime.strptime(context['params']['date_from'], date_format)
    
    if 'date_to' in context['params']:
        date_to = datetime.strptime(context['params']['date_to'], date_format)

    dateFrom = date_from.strftime("%Y%m%d%H%M%S")
    dateTo = date_to.strftime("%Y%m%d%H%M%S")

    timeStr = time.strftime("%Y%m%d%H%M%S")
    qs = "method=GetAllBetDetailsForTimeInterval"
    qs = qs + "&Key=" + secret_key
    qs = qs + "&Time=" + timeStr
    qs = qs + "&FromTime=" + dateFrom
    qs = qs + "&ToTime=" + dateTo

    q = DESEnCode(qs, encrypt_key)
    sig = BuildMD5(qs + md5_key + timeStr + secret_key)

    data = urllib.parse.urlencode({'q': q, 's': sig})

    res = requests.post(url, data=data)

    root = ET.fromstring(res.text)

    error_msg_id = root.findtext('ErrorMsgId')
    error_msg = root.findtext('ErrorMsg')

    if error_msg != None or error_msg_id != None:
        print("An Error Occurred Fetching the API")
        print(error_msg_id, error_msg)

    bet_details_df = pd.read_xml(StringIO(res.text), xpath=".//BetDetail")

    return bet_details_df


def get_year_month(x):
    year = x.strftime("%Y")
    month = x.strftime("%m")

    return int(f"{year}{month}")


def get_existing_data(df, conn):
    import pandas as pd

    exists_df = pd.DataFrame()

    ranges = df['bet_id_range'].unique()

    for range in ranges:

        range_df = df[df['bet_id_range'] == range]
        range_df = range_df.reset_index(drop=True)

        rawCql = f"""
            SELECT bet_id, username
            FROM wagers.simpleplay_by_id
            WHERE bet_id_range = ?
            AND bet_id IN (? {",?" * (range_df.shape[0] - 1)})
        """

        parameters = [range]
        for _, row in range_df.iterrows():
            parameters.append(row['bet_id'])

        prepared_query = conn.prepare(rawCql)
        result = conn.execute(prepared_query, parameters)

        result_df = pd.DataFrame(result)

        exists_df = pd.concat([exists_df, result_df])

    return exists_df


def get_inserted_data(df, conn):
    import pandas as pd

    inserted_df = pd.DataFrame()

    ranges = df['bet_id_range'].unique()

    for range in ranges:

        range_df = df[df['bet_id_range'] == range]
        range_df = range_df.reset_index(drop=True)

        rawCql = f"""
            SELECT *
            FROM wagers.simpleplay_by_id
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

    
def insert_into_simpleplay_by_id(row, conn):
    from datetime import datetime

    now = datetime.now()

    rawCql = f"""INSERT INTO wagers.simpleplay_by_id (
            bet_id_range,
            bet_id, 
            bet_time, 
            payout_time, 
            username, 
            host_id, 
            detail,
            game_id, 
            round, 
            "set", 
            bet_amount, 
            rolling, 
            result_amount,
            balance, 
            game_type, 
            bet_type, 
            bet_source, 
            transaction_id,
            game_result, 
            state,
            create_at,
            update_at
        ) VALUES (? {",?" * 21})
       """

    parameters = [
       row['bet_id_range'],
       row['bet_id'], 
       row['bet_time'], 
       row['payout_time'], 
       row['username'], 
       row['host_id'], 
       row['detail'],
       row['game_id'], 
       row['round'], 
       row['set'], 
       row['bet_amount'], 
       row['rolling'], 
       row['result_amount'],
       row['balance'], 
       row['game_type'], 
       row['bet_type'], 
       row['bet_source'], 
       row['transaction_id'],
       row['game_result'], 
       row['state'],
       now,
       now,
       ]

    prepared_query = conn.prepare(rawCql)
    conn.execute(prepared_query, parameters)


def insert_into_simpleplay_by_member(row, conn):
    from datetime import datetime

    now = datetime.now()

    rawCql = f"""INSERT INTO wagers.simpleplay_by_member (
            username, 
            bet_time, 
            bet_id, 
            payout_time, 
            host_id, 
            detail,
            game_id, 
            round, 
            "set", 
            bet_amount, 
            rolling, 
            result_amount,
            balance, 
            game_type, 
            game_result,
            bet_type, 
            bet_source, 
            transaction_id,
            state,
            create_at,
            update_at
        ) VALUES (? {",?" * 20})
       """

    parameters = [
       row['username'], 
       row['bet_time'], 
       row['bet_id'], 
       row['payout_time'], 
       row['host_id'], 
       row['detail'],
       row['game_id'], 
       row['round'], 
       row['set'], 
       row['bet_amount'], 
       row['rolling'], 
       row['result_amount'],
       row['balance'], 
       row['game_type'], 
       row['game_result'], 
       row['bet_type'], 
       row['bet_source'], 
       row['transaction_id'],
       row['state'],
       now,
       now,
       ]

    prepared_query = conn.prepare(rawCql)
    conn.execute(prepared_query, parameters)


def insert_into_simpleplay_by_date(row, conn):
    from datetime import datetime

    now = datetime.now()

    rawCql = f"""INSERT INTO wagers.simpleplay_by_date (
            year_month,
            bet_time, 
            bet_id, 
            payout_time, 
            username, 
            host_id, 
            detail,
            game_id, 
            round, 
            "set", 
            bet_amount, 
            rolling, 
            result_amount,
            balance, 
            game_type, 
            game_result,
            bet_type, 
            bet_source, 
            transaction_id,
            state,
            create_at,
            update_at
        ) VALUES (? {",?" * 21})
       """

    parameters = [
       row['year_month'],
       row['bet_time'], 
       row['bet_id'], 
       row['payout_time'], 
       row['username'], 
       row['host_id'], 
       row['detail'],
       row['game_id'], 
       row['round'], 
       row['set'], 
       row['bet_amount'], 
       row['rolling'], 
       row['result_amount'],
       row['balance'], 
       row['game_type'], 
       row['game_result'], 
       row['bet_type'], 
       row['bet_source'], 
       row['transaction_id'],
       row['state'],
       now,
       now,
       ]

    prepared_query = conn.prepare(rawCql)
    conn.execute(prepared_query, parameters)


def fetch_simpleplay_wager(**context):
    import math
    import time
    import requests
    import pandas as pd
    from airflow.providers.apache.hive.hooks.hive import AirflowException
    from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook

    try: 
        df = get_simpleplay_df(**context)

        # Renaming for Consistency
        df = df.rename(columns={
            'BetID':'bet_id', 
            'BetTime':'bet_time', 
            'PayoutTime':'payout_time', 
            'Username':'username', 
            'HostID':'host_id', 
            'Detail':'detail',
            'GameID':'game_id', 
            'Round':'round', 
            'Set':'set', 
            'BetAmount':'bet_amount', 
            'Rolling':'rolling', 
            'ResultAmount':'result_amount',
            'Balance':'balance', 
            'GameType':'game_type', 
            'BetType':'bet_type', 
            'BetSource':'bet_source', 
            'TransactionID':'transaction_id',
            'GameResult':'game_result', 
            'State':'state'
           })

        # Partitioning
        df['bet_time'] = pd.to_datetime(df['bet_time'])
        df['year_month'] = df['bet_time'].apply(lambda x: get_year_month(x))
        df['bet_id_range'] = df['bet_id'].apply(lambda x: math.ceil(x * 1e-6))

        # Type Corrections
        df['bet_time'] = df['bet_time'].apply(lambda x: x.to_pydatetime())
        df['payout_time'] = pd.to_datetime(df['payout_time'])
        df['payout_time'] = df['payout_time'].apply(lambda x: x.to_pydatetime())
        df = df.astype({
            'username':'string',
            'detail':'string',
            'game_id':'string',
            'game_result':'string',
            'game_type':'string',
            'state':'string',
            })

        # Create a Cassandra Hook
        cassandra_hook = CassandraHook( cassandra_conn_id='cassandra_conn_id' )
        conn = cassandra_hook.get_conn()

        tic = time.perf_counter()
        exists_df = get_existing_data(df, conn)
        toc = time.perf_counter()
        print(f"Time for fetching existing data: {toc - tic:0.4f} seconds")

        # Same bet_id different player
        if not exists_df.empty:
            df['duplicate'] = df.apply(lambda row: (row['bet_id'] in exists_df['bet_id'].values) and (row['username'] not in exists_df.loc[exists_df['bet_id'] == row['bet_id'], 'username'].values), axis=1)
            df = df[~df['duplicate']]

        if df.shape[0] == 0:
            print("No New Data Found")
            return

        # Insert new data
        tic = time.perf_counter()
        for _, row in df.iterrows():

            insert_into_simpleplay_by_id(row, conn)

        toc = time.perf_counter()
        print(f"Time for inserting data to pgsoft_by_id: {toc - tic:0.4f} seconds")

        tic = time.perf_counter()
        inserted_df = get_inserted_data(df, conn)
        inserted_df['year_month'] = inserted_df['bet_time'].apply(lambda x: get_year_month(x))
        toc = time.perf_counter()
        print(f"Time for retrieving inserted data: {toc - tic:0.4f} seconds")

        tic = time.perf_counter()
        for _, row in inserted_df.iterrows():
            insert_into_simpleplay_by_member(row, conn)
            insert_into_simpleplay_by_date(row, conn)
        toc = time.perf_counter()
        print(f"Time for inserting to pgsoft_by_member and pgsoft_by_date: {toc - tic:0.4f} seconds")
        conn.shutdown()

    except requests.exceptions.RequestException as err:
        print("Request error:", err)
        raise AirflowException

    except Exception as Argument:
        print(f"Error occurred: {Argument}")
        raise AirflowException


download_simpleplay = PythonOperator(
    task_id='download_simpleplay',
    python_callable=fetch_simpleplay_wager,
    dag=dag
)

download_simpleplay
