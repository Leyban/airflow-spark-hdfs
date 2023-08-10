from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook

import requests
import logging

import base64
from pyDes import des, ECB, PAD_PKCS5
import hashlib
import urllib.parse

from datetime import datetime, timedelta
import xml.etree.ElementTree as ET


# Define Constants and Variables

# SimplePlay Constants
SP_Method = "GetAllBetDetailsForTimeInterval"
SP_Secret_Key = "494DA5A7FC954AA4973456A08609C2F9"
SP_Md5_Key    = "GgaIMaiNNtg"
SP_Encryp_Key = "g9G16nTs"
SP_Url = 'http://localhost:8800'

conn_collector_pg_hook = PostgresHook(postgres_conn_id='collector_conn_id')

def DESEnCode(mystr, deskey):
    mytext = mystr.encode('utf-8')
    key = deskey.encode('utf-8')
    iv = deskey.encode('utf-8')
    
    # DES Encryption
    k = des(key, ECB, IV=iv, pad=None, padmode=PAD_PKCS5)
    cryptoText = k.encrypt(mytext)
    
    # Encoding to base64
    encoded = base64.b64encode(cryptoText).decode('utf-8')
    return encoded

def BuildMD5(mystr):
    h = hashlib.md5()
    h.update(mystr.encode('utf-8'))
    data = h.hexdigest()
    return data


def fetch_simpleplay():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("fetch_simpleplay")
    
    try: 
        logical_time = datetime.now() # TODO: Utilize logical_time

        yesterday = logical_time - timedelta(days=1)

        dateFrom = yesterday.strftime("%Y%m%d%H%M%S")
        dateTo = logical_time.strftime("%Y%m%d%H%M%S")
        timeStr = logical_time.strftime("%Y%m%d%H%M%S")
        qs = "method" + SP_Method
        qs = qs + "&Key=" + SP_Secret_Key
        qs = qs + "&Time=" + timeStr
        qs = qs + "&FromTime=" + dateFrom
        qs = qs + "&ToTime=" + dateTo

        q = DESEnCode(qs, SP_Encryp_Key)
        sig = BuildMD5(qs + SP_Md5_Key + timeStr + SP_Secret_Key)

        data = urllib.parse.urlencode({'q': q, 's': sig})

        res = requests.post(SP_Url, data=data)
        if res.status_code != 201:
            logger.error(f"Request to Simpleplay API failed with status code: {res.status_code}")

        logger.info(res.text)

        root = ET.fromstring(res.text)

        error_msg_id = root.find('ErrorMsgId').text
        error_msg = root.find('ErrorMsg').text
        
        if error_msg != "":
            logger.error(f"Simpleplay API responded with error message id: {error_msg_id} with error message: {error_msg}")

        connection = conn_collector_pg_hook.get_conn()
        cursor = connection.cursor()

        sql = """
            INSERT INTO simpleplay_wager(bet_id, bet_time, payout_time, username, host_id, detail, game_id,
                round, set, bet_amount, rolling, result_amount, balance, game_type,
                bet_type, bet_source, transaction_id, state)
            VALUES(%s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s)
            ON CONFLICT (bet_id) DO NOTHING;
        """
        
        for b in root.findall('.//BetDetail'):
            cursor.execute(sql, (b.find('BetID').text, b.find('BetTime').text, b.find('PayoutTime').text, b.find('Username').text, b.find('HostID').text, b.find('Detail').text, b.find('GameID').text, 
                b.find('Round').text, b.find('Set').text, b.find('BetAmount').text, b.find('Rolling').text, b.find('ResultAmount').text, b.find('Balance').text, b.find('GameType').text, 
                b.find('BetType').text, b.find('BetSource').text, b.find('TransactionID').text, b.find('State').text))
                        
        
        connection.commit()
            
    except Exception as e: 
        logger.exception("An unexpected error occurred while fetching Simpleplay data:", e)
        connection.rollback()
    
    finally:
        cursor.close()
        connection.close()



default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    default_args=default_args,
    dag_id='fetch_simpleplay_bets',
    start_date=datetime(2023, 6, 20),
    schedule_interval="0 10 * * *",
    catchup=True
) as dag:
    fetch = PythonOperator(
        task_id='extract_data_from_simpleplay_api',
        python_callable=fetch_simpleplay
    )

    fetch