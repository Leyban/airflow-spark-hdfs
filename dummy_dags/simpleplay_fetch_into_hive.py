from airflow import DAG
from airflow.operators.python import PythonOperator


import requests

import base64
from pyDes import des, ECB, PAD_PKCS5
import hashlib

import time
import urllib.parse
from datetime import datetime, timedelta

import xml.etree.ElementTree as ET

from pyhive import hive


"""
=========================================================
                Constants and Variables  
=========================================================
"""

# --------------- Simpleplay variables ------------------

defaultSPSecretKey = "494DA5A7FC954AA4973456A08609C2F9"
defaultSPMd5key    = "GgaIMaiNNtg"
defaultSPEncrypKey = "g9G16nTs"

# Get the current date
today = datetime.now()

# Calculate the date one day ago from today
yesterday = today - timedelta(days=1)

# Format the dates as strings in the format "YYYYMMDDHHMMSS"
dateFrom = yesterday.strftime("%Y%m%d%H%M%S")
dateTo = today.strftime("%Y%m%d%H%M%S")




# --------------- Hive variables ------------------

url = 'http://localhost:8800/'

temp = {
    "database_name" : "temp_collector"
}

# Connection parameters
hive_params = {
    "host" : "172.17.0.1",
    "port" : 10000,  # Default Hive server port
    "database" : temp['database_name'],
    "username" : "scott", 
    "password" : "tiger",
    "auth" : 'CUSTOM'
}


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


def fetch_simpleplay_into_hive():
    timeStr = time.strftime("%Y%m%d%H%M%S")
    qs = "method=GetAllBetDetailsForTimeInterval"
    qs = qs + "&Key=" + '494DA5A7FC954AA4973456A08609C2F9'
    qs = qs + "&Time=" + timeStr
    qs = qs + "&FromTime=" + dateFrom
    qs = qs + "&ToTime=" + dateTo

    q = DESEnCode(qs, defaultSPEncrypKey)
    sig = BuildMD5(qs + defaultSPMd5key + timeStr + defaultSPSecretKey)

    data = urllib.parse.urlencode({'q': q, 's': sig})


    res = requests.post(url, data=data)


    # Parse the XML response
    root = ET.fromstring(res.text)


    # Access elements in the XML and extract data
    error_msg_id = root.find('ErrorMsgId').text
    error_msg = root.find('ErrorMsg').text


    # Access nested elements
    bet_details = []
    for b in root.findall('.//BetDetail'):
        # Create a dictionary to store the data for each bet detail
        bet_data = {
            'BetTime': b.find('BetTime').text,
            'PayoutTime': b.find('PayoutTime').text,
            'BetTime': b.find('BetTime').text,
            'PayoutTime': b.find('PayoutTime').text,
            'Username': b.find('Username').text,
            'HostID': b.find('HostID').text,
            'Detail': b.find('Detail').text,
            'GameID': b.find('GameID').text,
            'Round': b.find('Round').text,
            'Set': b.find('Set').text,
            'BetID': b.find('BetID').text,
            'BetAmount': b.find('BetAmount').text,
            'Rolling': b.find('Rolling').text,
            'ResultAmount': b.find('ResultAmount').text,
            'Balance': b.find('Balance').text,
            'GameType': b.find('GameType').text,
            'BetType': b.find('BetType').text,
            'BetSource': b.find('BetSource').text,
            'TransactionID': b.find('TransactionID').text,
            'GameResult': b.find('GameResult').text,
            'State': b.find('State').text,
        }
        bet_details.append(bet_data)

    # Create a dictionary to store the entire response
    history = {
        'ErrorMsgID': error_msg_id,
        'ErrorMsg': error_msg,
        'BetDetailList': bet_details,
    }


    # Establish the connection
    conn = hive.Connection(**hive_params)

    cursor = conn.cursor()

    # Create a table for simpleplay_wager
    hive_sql = """CREATE TABLE IF NOT EXISTS simpleplay_wager (
        bet_time STRING,
        payout_time STRING,
        username STRING,
        host_id SMALLINT,
        detail STRING,
        game_id STRING,
        round_num INT,
        set_num INT,
        bet_id BIGINT,
        bet_amount DOUBLE,
        rolling DOUBLE,
        result_amount DOUBLE,
        balance DOUBLE,
        game_type STRING,
        bet_type INT,
        bet_source INT,
        transaction_id BIGINT,
        game_result STRING,
        state BOOLEAN
        )"""


    # Execute SQL
    cursor.execute(hive_sql)


    insert_query_template = """
    INSERT INTO simpleplay_wager (
        bet_time,
        payout_time,
        username,
        host_id,
        detail,
        game_id,
        round_num,
        set_num,
        bet_id,
        bet_amount,
        rolling,
        result_amount,
        balance,
        game_type,
        bet_type,
        bet_source,
        transaction_id,
        game_result,
        state
    )
    VALUES (
        %(BetTime)s,
        %(PayoutTime)s,
        %(Username)s,
        %(HostID)s,
        %(Detail)s,
        %(GameID)s,
        %(Round)s,
        %(Set)s,
        %(BetID)s,
        %(BetAmount)s,
        %(Rolling)s,
        %(ResultAmount)s,
        %(Balance)s,
        %(GameType)s,
        %(BetType)s,
        %(BetSource)s,
        %(TransactionID)s,
        %(GameResult)s,
        %(State)s
    )
    """


    for row_data in bet_details:
        cursor.execute(insert_query_template, row_data)


    conn.commit()
    conn.close()


default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    default_args=default_args,
    dag_id='fetch_simpleplay_into_hive',
    start_date=datetime(2023, 7, 24),
    schedule_interval="0 10 * * *",
    catchup=False
) as dag:
    fetch = PythonOperator(
        task_id='extract_data_from_simpleplay_api',
        python_callable=fetch_simpleplay_into_hive
    )

    fetch
