from airflow import DAG
from airflow.operators.python import PythonOperator

import psycopg2
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
SP_Url = 'https://om6wq.mocklab.io/xml'

# Postgres Constants
PG_database="collectorDB"
PG_host='172.17.0.1'
PG_user='airflow'
PG_password='airflow'
PG_port='5432'

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
    # Initialize logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("fetch_simpleplay")
    
    try: 
        # Get the current date
        today = datetime.now()

        # Calculate the date one year ago from today
        one_year_ago = today - timedelta(days=365)

        # Define Params
        dateFrom = one_year_ago.strftime("%Y%m%d%H%M%S")
        dateTo = today.strftime("%Y%m%d%H%M%S")
        timeStr = today.strftime("%Y%m%d%H%M%S")
        qs = "method" + SP_Method
        qs = qs + "&Key=" + SP_Secret_Key
        qs = qs + "&Time=" + timeStr
        qs = qs + "&FromTime=" + dateFrom
        qs = qs + "&ToTime=" + dateTo

        # Encryption
        q = DESEnCode(qs, SP_Encryp_Key)
        sig = BuildMD5(qs + SP_Md5_Key + timeStr + SP_Secret_Key)

        # Encode
        data = urllib.parse.urlencode({'q': q, 's': sig})

        # Fetch
        res = requests.post(SP_Url, data=data)
        if res.status_code != 201:
            logger.error(f"Request to Simpleplay API failed with status code: {res.status_code}")

        # Log Response
        logger.info(res.text)

        # Parse the XML response
        root = ET.fromstring(res.text)

        # Access elements in the XML and extract data
        error_msg_id = root.find('ErrorMsgId').text
        error_msg = root.find('ErrorMsg').text
        
        # Check for error messages
        if error_msg != "":
            logger.error(f"Simpleplay API responded with error message id: {error_msg_id} with error message: {error_msg}")

        # Access nested elements
        bet_details = []
        for b in root.findall('.//BetDetail'):
            # Create a dictionary to store the data for each bet detail
            bet_data = {
                'BetTime': b.find('BetTime').text,
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

        # Define history dict
        history = {
            'ErrorMsgID': error_msg_id,
            'ErrorMsg': error_msg,
            'BetDetailList': bet_details,
        }

        # Establish connection
        conn = psycopg2.connect(
            database=PG_database,
            host=PG_host,
            user=PG_user,
            password=PG_password,
            port=PG_port
        )

        cursor = conn.cursor()

        # Define SQL
        sql = """
            INSERT INTO simpleplay_wager(bet_id, bet_time, payout_time, username, host_id, detail, game_id,
                round, set, bet_amount, rolling, result_amount, balance, game_type,
                bet_type, bet_source, transaction_id, state)
            VALUES(%s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s);
        """

        # Iterate Insertion
        for b in history['BetDetailList']:
            cursor.execute(sql, (b['BetID'], b['BetTime'], b['PayoutTime'], b['Username'], b['HostID'], b['Detail'], b['GameID'], 
                                b['Round'], b['Set'], b['BetAmount'], b['Rolling'], b['ResultAmount'], b['Balance'], b['GameType'], 
                                b['BetType'], b['BetSource'], b['TransactionID'], b['State']))
        
        # Commit Insert
        conn.commit()
        cursor.close()
    
    except Exception as e: 
        logger.exception("An unexpected error occurred while fetching Simpleplay data:", e)



default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    default_args=default_args,
    dag_id='fetch_simpleplay_bets',
    start_date=datetime(2023, 7, 24),
    schedule_interval="0 10 * * *",
    catchup=False
) as dag:
    fetch = PythonOperator(
        task_id='extract_data_from_simpleplay_api',
        python_callable=fetch_simpleplay
    )

    fetch