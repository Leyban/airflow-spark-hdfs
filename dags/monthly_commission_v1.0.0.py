from airflow.decorators import dag, task_group
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from datetime import  datetime,timedelta
from pandas import DataFrame
import time

from pandas.io.formats.format import get_adjustment


# Commmission Fee Type
COMMISSION_FEE_TYPE_DP = 1
COMMISSION_FEE_TYPE_WD = 2
COMMISSION_FEE_TYPE_ADJ = 3

# Adjustment Transaction Type
ADJUSTMENT_TRANSACTION_TYPE_CREDIT = 3

# Transaction Status
ADJUSTMENT_STATUS_SUCCESSFUL = 2
WITHDRAWAL_STATUS_SUCCESSFUL = 6
DEPOSIT_STATUS_SUCCESSFUL = 2

# Table Names
DEPOSIT_TABLE = "deposit"
WITHDRAWAL_TABLE = "withdrawal"

# Sqlite file directories
SQLITE_TRANSACTIONS_PATH = "./data/monthly_commission/transactions"
SQLITE_WAGERS_PATH = "./data/monthly_commission/wagers"

# Product Codes
PRODUCT_CODE_ALLBET = "allbet"
PRODUCT_CODE_ASIAGAMING = "asiagaming"
PRODUCT_CODE_AGSLOT = "agslot"
PRODUCT_CODE_AGYOPLAY = "agyoplay"
PRODUCT_CODE_SAGAMING = "sagaming"
PRODUCT_CODE_SPSLOT = "simpleplay"
PRODUCT_CODE_SPFISH = "simpleplayfisher"
PRODUCT_CODE_SABACV = "sabacv"
PRODUCT_CODE_PGSOFT = "pgsoft" 
PRODUCT_CODE_EBETGAMING = "ebet"
PRODUCT_CODE_BTISPORTS = "bti"
PRODUCT_CODE_TFGAMING = "tfgaming"
PRODUCT_CODE_EVOLUTION = "evolution"
PRODUCT_CODE_GENESIS = "genesis"
PRODUCT_CODE_SABA = "saba"
PRODUCT_CODE_SABANUMBERGAME = "sabanumbergames"
PRODUCT_CODE_SABAVIRTUAL = "sabavirtualsport"
PRODUCT_CODE_DIGITAIN = "digitain"
PRODUCT_CODE_WEWORLD = "weworld"

PRODUCT_CODES = [
        PRODUCT_CODE_ALLBET,
        PRODUCT_CODE_ASIAGAMING,
        PRODUCT_CODE_AGSLOT,
        PRODUCT_CODE_AGYOPLAY,
        PRODUCT_CODE_SAGAMING,
        PRODUCT_CODE_SPSLOT,
        PRODUCT_CODE_SPFISH,
        PRODUCT_CODE_SABACV,
        PRODUCT_CODE_PGSOFT,
        PRODUCT_CODE_EBETGAMING,
        PRODUCT_CODE_BTISPORTS,
        PRODUCT_CODE_TFGAMING,
        PRODUCT_CODE_EVOLUTION,
        PRODUCT_CODE_GENESIS,
        PRODUCT_CODE_SABA,
        PRODUCT_CODE_SABANUMBERGAME,
        PRODUCT_CODE_SABAVIRTUAL,
        PRODUCT_CODE_DIGITAIN,
        PRODUCT_CODE_WEWORLD,
        ]

# Transaction Types
TRANSACTION_TYPE_DEPOSIT = "deposit"
TRANSACTION_TYPE_WITHDRAWAL = "withdrawal"
TRANSACTION_TYPE_ADJUSTMENT = "adjustment"

TRANSACTION_TYPES = [
        TRANSACTION_TYPE_DEPOSIT,
        TRANSACTION_TYPE_WITHDRAWAL,
        TRANSACTION_TYPE_ADJUSTMENT,
        ]


def init_sqlite():
    import os
    import sqlite3

    # Create Directories
    for code in PRODUCT_CODES:
        if not os.path.exists(f"{SQLITE_WAGERS_PATH}/{code}/"):
            os.makedirs(f"{SQLITE_WAGERS_PATH}/{code}/")

    for ttype in TRANSACTION_TYPES:
        if not os.path.exists(f"{SQLITE_TRANSACTIONS_PATH}/{ttype}/"):
            os.makedirs(f"{SQLITE_TRANSACTIONS_PATH}/{ttype}/")

    conn = sqlite3.connect("./data/member.db") # This should be in Cassandra or something
    curs = conn.cursor()

    get_table_list = "SELECT name FROM sqlite_master WHERE type='table' AND name='member'"

    res = curs.execute(get_table_list)
    tables = res.fetchall()

    if len( tables ) == 0:
        create_member_table_sql = """
            CREATE TABLE member(
                    id integer,
                    affiliate_id integer,
                    login_name text,
                    currency text
                    ) 
        """
        curs.execute(create_member_table_sql)

    curs.close()


def create_wager_table_task(product: str, **kwargs):
    import sqlite3

    datestamp = kwargs['ds_nodash']

    table_name = f"{product}_{datestamp}"

    filepath = f"{SQLITE_WAGERS_PATH}/{product}/{table_name}.db"

    print(f"Creating file: {filepath}")
    conn = sqlite3.connect(filepath)
    curs = conn.cursor()

    get_table_list = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"

    res = curs.execute(get_table_list)
    tables = res.fetchall()

    if len( tables ) == 0:
        curs.execute(f"""
                     CREATE TABLE {table_name} (
                         bet_id integer, 
                         eligible_stake_amount real, 
                         login_name text, 
                         currency text
                     )
                     """)
        conn.commit()

    conn.close()


def create_transaction_table_task(transaction_type, **kwargs):
    import sqlite3

    datestamp = kwargs['ds_nodash']
    
    table_name = f"{transaction_type}_{datestamp}"

    filepath = f"{SQLITE_TRANSACTIONS_PATH}/{transaction_type}/{table_name}.db"

    print(f"Creating file: {filepath}")
    conn = sqlite3.connect(filepath)
    curs = conn.cursor()

    get_table_list = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"

    res = curs.execute(get_table_list)
    tables = res.fetchall()

    if len( tables ) == 0:
        curs.execute(f"""
                     CREATE TABLE {table_name} (
                         affiliate_id integer,
                         amount real,
                         currency text,
                         affiliate_fee real
                     )
                     """)
        conn.commit()

    conn.close()

  
def save_transaction_to_sqlite(transaction_type, transaction_df, **kwargs):
    import sqlite3

    datestamp = kwargs['ds_nodash']
    
    table_name = f"{transaction_type}_{datestamp}"

    filepath = f"{SQLITE_TRANSACTIONS_PATH}/{transaction_type}/{table_name}.db"

    print(f"Saving {transaction_df.shape[0]} rows to file: {filepath}")
    conn = sqlite3.connect(filepath)

    transaction_df.to_sql(table_name, conn, if_exists='replace')
    print("Saved Successfully")


def get_transaction_df(transaction_type: str, **kwargs) -> DataFrame:
    import sqlite3
    import pandas as pd

    datestamp = kwargs['ds_nodash']
    table_name = f"{transaction_type}_{datestamp}"

    filepath = f"{SQLITE_TRANSACTIONS_PATH}/{transaction_type}/{table_name}.db"

    print(f"Connecting to {filepath}")
    conn = sqlite3.connect(filepath)

    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)

    df = df.drop(['index'], axis=1)

    return df


def drop_prev_transaction_table(transaction_type: str, **kwargs):
    import os
    datestamp = kwargs['ds_nodash']

    table_name = f"{transaction_type}_{datestamp}"
    filepath = f"{SQLITE_TRANSACTIONS_PATH}/{transaction_type}/{table_name}.db"

    print("Deleting File", filepath)
    os.remove(filepath)


def drop_prev_wager_table(product: str, **kwargs):
    import os
    datestamp = kwargs['ds_nodash']

    table_name = f"{product}_{datestamp}"
    filepath = f"{SQLITE_WAGERS_PATH}/{product}/{table_name}.db"

    print("Deleting File", filepath)
    os.remove(filepath)


def get_withdrawal_data(date_from, date_to):
    import pandas as pd

    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')

    raw_sql = f"""
        SELECT 
            ma.affiliate_id,
            ma.member_id,
            w.login_name as member_name,
            w.withdrawal_amount as amount,
            w.currency as m_currency,
            pmc.fees->>'affiliate_fee' as affiliate_fee
        FROM {WITHDRAWAL_TABLE} as w
        LEFT JOIN payment_method as pm on w.payment_method_code = pm.code
        LEFT JOIN payment_method_currency as pmc on pm.id = pmc.payment_method_id
        LEFT JOIN member_account as ma on w.member_id = ma.member_id
        WHERE w.created_at > '{date_from}'
        AND w.created_at <= '{date_to}'
        AND ma.affiliate_id > 0
        AND w.status = {WITHDRAWAL_STATUS_SUCCESSFUL}
    """

    df = conn_payment_pg_hook.get_pandas_df(raw_sql)

    df = df[pd.to_numeric(df['affiliate_fee']).fillna(0) > 0]

    df['type'] = COMMISSION_FEE_TYPE_WD

    return df


def get_deposit_data(date_from, date_to):
    import pandas as pd

    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')

    raw_sql = f"""
        SELECT 
            ma.affiliate_id,
            ma.member_id,
            d.login_name as member_name,
            d.net_amount as amount,
            d.currency as m_currency,
            pmc.fees->>'affiliate_fee' as affiliate_fee
        FROM {DEPOSIT_TABLE} as d
        LEFT JOIN payment_method as pm on d.payment_method_code = pm.code
        LEFT JOIN payment_method_currency as pmc on pm.id = pmc.payment_method_id
        LEFT JOIN member_account as ma on d.member_id = ma.member_id
        WHERE d.created_at > '{date_from}'
        AND d.created_at <= '{date_to}'
        AND ma.affiliate_id > 0
        AND d.status = {DEPOSIT_STATUS_SUCCESSFUL}
    """

    df = conn_payment_pg_hook.get_pandas_df(raw_sql)

    df = df[pd.to_numeric(df['affiliate_fee']).fillna(0) > 0]

    df['type'] = COMMISSION_FEE_TYPE_DP

    return df


def get_adjustment_data(date_from, date_to):
    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')

    raw_sql = f"""
        SELECT
            ma.affiliate_id as affiliate_id,
            ma.member_id as member_id,
            a.login_name as member_name,
            a.transaction_id,
            (CASE
                 WHEN a.transaction_type = {ADJUSTMENT_TRANSACTION_TYPE_CREDIT} 
                 THEN a.amount 
                 ELSE -a.amount 
            END) as amount,
            a.currency as m_currency
        FROM adjustment as a
        LEFT JOIN adjustment_reason as ar on a.reason_id = ar.id
        LEFT JOIN member_account as ma on a.login_name = ma.login_name
        WHERE a.created_at > '{date_from}'
        AND a.created_at <= '{date_to}'
        AND ma.affiliate_id > 0
        AND a.status = {ADJUSTMENT_STATUS_SUCCESSFUL}
    """

    df = conn_payment_pg_hook.get_pandas_df(raw_sql)

    df['affiliate_fee'] = 100

    df['type'] = COMMISSION_FEE_TYPE_ADJ

    return df


def retrieve_transactions(get_transaction_func, transaction_type, **kwargs):
    from dateutil.relativedelta import relativedelta

    ds = datetime.strptime(kwargs['ds'], '%Y-%m-%d')

    date_from = (ds - relativedelta(months=1)).strftime("%Y-%m-%d %H:%M:%S")
    date_to = ds.strftime("%Y-%m-%d %H:%M:%S")

    transaction_df = get_transaction_func(date_from, date_to)

    save_transaction_to_sqlite(transaction_type, transaction_df, **kwargs)


def calc_transaction_fees(transaction_type, **kwargs):
    transaction_df = get_transaction_df(transaction_type, **kwargs)

    transaction_df['amount'] = transaction_df['amount'] * transaction_df['affiliate_fee'] * 0.01

    transaction_df = transaction_df.groupby(['affiliate_id'])


@dag(
    dag_id='monthly_commission-v1.0.0',
    description='calculates commission for affiliates every month',
    schedule="@monthly",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=5,
    )
def monthly_commission():
    init_sqlite_task = PythonOperator(
            task_id="init_sqlite",
            python_callable=init_sqlite,
            )
    
    @task_group
    def get_transactions():

        create_deposit_table = PythonOperator(
                task_id="create_deposit_table",
                python_callable=create_transaction_table_task,
                op_args=[ "deposit" ]
                )
        create_withdrawal_table = PythonOperator(
                task_id="create_withdrawal_table",
                python_callable=create_transaction_table_task,
                op_args=[ "withdrawal" ]
                )
        create_adjustment_table = PythonOperator(
                task_id="create_adjustment_table",
                python_callable=create_transaction_table_task,
                op_args=[ "adjustment" ]
                )
        
        retrieve_withdrawal_task = PythonOperator(
                task_id="retrieve_withdrawal",
                python_callable=retrieve_transactions,
                op_args=[ get_withdrawal_data ]
                )
        retrieve_deposits_task = PythonOperator(
                task_id="retrieve_deposit",
                python_callable=retrieve_transactions,
                op_args=[ get_deposit_data ]
                )
        retrieve_adjustment_task = PythonOperator(
                task_id="retrieve_adjustment",
                python_callable=retrieve_transactions,
                op_args=[ get_adjustment_data ]
                )

        create_withdrawal_table >> retrieve_withdrawal_task
        create_deposit_table >> retrieve_deposits_task
        create_adjustment_table >> retrieve_adjustment_task

    init_sqlite_task >> get_transactions()

monthly_commission()
