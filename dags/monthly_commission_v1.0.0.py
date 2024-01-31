from airflow.decorators import dag, task_group, task
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule
from datetime import  datetime,timedelta
from pandas import DataFrame, Series

# Constants
CURRENCY_VND = "VND"
CURRENCY_THB = "THB"
CURRENCY_RMB = "RMB"
CURRENCY_USD = "USD"

# Payout Frequency
PAYOUT_FREQUENCY_WEEKLY = 'weekly'
PAYOUT_FREQUENCY_BI_MONTHLY = 'bi_monthly'
PAYOUT_FREQUENCY_MONTHLY = 'monthly'

# Payout Frequency Calculation Day
WEEKLY_DAY = 0          # Day of the Week; 0 = Monday, 6 = Sunday
BI_MONTHLY_DAY = 15     # Day of Month
MONTHLY_DAY = 1         # First Day of Month

# Adjustment Transaction Type
ADJUSTMENT_TRANSACTION_TYPE_CREDIT = 3
ADJUSTMENT_TYPE_COMMISSION = 1

# Transaction Status
ADJUSTMENT_STATUS_SUCCESSFUL = 2
WITHDRAWAL_STATUS_SUCCESSFUL = 6
DEPOSIT_STATUS_SUCCESSFUL = 2

# Commission status and payment status
COMMISSION_STATUS_PROCESSING = 1
COMMISSION_STATUS_THRESHOLD = 2

COMMISSION_PAYMENT_STATUS_PROCESSING = 1
COMMISSION_PAYMENT_STATUS_THRESHOLD = 2

# Table Names
AFFILIATE_TABLE = "affiliate"
COMMISSION_TABLE = "cm_commission"

ADJUSTMENT_TABLE = "adjustment"

DEPOSIT_TABLE = "deposit"
WITHDRAWAL_TABLE = "withdrawal"

ALL_BET_WAGER_TABLE = "allbet_wager"
ASIAGAMING_WAGER_TABLE = "asiagaming_wager"
SAGAMING_WAGER_TABLE = "sagaming_wager"
SIMPLEPLAY_WAGER_TABLE = "simpleplay_wager"
PGSOFT_WAGER_TABLE = "pgsoft_wager"
EBET_WAGER_TABLE = "ebet_wager"
BTI_WAGER_TABLE = "bti_wager"
SABACV_WAGER_TABLE = "sabacv_wager"
SABA_WAGER_TABLE = "saba_wager"
SABA_NUMBER_TABLE = "saba_number"
SABA_VIRTUAL_TABLE = "saba_virtual"
TFGAMING_TABLE = "tfgaming_wager"
EVOLUTION_TABLE = "evolution_wager"
GENESIS_TABLE = "genesis_wager"
WEWORLD_TABLE = "weworld_wager"

# Sqlite file directories
SQLITE_TRANSACTIONS_PATH = "./data/monthly_commission/transactions"
SQLITE_WAGERS_PATH = "./data/monthly_commission/wagers"
SQLITE_ADJUSTMENTS_PATH = "./data/monthly_commission/adjustments"
SQLITE_MEMBERS_FILEPATH = "./data/member.db"
SQLITE_AFFILIATE_ACCOUNT_FILEPATH = "./data/affiliate.db"

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

UTC_EXEC_TIME = 16


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

    if not os.path.exists(f"{SQLITE_ADJUSTMENTS_PATH}/"):
        os.makedirs(f"{SQLITE_ADJUSTMENTS_PATH}/")

    conn = sqlite3.connect(SQLITE_MEMBERS_FILEPATH)
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

    conn = sqlite3.connect(SQLITE_MEMBERS_FILEPATH)
    curs = conn.cursor()

    get_table_list = "SELECT name FROM sqlite_master WHERE type='table' AND name='affiliate'"

    res = curs.execute(get_table_list)
    tables = res.fetchall()

    if len( tables ) == 0:
        create_member_table_sql = """
            CREATE TABLE affiliate(
                    affiliate_id integer,
                    login_name text,
                    commission_tier1 integer,
                    commission_tier2 integer,
                    commission_tier3 integer,
                    payout_frequency text,
                    min_active_player integer
                    ) 
        """
        curs.execute(create_member_table_sql)
    curs.close()


# Todo: I think this can be refactored better -- What a mess
def get_member_currency(wager_df) -> DataFrame:
    import pandas as pd

    if wager_df.shape[0] == 0:
        return pd.DataFrame()

    member_df = get_members_from_sqlite()

    if member_df.shape[0] != 0:
        wager_df = wager_df.merge(member_df, 'left', 'login_name')
        wager_df = wager_df.drop(columns=['member_id'])
 
    # Get Missing Members
    missing_member_df = pd.DataFrame()

    # Some members are missing
    if 'currency' in wager_df.columns:
        lacking_wager_df = wager_df[wager_df['currency'].isna()].loc[:,['login_name']]
        missing_member_df = lacking_wager_df.loc[:,['login_name']]
        wager_df = wager_df.dropna(subset=['currency'])

        if missing_member_df.shape[0] > 0:
            new_member_df = update_members_on_sqlite(missing_member_df)
            lacking_wager_df = lacking_wager_df.merge(new_member_df, 'left', 'login_name')

            wager_df = pd.concat([wager_df, lacking_wager_df])

    # All members are missing
    else:
        missing_member_df = wager_df.loc[:,['login_name']]

        new_member_df = update_members_on_sqlite(missing_member_df)
        wager_df = wager_df.merge(new_member_df, 'left', 'login_name')

    wager_df = wager_df.dropna(subset=['currency']) # Drop Members Not found
    if 'member_id' in wager_df:
        wager_df = wager_df.drop(columns=['member_id'])

    wager_df = wager_df.reset_index(drop=True)

    return wager_df


def insert_member_into_sqlite(new_member_df):
    import sqlite3
    conn = sqlite3.connect(SQLITE_MEMBERS_FILEPATH)
    curs = conn.cursor()

    new_member_df = new_member_df.reset_index(drop=True)

    print("Inserting ", new_member_df.shape[0], " Members into Sqlite member table")

    for _, row in new_member_df.iterrows():
        query = f"""
        INSERT INTO member (
            id,
            affiliate_id,
            login_name,
            currency
        )  VALUES (
            {row['id']},
            {row['affiliate_id']},
            '{row['login_name']}',
            '{row['currency']}'
        )"""

        curs.execute(query)

    conn.close()


def update_members_on_sqlite(missing_member_df) -> DataFrame:
    import pandas as pd

    conn_identity_pg_hook = PostgresHook(postgres_conn_id='identity_conn_id')

    member_df = missing_member_df.drop_duplicates(subset=['login_name'])
    member_df = member_df.astype('str')

    print("Fetching Missing Members from Sqlite: ", member_df.shape[0])
    member_df = member_df.reset_index(drop=True)

    found_members_df = pd.DataFrame()

    rawsql = f"""
        SELECT
            id,
            affiliate_id,
            login_name,
            currency
        FROM member
        WHERE login_name IN (
    """

    for i, row in member_df.iterrows():
        rawsql += f" '{row['login_name']}' "

        if i != member_df.shape[0] - 1:
            rawsql += ","

    rawsql += """)
        AND affiliate_id > 0
    """

    df = conn_identity_pg_hook.get_pandas_df(rawsql)

    found_members_df = pd.concat([found_members_df, df])

    # Print Missing Members
    if found_members_df.shape[0] != member_df.shape[0]:
        print("Some Members are missing: ")
        merged_df = member_df.merge(found_members_df, 'left', 'login_name')
        not_found_df = merged_df[merged_df['currency'].isna()]
        for _, row in not_found_df.iterrows():
            print(f"Missing Member: {row['login_name']}")

    insert_member_into_sqlite(found_members_df)
    
    new_member_df = found_members_df.rename(columns={'id': 'member_id'})

    return new_member_df


def get_members_from_sqlite() -> DataFrame:
    import sqlite3
    import pandas as pd

    conn = sqlite3.connect(SQLITE_MEMBERS_FILEPATH)

    rawsql = f"""
        SELECT
            id AS member_id,
            affiliate_id,
            login_name,
            currency
        FROM member
    """

    members_df = pd.read_sql_query(rawsql, conn)

    return members_df


@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def create_scheduled_wager_table(product: str, payout_frequency: str, **kwargs):
    import sqlite3

    datestamp = kwargs['ds_nodash']
    table_name = f"{product}_{payout_frequency}_{datestamp}"
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
                         affiliate_id integer,
                         win_loss real, 
                         total_members integer,
                         currency text
                     )
                     """)
        conn.commit()

    conn.close()


@task
def create_daily_wager_table(product: str, **kwargs):
    import sqlite3

    datestamp = (datetime.strptime(kwargs['ds'], "%Y-%m-%d") - timedelta(days=1)).strftime("%Y%m%d")

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
                         affiliate_id integer,
                         win_loss real, 
                         login_name text,
                         currency text
                     )
                     """)
        conn.commit()

    conn.close()


@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def create_scheduled_transaction_table(transaction_type, payout_frequency, **kwargs):
    import sqlite3

    datestamp = kwargs['ds_nodash']

    table_name = f"{transaction_type}_{payout_frequency}_{datestamp}"

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
                         currency text
                     )
                     """)
        conn.commit()

    conn.close()

  
@task
def create_daily_transaction_table(transaction_type, **kwargs):
    import sqlite3

    datestamp = (datetime.strptime(kwargs['ds'], "%Y-%m-%d") - timedelta(days=1)).strftime("%Y%m%d")
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

  
def save_transaction_to_sqlite(transaction_type, transaction_df: DataFrame, datestamp):
    import sqlite3

    table_name = f"{transaction_type}_{datestamp}"

    filepath = f"{SQLITE_TRANSACTIONS_PATH}/{transaction_type}/{table_name}.db"

    print(f"Saving {transaction_df.shape[0]} rows to file: {filepath}")
    print(transaction_df.columns)
    conn = sqlite3.connect(filepath)

    transaction_df.to_sql(table_name, conn, if_exists='replace', index=False)
    print("Saved Successfully")


def get_transaction_df(transaction_type: str, datestamp):
    import sqlite3
    import pandas as pd

    table_name = f"{transaction_type}_{datestamp}"

    filepath = f"{SQLITE_TRANSACTIONS_PATH}/{transaction_type}/{table_name}.db"

    print(f"Connecting to {filepath}")
    conn = sqlite3.connect(filepath)

    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)

    return df, filepath


def drop_prev_transaction_table(transaction_type: str, **kwargs):
    import os
    datestamp = kwargs['ds_nodash']

    table_name = f"{transaction_type}_{datestamp}"
    filepath = f"{SQLITE_TRANSACTIONS_PATH}/{transaction_type}/{table_name}.db"

    print("Deleting File", filepath)
    os.remove(filepath)


def get_withdrawal_data(date_from, date_to):
    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')

    raw_sql = f"""
        SELECT 
            ma.affiliate_id,
            w.withdrawal_amount as amount,
            w.currency as currency,
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

    df[['affiliate_fee']] = df[['affiliate_fee']].fillna(0)
    df['affiliate_fee'] = df['affiliate_fee'].astype(float)
    df = df[df['affiliate_fee'] > 0]

    return df


def get_deposit_data(date_from, date_to):
    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')

    raw_sql = f"""
        SELECT 
            ma.affiliate_id,
            d.net_amount as amount,
            d.currency as currency,
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

    df[['affiliate_fee']] = df[['affiliate_fee']].fillna(0)
    df['affiliate_fee'] = df['affiliate_fee'].astype(float)
    df = df[df['affiliate_fee'] > 0]

    return df


def get_adjustment_data(date_from, date_to):
    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')

    raw_sql = f"""
        SELECT
            ma.affiliate_id as affiliate_id,
            (CASE
                 WHEN a.transaction_type = {ADJUSTMENT_TRANSACTION_TYPE_CREDIT} 
                 THEN a.amount 
                 ELSE -a.amount 
            END) as amount,
            a.currency as currency
        FROM adjustment as a
        LEFT JOIN member_account as ma on a.member_id = ma.member_id
        WHERE a.created_at > '{date_from}'
        AND a.created_at <= '{date_to}'
        AND ma.affiliate_id > 0
        AND a.status = {ADJUSTMENT_STATUS_SUCCESSFUL}
    """

    df = conn_payment_pg_hook.get_pandas_df(raw_sql)

    df['affiliate_fee'] = 100

    return df


@task
def update_transaction_table(get_transaction_func, transaction_type, **kwargs):
    import pandas as pd

    exec_date = datetime.strptime(kwargs['ds'], "%Y-%m-%d")

    target_date = exec_date.replace(
            hour=UTC_EXEC_TIME,
            minute=0,
            second=0,
            microsecond=0
            )

    date_from = target_date - timedelta(days=1)
    date_to = target_date - timedelta(microseconds=1)

    transaction_df = get_transaction_func(date_from, date_to)
    print(transaction_df.shape[0], transaction_type, " Found")

    if transaction_df.shape[0] == 0:
        print(f"No {transaction_type}s found")
        raise AirflowSkipException

    transaction_df = transaction_df.groupby(['affiliate_id', 'currency', 'affiliate_fee']).sum().reset_index()

    transaction_df['affiliate_fee'] = pd.to_numeric(transaction_df['affiliate_fee'], errors='coerce')

    transaction_df['amount'] = transaction_df['amount'] * transaction_df['affiliate_fee'] * 0.01

    datestamp = date_from.strftime("%Y%m%d")
    save_transaction_to_sqlite(transaction_type, transaction_df, datestamp)


def get_allbet_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT
            win_or_loss_amount AS win_loss,
            login_name
        FROM {ALL_BET_WAGER_TABLE}
        WHERE bet_time < '{date_to}'
        AND bet_time > '{date_from}'
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    df = get_member_currency(df)

    return df 


@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def aggregate_scheduled_transactions(transaction_type: str, payout_frequency: str, **kwargs):
    import sqlite3
    import pandas as pd

    aggregated_transactions = pd.DataFrame()

    datestamps = get_datestamps(payout_frequency, **kwargs)

    for datestamp in datestamps:
        table_name = f"{transaction_type}_{datestamp}"
        filepath = f"{SQLITE_TRANSACTIONS_PATH}/{transaction_type}/{table_name}.db"

        conn = sqlite3.connect(filepath)
        curs = conn.cursor()

        get_table_list = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"

        res = curs.execute(get_table_list)
        tables = res.fetchall()

        if len(tables) != 0:
            daily_transactions = pd.read_sql(f"SELECT * FROM {table_name}", conn)
            print(f"{daily_transactions.shape[0]} Affiliates Updated on {datestamp}")

            aggregated_transactions = pd.concat([aggregated_transactions, daily_transactions])

    if aggregated_transactions.shape[0] == 0:
        print("No Transaction Data Found")
        return

    aggregated_transactions = aggregated_transactions.drop(columns=['affiliate_fee'])
    aggregated_transactions = aggregated_transactions.groupby(['affiliate_id', 'currency']).sum().reset_index()
    print(f"Total Affiliate Data for this {payout_frequency} period: ", aggregated_transactions.shape[0])

    datestamp = payout_frequency + "_" + kwargs['ds_nodash']

    save_transaction_to_sqlite(transaction_type, aggregated_transactions, datestamp)


def get_asiagaming_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT
            net_amount AS win_loss,
            player_name AS login_name
        FROM {ASIAGAMING_WAGER_TABLE}
        WHERE bet_time < '{date_to}'
        AND bet_time > '{date_from}'
        AND flag = 1 and game_type_code = 'AGIN' and platform_type <> 'YOPLAY'
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    df = get_member_currency(df)

    return df 


def get_agslot_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT
            net_amount AS win_loss,
            player_name AS login_name
        FROM {ASIAGAMING_WAGER_TABLE}
        WHERE bet_time <'{date_to}'
        AND bet_time > '{date_from}'
        AND flag = 1 and game_type_code = 'SLOT' and platform_type in ('AGIN' ,'XIN')
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    df = get_member_currency(df)

    return df


def get_agyoplay_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT
            player_name AS login_name,
            net_amount AS win_loss
        FROM {ASIAGAMING_WAGER_TABLE}
        WHERE bet_time <'{date_to}'
        AND bet_time > '{date_from}'
        AND flag = 1 and game_type_code = 'AGIN' and platform_type = 'YOPLAY'
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    df = get_member_currency(df)

    return df


def get_sagaming_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT
            username AS login_name,
            CASE WHEN rolling != 0 THEN result_amount ELSE 0 END AS win_loss
        FROM {SAGAMING_WAGER_TABLE}
        WHERE bet_time <'{date_to}'
        AND bet_time > '{date_from}'
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    df = get_member_currency(df)

    return df


def get_simpleplay_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT
            username AS login_name,
            result_amount AS win_loss
        FROM {SIMPLEPLAY_WAGER_TABLE}
        WHERE bet_time <'{date_to}'
        AND bet_time > '{date_from}'
        AND game_type = 'slot'
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    df = get_member_currency(df)

    return df


def get_simpleplayfisher_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT
            username AS login_name,
            result_amount AS win_loss
        FROM {SIMPLEPLAY_WAGER_TABLE}
        WHERE bet_time <'{date_to}'
        AND bet_time > '{date_from}'
        AND game_type != 'slot'
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    df = get_member_currency(df)

    return df


def get_pgsoft_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT
            player_name AS login_name,
            CASE WHEN win_amount >= 0 THEN win_amount - bet_amount ELSE 0 END AS win_loss
        FROM {PGSOFT_WAGER_TABLE}
        WHERE bet_time <'{date_to}'
        AND bet_time > '{date_from}'
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    df = get_member_currency(df)

    return df 


def get_ebet_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT
            user_name AS login_name,
            CASE WHEN valid_bet != 0 THEN payout - bet ELSE 0 END AS win_loss
        FROM {EBET_WAGER_TABLE}
        WHERE create_time <'{date_to}'
        AND create_time > '{date_from}'
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    df = get_member_currency(df)

    return df


def get_bti_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT
            purchase_id AS bet_id,
            valid_stake AS eligible_stake_amount,
            username AS login_name,
            CASE WHEN bet_status = 'Cashout' THEN (total_stake - return) ELSE pl END as win_loss,
            CASE WHEN bet_status = 'Cashout' THEN (total_stake - return) ELSE valid_stake END as stake,
    	    odds_in_user_style,
            odds_style_of_user
        FROM {BTI_WAGER_TABLE}
        WHERE creation_date <'{date_to}'
        AND creation_date > '{date_from}'
        AND bet_status NOT IN ('Canceled', 'Open')
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    df['odds_in_user_style'] = df['odds_in_user_style'].astype(float)

    conditions = (
            ((df.odds_style_of_user == 'European') & (df.odds_in_user_style >= 1.65)) |
            ((df.odds_style_of_user == 'Hongkong') & (df.odds_in_user_style >= 0.65)) |
            ((df.odds_style_of_user == 'Malay') & (((df.odds_in_user_style >= -0.99) & (df.odds_in_user_style <= -0.1)) | 
                                                   ((df.odds_in_user_style >= 0.65) & (df.odds_in_user_style <= 1)))) |
            ((df.odds_style_of_user == 'Indo') & (((df.odds_in_user_style >= -1.54) & (df.odds_in_user_style <= -0.1)) | 
                                                  ((df.odds_in_user_style >= 1) & (df.odds_in_user_style <= 9)))))

    df['eligible_stake_amount'] = df['stake'].loc[conditions]

    df['win_loss'] = df.apply(lambda x: x['win_loss'] if x['eligible_stake_amount'] > 0 else 0, axis=1)

    df = df.loc[:, ['login_name', 'win_loss']]

    df = get_member_currency(df)

    return df 


def get_sabacv_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT
            trans_id AS bet_id,
            stake AS eligible_stake_amount, 
            vendor_member_id AS login_name,
            ticket_status,
            odds,
            odds_type,
            winlost_amount
        FROM {SABACV_WAGER_TABLE}
        WHERE transaction_time <'{date_to}'
        AND transaction_time > '{date_from}'
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    conditions = (
            ((df.odds_type == 1) & (((df.odds >= -0.99) & (df.odds <= -0.1)) | ((df.odds >= 0.65) & (df.odds <= 1)))) |
            ((df.odds_type == 2) & (df.odds >= 0.65)) |
            ((df.odds_type == 3) & (df.odds >= 1.65)) |
            ((df.odds_type == 4) & (((df.odds >= -1.54) & (df.odds <= -0.1)) | ((df.odds >= 1) & (df.odds <= 9)))) |
            ((df.odds_type == 5) & (((df.odds >= -154) & (df.odds <= -10)) | ((df.odds >= 100) & (df.odds <= 900))))
            ) & ((df.ticket_status != 'waiting') & (df.ticket_status != 'running') & (df.winlost_amount != 0))

    df['eligible_stake_amount'] = df['eligible_stake_amount'].loc[conditions]

    df['win_loss'] = df.apply(lambda x: x['winlost_amount'] if x['eligible_stake_amount'] > 0 else 0, axis=1)

    df = df.loc[:, ['win_loss', 'login_name']]

    df = get_member_currency(df)

    return df 


def get_saba_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT
            wg.trans_id AS bet_id,
            wg.stake AS eligible_stake_amount, 
            wg.vendor_member_id AS login_name,
            wg.odds,
            wg.odds_type,
            wg.ticket_status,
            wg.winlost_amount + COALESCE(cash.buyback_amount, 0) + COALESCE(cash.winlost_amount, 0) AS winlost_amount
        FROM {SABA_WAGER_TABLE} AS wg
        LEFT JOIN saba_cashout cash ON cash.trans_id = wg.trans_id
        WHERE wg.transaction_time <'{date_to}'
        AND wg.transaction_time > '{date_from}'
        GROUP BY
            wg.transaction_time,
            wg.stake,
            wg.ticket_status,
            wg.odds,
            wg.odds_type,
            wg.vendor_member_id,
            wg.winlost_amount,
            cash.buyback_amount,
            cash.winlost_amount,
            wg.sport_type,
            wg.trans_id
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    conditions = (
        ((df.odds_type == 1) & (((df.odds >= -0.99) & (df.odds <= -0.1)) | ((df.odds >= 0.65) & (df.odds <= 1)))) |
        ((df.odds_type == 2) & (df.odds >= 0.65)) |
        ((df.odds_type == 3) & (df.odds >= 1.65)) |
        ((df.odds_type == 4) & (((df.odds >= -1.54) & (df.odds <= -0.1)) | ((df.odds >= 1) & (df.odds <= 9)))) |
        ((df.odds_type == 5) & (((df.odds >= -154) & (df.odds <= -10)) | ((df.odds >= 100) & (df.odds <= 900))))
        ) & ((df.ticket_status != 'waiting') & (df.ticket_status != 'running') & (df.winlost_amount != 0))

    df['eligible_stake_amount'] = df['eligible_stake_amount'].loc[conditions]

    df = df.rename(columns={'winlost_amount': 'win_loss'})

    df = df.loc[:, ['win_loss', 'login_name']]

    df = get_member_currency(df)

    return df 


def get_saba_number(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT
            vendor_member_id AS login_name,
            CASE WHEN winlost_amount <> 0 AND ticket_status <> 'waiting' AND ticket_status <> 'running' THEN winlost_amount ELSE 0 END AS winlost_amount
        FROM {SABA_NUMBER_TABLE}
        WHERE transaction_time <'{date_to}'
        AND transaction_time > '{date_from}'
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    df = df.rename(columns={'winlost_amount': 'win_loss'})

    df = get_member_currency(df)

    return df 


def get_saba_virtual(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT
            trans_id AS bet_id,
            stake AS eligible_stake_amount, 
            vendor_member_id AS login_name,
            odds,
            odds_type,
            winlost_amount,
            ticket_status
        FROM {SABA_VIRTUAL_TABLE}
        WHERE transaction_time <'{date_to}'
        AND transaction_time > '{date_from}'
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    conditions = (
        ((df.odds_type == 1) & (((df.odds >= -0.99) & (df.odds <= -0.1)) | ((df.odds >= 0.65) & (df.odds <= 1)))) |
        ((df.odds_type == 2) & (df.odds >= 0.65)) |
        ((df.odds_type == 3) & (df.odds >= 1.65)) |
        ((df.odds_type == 4) & (((df.odds >= -1.54) & (df.odds <= -0.1)) | ((df.odds >= 1) & (df.odds <= 9)))) |
        ((df.odds_type == 5) & (((df.odds >= -154) & (df.odds <= -10)) | ((df.odds >= 100) & (df.odds <= 900))))
        ) & ((df.ticket_status != 'waiting') & (df.ticket_status != 'running') & (df.winlost_amount != 0))

    df['eligible_stake_amount'] = df['eligible_stake_amount'].loc[conditions]

    df = df.rename(columns={'winlost_amount': 'win_loss'})

    df = df.loc[:, ['win_loss', 'login_name']]

    df = get_member_currency(df)

    return df 


def get_tfgaming_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT
            amount AS eligible_stake_amount, 
            member_code AS login_name,
            member_odds, 
            member_odds_style,
            earnings,
            settlement_status
        FROM {TFGAMING_TABLE}
        WHERE date_created <'{date_to}'
        AND date_created > '{date_from}'
        AND result_status != 'CANCELLED'
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    df['member_odds'] = df['member_odds'].astype(float)

    conditions = ((
        ((df.member_odds_style == 'euro') & (df.member_odds >= 1.65)) |
        ((df.member_odds_style == 'hongkong') & (df.member_odds >= 0.65)) |
        ((df.member_odds_style == 'malay') & (
            ((df.member_odds >= -0.99) & (df.member_odds <= -0.1)) |
            ((df.member_odds >= 0.65) & (df.member_odds <= 1)))) |
        ((df.member_odds_style == 'indo') & (
            ((df.member_odds >= -1.54) & (df.member_odds <= -0.1)) |
            ((df.member_odds >= 1) & (df.member_odds <= 9))))
        )) & (df.settlement_status == "settled")

    df['eligible_stake_amount'] = df['eligible_stake_amount'].loc[conditions]

    df['win_loss'] = df.apply(lambda x: x['earnings'] if x['eligible_stake_amount'] > 0 else 0, axis=1)
    
    df = df.loc[:, ['win_loss', 'login_name']]

    df = get_member_currency(df)

    return df 


def get_evolution_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT
            player_id AS login_name,
            CASE WHEN stake <> payout AND status = 'Resolved' THEN payout - stake ELSE 0 END AS win_loss
        FROM {EVOLUTION_TABLE}
        WHERE placed_on <'{date_to}'
        AND placed_on > '{date_from}'
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    df = get_member_currency(df)

    return df 


def get_genesis_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT
            user_name AS login_name,
            CASE WHEN valid_bet != 0 THEN payout - bet ELSE 0 END AS win_loss
        FROM {GENESIS_TABLE}
        WHERE create_time <'{date_to}'
        AND create_time > '{date_from}'
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    df = get_member_currency(df)

    return df 


def get_weworld_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT
            player_id AS login_name,
            winloss_amount as win_loss
        FROM {WEWORLD_TABLE}
        WHERE bet_datetime <'{date_to}'
        AND bet_datetime > '{date_from}'
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    df = get_member_currency(df)

    return df


def get_digitain_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT
            u.login_name as login_name,
            o.order_number AS bet_id,
            b.bet_factor AS odds,
            o.partner_client_id AS client_id,
            CASE WHEN b.is_cash_out IS true THEN 0 ELSE o.amount END AS amount,
            CASE WHEN b.is_cash_out IS true THEN 0 ELSE o.win_amount - o.amount END AS win_amount,
            CASE WHEN b.argument2 > 1 THEN 1 ELSE 0 END AS is_parlay
        FROM digitain_order_wager AS o
        INNER JOIN digitain_order_bet_wager AS b ON o.order_number = b.order_number
        INNER JOIN digitain_order_bet_stake_wager AS bs ON o.order_number = bs.order_number
        LEFT JOIN digitain_user AS u ON u.id = o.partner_client_id
        WHERE o.fill_date < '{date_to}'
        AND o.fill_date > '{date_from}'
        AND o.payout_fill_date IS NOT NULL
        AND bs.stake_status NOT IN (4,7,8)
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    EuroValue      = 1.65
    HongkongValue  = 0.65
    MalayValue     = 0.65
    IndoValue      = -1.54

    conditions = (
                    (df.odds >= EuroValue) |
                    (df.odds >= HongkongValue) |
                    (((df.odds >= -0.99) & (df.odds <= -0.1)) | ((df.odds >= MalayValue) & (df.odds <= 1))) |
                    (((df.odds >= IndoValue) & (df.odds <= -0.1)) | ((df.odds >= 1) & (df.odds <= 9))) |
                    (df.is_parlay == 1)
                )

    df['eligible_stake_amount'] = df['amount'].loc[conditions]

    df['win_loss'] = df.apply(lambda x: x['win_amount'] if x['eligible_stake_amount'] > 0 else 0, axis=1)

    df = df.loc[:, ['win_loss', 'login_name']]

    df = get_member_currency(df)

    return df


def get_datestamps(payout_frequency, **kwargs):
    datestamps = []
    exec_date = datetime.strptime(kwargs['ds'], '%Y-%m-%d')

    _, _, from_transaction_date, to_transaction_date = get_transaction_dates(payout_frequency, exec_date)

    from_date = datetime.strptime(from_transaction_date, "%Y-%m-%d")
    to_date = datetime.strptime(to_transaction_date, "%Y-%m-%d")

    date_iter = from_date
    while date_iter <= to_date:
        datestamp = date_iter.strftime("%Y%m%d")
        datestamps.append(datestamp)
        date_iter += timedelta(days=1)

    return datestamps


def save_wager_to_sqlite(product, wager_df, datestamp):
    import sqlite3

    table_name = f"{product}_{datestamp}"

    filepath = f"{SQLITE_WAGERS_PATH}/{product}/{table_name}.db"

    print(f"Saving {wager_df.shape[0]} rows to file: {filepath}")
    print(wager_df.columns)
    print(wager_df[:10])
    conn = sqlite3.connect(filepath)

    wager_df.to_sql(table_name, conn, if_exists='replace', index=False)
    print("Saved Successfully")


@task
def update_wager_table(product, fetch_wager_func, **kwargs):

    exec_date = datetime.strptime(kwargs['ds'], "%Y-%m-%d")
    datestamp = (datetime.strptime(kwargs['ds'], "%Y-%m-%d") - timedelta(days=1)).strftime("%Y%m%d")

    target_date = exec_date.replace(
            hour=UTC_EXEC_TIME,
            minute=0,
            second=0,
            microsecond=0
            )

    date_from = target_date - timedelta(days=1)
    date_to = target_date - timedelta(microseconds=1)

    print("Processing date range")
    print("date_from", date_from)
    print("date_to", date_to)

    wager_df: DataFrame = fetch_wager_func(date_from, date_to)

    print(f"Updating sqlite table {product}_{datestamp} with {wager_df.shape[0]} data")
    
    if wager_df.shape[0] == 0:
        print("No new data found")
        raise AirflowSkipException

    # Group by member
    wager_df = wager_df.groupby(['affiliate_id', 'currency', 'login_name']).sum().reset_index()

    save_wager_to_sqlite(product, wager_df, datestamp)


@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def aggregate_scheduled_wagers(product, payout_frequency, **kwargs):
    import sqlite3
    import pandas as pd

    wager_df = pd.DataFrame()

    datestamps = get_datestamps(payout_frequency, **kwargs)

    for datestamp in datestamps:
        table_name = f"{product}_{datestamp}"
        filepath = f"{SQLITE_WAGERS_PATH}/{product}/{table_name}.db"

        conn = sqlite3.connect(filepath)
        curs = conn.cursor()

        get_table_list = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"

        res = curs.execute(get_table_list)
        tables = res.fetchall()

        if len( tables ) != 0:
            product_df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
            print(f"{product_df.shape[0]} Affiliates Updated on {datestamp}")
            print(product_df.columns)

            wager_df = pd.concat([wager_df, product_df])

    if wager_df.shape[0] == 0:
        print("No Wager Data Found")
        return

    # Group By login_name
    print(wager_df.columns)
    wager_df = wager_df.groupby(['affiliate_id', 'currency', 'login_name']).sum().reset_index()

    wager_df['total_members'] = 1
    wager_df = wager_df.drop(['login_name'], axis=1)
    wager_df = wager_df.groupby(['affiliate_id', 'currency']).sum().reset_index()

    datestamp = payout_frequency + "_" + kwargs['ds_nodash']

    save_wager_to_sqlite(product, wager_df, datestamp)


@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def create_scheduled_adjustment_table(payout_frequency: str, **kwargs):
    import sqlite3

    datestamp = kwargs['ds_nodash']

    table_name = f"adjustment_{payout_frequency}_{datestamp}"

    filepath = f"{SQLITE_ADJUSTMENTS_PATH}/{table_name}.db"

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
                         currency text
                     )
                     """)
        conn.commit()

    conn.close()


@task
def create_daily_adjustment_table(**kwargs):
    import sqlite3

    datestamp = (datetime.strptime(kwargs['ds'], "%Y-%m-%d") - timedelta(days=1)).strftime("%Y%m%d")

    table_name = f"adjustment_{datestamp}"

    filepath = f"{SQLITE_ADJUSTMENTS_PATH}/{table_name}.db"

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
                         currency text
                     )
                     """)
        conn.commit()

    conn.close()


def save_adjustment_to_sqlite(adjustment_df, datestamp):
    import sqlite3

    table_name = f"adjustment_{datestamp}"

    filepath = f"{SQLITE_ADJUSTMENTS_PATH}/{table_name}.db"

    print(f"Saving {adjustment_df.shape[0]} rows to file: {filepath}")
    print(adjustment_df.columns)
    conn = sqlite3.connect(filepath)

    adjustment_df.to_sql(table_name, conn, if_exists='replace', index=False)
    print("Saved Successfully")


def get_affiliate_adjustment_data(date_from, date_to):
    conn_payment_pg_hook = PostgresHook(postgres_conn_id='affiliate_conn_id')

    raw_sql = f"""
        SELECT
            (CASE WHEN transaction_type = {ADJUSTMENT_TRANSACTION_TYPE_CREDIT} THEN amount ELSE -amount END) as amount,
            affiliate_id,
            currency
        FROM adjustment
        WHERE created_at >= '{date_from}'
        AND created_at < '{date_to}'
        AND status = {ADJUSTMENT_STATUS_SUCCESSFUL}
        AND type = {ADJUSTMENT_TYPE_COMMISSION}
        """

    df = conn_payment_pg_hook.get_pandas_df(raw_sql)

    return df


@task
def update_adjustment_table(**kwargs):
    exec_date = datetime.strptime(kwargs['ds'], "%Y-%m-%d")

    target_date = exec_date.replace(
            hour=UTC_EXEC_TIME,
            minute=0,
            second=0,
            microsecond=0
            )

    date_from = target_date - timedelta(days=1)
    date_to = target_date - timedelta(microseconds=1)

    adjustment_df = get_affiliate_adjustment_data(date_from, date_to)

    if adjustment_df.shape[0] == 0:
        print(f"No adjustments found")
        raise AirflowSkipException

    adjustment_df = adjustment_df.groupby(['affiliate_id', 'currency']).sum().reset_index()

    datestamp = date_from.strftime("%Y%m%d")
    save_adjustment_to_sqlite(adjustment_df, datestamp)


@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def aggregate_scheduled_adjustments(payout_frequency: str, **kwargs):
    import sqlite3
    import pandas as pd

    aggregated_adjustments = pd.DataFrame()

    datestamps = get_datestamps(payout_frequency, **kwargs)

    for datestamp in datestamps:
        table_name = f"adjustment_{datestamp}"
        filepath = f"{SQLITE_ADJUSTMENTS_PATH}/{table_name}.db"

        conn = sqlite3.connect(filepath)
        curs = conn.cursor()

        get_table_list = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"

        res = curs.execute(get_table_list)
        tables = res.fetchall()

        if len( tables ) != 0:
            adjustment_df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
            print(f"{adjustment_df.shape[0]} Affiliates Updated on {datestamp}")

            if not adjustment_df.empty:
                aggregated_adjustments = pd.concat([aggregated_adjustments, adjustment_df])

    if aggregated_adjustments.shape[0] == 0:
        print("No Adjustment Data Found")
        return
    
    print(aggregated_adjustments.shape[0])
    print(aggregated_adjustments[:10])
    aggregated_adjustments = aggregated_adjustments.groupby(['affiliate_id', 'currency']).sum().reset_index()
    print(aggregated_adjustments.shape[0])
    print(aggregated_adjustments[:10])

    datestamp = payout_frequency + "_" + kwargs['ds_nodash']

    save_adjustment_to_sqlite(aggregated_adjustments, datestamp)


def get_aggregated_transactions(datestamp):
    import sqlite3
    import pandas as pd

    transaction_df = pd.DataFrame()

    for transaction_type in TRANSACTION_TYPES:
        table_name = f"{transaction_type}_{datestamp}"
        filepath = f"{SQLITE_TRANSACTIONS_PATH}/{transaction_type}/{table_name}.db"

        conn = sqlite3.connect(filepath)
        
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)

        if transaction_type == TRANSACTION_TYPE_ADJUSTMENT:
            df = df.rename(columns={'amount': 'other_fee'})
        else:
            df = df.rename(columns={'amount': 'expenses'})

        transaction_df = pd.concat([transaction_df, df])

    transaction_df['affiliate_id'] = transaction_df['affiliate_id'].astype(int)

    # List of columns to exclude from conversion to float
    exclude_columns = ['affiliate_id', 'currency']

    # Convert all columns (except the excluded ones) to float
    transaction_df.loc[:, transaction_df.columns.difference(exclude_columns)] = transaction_df.loc[:, transaction_df.columns.difference(exclude_columns)].astype(float)

    return transaction_df


def get_aggregated_wagers(datestamp):
    import sqlite3
    import pandas as pd

    wager_df = pd.DataFrame()

    for product in PRODUCT_CODES:
        table_name = f"{product}_{datestamp}"
        filepath = f"{SQLITE_WAGERS_PATH}/{product}/{table_name}.db"

        conn = sqlite3.connect(filepath)
        
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)

        if df.shape[0] == 0:
            continue

        df = df.rename(columns={'win_loss': 'company_win_loss'})
        print(product, df.columns)
        print(product, df.head())
        wager_df = pd.concat([wager_df, df])

    wager_df['affiliate_id'] = wager_df['affiliate_id'].astype(int)

    # List of columns to exclude from conversion to float
    exclude_columns = ['affiliate_id', 'currency']

    # Convert all columns (except the excluded ones) to float
    wager_df.loc[:, wager_df.columns.difference(exclude_columns)] = wager_df.loc[:, wager_df.columns.difference(exclude_columns)].astype(float)

    wager_df['company_win_loss'] = -wager_df['company_win_loss'] # Negate Win Loss
    return wager_df


def get_aggregated_adjustments(datestamp):
    import sqlite3
    import pandas as pd

    table_name = f"adjustment_{datestamp}"
    filepath = f"{SQLITE_ADJUSTMENTS_PATH}/{table_name}.db"

    conn = sqlite3.connect(filepath)
    
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)

    df = df.rename(columns={'amount': 'total_adjustment'})

    df['affiliate_id'] = df['affiliate_id'].astype(int)
    return df


@task(trigger_rule=TriggerRule.ALL_DONE)
def check_schedule(payout_frequency:str, **kwargs):
    exec_date = datetime.strptime(kwargs['ds'], "%Y-%m-%d")
    print("Checking Schedule: ", kwargs['ds'])

    if payout_frequency == PAYOUT_FREQUENCY_WEEKLY:
        if exec_date.weekday() != WEEKLY_DAY:
            print("Skipping Today")
            raise AirflowSkipException

    if payout_frequency == PAYOUT_FREQUENCY_MONTHLY:
        if exec_date.day != MONTHLY_DAY:
            print("Skipping Today")
            raise AirflowSkipException

    if payout_frequency == PAYOUT_FREQUENCY_BI_MONTHLY:
        if exec_date.day != 1 and exec_date.day != BI_MONTHLY_DAY:
            print("Skipping Today")
            raise AirflowSkipException


@task
def update_affiliate_table():
    import sqlite3
    from airflow.models import Variable

    DEFAULT_COMMISSION_TIER1 = Variable.get("DEFAULT_COMMISSION_TIER1", 28)
    DEFAULT_COMMISSION_TIER2 = Variable.get("DEFAULT_COMMISSION_TIER2", 38)
    DEFAULT_COMMISSION_TIER3 = Variable.get("DEFAULT_COMMISSION_TIER3", 48)
    DEFAULT_MIN_ACTIVE_PLAYER = Variable.get("DEFAULT_MIN_ACTIVE_PLAYER", 5)

    conn_affiliate_pg_hook = PostgresHook(postgres_conn_id='affiliate_conn_id')

    raw_sql = f"""
        SELECT 
            affiliate_id,
            login_name,
            commission_tier1,
            commission_tier2,
            commission_tier3,
            payout_frequency,
            min_active_player
        FROM affiliate_account
    """

    affiliate_df: DataFrame = conn_affiliate_pg_hook.get_pandas_df(raw_sql)

    table_name = "affiliate"
    conn = sqlite3.connect(SQLITE_AFFILIATE_ACCOUNT_FILEPATH)

    print("Inserting ", affiliate_df.shape[0], " Affiliates to Sqlite affiliate table")
    print(affiliate_df.columns)

    affiliate_df[['commission_tier1']] = affiliate_df[['commission_tier1']].fillna(value=DEFAULT_COMMISSION_TIER1)
    affiliate_df[['commission_tier2']] = affiliate_df[['commission_tier2']].fillna(value=DEFAULT_COMMISSION_TIER2)
    affiliate_df[['commission_tier3']] = affiliate_df[['commission_tier3']].fillna(value=DEFAULT_COMMISSION_TIER3)
    affiliate_df[['min_active_player']] = affiliate_df[['min_active_player']].fillna(value=DEFAULT_MIN_ACTIVE_PLAYER)

    affiliate_df.to_sql(table_name, conn, if_exists='replace', index=False)


def get_affiliate_df(payout_frequency):
    import sqlite3
    import pandas as pd

    conn = sqlite3.connect(SQLITE_AFFILIATE_ACCOUNT_FILEPATH)
    df = pd.read_sql(f"SELECT * FROM affiliate WHERE payout_frequency = '{payout_frequency}'", conn)

    df['affiliate_id'] = df['affiliate_id'].astype(int)
    return df


def get_usd_rate(row: Series):
    from airflow.models import Variable

    DEFAULT_COMMISSION_VND_USD_RATE = 0.041
    DEFAULT_COMMISSION_THB_USD_RATE = 0.028
    DEFAULT_COMMISSION_RMB_USD_RATE = 0.14

    if row['currency'] == CURRENCY_VND:
        return Variable.get("COMMISSION_VND_USD_RATE", DEFAULT_COMMISSION_VND_USD_RATE)
    if row['currency'] == CURRENCY_THB:
        return Variable.get("COMMISSION_THB_USD_RATE", DEFAULT_COMMISSION_THB_USD_RATE)
    if row['currency'] == CURRENCY_RMB:
        return Variable.get("COMMISSION_RMB_USD_RATE", DEFAULT_COMMISSION_RMB_USD_RATE)
    if row['currency'] == CURRENCY_USD:
        return 1

    print("Unsupported Currency: ", row['currency'])
    return 0


def convert_to_usd(commission_df: DataFrame):
    commission_df['expenses'] = commission_df['expenses'] * commission_df['usd_rate']
    commission_df['other_fee'] = commission_df['other_fee'] * commission_df['usd_rate']
    commission_df['company_win_loss'] = commission_df['company_win_loss'] * commission_df['usd_rate']
    commission_df['currency'] = CURRENCY_USD

    return commission_df


def calc_total_amount(row: Series):
    from airflow.models import Variable

    COMMISSION_MAX_NET_TIER1 = Variable.get("COMMISSION_MAX_NET_TIER1", 10_000)
    COMMISSION_MAX_NET_TIER2 = Variable.get("COMMISSION_MAX_NET_TIER2", 100_000)
    COMMISSION_MIN_NET = Variable.get("COMMISSION_MIN_NET", 100)

    total_amount = 0

    row['commission_status'] = COMMISSION_STATUS_THRESHOLD
    row['payment_status'] = COMMISSION_PAYMENT_STATUS_THRESHOLD

    if row['net_company_win_loss'] < COMMISSION_MAX_NET_TIER1:
        row['tier'] = 1
        total_amount = row['net_company_win_loss'] * row['commission_tier1']

    elif row['net_company_win_loss'] < COMMISSION_MAX_NET_TIER2:
        row['tier'] = 2
        tier2_win_loss = row['net_company_win_loss'] - COMMISSION_MAX_NET_TIER1 

        total_amount = COMMISSION_MAX_NET_TIER1 * row['commission_tier1']
        total_amount += tier2_win_loss * row['commission_tier2']

    else:
        row['tier'] = 3
        tier2_win_loss = COMMISSION_MAX_NET_TIER2 - COMMISSION_MAX_NET_TIER1 
        tier3_win_loss = row['net_company_win_loss'] - COMMISSION_MAX_NET_TIER1 - COMMISSION_MAX_NET_TIER2 

        total_amount = COMMISSION_MAX_NET_TIER1 * row['commission_tier1']
        total_amount += tier2_win_loss * row['commission_tier2']
        total_amount += tier3_win_loss * row['commission_tier3']

    row['total_amount'] = total_amount
    row['grand_total'] = total_amount

    if row['total_members'] < row['min_active_player']:
        row['rollover_next_month'] = row['previous_settlement']

    else:
        row['grand_total'] = row['grand_total'] + row['previous_settlement']

        if row['grand_total'] < COMMISSION_MIN_NET:
            row['rollover_next_month'] = row['grand_total']

        else:
            row['commission_status'] = COMMISSION_STATUS_PROCESSING
            row['payment_status'] = COMMISSION_PAYMENT_STATUS_PROCESSING
            row['rollover_next_month'] = 0

    return row


def calc_net_company_win_loss(row: Series):
    from airflow.models import Variable

    commission_platform_fee = Variable.get("COMMISSION_PLATFORM_FEE", 2)

    row['platform_fee'] = row.company_win_loss * commission_platform_fee * 0.01

    temp_platform_fee = 0
    if row['platform_fee'] > 0:
        temp_platform_fee = row['platform_fee'] 

    fee_total = row.expenses + row.other_fee + temp_platform_fee

    row['net_company_win_loss'] = row.company_win_loss - fee_total

    return row


def get_previous_settlement_df(from_transaction_date, to_transaction_date)->DataFrame:
    from airflow.models import Variable
    conn_affiliate_pg_hook = PostgresHook(postgres_conn_id='affiliate_conn_id')

    DEFAULT_MIN_ACTIVE_PLAYER = Variable.get("DEFAULT_MIN_ACTIVE_PLAYER", 5)

    raw_sql = f"""
        SELECT
            DISTINCT ON(cm.affiliate_id) cm.affiliate_id,
            cm.rollover_next_month as previous_settlement,
            cm.currency
        FROM {COMMISSION_TABLE} as cm
		LEFT JOIN affiliate_account as aa ON aa.affiliate_id = cm.affiliate_id
        WHERE commission_status = {COMMISSION_STATUS_THRESHOLD}
        AND from_transaction_date = '{from_transaction_date}'
        AND to_transaction_date = '{to_transaction_date}'
        AND (cm.total_members >= aa.min_active_player or (aa.min_active_player IS NULL AND cm.total_members >= {DEFAULT_MIN_ACTIVE_PLAYER}))
    """

    df = conn_affiliate_pg_hook.get_pandas_df(raw_sql)

    df['affiliate_id'] = df['affiliate_id'].astype(int)
    return df


def get_transaction_dates(payout_frequency: str, exec_date: datetime):
    import calendar

    to_date = datetime.now()
    from_date = datetime.now()
    prev_to_date = datetime.now()
    prev_from_date = datetime.now()
    
    if payout_frequency == PAYOUT_FREQUENCY_WEEKLY:
        to_date = exec_date - timedelta(days=1)
        from_date = to_date - timedelta(days=6)

        prev_to_date = from_date - timedelta(days=1)
        prev_from_date = prev_to_date - timedelta(days=6)

    if payout_frequency == PAYOUT_FREQUENCY_MONTHLY:
        to_date = exec_date - timedelta(days=1)

        year = to_date.year
        month = to_date.month
        num_days = calendar.monthrange(year, month)[1]

        from_date = to_date - timedelta(days=num_days-1)

        prev_to_date = from_date - timedelta(days=1)

        prev_year = prev_to_date.year
        prev_month = prev_to_date.month
        num_days = calendar.monthrange(prev_year, prev_month)[1]

        prev_from_date = prev_to_date - timedelta(days=num_days-1)

    if payout_frequency == PAYOUT_FREQUENCY_BI_MONTHLY:
        if exec_date.day == 1:
            to_date = exec_date - timedelta(days=1)
            from_date = to_date.replace(day=BI_MONTHLY_DAY)

            prev_to_date = from_date - timedelta(days=1)
            prev_from_date = prev_to_date.replace(day=1)

        else:
            to_date = exec_date - timedelta(days=1)
            from_date = to_date.replace(day=1)

            prev_to_date = from_date - timedelta(days=1)
            prev_from_date = prev_to_date.replace(day=BI_MONTHLY_DAY)

    return prev_from_date.strftime("%Y-%m-%d"), prev_to_date.strftime("%Y-%m-%d"), from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d")

    
@task
def calculate_affiliate_fees(payout_frequency, **kwargs):
    import pandas as pd

    exec_date = datetime.strptime(kwargs['ds'], "%Y-%m-%d")
    datestamp = payout_frequency + "_" + kwargs['ds_nodash']

    prev_from_transaction_date, prev_to_transaction_date, from_transaction_date, to_transaction_date = get_transaction_dates(payout_frequency, exec_date)
    
    transaction_df: DataFrame = get_aggregated_transactions(datestamp)
    wager_df = get_aggregated_wagers(datestamp)
    adjustment_df = get_aggregated_adjustments(datestamp)

    prev_settlement_df = get_previous_settlement_df(prev_from_transaction_date, prev_to_transaction_date)
    affiliate_df = get_affiliate_df(payout_frequency)

    print(transaction_df.columns, transaction_df.shape[0])
    print(wager_df.columns, wager_df.shape[0])
    print(wager_df[:10])

    commission_df = pd.concat([transaction_df, wager_df]).fillna(0)
    commission_df = commission_df.groupby(['affiliate_id', 'currency']).sum().reset_index()

    print("Merged Transaction and Wager Data")
    print(commission_df.columns, commission_df.shape[0])
    print(commission_df[:10])

    # Convert Currency to USD
    commission_df['usd_rate'] = commission_df.apply(lambda x: get_usd_rate(x), axis=1)
    commission_df = convert_to_usd(commission_df)

    # Everything should be USD from here on

    print("Converted to USD")
    print(commission_df.columns, commission_df.shape[0])
    print(commission_df[:10])

    commission_df = pd.concat([commission_df, adjustment_df, prev_settlement_df]).fillna(0)
    commission_df = commission_df.groupby(['affiliate_id', 'currency']).sum().reset_index()

    commission_df = commission_df.merge(affiliate_df, how='inner', on=['affiliate_id'])

    commission_df = commission_df.fillna(0)

    print("Merged Adjustment, Previous Settlement, and Affiliate Data")
    print(commission_df.columns, commission_df.shape[0])
    print(commission_df[:10])

    commission_df = commission_df.apply(lambda x: calc_net_company_win_loss(x), axis=1)
    commission_df = commission_df.apply(lambda x: calc_total_amount(x), axis=1)

    print("Calculated Net Company Win Loss and Grand Total")
    print(commission_df.columns, commission_df.shape[0])
    print(commission_df[:10])

    commission_df['from_transaction_date'] = from_transaction_date
    commission_df['to_transaction_date'] = to_transaction_date
    commission_df['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    commission_df['updated_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    commission_df = commission_df.drop(columns=['commission_tier1', 'commission_tier2', 'commission_tier3', 'min_active_player'])

    print("Completed Commission Data")
    print(commission_df.columns, commission_df.shape[0])
    print(commission_df[:10])

    conn_affiliate_pg_hook = PostgresHook(postgres_conn_id='affiliate_conn_id')
    engine_affiliate = conn_affiliate_pg_hook.get_sqlalchemy_engine()

    # Removing Previous Data From Previous Run
    delete_sql = f"""DELETE FROM {COMMISSION_TABLE} WHERE from_transaction_date = '{from_transaction_date}' AND to_transaction_date = '{to_transaction_date}'"""
    conn_affiliate_pg_hook.run(delete_sql)

    print("Inserting ", commission_df.shape[0], f" Commission Data to Postgres {COMMISSION_TABLE} Table")

    commission_df.to_sql(COMMISSION_TABLE, engine_affiliate, if_exists='append', index=False)


@task
def delete_old_files(**kwargs):
    from dateutil.relativedelta import relativedelta
    from glob import glob
    import os

    exec_date = datetime.strptime(kwargs['ds'], "%Y-%m-%d")

    if exec_date.day != 1 :
        print("Not yet cleanup day!")
        raise AirflowSkipException

    abs_path = os.path.abspath("./data/monthly_commission")
    target_month = exec_date - relativedelta(months=3)
    month = target_month.month
    year = target_month.year
    
    date_iter = target_month
    while date_iter.month == month and date_iter.year == year:
        datestamp = date_iter.strftime("%Y%m%d")

        filepaths = glob(abs_path + f"/**/*{datestamp}.db", recursive=True)
        print(f"Checking {datestamp}")
        if len(filepaths) > 0:
            print(f"Deleting {len(filepaths)} files")

        for filepath in filepaths:
            print("Removing File: ", filepath)
            os.remove(filepath)

        date_iter += timedelta(days=1)


@dag(
    dag_id='monthly_commission-v1.0.0',
    description='Calculates commission for affiliates every month',
    schedule="0 16 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=3,
    )
def monthly_commission():
    init_sqlite_task = PythonOperator(
            task_id="init_sqlite",
            python_callable=init_sqlite,
            )

    @task_group
    def get_transactions():

        def transaction_task_group(transaction_type, get_transaction_func):
            @task_group(group_id=transaction_type)
            def gather_transactions():

                create_daily_table = create_daily_transaction_table(transaction_type)
                update_daily_table = update_transaction_table(get_transaction_func, transaction_type)

                create_daily_table >> update_daily_table

            return gather_transactions

        transaction_task_group(TRANSACTION_TYPE_DEPOSIT, get_deposit_data)()
        transaction_task_group(TRANSACTION_TYPE_WITHDRAWAL, get_withdrawal_data)()
        transaction_task_group(TRANSACTION_TYPE_ADJUSTMENT, get_adjustment_data)()
    
    @task_group
    def aggregate_transactions_group(payout_frequency):

        def transaction_aggregation(transaction_type):
            @task_group(group_id=transaction_type)
            def aggregate_transactions_task_group():

                create_monthly_table = create_scheduled_transaction_table(transaction_type, payout_frequency)
                aggregate_transactions_task = aggregate_scheduled_transactions(transaction_type, payout_frequency)

                create_monthly_table >> aggregate_transactions_task

            return aggregate_transactions_task_group

        transaction_aggregation(TRANSACTION_TYPE_DEPOSIT)()
        transaction_aggregation(TRANSACTION_TYPE_WITHDRAWAL)()
        transaction_aggregation(TRANSACTION_TYPE_ADJUSTMENT)()

    @task_group
    def get_wagers():

        def wager_task_group(product, get_wager_func):
            @task_group(group_id=product)
            def gather_wagers():

                create_daily_table = create_daily_wager_table(product)
                update_daily_table = update_wager_table(product, get_wager_func)

                create_daily_table >> update_daily_table

            return gather_wagers
        
        wager_task_group(PRODUCT_CODE_ALLBET, get_allbet_wager)()
        wager_task_group(PRODUCT_CODE_ASIAGAMING, get_asiagaming_wager)()
        wager_task_group(PRODUCT_CODE_AGSLOT, get_agslot_wager)()
        wager_task_group(PRODUCT_CODE_AGYOPLAY, get_agyoplay_wager)()
        wager_task_group(PRODUCT_CODE_SAGAMING, get_sagaming_wager)()
        wager_task_group(PRODUCT_CODE_SPSLOT, get_simpleplay_wager)()
        wager_task_group(PRODUCT_CODE_SPFISH, get_simpleplayfisher_wager)()
        wager_task_group(PRODUCT_CODE_SABACV, get_sabacv_wager)()
        wager_task_group(PRODUCT_CODE_PGSOFT, get_pgsoft_wager)()
        wager_task_group(PRODUCT_CODE_EBETGAMING, get_ebet_wager)()
        wager_task_group(PRODUCT_CODE_BTISPORTS, get_bti_wager)()
        wager_task_group(PRODUCT_CODE_TFGAMING, get_tfgaming_wager)()
        wager_task_group(PRODUCT_CODE_EVOLUTION, get_evolution_wager)()
        wager_task_group(PRODUCT_CODE_GENESIS, get_genesis_wager)()
        wager_task_group(PRODUCT_CODE_SABA, get_saba_wager)()
        wager_task_group(PRODUCT_CODE_SABANUMBERGAME, get_saba_number)()
        wager_task_group(PRODUCT_CODE_SABAVIRTUAL, get_saba_virtual)()
        wager_task_group(PRODUCT_CODE_WEWORLD, get_weworld_wager)()
        wager_task_group(PRODUCT_CODE_DIGITAIN, get_digitain_wager)()

    @task_group
    def aggregate_wagers(payout_frequency):

        def wager_aggregation(product):
            @task_group(group_id=product)
            def aggregate_wagers_task_group():

                create_scheduled_table = create_scheduled_wager_table(product, payout_frequency)
                aggregate_wagers_task = aggregate_scheduled_wagers(product, payout_frequency)

                create_scheduled_table >> aggregate_wagers_task

            return aggregate_wagers_task_group

        wager_aggregation(PRODUCT_CODE_ALLBET)()
        wager_aggregation(PRODUCT_CODE_ASIAGAMING)()
        wager_aggregation(PRODUCT_CODE_AGSLOT)()
        wager_aggregation(PRODUCT_CODE_AGYOPLAY)()
        wager_aggregation(PRODUCT_CODE_SAGAMING)()
        wager_aggregation(PRODUCT_CODE_SPSLOT)()
        wager_aggregation(PRODUCT_CODE_SPFISH)()
        wager_aggregation(PRODUCT_CODE_SABACV)()
        wager_aggregation(PRODUCT_CODE_PGSOFT)()
        wager_aggregation(PRODUCT_CODE_EBETGAMING)()
        wager_aggregation(PRODUCT_CODE_BTISPORTS)()
        wager_aggregation(PRODUCT_CODE_TFGAMING)()
        wager_aggregation(PRODUCT_CODE_EVOLUTION)()
        wager_aggregation(PRODUCT_CODE_GENESIS)()
        wager_aggregation(PRODUCT_CODE_SABA)()
        wager_aggregation(PRODUCT_CODE_SABANUMBERGAME)()
        wager_aggregation(PRODUCT_CODE_SABAVIRTUAL)()
        wager_aggregation(PRODUCT_CODE_WEWORLD)()
        wager_aggregation(PRODUCT_CODE_DIGITAIN)()

    @task_group
    def get_adjustments():
        create_daily_table = create_daily_adjustment_table()
        update_daily_table = update_adjustment_table()

        create_daily_table >> update_daily_table

    @task_group
    def aggregate_adjustments(payout_frequency):
        create_monthly_table = create_scheduled_adjustment_table(payout_frequency)
        aggregate_adjustments_task = aggregate_scheduled_adjustments(payout_frequency)

        create_monthly_table >> aggregate_adjustments_task

    @task_group
    def daily_data_gathering():
        get_transactions()
        get_wagers()
        get_adjustments()


    def scheduled_calculation_task_group(payout_frequency):
        @task_group(group_id=f"{payout_frequency}_calculation")
        def scheduled_calculation():

            def timely_aggregation():
                @task_group(group_id=f"{payout_frequency}_aggregation")
                def frequency_based_aggregation():
                    aggregate_transactions_group(payout_frequency)
                    aggregate_wagers(payout_frequency)
                    aggregate_adjustments(payout_frequency)

                return frequency_based_aggregation

            schedule_checker = check_schedule(payout_frequency)
            aggregate_task_group = timely_aggregation()()
            get_affiliates = update_affiliate_table()
            calculation_task = calculate_affiliate_fees(payout_frequency)

            schedule_checker >> aggregate_task_group >> get_affiliates >> calculation_task
            schedule_checker >> get_affiliates
            schedule_checker >> calculation_task

            if payout_frequency == PAYOUT_FREQUENCY_MONTHLY:
                calculation_task >> delete_old_files()

        return scheduled_calculation

    daily_data_gathering_task = daily_data_gathering()

    init_sqlite_task >> daily_data_gathering_task

    daily_data_gathering_task >> [
            scheduled_calculation_task_group(PAYOUT_FREQUENCY_WEEKLY)(),
            scheduled_calculation_task_group(PAYOUT_FREQUENCY_MONTHLY)(),
            scheduled_calculation_task_group(PAYOUT_FREQUENCY_BI_MONTHLY)()
            ]

monthly_commission()
