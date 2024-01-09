# Todo: Implement Commission Tier on Win Loss
# Todo: Implement Minimum Players
# Todo: Implement Payout Frequency
# Todo: Implement Weekly, Bi-Monthly, and Monthly Option based on Affiliate
# Todo: Implement Idempotency

from airflow.decorators import dag, task_group, task
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule
from datetime import  datetime,timedelta
from pandas import DataFrame

# Payout
WEEKLY_DAY = 1
BI_MONTHLY_DAY = 8
MONTHLY_DAY = 1

# Commmission Fee Type
COMMISSION_FEE_TYPE_DP = 1
COMMISSION_FEE_TYPE_WD = 2
COMMISSION_FEE_TYPE_ADJ = 3

# Adjustment Transaction Type
ADJUSTMENT_TRANSACTION_TYPE_CREDIT = 3
ADJUSTMENT_TYPE_COMMISSION = 1

# Transaction Status
ADJUSTMENT_STATUS_SUCCESSFUL = 2
WITHDRAWAL_STATUS_SUCCESSFUL = 6
DEPOSIT_STATUS_SUCCESSFUL = 2

# Table Names
AFFILIATE_TABLE = "adjustment"

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
SQLITE_COMMISSION_PATH = "./data/monthly_commission/commission"
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

    conn = sqlite3.connect(SQLITE_MEMBERS_FILEPATH) # This should be in Cassandra or something
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

    conn = sqlite3.connect(SQLITE_MEMBERS_FILEPATH) # This should be in Cassandra or something
    curs = conn.cursor()

    get_table_list = "SELECT name FROM sqlite_master WHERE type='table' AND name='affiliate'"

    res = curs.execute(get_table_list)
    tables = res.fetchall()

    if len( tables ) == 0:
        create_member_table_sql = """
            CREATE TABLE affiliate(
                    affiliate_id integer,
                    commission_tier1 integer,
                    commission_tier2 integer,
                    commission_tier3 integer,
                    payout_frequency text,
                    min_active_player integer
                    ) 
        """
        curs.execute(create_member_table_sql)
    curs.close()


# Todo: I think this can be refactored better
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

    print(wager_df.columns)
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

    print("Inserting ", new_member_df.shape[0], " Data into Sqlite")

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


@task(trigger_rule=TriggerRule.ALL_DONE)
def create_bimonthly_wager_table(product: str, **kwargs):
    exec_date = datetime.strptime(kwargs['ds'], "%Y-%m-%d")
    if exec_date.day != 1 and exec_date.day != BI_MONTHLY_DAY:
        print("Will only run on 1st and 15th day of the month")
        raise AirflowSkipException

    import sqlite3

    datestamp = kwargs['ds_nodash']
    table_name = f"{product}_{datestamp}_{exec_date.day}"
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
                         win_loss real, 
                         affiliate_id integer,
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
                         win_loss real, 
                         affiliate_id integer,
                         currency text
                     )
                     """)
        conn.commit()

    conn.close()


@task(trigger_rule=TriggerRule.ALL_DONE)
def create_bimonthly_transaction_table(transaction_type, **kwargs):
    exec_date = datetime.strptime(kwargs['ds'], "%Y-%m-%d")
    if exec_date.day != 1 and exec_date.day != BI_MONTHLY_DAY:
        print(f"Will only run on 1st and {BI_MONTHLY_DAY}th day of the month")
        raise AirflowSkipException

    import sqlite3

    datestamp = kwargs['ds_nodash']

    table_name = f"{transaction_type}_{datestamp}_{exec_date.day}"

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

  
def save_transaction_to_sqlite(transaction_type, transaction_df, datestamp):
    import sqlite3

    table_name = f"{transaction_type}_{datestamp}"

    filepath = f"{SQLITE_TRANSACTIONS_PATH}/{transaction_type}/{table_name}.db"

    print(f"Saving {transaction_df.shape[0]} rows to file: {filepath}")
    conn = sqlite3.connect(filepath)

    transaction_df.to_sql(table_name, conn, if_exists='replace')
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
    import pandas as pd

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

    df: DataFrame = conn_payment_pg_hook.get_pandas_df(raw_sql)

    df = df[pd.to_numeric(df['affiliate_fee']).fillna(0) > 0]

    return df


def get_deposit_data(date_from, date_to):
    import pandas as pd

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

    df = df[pd.to_numeric(df['affiliate_fee']).fillna(0) > 0]

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

    exec_date = datetime.strptime(kwargs['ds'], "%Y-%m-%d")

    target_date = exec_date.replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0
            )

    date_from = target_date - timedelta(days=1)
    date_to = target_date - timedelta(microseconds=1)

    transaction_df = get_transaction_func(date_from, date_to)

    if transaction_df.shape[0] == 0:
        print(f"No {transaction_type}s found")
        raise AirflowSkipException

    transaction_df = transaction_df.groupby(['affiliate_id', 'currency', 'affiliate_fee']).sum().reset_index()

    transaction_df['amount'] = transaction_df['amount'] * transaction_df['affiliate_fee'].astype(float) * 0.01

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

    df['product'] = PRODUCT_CODE_ALLBET

    df = get_member_currency(df)

    return df 


@task
def aggregate_bimonthly_transactions(transaction_type, **kwargs):
    import sqlite3
    import pandas as pd

    monthly_transactions = pd.DataFrame()

    datestamps = get_datestamps(**kwargs)

    for datestamp in datestamps:
        table_name = f"{transaction_type}_{datestamp}"
        filepath = f"{SQLITE_TRANSACTIONS_PATH}/{transaction_type}/{table_name}.db"

        conn = sqlite3.connect(filepath)
        curs = conn.cursor()

        get_table_list = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"

        res = curs.execute(get_table_list)
        tables = res.fetchall()

        if len( tables ) != 0:
            daily_transactions = pd.read_sql(f"SELECT * FROM {table_name}", conn)
            print(f"{daily_transactions.shape[0]} Affiliates Updated on {datestamp}")

            monthly_transactions = pd.concat([monthly_transactions, daily_transactions])

    if monthly_transactions.shape[0] == 0:
        print("No Wager Data Found")
        raise AirflowSkipException

    monthly_transactions = monthly_transactions.groupby(['affiliate_id', 'currency']).sum().reset_index()
    
    exec_date = datetime.strptime(kwargs['ds'], "%Y-%m-%d")
    datestamp = exec_date.strftime("%Y%m%d") + f"_{exec_date.day}"

    save_transaction_to_sqlite(transaction_type, monthly_transactions, datestamp)


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

    df['product'] = PRODUCT_CODE_ASIAGAMING

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

    df['product'] = PRODUCT_CODE_AGSLOT

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

    df['product'] = PRODUCT_CODE_AGYOPLAY

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

    df['product'] = PRODUCT_CODE_SAGAMING

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

    df['product'] = PRODUCT_CODE_SPSLOT

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

    df['product'] = PRODUCT_CODE_SPFISH

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

    df['product'] = PRODUCT_CODE_PGSOFT

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

    df['product'] = PRODUCT_CODE_EBETGAMING

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

    df['product'] = PRODUCT_CODE_BTISPORTS

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

    df['product'] = PRODUCT_CODE_SABACV

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

    df['product'] = PRODUCT_CODE_SABA

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

    df['product'] = PRODUCT_CODE_SABANUMBERGAME

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

    df['product'] = PRODUCT_CODE_SABAVIRTUAL

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

    df['product'] = PRODUCT_CODE_TFGAMING

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

    df['product'] = PRODUCT_CODE_EVOLUTION

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

    df['product'] = PRODUCT_CODE_GENESIS

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

    df['product'] = PRODUCT_CODE_WEWORLD

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

    df['product'] = PRODUCT_CODE_DIGITAIN

    df = get_member_currency(df)

    return df


def get_datestamps(**kwargs):
    import calendar

    year, month = 0, 0
    datestamps = []

    exec_date = datetime.strptime(kwargs['ds'], '%Y-%m-%d').replace(
            hour=UTC_EXEC_TIME,
            minute=0,
            second=0,
            microsecond=0
            )

    if exec_date.day == BI_MONTHLY_DAY:
        year = exec_date.year
        month = exec_date.month

        for day in range(BI_MONTHLY_DAY):
            datestamp = exec_date.replace(day=day +1).strftime("%Y%m%d")
            datestamps.append(datestamp)

    if exec_date.day == 1:
        year = (exec_date - timedelta(days=1)).year
        month = (exec_date - timedelta(days=1)).month
        num_days = calendar.monthrange(year, month)[1]

        for day in range(BI_MONTHLY_DAY - 1,num_days):
            datestamp = exec_date.replace(day=day+1).strftime("%Y%m%d")
            datestamps.append(datestamp)

    return datestamps


def save_wager_to_sqlite(product, wager_df, datestamp):
    import sqlite3

    table_name = f"{product}_{datestamp}"

    filepath = f"{SQLITE_WAGERS_PATH}/{product}/{table_name}.db"

    print(f"Saving {wager_df.shape[0]} rows to file: {filepath}")
    conn = sqlite3.connect(filepath)

    wager_df.to_sql(table_name, conn, if_exists='replace')
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

    wager_df = fetch_wager_func(date_from, date_to)

    print(f"Updating sqlite table {product}_{datestamp} with {wager_df.shape[0]} data")
    
    if wager_df.shape[0] == 0:
        print("No new data found")
        raise AirflowSkipException

    wager_df = wager_df.drop(columns=['login_name']).reset_index()
    wager_df = wager_df.groupby(['affiliate_id', 'currency']).sum()
    
    save_wager_to_sqlite(product, wager_df, datestamp)


@task
def aggregate_bimonthly_wagers(product, **kwargs):
    import sqlite3
    import pandas as pd

    wager_df = pd.DataFrame()

    datestamps = get_datestamps(**kwargs)

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

            wager_df = pd.concat([wager_df, product_df])

    if wager_df.shape[0] == 0:
        print("No Wager Data Found")
        raise AirflowSkipException

    wager_df = wager_df.groupby(['affiliate_id', 'currency']).sum().reset_index()

    exec_date = datetime.strptime(kwargs['ds'], "%Y-%m-%d")
    datestamp = exec_date.strftime("%Y%m%d") + f"_{exec_date.day}"

    save_wager_to_sqlite(product, wager_df, datestamp)


@task(trigger_rule=TriggerRule.ALL_DONE)
def create_bimonthly_adjustment_table(**kwargs):
    exec_date = datetime.strptime(kwargs['ds'], "%Y-%m-%d")
    print(exec_date.day)
    if exec_date.day != 1 and exec_date.day != BI_MONTHLY_DAY:
        print("Will only run on 1st and 15th day of the month")
        raise AirflowSkipException

    import sqlite3

    datestamp = kwargs['ds_nodash']

    table_name = f"adjustment_{datestamp}_{exec_date.day}"

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

    filepath = f"{SQLITE_TRANSACTIONS_PATH}/adjustment/{table_name}.db"

    print(f"Saving {adjustment_df.shape[0]} rows to file: {filepath}")
    conn = sqlite3.connect(filepath)

    adjustment_df.to_sql(table_name, conn, if_exists='replace')
    print("Saved Successfully")


def get_affiliate_adjustment_data(date_from, date_to):
    conn_payment_pg_hook = PostgresHook(postgres_conn_id='affiliate_conn_id')

    raw_sql = f"""
        SELECT
            (CASE WHEN transaction_type = {ADJUSTMENT_TRANSACTION_TYPE_CREDIT} THEN amount ELSE -amount END) as amount,
            affiliate_id,
            currency
        FROM adjustment
        WHERE create_at >= '{date_from}'
        AND create_at <= '{date_to}'
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


@task
def aggregate_bimonthly_adjustments(**kwargs):
    import sqlite3
    import pandas as pd

    monthly_transactions = pd.DataFrame()

    datestamps = get_datestamps(**kwargs)

    for datestamp in datestamps:
        table_name = f"adjustment_{datestamp}"
        filepath = f"{SQLITE_TRANSACTIONS_PATH}/{table_name}.db"

        conn = sqlite3.connect(filepath)
        curs = conn.cursor()

        get_table_list = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"

        res = curs.execute(get_table_list)
        tables = res.fetchall()

        if len( tables ) != 0:
            transaction_df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
            print(f"{transaction_df.shape[0]} Affiliates Updated on {datestamp}")

            monthly_transactions = pd.concat([monthly_transactions, transaction_df])

    if monthly_transactions.shape[0] == 0:
        print("No Wager Data Found")
        raise AirflowSkipException

    monthly_transactions = monthly_transactions.groupby(['affiliate_id', 'currency']).sum().reset_index()
    
    exec_date = datetime.strptime(kwargs['ds'], "%Y-%m-%d")
    datestamp = exec_date.strftime("%Y%m%d") + f"_{exec_date.day}"

    table_name = f"adjustment_{datestamp}_{exec_date.day}"

    save_adjustment_to_sqlite(monthly_transactions, datestamp)


@task
def create_commission_table(**kwargs):
    import sqlite3

    datestamp = kwargs['ds_nodash']
    table_name = f"affiliate_{datestamp}"
    filepath = f"{SQLITE_COMMISSION_PATH}/{table_name}.db"

    conn = sqlite3.connect(filepath)

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
                         login_name text,
                         amount real,
                         currency text
                     )
                     """)
        conn.commit()

    conn.close()


def save_affiliate_fees(affiliate_df: DataFrame, **kwargs):
    import sqlite3

    datestamp = kwargs['ds_nodash']
    table_name = f"affiliate_{datestamp}"
    filepath = f"{SQLITE_COMMISSION_PATH}/{table_name}.db"

    print(f"Connecting to: {filepath}")
    conn = sqlite3.connect(filepath)
    curs = conn.cursor()

    get_table_list = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"

    res = curs.execute(get_table_list)
    tables = res.fetchall()

    if len( tables ) != 0:
        print(f"Saving {affiliate_df.shape[0]} affiliates")
        affiliate_df.to_sql(table_name, conn, if_exists='replace')


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

        df = df.rename(columns={'win_loss': 'company_win_loss'})

        wager_df = pd.concat([wager_df, df])

    return wager_df


def get_aggregated_adjustments(datestamp):
    import sqlite3
    import pandas as pd

    table_name = f"adjustment_{datestamp}"
    filepath = f"{SQLITE_TRANSACTIONS_PATH}/{table_name}.db"

    conn = sqlite3.connect(filepath)
    
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)

    df = df.rename(columns={'amount': 'total_adjustment'})

    return df


@task
def update_affiliate_table():
    import sqlite3

    conn_affiliate_pg_hook = PostgresHook(postgres_conn_id='affiliate_conn_id')

    raw_sql = f"""
        SELECT 
            affiliate_id,
            login_name as affiliate_name,
            commission_tier1,
            commission_tier2,
            commission_tier3,
            payout_frequency,
            min_active_player
        FROM affiliate_account
    """

    affiliate_df = conn_affiliate_pg_hook.get_pandas_df(raw_sql)

    table_name = "affiliate"
    conn = sqlite3.connect(SQLITE_AFFILIATE_ACCOUNT_FILEPATH)

    affiliate_df.to_sql(table_name, conn, if_exists='replace')


def get_affiliate_df():
    import sqlite3
    import pandas as pd

    conn = sqlite3.connect(SQLITE_AFFILIATE_ACCOUNT_FILEPATH)
    df = pd.read_sql(f"SELECT * FROM affiliate", conn)

    return df

    
@task
def calculate_affiliate_fees(**kwargs):
    exec_date = datetime.strptime(kwargs['ds'], "%Y-%m-%d")
    datestamp = exec_date.strftime("%Y%m%d") + f"_{exec_date.day}"
    
    transaction_df: DataFrame = get_aggregated_transactions(datestamp)
    wager_df = get_aggregated_wagers(datestamp)
    adjustment_df = get_aggregated_adjustments(datestamp)

    commission_df = transaction_df.merge(wager_df, how='left', on=['affiliate_id', 'currency'])
    commission_df = commission_df.merge(adjustment_df, how='left', on=['affiliate_id', 'currency'])

    commission_df = commission_df.fillna(0)

    commission_df['total_affiliate_fee'] = commission_df['adjustment'] - commission_df['win_loss'] - commission_df['expenses']

    conn_affiliate_pg_hook = PostgresHook(postgres_conn_id='affiliate_conn_id')
    engine_affiliate = conn_affiliate_pg_hook.get_sqlalchemy_engine()

    # Todo: Add Missing Table Columns
    commission_df.to_sql(AFFILIATE_TABLE, engine_affiliate, if_exists='append', index=False)


@dag(
    dag_id='monthly_commission-v1.0.0_testzone',
    description='Calculates commission for affiliates every month',
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
    def aggregate_transactions_group():

        def transaction_aggregation(transaction_type):
            @task_group(group_id=transaction_type)
            def aggregate_transactions_task_group():

                create_monthly_table = create_bimonthly_transaction_table(transaction_type)
                aggregate_transactions_monthly = aggregate_bimonthly_transactions(transaction_type)

                create_monthly_table >> aggregate_transactions_monthly

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
    def aggregate_wagers():

        def wager_aggregation(product):
            @task_group(group_id=product)
            def aggregate_wagers_task_group():

                create_monthly_table = create_bimonthly_wager_table(product)
                aggregate_wagers_monthly = aggregate_bimonthly_wagers(product)

                create_monthly_table >> aggregate_wagers_monthly

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
    def aggregate_adjustments():
        create_monthly_table = create_bimonthly_adjustment_table()
        aggregate_adjustments_monthly = aggregate_bimonthly_adjustments()

        create_monthly_table >> aggregate_adjustments_monthly

    @task_group
    def daily_data_gathering():
        get_transactions()
        get_wagers()
        get_adjustments()

    # Todo: Pass a payout argument and create a get_datestamps that uses that argument to produce.. well.. datestamps
    @task_group
    def bi_monthly_aggregation():
        aggregate_transactions_group()
        aggregate_wagers()
        aggregate_adjustments()

    init_sqlite_task >> daily_data_gathering() >> bi_monthly_aggregation()

monthly_commission()
