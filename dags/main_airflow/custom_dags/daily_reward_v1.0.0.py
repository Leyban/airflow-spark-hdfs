from airflow.decorators import dag, task_group
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from datetime import  datetime,timedelta

from pandas import DataFrame

DAILY_REBATE_TABLE = "daily_rebate"
DAILY_REWARD_TABLE = "daily_reward"
DAILY_VIP_TABLE = "daily_vip"
LEVEL_SETTING_TABLE = "level_setting"
POINT_SETTING_TABLE = "reward_point_setting"
PRODUCT_REBATE_RATIO_TABLE = "product_rebate_ratio"

REBATE_STATUS_UNCLAIMED = 1
REBATE_STATUS_CLAIMED = 2

REWARD_STATUS_UNCLAIMED = 1
REWARD_STATUS_CLAIMED = 2

VIP_STATUS_PENDING = 1
VIP_STATUS_COMPLETE = 2

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

UTC_EXEC_TIME = 16

def init_sqlite():
    import os
    import sqlite3

    # Create Directories
    if not os.path.exists(f"./data/wagers/{PRODUCT_CODE_ALLBET}/"):
        os.makedirs(f"./data/wagers/{PRODUCT_CODE_ALLBET}/")
    if not os.path.exists(f"./data/wagers/{PRODUCT_CODE_ASIAGAMING}/"):
        os.makedirs(f"./data/wagers/{PRODUCT_CODE_ASIAGAMING}/")
    if not os.path.exists(f"./data/wagers/{PRODUCT_CODE_AGSLOT}/"):
        os.makedirs(f"./data/wagers/{PRODUCT_CODE_AGSLOT}/")
    if not os.path.exists(f"./data/wagers/{PRODUCT_CODE_AGYOPLAY}/"):
        os.makedirs(f"./data/wagers/{PRODUCT_CODE_AGYOPLAY}/")
    if not os.path.exists(f"./data/wagers/{PRODUCT_CODE_SAGAMING}/"):
        os.makedirs(f"./data/wagers/{PRODUCT_CODE_SAGAMING}/")
    if not os.path.exists(f"./data/wagers/{PRODUCT_CODE_SPSLOT}/"):
        os.makedirs(f"./data/wagers/{PRODUCT_CODE_SPSLOT}/")
    if not os.path.exists(f"./data/wagers/{PRODUCT_CODE_SPFISH}/"):
        os.makedirs(f"./data/wagers/{PRODUCT_CODE_SPFISH}/")
    if not os.path.exists(f"./data/wagers/{PRODUCT_CODE_SABACV}/"):
        os.makedirs(f"./data/wagers/{PRODUCT_CODE_SABACV}/")
    if not os.path.exists(f"./data/wagers/{PRODUCT_CODE_PGSOFT}/"):
        os.makedirs(f"./data/wagers/{PRODUCT_CODE_PGSOFT}/")
    if not os.path.exists(f"./data/wagers/{PRODUCT_CODE_EBETGAMING}/"):
        os.makedirs(f"./data/wagers/{PRODUCT_CODE_EBETGAMING}/")
    if not os.path.exists(f"./data/wagers/{PRODUCT_CODE_BTISPORTS}/"):
        os.makedirs(f"./data/wagers/{PRODUCT_CODE_BTISPORTS}/")
    if not os.path.exists(f"./data/wagers/{PRODUCT_CODE_TFGAMING}/"):
        os.makedirs(f"./data/wagers/{PRODUCT_CODE_TFGAMING}/")
    if not os.path.exists(f"./data/wagers/{PRODUCT_CODE_EVOLUTION}/"):
        os.makedirs(f"./data/wagers/{PRODUCT_CODE_EVOLUTION}/")
    if not os.path.exists(f"./data/wagers/{PRODUCT_CODE_GENESIS}/"):
        os.makedirs(f"./data/wagers/{PRODUCT_CODE_GENESIS}/")
    if not os.path.exists(f"./data/wagers/{PRODUCT_CODE_SABA}/"):
        os.makedirs(f"./data/wagers/{PRODUCT_CODE_SABA}/")
    if not os.path.exists(f"./data/wagers/{PRODUCT_CODE_SABANUMBERGAME}/"):
        os.makedirs(f"./data/wagers/{PRODUCT_CODE_SABANUMBERGAME}/")
    if not os.path.exists(f"./data/wagers/{PRODUCT_CODE_SABAVIRTUAL}/"):
        os.makedirs(f"./data/wagers/{PRODUCT_CODE_SABAVIRTUAL}/")
    if not os.path.exists(f"./data/wagers/{PRODUCT_CODE_DIGITAIN}/"):
        os.makedirs(f"./data/wagers/{PRODUCT_CODE_DIGITAIN}/")
    if not os.path.exists(f"./data/wagers/{PRODUCT_CODE_WEWORLD}/"):
        os.makedirs(f"./data/wagers/{PRODUCT_CODE_WEWORLD}/")

    conn = sqlite3.connect("./data/member.db")
    curs = conn.cursor()

    get_table_list = "SELECT name FROM sqlite_master WHERE type='table' AND name='member'"

    res = curs.execute(get_table_list)
    tables = res.fetchall()

    if len( tables ) == 0:
        create_member_table_sql = """CREATE TABLE member(
                id integer,
                login_name text,
                currency text
            ) 
            """
        curs.execute(create_member_table_sql)

    curs.close()


def drop_prev_wager_table(product: str,**context):
    import os
    datestamp = context['ds_nodash']

    table_name = f"{product}_{datestamp}"
    filepath = f"./data/wagers/{product}/{table_name}.db"

    print("Deleting File", filepath)
    os.remove(filepath)


def create_wager_table_task(product: str, **context):
    import sqlite3

    datestamp = context['ds_nodash']

    table_name = f"{product}_{datestamp}"

    filepath = f"./data/wagers/{product}/{table_name}.db"

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


def update_wager_table_task(product: str, fetch_wager_func, **context):
    import sqlite3

    datestamp = context['ds_nodash']
    table_name = f"{product}_{datestamp}"

    # Date range for processing
    date_to = datetime.strptime(context['ds'], '%Y-%m-%d').replace(hour=UTC_EXEC_TIME, minute=0, second=0, microsecond=0)
    date_to = date_to - timedelta(days=1)
    date_from = date_to - timedelta(days=1)
    date_to = date_to - timedelta(milliseconds=1)

    print("Processing date range")
    print("date_to", date_to)
    print("date_from", date_from)

    conn = sqlite3.connect(f"./data/wagers/{product}/{table_name}.db")

    wager_df = fetch_wager_func(date_from, date_to)

    print(f"Updating sqlite table {table_name} with {wager_df.shape[0]} data")
    if wager_df.shape[0] == 0:
        print("No new data found")
        raise AirflowSkipException

    wager_df['product'] = product

    wager_df.to_sql(table_name, conn, if_exists='replace')


def get_wager_df(product: str, **context) -> DataFrame:
    import sqlite3
    import pandas as pd

    datestamp = context['ds_nodash']
    table_name = f"{product}_{datestamp}"

    filepath = f"./data/wagers/{product}/{table_name}.db"

    print(f"Connecting to {filepath}")
    conn = sqlite3.connect(filepath)

    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)

    df = df.drop(['index'], axis=1)

    return df
    

def get_members_from_sqlite() -> DataFrame:
    import sqlite3
    import pandas as pd

    conn = sqlite3.connect("./data/member.db")

    rawsql = f"""
        SELECT
            id AS member_id,
            login_name,
            currency
        FROM member
    """

    members_df = pd.read_sql_query(rawsql, conn)

    return members_df


def insert_member_into_sqlite(new_member_df):
    import sqlite3
    conn = sqlite3.connect("./data/member.db")
    curs = conn.cursor()

    new_member_df = new_member_df.reset_index(drop=True)

    print("Inserting ", new_member_df.shape[0], " Data into Sqlite")

    for _, row in new_member_df.iterrows():
        query = f"""
        INSERT INTO member (
            id,
            login_name,
            currency
        )  VALUES (
            {row['id']},
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
            login_name,
            currency
        FROM member
        WHERE login_name IN (
    """

    for i, row in member_df.iterrows():
        rawsql += f" '{row['login_name']}' "

        if i != member_df.shape[0] - 1:
            rawsql += ","

    rawsql += ")"

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
    wager_df = wager_df.dropna(subset=['currency']) # Not found in PG identity table
    if 'member_id' in wager_df:
        wager_df = wager_df.drop(columns=['member_id'])

    wager_df = wager_df.reset_index(drop=True)

    return wager_df
    

def get_allbet_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT 
            bet_num AS bet_id, 
            valid_amount AS eligible_stake_amount, 
            login_name, 
            currency
        FROM {ALL_BET_WAGER_TABLE}
        WHERE bet_time < '{date_to}' 
        AND bet_time > '{date_from}'
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    df['currency'] = df['currency'].replace("VND2", "VND")
    df['currency'] = df['currency'].replace("CNY", "RMB")
    df['product'] = PRODUCT_CODE_ALLBET

    # df = df.drop_duplicates(subset=['bet_id'])
    df = df.drop(['bet_id'], axis=1)

    return df 


def get_asiagaming_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT
            bill_no AS bet_id,
            valid_bet_amount AS eligible_stake_amount,
            player_name AS login_name,
            currency
        FROM {ASIAGAMING_WAGER_TABLE}
        WHERE bet_time < '{date_to}'
        AND bet_time > '{date_from}'
        AND flag = 1 and game_type_code = 'AGIN' and platform_type <> 'YOPLAY'
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)


    df['currency'] = df['currency'].replace("CNY", "RMB")
    df['product'] = PRODUCT_CODE_ASIAGAMING

    # df = df.drop_duplicates(subset=['bet_id'])
    df = df.drop(['bet_id'], axis=1)

    return df 


def get_agslot_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT 
            bill_no AS bet_id, 
            (CASE WHEN net_amount <> 0 THEN bet_amount ELSE 0 END) AS eligible_stake_amount, 
            player_name AS login_name, 
            currency
        FROM {ASIAGAMING_WAGER_TABLE}
        WHERE bet_time <'{date_to}' 
        AND bet_time > '{date_from}'
        AND flag = 1 and game_type_code = 'SLOT' and platform_type in ('AGIN' ,'XIN')
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    df['currency'] = df['currency'].replace("CNY", "RMB")
    df['product'] = PRODUCT_CODE_AGSLOT

    # df = df.drop_duplicates(subset=['bet_id'])
    df = df.drop(['bet_id'], axis=1)

    return df


def get_agyoplay_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT 
            bill_no AS bet_id, 
            (CASE WHEN net_amount <> 0 THEN bet_amount ELSE 0 END) AS eligible_stake_amount, 
            player_name AS login_name,
            currency
        FROM {ASIAGAMING_WAGER_TABLE}
        WHERE bet_time <'{date_to}' 
        AND bet_time > '{date_from}'
        AND flag = 1 and game_type_code = 'AGIN' and platform_type = 'YOPLAY'
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    df['currency'] = df['currency'].replace("CNY", "RMB")
    df['product'] = PRODUCT_CODE_AGYOPLAY

    # df = df.drop_duplicates(subset=['bet_id'])
    df = df.drop(['bet_id'], axis=1)

    return df 


def get_sagaming_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT 
            bet_id, 
            username AS login_name,
            rolling AS eligible_stake_amount
        FROM {SAGAMING_WAGER_TABLE}
        WHERE bet_time <'{date_to}' 
        AND bet_time > '{date_from}'
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    df['product'] = PRODUCT_CODE_SAGAMING

    # df = df.drop_duplicates(subset=['bet_id'])
    df = df.drop(['bet_id'], axis=1)

    df = get_member_currency(df)

    return df 


def get_simpleplay_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT
            bet_id,
            bet_amount AS eligible_stake_amount, 
            result_amount,
            username AS login_name
        FROM {SIMPLEPLAY_WAGER_TABLE}
        WHERE bet_time <'{date_to}'
        AND bet_time > '{date_from}'
        AND game_type = 'slot'
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    if df.shape[0] == 0:
        return df

    df['eligible_stake_amount'] = df.apply( lambda x: x['eligible_stake_amount'] if x['result_amount'] != 0 else 0, axis=1)

    df = df.loc[:, ['bet_id', 'eligible_stake_amount', 'login_name']]

    df['product'] = PRODUCT_CODE_SPSLOT

    # df = df.drop_duplicates(subset=['bet_id'])
    df = df.drop(['bet_id'], axis=1)

    df = get_member_currency(df)

    return df 


def get_simpleplayfisher_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT
            bet_id,
            bet_amount AS eligible_stake_amount, 
            result_amount,
            username AS login_name
        FROM {SIMPLEPLAY_WAGER_TABLE}
        WHERE bet_time <'{date_to}'
        AND bet_time > '{date_from}'
        AND game_type != 'slot'
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    if df.shape[0] == 0:
        return df

    df['eligible_stake_amount'] = df.apply( lambda x: x['eligible_stake_amount'] if x['result_amount'] != 0 else 0, axis=1)

    df = df.loc[:, ['bet_id', 'eligible_stake_amount', 'login_name']]

    df['product'] = PRODUCT_CODE_SPFISH

    # df = df.drop_duplicates(subset=['bet_id'])
    df = df.drop(['bet_id'], axis=1)

    df = get_member_currency(df)

    return df 


def get_pgsoft_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT
            bet_id,
            (CASE WHEN win_amount >= 0 THEN bet_amount ELSE 0 END) as eligible_stake_amount,
            player_name AS login_name,
            currency
        FROM {PGSOFT_WAGER_TABLE}
        WHERE bet_time <'{date_to}'
        AND bet_time > '{date_from}'
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    df['product'] = PRODUCT_CODE_PGSOFT

    # df = df.drop_duplicates(subset=['bet_id'])
    df = df.drop(['bet_id'], axis=1)

    return df 


def get_ebet_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT
            bet_history_id AS bet_id,
            valid_bet AS eligible_stake_amount, 
            user_name AS login_name
        FROM {EBET_WAGER_TABLE}
        WHERE create_time <'{date_to}'
        AND create_time > '{date_from}'
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    df['product'] = PRODUCT_CODE_EBETGAMING

    # df = df.drop_duplicates(subset=['bet_id'])
    df = df.drop(['bet_id'], axis=1)

    df = get_member_currency(df)

    return df 


def get_bti_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT
            purchase_id AS bet_id,
            valid_stake AS eligible_stake_amount,
            username AS login_name,
            currency,
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

    df = df.loc[:, ['bet_id', 'eligible_stake_amount', 'login_name', 'currency']]

    df['product'] = PRODUCT_CODE_BTISPORTS

    # df = df.drop_duplicates(subset=['bet_id'])
    df = df.drop(['bet_id'], axis=1)

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

    df = df.loc[:, ['bet_id', 'eligible_stake_amount', 'login_name']]

    df['product'] = PRODUCT_CODE_SABACV

    # df = df.drop_duplicates(subset=['bet_id'])
    df = df.drop(['bet_id'], axis=1)

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

    df = df.loc[:, ['bet_id', 'eligible_stake_amount', 'login_name']]

    df['product'] = PRODUCT_CODE_SABA

    # df = df.drop_duplicates(subset=['bet_id'])
    df = df.drop(['bet_id'], axis=1)

    df = get_member_currency(df)

    return df 


def get_saba_number(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT
            trans_id AS bet_id,
            (CASE WHEN winlost_amount <> 0 AND ticket_status <> 'waiting' THEN stake ELSE 0 END) AS eligible_stake_amount, 
            vendor_member_id AS login_name
        FROM {SABA_NUMBER_TABLE}
        WHERE transaction_time <'{date_to}'
        AND transaction_time > '{date_from}'
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    df['product'] = PRODUCT_CODE_SABANUMBERGAME

    # df = df.drop_duplicates(subset=['bet_id'])
    df = df.drop(['bet_id'], axis=1)

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

    df = df.loc[:, ['bet_id', 'eligible_stake_amount', 'login_name']]

    df['product'] = PRODUCT_CODE_SABAVIRTUAL

    # df = df.drop_duplicates(subset=['bet_id'])
    df = df.drop(['bet_id'], axis=1)

    df = get_member_currency(df)

    return df 


def get_tfgaming_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT
            bet_history_id AS bet_id,
            amount AS eligible_stake_amount, 
            member_code AS login_name,
            currency,
            member_odds, 
            member_odds_style,
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

    df = df.loc[:, ['bet_id','eligible_stake_amount', 'login_name', 'currency']]

    df['product'] = PRODUCT_CODE_TFGAMING

    # df = df.drop_duplicates(subset=['bet_id'])
    df = df.drop(['bet_id'], axis=1)

    return df 


def get_evolution_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT
            transaction_id AS bet_id,
            (CASE WHEN stake <> payout THEN stake ELSE 0 END) as eligible_stake_amount, 
            player_id AS login_name,
            currency
        FROM {EVOLUTION_TABLE}
        WHERE placed_on <'{date_to}'
        AND placed_on > '{date_from}'
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    df['currency'] = df['currency'].replace("VN2", "VND")
    df['currency'] = df['currency'].replace("CNY", "RMB")
    df['product'] = PRODUCT_CODE_EVOLUTION

    # df = df.drop_duplicates(subset=['bet_id'])
    df = df.drop(['bet_id'], axis=1)

    return df 


def get_genesis_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT
            bet_history_id AS bet_id,
            valid_bet AS eligible_stake_amount, 
            user_name AS login_name
        FROM {GENESIS_TABLE}
        WHERE create_time <'{date_to}'
        AND create_time > '{date_from}'
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    df['product'] = PRODUCT_CODE_GENESIS

    # df = df.drop_duplicates(subset=['bet_id'])
    df = df.drop(['bet_id'], axis=1)

    df = get_member_currency(df)

    return df 


def get_weworld_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT
            bet_id,
            valid_bet_amount AS eligible_stake_amount,
            player_id AS login_name
        FROM {WEWORLD_TABLE}
        WHERE bet_datetime <'{date_to}'
        AND bet_datetime > '{date_from}'
    """

    df = conn_wager_pg_hook.get_pandas_df(rawsql)

    df['product'] = PRODUCT_CODE_WEWORLD

    # df = df.drop_duplicates(subset=['bet_id'])
    df = df.drop(['bet_id'], axis=1)

    df = get_member_currency(df)

    return df


def get_digitain_wager(date_from, date_to) -> DataFrame:
    conn_wager_pg_hook = PostgresHook(postgres_conn_id='wager_conn_id')

    rawsql = f"""
        SELECT
            u.login_name as login_name,
            o.currency AS currency,
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

    df = df.loc[:, ['bet_id', 'eligible_stake_amount', 'login_name', 'currency']]

    df['product'] = PRODUCT_CODE_DIGITAIN

    # df = df.drop_duplicates(subset=['bet_id'])
    df = df.drop(['bet_id'], axis=1)

    return df


def calc_wager_rebate(product:str, **context):
    wager_df = get_wager_df(product, **context)
    print("Processing ", wager_df.shape[0], " Data")
    if wager_df.shape[0] == 0:
        print("No Data Found")
        raise AirflowSkipException

    process_wagers_rebate(wager_df, product, **context)


def calc_wager_reward(product:str, **context):
    wager_df = get_wager_df(product, **context)
    print("Processing ", wager_df.shape[0], " Data")
    if wager_df.shape[0] == 0:
        print("No Data Found")
        raise AirflowSkipException

    process_wagers_reward(wager_df, product, **context)


def calc_wager_vip(product:str, **context):
    wager_df = get_wager_df(product, **context)
    print("Processing ", wager_df.shape[0], " Data")
    if wager_df.shape[0] == 0:
        print("No Data Found")
        raise AirflowSkipException

    process_wagers_vip(wager_df, product, **context)


def get_product_rebate_ratio(conn) -> DataFrame:
    rawsql = f"""
        SELECT 
            product,
            currency,
            rebate_ratio,
            level
        FROM {PRODUCT_REBATE_RATIO_TABLE}
    """
        
    df = conn.get_pandas_df(rawsql)
        
    return df 


def get_level_setting(conn) -> DataFrame:
    rawsql = f"""
        SELECT 
            currency,
            level,
            total_eligible_stake_amount,
            reward_point,
            reward_point_percent
        FROM {LEVEL_SETTING_TABLE}
    """
        
    df = conn.get_pandas_df(rawsql)

    return df 


def get_point_setting(conn):
    rawsql = f"""
        SELECT 
            currency,
            total_eligible_stake_amount AS point_divisor
        FROM {POINT_SETTING_TABLE}
    """
        
    df = conn.get_pandas_df(rawsql)

    return df 


def get_level(wager_df, conn):
    rawsql = f"""
        SELECT 
            level,
            login_name,
            currency
        FROM member_account
    """

    member_df = conn.get_pandas_df(rawsql)

    wager_df = wager_df.merge(member_df, how='left', on=['login_name', 'currency'])
    wager_df['level'] = wager_df['level'].fillna(1)
    
    return wager_df


def calc_rebate_amount(x):
    rebate_amount = x['eligible_stake_amount'] * x['rebate_ratio'] * 0.01

    return round(rebate_amount, 5)


def get_rebate_ratio(x, product_rebate_ratio_df):
    rebate_df = product_rebate_ratio_df[
        (product_rebate_ratio_df['product'] == x['product']) &
        (product_rebate_ratio_df['level'] == x['level']) &
        (product_rebate_ratio_df['currency'] == x['currency'])
    ]

    if rebate_df.shape[0] == 0:
        print("Currency not supported: ", x['currency'])
        return 0
        
    rebate_ratio = rebate_df.iloc[0]['rebate_ratio']

    return rebate_ratio


def get_reward_point(wager_df, point_setting_df) -> DataFrame:
    import numpy as np

    wager_df = wager_df.merge(point_setting_df, 'left', 'currency')

    wager_df['reward_point'] = np.where(wager_df['point_divisor'].notnull(), wager_df['eligible_stake_amount'] / wager_df['point_divisor'], 0)

    wager_df = wager_df.drop(['point_divisor'], axis=1)

    return wager_df


def drop_members_claimed_via_bonus(wager_df, conn) -> DataFrame:
    rawsql = f"""
        SELECT 
            ma.login_name AS login_name,
            ma.currency AS currency,
            mb.product_code AS product,
            'claimed' AS bonus
        FROM member_bonus as mb
        LEFT JOIN bonus as b ON mb.bonus_id = b.id
        LEFT JOIN member_account as ma ON mb.member_id = ma.member_id
    """

    bonus_df = conn.get_pandas_df(rawsql)

    if bonus_df.shape[0] > 0:
        merged_df = wager_df.merge(bonus_df, how='left', on=['login_name', 'currency', 'product'])

        wager_df = merged_df[merged_df['bonus'].isna()]
        wager_df = wager_df.drop(['bonus'], axis=1)

    return wager_df 


def deduct_claimed_rebate_amt(wager_df, product, execution_date, conn):
    rawsql = f"""
        SELECT 
            login_name,
            currency, 
            product,
            rebate_amt AS claimed_amount,
            eligible_stake_amount AS claimed_stake_amount
        FROM {DAILY_REBATE_TABLE} 
        WHERE product = '{product}' 
        AND rebate_date = '{execution_date}' 
        AND status = {REBATE_STATUS_CLAIMED}
    """

    claimed_df = conn.get_pandas_df(rawsql)

    if claimed_df.empty:
        return wager_df

    merged_df = wager_df.merge(claimed_df, how='left', on=['login_name', 'currency', 'product'])
    merged_df['claimed_amount'] = merged_df['claimed_amount'].fillna(0)
    merged_df['claimed_stake_amount'] = merged_df['claimed_stake_amount'].fillna(0)

    merged_df.reset_index(inplace=True, drop=True)
    wager_df.reset_index(inplace=True, drop=True)

    wager_df['rebate_amt'] =  merged_df['rebate_amt'] - merged_df['claimed_amount']
    wager_df['eligible_stake_amount'] =  merged_df['eligible_stake_amount'] - merged_df['claimed_stake_amount']

    return wager_df


def deduct_claimed_reward_point(wager_df, product, execution_date, conn) -> DataFrame:
    rawsql = f"""
        SELECT 
            login_name,
            currency, 
            product,
            reward_point AS claimed_amount,
            eligible_stake_amount AS claimed_stake_amount
        FROM {DAILY_REWARD_TABLE} 
        WHERE product = '{product}' 
        AND reward_date = '{execution_date}' 
        AND status = {REWARD_STATUS_CLAIMED}
    """

    claimed_df = conn.get_pandas_df(rawsql)

    if claimed_df.empty:
        return wager_df

    merged_df = wager_df.merge(claimed_df, how='left', on=['login_name', 'currency', 'product'])
    merged_df['claimed_amount'] = merged_df['claimed_amount'].fillna(0)
    merged_df['claimed_stake_amount'] = merged_df['claimed_stake_amount'].fillna(0)

    merged_df.reset_index(inplace=True, drop=True)
    wager_df.reset_index(inplace=True, drop=True)
    
    wager_df['reward_point'] =  merged_df['reward_point'] - merged_df['claimed_amount']
    wager_df['eligible_stake_amount'] =  merged_df['eligible_stake_amount'] - merged_df['claimed_stake_amount']

    return wager_df


def deduct_claimed_vip_eligible_stake_amount(wager_df: DataFrame, product, execution_date, conn) -> DataFrame:
    rawsql = f"""
        SELECT
            login_name,
            currency, 
            product,
            eligible_stake_amount AS claimed_amount
        FROM {DAILY_VIP_TABLE} 
        WHERE product = '{product}' 
        AND vip_date = '{execution_date}' 
        AND status = {VIP_STATUS_COMPLETE}
    """

    claimed_df = conn.get_pandas_df(rawsql)

    if claimed_df.empty:
        return wager_df

    merged_df = wager_df.merge(claimed_df, how='left', on=['login_name', 'currency', 'product'])
    merged_df['claimed_amount'] = merged_df['claimed_amount'].fillna(0)

    merged_df.reset_index(inplace=True, drop=True)
    wager_df.reset_index(inplace=True, drop=True)

    wager_df['eligible_stake_amount'] = merged_df['eligible_stake_amount'] - merged_df['claimed_amount']

    return wager_df


def process_wagers_rebate(wager_df, product, **context):
    conn_reward_pg_hook = PostgresHook(postgres_conn_id='reward_conn_id')
    engine_reward = conn_reward_pg_hook.get_sqlalchemy_engine()

    execution_date = datetime.strptime(context['ds'], '%Y-%m-%d') - timedelta(days=1)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    wager_df = wager_df.groupby(['login_name', 'currency', 'product'], as_index=False, sort=False).sum()

    wager_df = drop_members_claimed_via_bonus(wager_df, conn_reward_pg_hook)

    ratio_df = get_product_rebate_ratio(conn_reward_pg_hook)

    wager_df = get_level(wager_df, conn_reward_pg_hook)
    wager_df['rebate_ratio'] = wager_df.apply(lambda x: get_rebate_ratio(x, ratio_df), axis=1)
    wager_df['rebate_amt'] = wager_df.apply(lambda x: calc_rebate_amount(x), axis=1)

    wager_df = deduct_claimed_rebate_amt(wager_df, product, execution_date, conn_reward_pg_hook)
    wager_df = wager_df[wager_df['rebate_amt'] != 0]

    wager_df['status'] = REBATE_STATUS_UNCLAIMED

    wager_df['rebate_date'] = execution_date

    wager_df['create_at'] = now
    wager_df['update_at'] = now

    wager_df = wager_df.drop(['level'], axis=1)
    print("Inserting ", wager_df.shape[0], " Data")

    clear_old_data_sql = f"DELETE FROM {DAILY_REBATE_TABLE} WHERE product = '{product}' AND rebate_date = '{execution_date}' AND status = {REBATE_STATUS_UNCLAIMED}"
    conn_reward_pg_hook.run(clear_old_data_sql)

    wager_df.to_sql(DAILY_REBATE_TABLE, con=engine_reward, if_exists='append', index=False)
    

def process_wagers_reward(wager_df, product, **context):
    conn_reward_pg_hook = PostgresHook(postgres_conn_id='reward_conn_id')
    engine_reward = conn_reward_pg_hook.get_sqlalchemy_engine()

    execution_date = datetime.strptime(context['ds'], '%Y-%m-%d') - timedelta(days=1)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    point_setting_df = get_point_setting(conn_reward_pg_hook)

    wager_df = wager_df.groupby(['login_name', 'currency', 'product'], as_index=False, sort=False).sum()

    wager_df = drop_members_claimed_via_bonus(wager_df,conn_reward_pg_hook)

    wager_df = get_reward_point(wager_df, point_setting_df)

    wager_df = deduct_claimed_reward_point(wager_df, product, execution_date, conn_reward_pg_hook)
    wager_df = wager_df[wager_df['reward_point'] != 0]

    wager_df['status'] = REWARD_STATUS_UNCLAIMED

    wager_df['reward_date'] = execution_date
    wager_df['create_at'] = now
    wager_df['update_at'] = now

    print("Inserting ", wager_df.shape[0], " Data")

    clear_old_data_sql = f"DELETE FROM {DAILY_REWARD_TABLE} WHERE product = '{product}' AND reward_date = '{execution_date}' AND status = '{REWARD_STATUS_UNCLAIMED}'"
    conn_reward_pg_hook.run(clear_old_data_sql)

    wager_df.to_sql(DAILY_REWARD_TABLE, con=engine_reward, if_exists='append', index=False)


def process_wagers_vip(wager_df, product, **context):
    conn_reward_pg_hook = PostgresHook(postgres_conn_id='reward_conn_id')
    engine_reward = conn_reward_pg_hook.get_sqlalchemy_engine()

    execution_date = datetime.strptime(context['ds'], '%Y-%m-%d') - timedelta(days=1)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    wager_df = wager_df.groupby(['login_name', 'currency', 'product'], as_index=False, sort=False).sum()

    wager_df['status'] = VIP_STATUS_PENDING

    wager_df['vip_date'] = execution_date
    wager_df['create_at'] = now
    wager_df['update_at'] = now

    wager_df = drop_members_claimed_via_bonus(wager_df,conn_reward_pg_hook)

    wager_df = deduct_claimed_vip_eligible_stake_amount(wager_df, product, execution_date, conn_reward_pg_hook)
    wager_df = wager_df[wager_df['eligible_stake_amount'] != 0]

    print("Inserting ", wager_df.shape[0], " Data")

    clear_old_data_sql = f"DELETE FROM {DAILY_VIP_TABLE} WHERE product = '{product}' AND vip_date = '{execution_date}' AND status = '{VIP_STATUS_PENDING}'"
    conn_reward_pg_hook.run(clear_old_data_sql)

    wager_df.to_sql(DAILY_VIP_TABLE, con=engine_reward, if_exists='append', index=False)


@dag(
    'daily_reward-v1.0.0',
    description='Calculates for Daily Reward',
    schedule_interval='0 8 * * *',
    start_date=datetime(2023, 1, 14),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=5,
)
def daily_reward():

    # DAG FLOW DEFINITION
   
    init = PythonOperator(
            task_id="initialize_sqlite",
            python_callable=init_sqlite
            )

    def wager_task_group(product:str, get_wager_func):
        @task_group(group_id=product)
        def wager_task_group():
            create_table = PythonOperator(
                    task_id=f"create_table_{product}",
                    python_callable=create_wager_table_task,
                    op_args=[product]
                    )

            update_table = PythonOperator(
                    task_id=f"update_table_{product}",
                    python_callable=update_wager_table_task,
                    op_args=[product, get_wager_func]
                    )

            @task_group
            def calc():
                rebate = PythonOperator(
                        task_id=f"calc_rebate_{product}",
                        python_callable=calc_wager_rebate,
                        op_args=[product]
                        )
                reward = PythonOperator(
                        task_id=f"calc_reward_{product}",
                        python_callable=calc_wager_reward,
                        op_args=[product]
                        )
                vip = PythonOperator(
                        task_id=f"calc_vip_{product}",
                        python_callable=calc_wager_vip,
                        op_args=[product]
                        )

                rebate
                reward
                vip

            drop_old_table = PythonOperator(
                    task_id=f"drop_old_{product}_table",
                    python_callable=drop_prev_wager_table,
                    op_args=[product]
                    )

            create_table >> update_table >> calc() >> drop_old_table

        return wager_task_group

    allbet = wager_task_group(PRODUCT_CODE_ALLBET, get_allbet_wager)
    asiagaming = wager_task_group(PRODUCT_CODE_ASIAGAMING, get_asiagaming_wager)
    agslot = wager_task_group(PRODUCT_CODE_AGSLOT, get_agslot_wager)
    agyoplay = wager_task_group(PRODUCT_CODE_AGYOPLAY, get_agyoplay_wager)
    sagaming = wager_task_group(PRODUCT_CODE_SAGAMING, get_sagaming_wager)
    spslot = wager_task_group(PRODUCT_CODE_SPSLOT, get_simpleplay_wager)
    spfish = wager_task_group(PRODUCT_CODE_SPFISH, get_simpleplayfisher_wager)
    sabacv = wager_task_group(PRODUCT_CODE_SABACV, get_sabacv_wager)
    pgsoft = wager_task_group(PRODUCT_CODE_PGSOFT, get_pgsoft_wager)
    ebetgaming = wager_task_group(PRODUCT_CODE_EBETGAMING, get_ebet_wager)
    btisports = wager_task_group(PRODUCT_CODE_BTISPORTS, get_bti_wager)
    tfgaming = wager_task_group(PRODUCT_CODE_TFGAMING, get_tfgaming_wager)
    evolution = wager_task_group(PRODUCT_CODE_EVOLUTION, get_evolution_wager)
    genesis = wager_task_group(PRODUCT_CODE_GENESIS, get_genesis_wager)
    saba = wager_task_group(PRODUCT_CODE_SABA, get_saba_wager)
    sabanumbergame = wager_task_group(PRODUCT_CODE_SABANUMBERGAME, get_saba_number)
    sabavirtual = wager_task_group(PRODUCT_CODE_SABAVIRTUAL, get_saba_virtual)
    weworld = wager_task_group(PRODUCT_CODE_WEWORLD, get_weworld_wager)
    digitain = wager_task_group(PRODUCT_CODE_DIGITAIN, get_digitain_wager)


    init >> allbet()
    init >> asiagaming()
    init >> agslot()
    init >> agyoplay()
    init >> sagaming()
    init >> spslot()
    init >> spfish()
    init >> sabacv()
    init >> pgsoft()
    init >> ebetgaming()
    init >> btisports()
    init >> tfgaming()
    init >> evolution()
    init >> genesis()
    init >> saba()
    init >> sabanumbergame()
    init >> sabavirtual()
    init >> weworld()
    init >> digitain()


daily_reward()
