from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta

DAILY_REBATE_TABLE = "daily_rebate"
LEVEL_SETTING_TABLE = "level_setting"
PRODUCT_REBATE_RATIO_TABLE = "product_rebate_ratio"

REBATE_STATUS_PROCESSING = 1

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

PRODUCT_CODE_ALLBET = "allbet"  # 15 game_type
PRODUCT_CODE_ASIAGAMING = "asiagaming"  # Asiagaming -- HAS game_type_code
PRODUCT_CODE_AGSLOT = "agslot"  # Asiagaming game_type_code = SLOT
PRODUCT_CODE_AGYOPLAY = "agyoplay"  # Asiagaming game_type_code = AGIN ?
PRODUCT_CODE_SAGAMING = "sagaming"  # no currency
PRODUCT_CODE_SPSLOT = "simpleplay"  # game_type slot -- no currency
PRODUCT_CODE_SPFISH = "simpleplayfisher"  # game_type multiplayer -- no currency
PRODUCT_CODE_SABACV = "sabacv"  # currency are numbers what
PRODUCT_CODE_PGSOFT = "pgsoft"
PRODUCT_CODE_EBETGAMING = "ebetgaming"  # 22 game names
PRODUCT_CODE_BTISPORTS = "btisports"  # Has bet_type_name
PRODUCT_CODE_TFGAMING = "tfgaming"  # 21 bet_type_name
PRODUCT_CODE_EVOLUTION = "evolution"  # 47 game_type
PRODUCT_CODE_GENESIS = "genesis"  # no currency
PRODUCT_CODE_SABA = "saba"  # currency are numbers
PRODUCT_CODE_SABANUMBERGAME = "sabanumbergames"  # saba_number -- currency are numbers
PRODUCT_CODE_SABAVIRTUAL = "sabavirtual"  # saba_virtual -- currency are numbers
PRODUCT_CODE_DIGITAIN = "digitain"  # no username
PRODUCT_CODE_WEWORLD = "weworld"  # 13 game_type -- no currency

UTC_EXEC_TIME = 0


def init_identity_member_table():
    from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
    from airflow.models import Variable

    HIVE_WAREHOUSE_DIR = Variable.get("HIVE_WAREHOUSE_DIR")

    hive_hook = HiveServer2Hook(
        hiveserver2_conn_id="hive_servicer2_conn_id",
    )

    create_identity_db_sql = (
        f"CREATE DATABASE IF NOT EXISTS identity LOCATION '{HIVE_WAREHOUSE_DIR}'"
    )
    create_member_table_sql = """CREATE TABLE IF NOT EXISTS identity.member(
            id BIGINT,
            uuid STRING,
            login_name STRING,
            full_name STRING,
            password STRING,
            status INT,
            currency STRING,
            email STRING,
            phone STRING,
            email_verify_status INT,
            phone_verify_status INT,
            language STRING,
            address STRING, 
            country STRING, 
            city STRING,
            postal_code INT,
            account_type INT,
            last_login_date INT,
            date_of_birth TIMESTAMP,
            create_at TIMESTAMP,
            update_at TIMESTAMP
        ) 
        STORED AS PARQUET
        """

    hive_hook.run(sql=create_identity_db_sql)
    hive_hook.run(sql=create_member_table_sql)


def get_members_from_hive():
    from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook

    hive_hook = HiveServer2Hook(
        hiveserver2_conn_id="hive_servicer2_conn_id", schema="identity"
    )

    rawsql = f"""
        SELECT 
            id AS member_id,
            login_name,
            currency
        FROM identity.member
    """

    df = hive_hook.get_pandas_df(rawsql)

    return df


def nullableString(s):
    if s != None:
        return f"'{s}'"
    return "NULL"


def nullableInt(i):
    if i != None:
        return f"{i}"
    return "NULL"


def insert_into_hive(new_member_df):
    from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook

    new_member_df = new_member_df.reset_index(drop=True)

    print("Inserting ", new_member_df.shape[0], " Data into Hive")

    hive_hook = HiveServer2Hook(
        hiveserver2_conn_id="hive_servicer2_conn_id", schema="identity"
    )

    query = """INSERT INTO identity.member (
        id,
        uuid,
        login_name,
        full_name,
        password,
        status,
        currency,
        email,
        phone,
        email_verify_status,
        phone_verify_status,
        language,
        address, 
        country, 
        city,
        postal_code,
        account_type,
        last_login_date,
        date_of_birth,
        create_at,
        update_at
    )  VALUES """

    for i, row in new_member_df.iterrows():
        query += f"""(
        {row['id']},
        '{row['uuid']}',
        '{row['login_name']}',
        '{row['full_name']}',
        '{row['password']}',
        {row['status']},
        '{row['currency']}',
        {nullableString(row['email'])},
        '{row['phone']}',
        {row['email_verify_status']},
        {row['phone_verify_status']},
        '{row['language']}',
        {nullableString(row['address'])}, 
        '{row['country']}', 
        {nullableString(row['city'])},
        {nullableInt(row['postal_code'])},
        {row['account_type']},
        {nullableString(row['last_login_date'])},
        {nullableString(row['date_of_birth'])},
        '{row['create_at']}',
        '{row['update_at']}'
        )"""

        if i != new_member_df.shape[0] - 1:
            query += """,
            """

    hive_hook.run(sql=query)


def update_members_on_hive(missing_member_df):
    import pandas as pd

    conn_identity_pg_hook = PostgresHook(postgres_conn_id="identity_conn_id")

    member_df = missing_member_df.drop_duplicates(subset=["login_name"])
    member_df = member_df.astype("str")

    print("Fetching Missing Members from Hive: ", member_df.shape[0])
    member_df = member_df.reset_index(drop=True)

    found_members_df = pd.DataFrame()

    rawsql = """
        SELECT
            id,
            uuid,
            login_name,
            full_name,
            password,
            status,
            currency,
            email,
            phone,
            email_verify_status,
            phone_verify_status,
            language,
            address, 
            country, 
            city,
            postal_code,
            account_type,
            last_login_date,
            date_of_birth,
            create_at,
            update_at
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

    if found_members_df.shape[0] != member_df.shape[0]:
        print("Some Members are missing: ")
        merged_df = member_df.merge(found_members_df, "left", "login_name")
        not_found_df = merged_df[merged_df["currency"].isna()]
        for _, row in not_found_df.iterrows():
            print(f"Missing Member: {row['login_name']}")

        raise AirflowSkipException

    insert_into_hive(found_members_df)

    new_member_df = found_members_df.loc[:, ["id", "login_name", "currency"]]
    new_member_df = new_member_df.rename(columns={"id": "member_id"})

    return new_member_df


def get_member_currency(wager_df):
    import pandas as pd

    member_df = get_members_from_hive()

    if member_df.shape[0] != 0:
        wager_df = wager_df.merge(member_df, "left", "login_name")
        wager_df = wager_df.drop(columns=["member_id"])

    # Get Missing Members
    missing_member_df = pd.DataFrame()

    # Some members are missing
    if "currency" in wager_df.columns:
        lacking_wager_df = wager_df[wager_df["currency"].isna()].loc[:, ["login_name"]]
        missing_member_df = lacking_wager_df.loc[:, ["login_name"]]
        wager_df = wager_df.dropna(subset=["currency"])

        if missing_member_df.shape[0] > 0:
            new_member_df = update_members_on_hive(missing_member_df)
            lacking_wager_df = lacking_wager_df.merge(
                new_member_df, "left", "login_name"
            )

            wager_df = pd.concat([wager_df, lacking_wager_df])

    # All members are missing
    else:
        missing_member_df = wager_df.loc[:, ["login_name"]]

        new_member_df = update_members_on_hive(missing_member_df)
        wager_df = wager_df.merge(new_member_df, "left", "login_name")

    print(wager_df.columns)
    wager_df = wager_df.dropna(subset=["currency"])  # Not found in PG identity table
    if "member_id" in wager_df:
        wager_df = wager_df.drop(columns=["member_id"])

    wager_df = wager_df.reset_index(drop=True)

    return wager_df


def get_allbet_wager():
    conn_collector_pg_hook = PostgresHook(postgres_conn_id="collector_conn_id")
    today = datetime.utcnow().replace(
        hour=UTC_EXEC_TIME, minute=0, second=0, microsecond=0
    )
    yesterday = today - timedelta(days=1)

    rawsql = f"""
        SELECT 
            bet_num AS bet_id, 
            valid_amount AS eligible_stake_amount, 
            login_name, 
            currency
        FROM {ALL_BET_WAGER_TABLE}
        WHERE bet_time <'{today}' 
        AND bet_time > '{yesterday}'
    """

    df = conn_collector_pg_hook.get_pandas_df(rawsql)

    return df


def get_asiagaming_wager():
    conn_collector_pg_hook = PostgresHook(postgres_conn_id="collector_conn_id")
    today = datetime.utcnow().replace(
        hour=UTC_EXEC_TIME, minute=0, second=0, microsecond=0
    )
    yesterday = today - timedelta(days=1)

    rawsql = f"""
        SELECT 
            bill_no AS bet_id, 
            valid_bet_amount AS eligible_stake_amount, 
            player_name AS login_name, 
            currency
        FROM {ASIAGAMING_WAGER_TABLE}
        WHERE bet_time <'{today}' 
        AND bet_time > '{yesterday}'
        AND flag = 1 and game_type_code = 'AGIN' and platform_type <> 'YOPLAY'
    """

    df = conn_collector_pg_hook.get_pandas_df(rawsql)

    return df


def get_agslot_wager():
    conn_collector_pg_hook = PostgresHook(postgres_conn_id="collector_conn_id")
    today = datetime.utcnow().replace(
        hour=UTC_EXEC_TIME, minute=0, second=0, microsecond=0
    )
    yesterday = today - timedelta(days=1)

    rawsql = f"""
        SELECT 
            bill_no AS bet_id, 
            valid_bet_amount AS eligible_stake_amount, 
            player_name AS login_name, 
            currency
        FROM {ASIAGAMING_WAGER_TABLE}
        WHERE bet_time <'{today}' 
        AND bet_time > '{yesterday}'
        AND flag = 1 and game_type_code = 'SLOT' and platform_type in ('AGIN' ,'XIN')
    """

    df = conn_collector_pg_hook.get_pandas_df(rawsql)

    return df


def get_agyoplay_wager():
    conn_collector_pg_hook = PostgresHook(postgres_conn_id="collector_conn_id")
    today = datetime.utcnow().replace(
        hour=UTC_EXEC_TIME, minute=0, second=0, microsecond=0
    )
    yesterday = today - timedelta(days=1)

    rawsql = f"""
        SELECT 
            bill_no AS bet_id, 
            valid_bet_amount AS eligible_stake_amount, 
            player_name AS login_name, 
            currency
        FROM {ASIAGAMING_WAGER_TABLE}
        WHERE bet_time <'{today}' 
        AND bet_time > '{yesterday}'
        AND flag = 1 and game_type_code = 'AGIN' and platform_type = 'YOPLAY'
    """

    df = conn_collector_pg_hook.get_pandas_df(rawsql)

    return df


def get_sagaming_wager():
    conn_collector_pg_hook = PostgresHook(postgres_conn_id="collector_conn_id")
    today = datetime.utcnow().replace(
        hour=UTC_EXEC_TIME, minute=0, second=0, microsecond=0
    )
    yesterday = today - timedelta(days=1)

    rawsql = f"""
        SELECT 
            bet_id, 
            bet_amount AS eligible_stake_amount, 
            username AS login_name
        FROM {SAGAMING_WAGER_TABLE}
        WHERE bet_time <'{today}' 
        AND bet_time > '{yesterday}'
    """

    df = conn_collector_pg_hook.get_pandas_df(rawsql)

    return df


def get_simpleplay_wager():
    conn_collector_pg_hook = PostgresHook(postgres_conn_id="collector_conn_id")
    today = datetime.utcnow().replace(
        hour=UTC_EXEC_TIME, minute=0, second=0, microsecond=0
    )
    yesterday = today - timedelta(days=1)

    rawsql = f"""
        SELECT
            bet_id,
            bet_amount AS eligible_stake_amount, 
            username AS login_name
        FROM {SIMPLEPLAY_WAGER_TABLE}
        WHERE bet_time <'{today}'
        AND bet_time > '{yesterday}'
        AND game_type = 'slot'
    """

    df = conn_collector_pg_hook.get_pandas_df(rawsql)

    return df


def get_simpleplayfisher_wager():
    conn_collector_pg_hook = PostgresHook(postgres_conn_id="collector_conn_id")
    today = datetime.utcnow().replace(
        hour=UTC_EXEC_TIME, minute=0, second=0, microsecond=0
    )
    yesterday = today - timedelta(days=1)

    rawsql = f"""
        SELECT
            bet_id,
            bet_amount AS eligible_stake_amount, 
            username AS login_name
        FROM {SIMPLEPLAY_WAGER_TABLE}
        WHERE bet_time <'{today}'
        AND bet_time > '{yesterday}'
        AND game_type = 'multiplayer'
    """

    df = conn_collector_pg_hook.get_pandas_df(rawsql)

    return df


def get_pgsoft_wager():
    conn_collector_pg_hook = PostgresHook(postgres_conn_id="collector_conn_id")
    today = datetime.utcnow().replace(
        hour=UTC_EXEC_TIME, minute=0, second=0, microsecond=0
    )
    yesterday = today - timedelta(days=1)

    rawsql = f"""
        SELECT
            bet_id,
            bet_amount AS eligible_stake_amount, 
            player_name AS login_name,
            currency
        FROM {PGSOFT_WAGER_TABLE}
        WHERE bet_time <'{today}'
        AND bet_time > '{yesterday}'
    """

    df = conn_collector_pg_hook.get_pandas_df(rawsql)

    return df


def get_ebet_wager():
    conn_collector_pg_hook = PostgresHook(postgres_conn_id="collector_conn_id")
    today = datetime.utcnow().replace(
        hour=UTC_EXEC_TIME, minute=0, second=0, microsecond=0
    )
    yesterday = today - timedelta(days=1)

    rawsql = f"""
        SELECT
            bet_history_id AS bet_id,
            valid_bet AS eligible_stake_amount, 
            user_name AS login_name
        FROM {EBET_WAGER_TABLE}
        WHERE create_time <'{today}'
        AND create_time > '{yesterday}'
    """

    df = conn_collector_pg_hook.get_pandas_df(rawsql)

    return df


def get_bti_wager():
    conn_collector_pg_hook = PostgresHook(postgres_conn_id="collector_conn_id")
    today = datetime.utcnow().replace(
        hour=UTC_EXEC_TIME, minute=0, second=0, microsecond=0
    )
    yesterday = today - timedelta(days=1)

    rawsql = f"""
        SELECT
            purchase_id AS bet_id,
            valid_stake AS eligible_stake_amount, 
            username AS login_name,
            currency
        FROM {BTI_WAGER_TABLE}
        WHERE creation_date <'{today}'
        AND creation_date > '{yesterday}'
    """

    df = conn_collector_pg_hook.get_pandas_df(rawsql)

    return df


def get_sabacv_wager():
    conn_collector_pg_hook = PostgresHook(postgres_conn_id="collector_conn_id")
    today = datetime.utcnow().replace(
        hour=UTC_EXEC_TIME, minute=0, second=0, microsecond=0
    )
    yesterday = today - timedelta(days=1)

    rawsql = f"""
        SELECT
            trans_id AS bet_id,
            stake AS eligible_stake_amount, 
            vendor_member_id AS login_name
            FROM {SABACV_WAGER_TABLE}
        WHERE transaction_time <'{today}'
        AND transaction_time > '{yesterday}'
    """

    df = conn_collector_pg_hook.get_pandas_df(rawsql)

    return df


def get_saba_wager():
    conn_collector_pg_hook = PostgresHook(postgres_conn_id="collector_conn_id")
    today = datetime.utcnow().replace(
        hour=UTC_EXEC_TIME, minute=0, second=0, microsecond=0
    )
    yesterday = today - timedelta(days=1)

    rawsql = f"""
        SELECT
            trans_id AS bet_id,
            stake AS eligible_stake_amount, 
            vendor_member_id AS login_name
            FROM {SABA_WAGER_TABLE}
        WHERE transaction_time <'{today}'
        AND transaction_time > '{yesterday}'
    """

    df = conn_collector_pg_hook.get_pandas_df(rawsql)

    return df


def get_saba_number():
    conn_collector_pg_hook = PostgresHook(postgres_conn_id="collector_conn_id")
    today = datetime.utcnow().replace(
        hour=UTC_EXEC_TIME, minute=0, second=0, microsecond=0
    )
    yesterday = today - timedelta(days=1)

    rawsql = f"""
        SELECT
            trans_id AS bet_id,
            stake AS eligible_stake_amount, 
            vendor_member_id AS login_name
            FROM {SABA_NUMBER_TABLE}
        WHERE transaction_time <'{today}'
        AND transaction_time > '{yesterday}'
    """

    df = conn_collector_pg_hook.get_pandas_df(rawsql)

    return df


def get_saba_virtual():
    conn_collector_pg_hook = PostgresHook(postgres_conn_id="collector_conn_id")
    today = datetime.utcnow().replace(
        hour=UTC_EXEC_TIME, minute=0, second=0, microsecond=0
    )
    yesterday = today - timedelta(days=1)

    rawsql = f"""
        SELECT
            trans_id AS bet_id,
            stake AS eligible_stake_amount, 
            vendor_member_id AS login_name
            FROM {SABA_VIRTUAL_TABLE}
        WHERE transaction_time <'{today}'
        AND transaction_time > '{yesterday}'
    """

    df = conn_collector_pg_hook.get_pandas_df(rawsql)

    return df


def get_tfgaming_wager():
    conn_collector_pg_hook = PostgresHook(postgres_conn_id="collector_conn_id")
    today = datetime.utcnow().replace(
        hour=UTC_EXEC_TIME, minute=0, second=0, microsecond=0
    )
    yesterday = today - timedelta(days=1)

    rawsql = f"""
        SELECT
            bet_history_id AS bet_id,
            amount AS eligible_stake_amount, 
            member_code AS login_name,
            currency
        FROM {TFGAMING_TABLE}
        WHERE date_created <'{today}'
        AND date_created > '{yesterday}'
    """

    df = conn_collector_pg_hook.get_pandas_df(rawsql)

    return df


def get_evolution_wager():
    conn_collector_pg_hook = PostgresHook(postgres_conn_id="collector_conn_id")
    today = datetime.utcnow().replace(
        hour=UTC_EXEC_TIME, minute=0, second=0, microsecond=0
    )
    yesterday = today - timedelta(days=1)

    rawsql = f"""
        SELECT
            transaction_id AS bet_id,
            stake AS eligible_stake_amount, 
            player_id AS login_name,
            currency
        FROM {EVOLUTION_TABLE}
        WHERE placed_on <'{today}'
        AND placed_on > '{yesterday}'
    """

    df = conn_collector_pg_hook.get_pandas_df(rawsql)

    return df


def get_genesis_wager():
    conn_collector_pg_hook = PostgresHook(postgres_conn_id="collector_conn_id")
    today = datetime.utcnow().replace(
        hour=UTC_EXEC_TIME, minute=0, second=0, microsecond=0
    )
    yesterday = today - timedelta(days=1)

    rawsql = f"""
        SELECT
            bet_history_id AS bet_id,
            bet AS eligible_stake_amount, 
            user_name AS login_name
        FROM {GENESIS_TABLE}
        WHERE create_time <'{today}'
        AND create_time > '{yesterday}'
    """

    df = conn_collector_pg_hook.get_pandas_df(rawsql)

    return df


def get_weworld_wager():
    conn_collector_pg_hook = PostgresHook(postgres_conn_id="collector_conn_id")
    today = datetime.utcnow().replace(
        hour=UTC_EXEC_TIME, minute=0, second=0, microsecond=0
    )
    yesterday = today - timedelta(days=1)

    rawsql = f"""
        SELECT
            bet_id,
            valid_bet_amount AS eligible_stake_amount, 
            player_id AS login_name
        FROM {WEWORLD_TABLE}
        WHERE bet_datetime <'{today}'
        AND bet_datetime > '{yesterday}'
    """

    df = conn_collector_pg_hook.get_pandas_df(rawsql)

    return df


def get_product_rebate_ratio():
    conn_reward_pg_hook = PostgresHook(postgres_conn_id="reward_conn_id")

    rawsql = f"""
        SELECT 
            product,
            currency,
            rebate_ratio,
            level
        FROM {PRODUCT_REBATE_RATIO_TABLE}
    """

    df = conn_reward_pg_hook.get_pandas_df(rawsql)

    return df


def get_level_setting():
    conn_reward_pg_hook = PostgresHook(postgres_conn_id="reward_conn_id")

    rawsql = f"""
        SELECT 
            currency,
            level,
            total_eligible_stake_amount
        FROM {LEVEL_SETTING_TABLE}
    """

    df = conn_reward_pg_hook.get_pandas_df(rawsql)

    return df


def get_level(x, level_setting_df):
    currency_ls = level_setting_df[level_setting_df["currency"] == x["currency"]]

    if currency_ls.shape[0] == 0:
        print("Currency Not Supported: ", x["currency"])
        print(x)

    level = 0
    for _, lv in currency_ls.iterrows():
        if level > lv["level"]:
            continue

        if x["eligible_stake_amount"] < lv["total_eligible_stake_amount"]:
            continue

        level = lv["level"]

    return level


def calc_rebate_amount(x):
    rebate_amount = x["eligible_stake_amount"] * x["rebate_ratio"] * 0.01

    return round(rebate_amount, 5)


def get_rebate_ratio(x, product_rebate_ratio_df):
    rebate_df = product_rebate_ratio_df[
        (product_rebate_ratio_df["product"] == x["product"])
        & (product_rebate_ratio_df["level"] == x["level"])
        & (product_rebate_ratio_df["currency"] == x["currency"])
    ]

    if rebate_df.shape[0] == 0:
        print("Currency not supported: ", x["currency"])
        return 0

    rebate_ratio = rebate_df.iloc[0]["rebate_ratio"]

    return rebate_ratio


def process_wagers(wager_df):
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    wager_df = wager_df.groupby(
        ["login_name", "currency", "product"], as_index=False, sort=False
    ).sum()

    ratio_df = get_product_rebate_ratio()
    level_df = get_level_setting()

    wager_df["level"] = wager_df.apply(lambda x: get_level(x, level_df), axis=1)
    wager_df["rebate_ratio"] = wager_df.apply(
        lambda x: get_rebate_ratio(x, ratio_df), axis=1
    )
    wager_df["rebate_amt"] = wager_df.apply(lambda x: calc_rebate_amount(x), axis=1)

    wager_df["status"] = REBATE_STATUS_PROCESSING

    wager_df["rebate_date"] = now
    wager_df["create_at"] = now
    wager_df["update_at"] = now

    wager_df = wager_df.drop(["level"], axis=1)
    print("Inserting ", wager_df.shape[0], " Data")

    conn_reward_pg_hook = PostgresHook(postgres_conn_id="reward_conn_id")
    engine_payment = conn_reward_pg_hook.get_sqlalchemy_engine()
    wager_df.to_sql(
        DAILY_REBATE_TABLE, con=engine_payment, if_exists="append", index=False
    )


@dag(
    "daily_rebate-v1.0.0",
    description="Calculates for Daily Rebate",
    schedule_interval="0 8 * * *",
    start_date=datetime(2023, 1, 14),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
)
def daily_rebate():
    init_identity_member_table()

    @task
    def calc_allbet_rebate():
        wager_df = get_allbet_wager()
        print("Processing ", wager_df.shape[0], " Data")
        if wager_df.shape[0] == 0:
            print("No Data Found")
            raise AirflowSkipException

        wager_df["currency"] = wager_df["currency"].replace("VND2", "VND")
        wager_df["currency"] = wager_df["currency"].replace("CNY", "RMB")
        wager_df["product"] = PRODUCT_CODE_ALLBET

        wager_df = wager_df.drop_duplicates(subset=["bet_id"])
        wager_df = wager_df.drop(["bet_id"], axis=1)

        process_wagers(wager_df)

    @task
    def calc_asiagaming_rebate():
        wager_df = get_asiagaming_wager()
        print("Processing ", wager_df.shape[0], " Data")
        if wager_df.shape[0] == 0:
            print("No Data Found")
            raise AirflowSkipException

        wager_df["currency"] = wager_df["currency"].replace("CNY", "RMB")
        wager_df["product"] = PRODUCT_CODE_ASIAGAMING

        wager_df = wager_df.drop_duplicates(subset=["bet_id"])
        wager_df = wager_df.drop(["bet_id"], axis=1)

        process_wagers(wager_df)

    @task
    def calc_agslot_rebate():
        wager_df = get_agslot_wager()
        print("Processing ", wager_df.shape[0], " Data")
        if wager_df.shape[0] == 0:
            print("No Data Found")
            raise AirflowSkipException

        wager_df["currency"] = wager_df["currency"].replace("CNY", "RMB")
        wager_df["product"] = PRODUCT_CODE_AGSLOT

        wager_df = wager_df.drop_duplicates(subset=["bet_id"])
        wager_df = wager_df.drop(["bet_id"], axis=1)

        process_wagers(wager_df)

    @task
    def calc_agyoplay_rebate():
        wager_df = get_agslot_wager()
        print("Processing ", wager_df.shape[0], " Data")
        if wager_df.shape[0] == 0:
            print("No Data Found")
            raise AirflowSkipException

        wager_df["currency"] = wager_df["currency"].replace("CNY", "RMB")
        wager_df["product"] = PRODUCT_CODE_AGYOPLAY

        wager_df = wager_df.drop_duplicates(subset=["bet_id"])
        wager_df = wager_df.drop(["bet_id"], axis=1)

        process_wagers(wager_df)

    @task
    def calc_sagaming_rebate():
        wager_df = get_sagaming_wager()
        print("Processing ", wager_df.shape[0], " Data")
        if wager_df.shape[0] == 0:
            print("No Data Found")
            raise AirflowSkipException

        wager_df["product"] = PRODUCT_CODE_SAGAMING

        wager_df = wager_df.drop_duplicates(subset=["bet_id"])
        wager_df = wager_df.drop(["bet_id"], axis=1)

        wager_df = get_member_currency(wager_df)

        process_wagers(wager_df)

    @task
    def calc_simpleplay_rebate():
        wager_df = get_simpleplay_wager()
        print("Processing ", wager_df.shape[0], " Data")
        if wager_df.shape[0] == 0:
            print("No Data Found")
            raise AirflowSkipException

        wager_df["product"] = PRODUCT_CODE_SPSLOT

        wager_df = wager_df.drop_duplicates(subset=["bet_id"])
        wager_df = wager_df.drop(["bet_id"], axis=1)

        wager_df = get_member_currency(wager_df)

        process_wagers(wager_df)

    @task
    def calc_simpleplayfisher_rebate():
        wager_df = get_simpleplayfisher_wager()
        print("Processing ", wager_df.shape[0], " Data")
        if wager_df.shape[0] == 0:
            print("No Data Found")
            raise AirflowSkipException

        wager_df["product"] = PRODUCT_CODE_SPFISH

        wager_df = wager_df.drop_duplicates(subset=["bet_id"])
        wager_df = wager_df.drop(["bet_id"], axis=1)

        wager_df = get_member_currency(wager_df)

        process_wagers(wager_df)

    @task
    def calc_pgsoft_rebate():
        wager_df = get_pgsoft_wager()
        print("Processing ", wager_df.shape[0], " Data")
        if wager_df.shape[0] == 0:
            print("No Data Found")
            raise AirflowSkipException

        wager_df["product"] = PRODUCT_CODE_PGSOFT

        wager_df = wager_df.drop_duplicates(subset=["bet_id"])
        wager_df = wager_df.drop(["bet_id"], axis=1)

        process_wagers(wager_df)

    @task
    def calc_ebet_rebate():
        wager_df = get_ebet_wager()
        print("Processing ", wager_df.shape[0], " Data")
        if wager_df.shape[0] == 0:
            print("No Data Found")
            raise AirflowSkipException

        wager_df["product"] = PRODUCT_CODE_EBETGAMING

        wager_df = wager_df.drop_duplicates(subset=["bet_id"])
        wager_df = wager_df.drop(["bet_id"], axis=1)

        wager_df = get_member_currency(wager_df)

        process_wagers(wager_df)

    @task
    def calc_bti_rebate():
        wager_df = get_bti_wager()
        print("Processing ", wager_df.shape[0], " Data")
        if wager_df.shape[0] == 0:
            print("No Data Found")
            raise AirflowSkipException

        wager_df["product"] = PRODUCT_CODE_BTISPORTS

        wager_df = wager_df.drop_duplicates(subset=["bet_id"])
        wager_df = wager_df.drop(["bet_id"], axis=1)

        process_wagers(wager_df)

    @task
    def calc_sabacv_rebate():
        wager_df = get_sabacv_wager()
        print("Processing ", wager_df.shape[0], " Data")
        if wager_df.shape[0] == 0:
            print("No Data Found")
            raise AirflowSkipException

        wager_df["product"] = PRODUCT_CODE_SABACV

        wager_df = wager_df.drop_duplicates(subset=["bet_id"])
        wager_df = wager_df.drop(["bet_id"], axis=1)

        wager_df = get_member_currency(wager_df)

        process_wagers(wager_df)

    @task
    def calc_saba_rebate():
        wager_df = get_saba_wager()
        print("Processing ", wager_df.shape[0], " Data")
        if wager_df.shape[0] == 0:
            print("No Data Found")
            raise AirflowSkipException

        wager_df["product"] = PRODUCT_CODE_SABA

        wager_df = wager_df.drop_duplicates(subset=["bet_id"])
        wager_df = wager_df.drop(["bet_id"], axis=1)

        wager_df = get_member_currency(wager_df)

        process_wagers(wager_df)

    @task
    def calc_sabanumbersgames_rebate():
        wager_df = get_saba_number()
        print("Processing ", wager_df.shape[0], " Data")
        if wager_df.shape[0] == 0:
            print("No Data Found")
            raise AirflowSkipException

        wager_df["product"] = PRODUCT_CODE_SABANUMBERGAME

        wager_df = wager_df.drop_duplicates(subset=["bet_id"])
        wager_df = wager_df.drop(["bet_id"], axis=1)

        wager_df = get_member_currency(wager_df)

        process_wagers(wager_df)

    @task
    def calc_sabavirtual_rebate():
        wager_df = get_saba_virtual()
        print("Processing ", wager_df.shape[0], " Data")
        if wager_df.shape[0] == 0:
            print("No Data Found")
            raise AirflowSkipException

        wager_df["product"] = PRODUCT_CODE_SABAVIRTUAL

        wager_df = wager_df.drop_duplicates(subset=["bet_id"])
        wager_df = wager_df.drop(["bet_id"], axis=1)

        wager_df = get_member_currency(wager_df)

        process_wagers(wager_df)

    @task
    def calc_tfgaming_rebate():
        wager_df = get_tfgaming_wager()
        print("Processing ", wager_df.shape[0], " Data")
        if wager_df.shape[0] == 0:
            print("No Data Found")
            raise AirflowSkipException

        wager_df["product"] = PRODUCT_CODE_TFGAMING

        wager_df = wager_df.drop_duplicates(subset=["bet_id"])
        wager_df = wager_df.drop(["bet_id"], axis=1)

        process_wagers(wager_df)

    @task
    def calc_evolution_rebate():
        wager_df = get_evolution_wager()
        print("Processing ", wager_df.shape[0], " Data")
        if wager_df.shape[0] == 0:
            print("No Data Found")
            raise AirflowSkipException

        wager_df["currency"] = wager_df["currency"].replace("VN2", "VND")
        wager_df["currency"] = wager_df["currency"].replace("CNY", "RMB")
        wager_df["product"] = PRODUCT_CODE_EVOLUTION

        wager_df = wager_df.drop_duplicates(subset=["bet_id"])
        wager_df = wager_df.drop(["bet_id"], axis=1)

        process_wagers(wager_df)

    @task
    def calc_genesis_rebate():
        wager_df = get_genesis_wager()
        print("Processing ", wager_df.shape[0], " Data")
        if wager_df.shape[0] == 0:
            print("No Data Found")
            raise AirflowSkipException

        wager_df["product"] = PRODUCT_CODE_GENESIS

        wager_df = wager_df.drop_duplicates(subset=["bet_id"])
        wager_df = wager_df.drop(["bet_id"], axis=1)

        wager_df = get_member_currency(wager_df)

        process_wagers(wager_df)

    @task
    def calc_weworld_rebate():
        wager_df = get_weworld_wager()
        print("Processing ", wager_df.shape[0], " Data")
        if wager_df.shape[0] == 0:
            print("No Data Found")
            raise AirflowSkipException

        wager_df["product"] = PRODUCT_CODE_WEWORLD

        wager_df = wager_df.drop_duplicates(subset=["bet_id"])
        wager_df = wager_df.drop(["bet_id"], axis=1)

        wager_df = get_member_currency(wager_df)

        process_wagers(wager_df)

    calc_allbet_rebate()
    calc_asiagaming_rebate()
    calc_agslot_rebate()
    calc_agyoplay_rebate()
    calc_sagaming_rebate()
    calc_simpleplay_rebate()
    calc_simpleplayfisher_rebate()
    calc_pgsoft_rebate()
    calc_ebet_rebate()
    calc_bti_rebate()
    calc_sabacv_rebate()
    calc_saba_rebate()
    calc_sabanumbersgames_rebate()
    calc_sabavirtual_rebate()
    calc_tfgaming_rebate()
    calc_evolution_rebate()
    calc_genesis_rebate()
    calc_weworld_rebate()


daily_rebate()
