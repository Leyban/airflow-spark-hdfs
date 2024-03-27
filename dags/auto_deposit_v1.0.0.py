from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

# Online Bank Data URLs TMO
VTB_TMO_URL = "https://api.louismmoo.com/api/viettinbank/transactions"
ACB_TMO_URL = "https://api.louismmoo.com/api/acb/transactions-latest"
BIDV_TMO_URL = "https://api.louismmoo.com/api/bidv/get-transaction"
VCB_TMO_URL = "https://api.louismmoo.com/api/vcb/transactions"
EXIM_TMO_URL = "https://api.louismmoo.com/api/eximbank/transactions"
MBBANK_TMO_URL = "https://api.louismmoo.com/api/mbbank/transactions"

# Online Bank Data URLs TMO
VCB_ATT_URL = "http://167.71.208.49/api/history"

# Providers
PROVIDER_NONE = 0
PROVIDER_TMO = 1
PROVIDER_CASSO = 2
PROVIDER_ATT = 3
PROVIDER_KMD = 4

Provider_Value = {
    PROVIDER_NONE: "None",
    PROVIDER_TMO: "TMO",
    PROVIDER_CASSO: "CASSO",
    PROVIDER_ATT: "ATT",
    PROVIDER_KMD: "KMD",
}

# Bank Codes
VTB_CODE = "VTB"
ACB_CODE = "ACB"
BIDV_CODE = "BIDV"
MBB_CODE = "MBB"
VCB_CODE = "VCB"
TECH_CODE = "TECH"
VPBANK_CODE = "VPBANK"
TIMO_CODE = "TIMO"
EXIM_CODE = "EXIM"

# Payment Types
PAYMENT_METHOD_CODE_LBT = "DLBT"
PAYMENT_METHOD_CODE_LBT60 = "DLBT60"
DEFAULT_CURRENCY = "VND"

BANK_ACCOUNT_STATUS_ACTIVE = 1
BANK_ACC_AUTO_STATUS = 2

BANK_ACCOUNT_TABLE = "bank_account"
ONLINE_BANK_ACCOUNT_TABLE = "online_bank_data"
DEPOSIT_TABLE = "deposit"

DEPOSIT_LOG_TABLE = "deposit_log"

DEPOSIT_STATUS_PROCESSING = 1


def get_old_online_bank_df(bank_acc=None, date_from=None, date_to=None):
    conn_payment_pg_hook = PostgresHook(postgres_conn_id="payment_conn_id")

    rawsql = """
        SELECT 
            hash_id
        FROM online_bank_data as d
    """

    params = []

    if date_from != None:
        begin_str = date_from.strftime("%m/%d/%Y")
        params.append(f" CAST (transaction_date AS DATE) >= '{begin_str}' ")

    if date_to != None:
        end_str = date_to.strftime("%m/%d/%Y")
        params.append(f" CAST (transaction_date AS DATE) <= '{end_str}' ")

    if bank_acc != None:
        params.append(f" bank_id = {bank_acc['bank_id']} ")
        params.append(f" bank_account_id = {bank_acc['bank_account_id']} ")

    if len(params) > 0:
        rawsql += " WHERE "
        separator = " AND "
        rawsql += separator.join(param for param in params)

    df = conn_payment_pg_hook.get_pandas_df(rawsql)

    return df


def compute_hash(row):
    from hashlib import sha1

    hash_input = (
        str(row["bank_reference"])
        + str(row["bank_description"])
        + str(row["amount"])
        + str(row["transaction_date"].day)
        + str(row["transaction_date"].month)
        + str(row["transaction_date"].year)
    )
    return sha1(hash_input.encode("utf-8")).hexdigest()


def fetch_EXIM_TMO_data(payload, bank_acc, **context):
    import requests
    import pandas as pd

    res = requests.post(EXIM_TMO_URL, data=payload)

    if res.json().get("success") == False:
        print(f"An Error Occurred when fetching {bank_acc['bank_code'].upper()} api")
        print(res.json().get("message"))
        error_details = {
            "Account Name": bank_acc["username"],
            "Account Number": bank_acc["account_no"],
            "Provider": Provider_Value[bank_acc["provider"]],
        }
        notify_skype(
            f"{bank_acc['code'].upper()} | {Provider_Value[bank_acc['provider']]} Fetching Failed",
            error_details,
            res.json().get("message"),
            **context,
        )
        raise AirflowSkipException

    result = res.json().get("data", {}).get("currentHistoryList", [])

    trans_df = pd.DataFrame.from_records(result)

    if trans_df.empty:
        return trans_df

    new_bank_df = trans_df.loc[:, ["tranRef", "tranDesc", "amount", "tranTimeNoFormat"]]
    new_bank_df = new_bank_df.rename(
        columns={
            "tranRef": "bank_reference",
            "tranDesc": "bank_description",
            "tranTimeNoFormat": "transaction_date",
        }
    )

    new_bank_df["transaction_date"] = pd.to_datetime(
        new_bank_df["transaction_date"].astype(str), format="%Y%m%d%H%M%S"
    )

    return new_bank_df


def fetch_ACB_TMO_data(payload, bank_acc, **context):
    import requests
    import pandas as pd

    res = requests.post(ACB_TMO_URL, data=payload)

    if res.json().get("success") == False:
        print(f"An Error Occurred when fetching {bank_acc['bank_code'].upper()} api")
        print(res.json().get("message"))
        error_details = {
            "Account Name": bank_acc["username"],
            "Account Number": bank_acc["account_no"],
            "Provider": Provider_Value[bank_acc["provider"]],
        }
        notify_skype(
            f"{bank_acc['code'].upper()} | {Provider_Value[bank_acc['provider']]} Fetching Failed",
            error_details,
            res.json().get("message"),
            **context,
        )
        raise AirflowSkipException

    result = res.json().get("transactions", [])

    trans_df = pd.DataFrame.from_records(result)

    if trans_df.empty:
        return trans_df

    new_bank_df = trans_df.loc[
        :, ["Reference", "Description", "Amount", "TransactionDateFull"]
    ]
    new_bank_df = new_bank_df.rename(
        columns={
            "Reference": "bank_reference",
            "Description": "bank_description",
            "Amount": "amount",
            "TransactionDateFull": "transaction_date",
        }
    )

    new_bank_df["transaction_date"] = pd.to_datetime(
        new_bank_df["transaction_date"], format="%d/%m/%Y %H:%M:%S"
    )
    new_bank_df["amount"] = new_bank_df["amount"].str.replace(",", "")

    return new_bank_df


def fetch_VTB_TMO_data(payload, bank_acc, **context):
    import requests
    import pandas as pd

    res = requests.post(VTB_TMO_URL, data=payload)

    if res.json().get("success") == False:
        print(f"An Error Occurred when fetching {bank_acc['code'].upper()} api")
        print(res.json().get("message"))
        error_details = {
            "Account Name": bank_acc["username"],
            "Account Number": bank_acc["account_no"],
            "Provider": Provider_Value[bank_acc["provider"]],
        }
        notify_skype(
            f"{bank_acc['code'].upper()} | {Provider_Value[bank_acc['provider']]} Fetching Failed",
            error_details,
            res.json().get("message"),
            **context,
        )
        raise AirflowSkipException

    result = res.json().get("data", {}).get("transactions", [])

    trans_df = pd.DataFrame.from_records(result)

    if trans_df.empty:
        return trans_df

    new_bank_df = trans_df.loc[:, ["trxId", "remark", "amount", "processDate"]]
    new_bank_df = new_bank_df.rename(
        columns={
            "trxId": "bank_reference",
            "remark": "bank_description",
            "amount": "amount",
            "processDate": "transaction_date",
        }
    )

    new_bank_df["transaction_date"] = pd.to_datetime(
        new_bank_df["transaction_date"], format="%d-%m-%Y %H:%M:%S"
    )

    return new_bank_df


def fetch_VCB_ATT_data(payload, bank_acc, **context):
    import requests
    import pandas as pd

    res = requests.get(VCB_ATT_URL, params=payload)

    if not res.ok:
        print(f"An Error Occurred when fetching {bank_acc['code'].upper()} api")
        print(res.content)
        error_details = {
            "Account Name": bank_acc["username"],
            "Account Number": bank_acc["account_no"],
            "Provider": Provider_Value[bank_acc["provider"]],
        }
        notify_skype(
            f"{bank_acc['code'].upper()} | {Provider_Value[bank_acc['provider']]} Fetching Failed",
            error_details,
            res.content,
            **context,
        )
        raise AirflowSkipException

    result = res.json().get("data", [])

    trans_df = pd.DataFrame.from_records(result)

    if trans_df.empty:
        return trans_df

    trans_df = trans_df[trans_df["type"] == "deposit"]

    new_bank_df = trans_df.loc[:, ["referenceNumber", "memo", "money", "createdAt"]]
    new_bank_df = new_bank_df.rename(
        columns={
            "referenceNumber": "bank_reference",
            "memo": "bank_description",
            "money": "amount",
            "createdAt": "transaction_date",
        }
    )

    new_bank_df["transaction_date"] = pd.to_datetime(
        new_bank_df["transaction_date"], unit="s"
    )
    new_bank_df["amount"] = (
        new_bank_df["amount"].astype(str).apply(lambda x: x.replace(",", ""))
    )
    new_bank_df["amount"] = (
        new_bank_df["amount"].astype(str).apply(lambda x: x.replace("+", ""))
    )

    return new_bank_df


def update_old_bank_data(new_bank_df, old_bank_df, row):
    import pandas as pd
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    conn_payment_pg_hook = PostgresHook(postgres_conn_id="payment_conn_id")
    engine_payment = conn_payment_pg_hook.get_sqlalchemy_engine()

    new_bank_df["bank_account_id"] = row["bank_account_id"]
    new_bank_df["bank_id"] = row["bank_id"]

    new_bank_df["hash_id"] = new_bank_df.apply(compute_hash, axis=1)

    print("New Data Count: ", new_bank_df.shape[0])

    bank_df = new_bank_df[~new_bank_df["hash_id"].isin(old_bank_df["hash_id"])]

    print("Inserting into DB: ", bank_df.shape[0])

    bank_df.to_sql(
        ONLINE_BANK_ACCOUNT_TABLE, con=engine_payment, if_exists="append", index=False
    )
    old_bank_df = pd.concat([old_bank_df, bank_df])

    return old_bank_df


def notify_skype(title, details=None, error_message=None, **context):
    from airflow.models import Variable
    from skpy import Skype

    username = Variable.get("SKYPE_USERNAME")
    password = Variable.get("SKYPE_PASSWORD")
    group_id = Variable.get("SKYPE_GROUP_ID")

    sk = Skype(username, password)
    ch = sk.chats[group_id]

    log_url = context.get("task_instance").log_url

    message = f"[{title}]({log_url}) -- {datetime.now()}"

    if details != None:
        for k in details:
            message += f"\n{k}: {details[k]}"
        if error_message != None:
            message = message + f"Error Message: {error_message}"

    ch.sendMsg(message)


def update_ACB_TMO(bank_acc, maxRows, **context):
    print(f"Fetching {bank_acc['bank_code'].upper()} Data via TMO")

    payload = {
        "username": bank_acc["username"],
        "password": bank_acc["password"],
        "accountNumber": bank_acc["account_no"],
        "maxRows": maxRows,
    }

    print(payload)
    trans_df = fetch_ACB_TMO_data(payload, bank_acc, **context)
    if trans_df.empty:
        print("No Data Received")
        return

    date_from = trans_df["transaction_date"].min()
    date_to = trans_df["transaction_date"].max()

    old_bank_df = get_old_online_bank_df(date_from=date_from, date_to=date_to)

    update_old_bank_data(trans_df, old_bank_df, bank_acc)


def update_VTB_TMO(bank_acc, date_from, date_to, **context):
    print(f"Fetching {bank_acc['bank_code'].upper()} Data via TMO")

    old_bank_df = get_old_online_bank_df(date_from=date_from, date_to=date_to)

    begin_str = date_from.strftime("%d/%m/%Y")
    end_str = date_to.strftime("%d/%m/%Y")

    payload = {
        "username": bank_acc["username"],
        "password": bank_acc["password"],
        "accountNumber": bank_acc["account_no"],
        "begin": begin_str,
        "end": end_str,
    }

    page = 0
    old_data_count = -1

    while old_data_count < old_bank_df.shape[0]:
        old_data_count = old_bank_df.shape[0]
        payload["page"] = page

        print(payload)
        trans_df = fetch_VTB_TMO_data(payload, bank_acc, **context)

        if trans_df.empty:
            print("No New Data Received")
            break

        old_bank_df = update_old_bank_data(trans_df, old_bank_df, bank_acc)
        page += 1


def update_EXIM_TMO(bank_acc, date_from, date_to, **context):
    print(f"Fetching {bank_acc['bank_code'].upper()} Data via TMO")

    old_bank_df = get_old_online_bank_df(date_from=date_from, date_to=date_to)

    begin_str = date_from.strftime("%d/%m/%Y")
    end_str = date_to.strftime("%d/%m/%Y")

    payload = {
        "username": bank_acc["username"],
        "password": bank_acc["password"],
        "accountNumber": bank_acc["account_no"],
        "fromDate": begin_str,
        "toDate": end_str,
    }

    print(payload)
    trans_df = fetch_EXIM_TMO_data(payload, bank_acc, **context)
    if trans_df.empty:
        print("No Data Received")
        return

    update_old_bank_data(trans_df, old_bank_df, bank_acc)


def update_VCB_ATT(bank_acc, **context):
    from airflow.models import Variable

    print(f"Fetching {bank_acc['bank_code'].upper()} Data via ATT")

    accessToken = Variable.get("VCB_ATT_ACCESS_TOKEN")
    per_page = 100

    payload = {
        "accountNumber": bank_acc["account_no"],
        "accessToken": accessToken,
        "limit": per_page,
    }

    old_bank_df = get_old_online_bank_df(bank_acc=bank_acc)

    page = 0
    old_data_count = -1

    while old_data_count < old_bank_df.shape[0]:
        old_data_count = old_bank_df.shape[0]
        payload["offset"] = page * per_page

        print(payload)
        trans_df = fetch_VCB_ATT_data(payload, bank_acc, **context)

        if trans_df.empty:
            print("No New Data Received")
            break

        old_bank_df = update_old_bank_data(trans_df, old_bank_df, bank_acc)
        page += 1


def update_online_bank_data(bank_acc, date_from, date_to, maxRows, **context):
    if (
        bank_acc["username"] == None
        or bank_acc["password"] == None
        or bank_acc["account_no"] == None
        or bank_acc["provider"] <= PROVIDER_NONE
    ):
        print("Invalid Bank Account")
        print(bank_acc)

        error_details = {
            "Account Name": bank_acc["username"],
            "Account Number": bank_acc["account_no"],
            "Provider": Provider_Value[bank_acc["provider"]],
        }

        notify_skype(
            f"{bank_acc['code'].upper()} | Invalid Bank Account",
            error_details,
            **context,
        )
        raise AirflowSkipException

    print(
        f" '{bank_acc['username']}' '{bank_acc['password']}' '{bank_acc['account_no']}' '{bank_acc['bank_code']}' '{date_from}' '{date_to}' "
    )

    if bank_acc["provider"] == PROVIDER_TMO:
        if bank_acc["bank_code"].upper() == VTB_CODE:
            update_VTB_TMO(bank_acc, date_from, date_to, **context)

        elif bank_acc["bank_code"].upper() == ACB_CODE:
            update_ACB_TMO(bank_acc, maxRows, **context)

        elif bank_acc["bank_code"].upper() == EXIM_CODE:
            update_EXIM_TMO(bank_acc, date_from, date_to, **context)

        elif bank_acc["bank_code"].upper() == VCB_CODE:
            print("Not Implemented")  # TODO: Implement this
            raise AirflowSkipException

        elif bank_acc["bank_code"].upper() == BIDV_CODE:
            print("Not Implemented")  # TODO: Implement this
            raise AirflowSkipException

        elif bank_acc["bank_code"].upper() == MBB_CODE:
            print("Not Implemented")  # TODO: Implement this
            raise AirflowSkipException

        # Not sure if TMO supports these
        elif bank_acc["bank_code"].upper() == VPBANK_CODE:
            print("Not Implemented")  # TODO: Implement this
            raise AirflowSkipException

        elif bank_acc["bank_code"].upper() == TIMO_CODE:
            print("Not Implemented")  # TODO: Implement this
            raise AirflowSkipException

        elif bank_acc["bank_code"].upper() == TECH_CODE:
            print("Not Implemented")  # TODO: Implement this
            raise AirflowSkipException

        else:
            print("Invalid Bank Code: ", bank_acc["bank_code"])
            raise AirflowSkipException

    if bank_acc["provider"] == PROVIDER_ATT:
        if bank_acc["bank_code"].upper() == VCB_CODE:
            update_VCB_ATT(bank_acc, **context)


def get_online_bank_data(begin, end):
    import pandas as pd

    conn_payment_pg_hook = PostgresHook(postgres_conn_id="payment_conn_id")
    rawsql = f"""
        SELECT 
            id as online_bank_data_id,
            bank_account_id,
            bank_id,
            bank_reference,
            bank_description,
            amount
        FROM online_bank_data as d
        WHERE deposit_id  = 0
        AND CAST (transaction_date as DATE) >= '{begin}'
        AND CAST (transaction_date as DATE) <= '{end}'
    """
    df = conn_payment_pg_hook.get_pandas_df(rawsql)

    df["amount"] = pd.to_numeric(df["amount"])

    return df


def get_deposits():
    conn_payment_pg_hook = PostgresHook(postgres_conn_id="payment_conn_id")
    rawsql = f"""
        SELECT 
            id as deposit_id,
            bank_account_id,
            transaction_id,
            ref_code,
            net_amount as amount,
            status,
            payment_method_code
        FROM deposit
        WHERE currency = '{DEFAULT_CURRENCY}'
        AND (payment_method_code = '{PAYMENT_METHOD_CODE_LBT}' OR payment_method_code = '{PAYMENT_METHOD_CODE_LBT60}') 
        AND status = {DEPOSIT_STATUS_PROCESSING}
    """
    df = conn_payment_pg_hook.get_pandas_df(rawsql)
    return df


def get_bank_accounts():
    conn_payment_pg_hook = PostgresHook(postgres_conn_id="payment_conn_id")
    rawsql = f"""
        SELECT 
            id as bank_account_id,
            bank_id,
            account_no
        FROM bank_account
        WHERE auto_status = {BANK_ACC_AUTO_STATUS}
    """
    df = conn_payment_pg_hook.get_pandas_df(rawsql)
    return df


def get_banks():
    conn_payment_pg_hook = PostgresHook(postgres_conn_id="payment_conn_id")
    rawsql = f"""
        SELECT 
            id as bank_id,
            code as bank_code
        FROM bank
    """
    df = conn_payment_pg_hook.get_pandas_df(rawsql)
    return df


def get_valid_deposits():
    deposit_df = get_deposits()

    bank_acc_df = get_bank_accounts()

    merged_df = deposit_df.merge(bank_acc_df, "left", "bank_account_id")

    bank_df = get_banks()

    merge_df = merged_df.merge(bank_df, "left", "bank_id")

    return merge_df


def clean_deposit_df(deposit_df):
    deposit_df = deposit_df.dropna(subset=["bank_id"])
    deposit_df.drop_duplicates(
        subset=["ref_code", "amount", "bank_account_id", "bank_code"],
        keep="first",
        inplace=True,
    )

    return deposit_df


def match(word, string):
    import re

    match_string = r"\b" + word + r"\b"
    return bool(re.search(match_string, string))


def filter_vcb(x):
    cd1 = x["bank_code"].upper() == VCB_CODE
    cd2 = x["amount_x"] == x["amount_y"]
    cd3 = match(str(x["ref_code"]), str(x["bank_description"]))
    return cd1 & cd2 & cd3


def filter_acb(x):
    cd1 = x["bank_code"].upper() == ACB_CODE
    cd2 = x["amount_x"] == x["amount_y"]
    cd3 = match(str(x["ref_code"]), str(x["bank_description"]))
    return cd1 & cd2 & cd3


def filter_vtb(x):
    cd1 = x["bank_code"].upper() == VTB_CODE
    cd2 = x["amount_x"] == x["amount_y"]
    # cd3 = match(str(x['ref_code']),str(x['bank_reference']).replace('credit','').replace(',','')) & (len(x['ref_code']) >=12)
    cd3 = match(str(x["ref_code"]), str(x["bank_description"]))
    return cd1 & cd2 & cd3


def filter_exim(x):
    cd1 = x["bank_code"].upper() == EXIM_CODE
    cd2 = x["amount_x"] == x["amount_y"]
    cd3 = match(str(x["ref_code"]), str(x["bank_description"]))
    return cd1 & cd2 & cd3


def get_matched(deposit_df, bank_df):
    import pandas as pd

    deposit_df = deposit_df.rename(columns={"amount": "amount_x"})
    bank_df = bank_df.rename(columns={"amount": "amount_y"})
    bank_df["amount_y"] = round(bank_df["amount_y"].astype(float) * 0.001, 5)

    merged = pd.merge(
        deposit_df, bank_df, how="left", on=["bank_id", "bank_account_id"]
    )

    conditions = (
        lambda x: filter_vtb(x) | filter_exim(x) | filter_acb(x) | filter_vcb(x)
    )

    merged["result"] = merged.apply(lambda x: conditions(x), axis=1)
    new_merged_df = merged[merged["result"]]

    # Cleaning
    new_merged_df = new_merged_df.drop_duplicates(subset=["deposit_id"])
    new_merged_df["deposit_id"] = new_merged_df["deposit_id"].astype("int")
    new_merged_df["online_bank_data_id"] = new_merged_df["online_bank_data_id"].astype(
        "int"
    )

    return new_merged_df


def update_bank_data(merged_df):
    conn_payment_pg_hook = PostgresHook(postgres_conn_id="payment_conn_id")
    print("Updating Online Bank Data: ", merged_df.shape[0])
    sqls = []

    for _, row in merged_df.iterrows():
        update_online_bank_sql = f""" 
            UPDATE {ONLINE_BANK_ACCOUNT_TABLE} 
            SET deposit_id = '{row['deposit_id']}',
                transaction_id = '{row['transaction_id']}',
                deposit_amount = '{row['amount_x']}'
            WHERE id ='{row['online_bank_data_id']}' 
        """

        sqls.append(update_online_bank_sql)

    conn_payment_pg_hook.run(sqls)


@dag(
    "auto_deposit_v1.0.0",
    description="Auto deposit proccessing",
    schedule_interval="*/1 * * * *",
    start_date=datetime(2023, 1, 14),
    catchup=False,
    max_active_runs=1,
)
def Auto_Deposit():
    def extract_bank_acc():
        conn_payment_pg_hook = PostgresHook(postgres_conn_id="payment_conn_id")

        rawsql = f"""
            SELECT 
                ba.code,
                ba.login_name as  username,
                ba.password,
                ba.account_no,
                ba.provider,
                ba.id as bank_account_id,
                b.code as bank_code,
                b.id as bank_id
            FROM bank_account AS ba 
            LEFT JOIN bank AS b ON b.id = ba.bank_id 
            WHERE auto_status = '{BANK_ACC_AUTO_STATUS}' 
            AND status = '{BANK_ACCOUNT_STATUS_ACTIVE}'
        """

        bank_acc_df = conn_payment_pg_hook.get_pandas_df(rawsql)

        return bank_acc_df.to_dict("records")

    @task(max_active_tis_per_dag=1)
    def update_OBD(bank_acc, **context):
        date_to = datetime.utcnow()
        date_from = date_to - timedelta(days=60)
        maxRows = 200

        # Taking Optional Date Parameters
        date_format = "%Y-%m-%d %H:%M:%S"
        if "date_from" in context["params"]:
            date_from = datetime.strptime(context["params"]["date_from"], date_format)

        if "date_to" in context["params"]:
            date_to = datetime.strptime(context["params"]["date_to"], date_format)

        if "max_rows" in context["params"]:
            maxRows = context["params"]["max_rows"]

        print("Updating Onlne Bank Data")
        update_online_bank_data(bank_acc, date_from, date_to, maxRows, **context)

    @task(trigger_rule="all_done")
    def auto_deposit(**context):
        date_to = datetime.utcnow()
        date_from = date_to - timedelta(days=60)

        # Taking Optional Date Parameters
        date_format = "%Y-%m-%d %H:%M:%S"
        if "date_from" in context["params"]:
            date_from = datetime.strptime(context["params"]["date_from"], date_format)

        if "date_to" in context["params"]:
            date_to = datetime.strptime(context["params"]["date_to"], date_format)

        print("Getting Online Bank Data")
        bank_df = get_online_bank_data(date_from, date_to)
        if bank_df.empty:
            print("No Bank Data Found")
            return

        print("Getting Deposits")
        deposit_df = get_valid_deposits()
        if deposit_df.empty:
            print("No Deposits Found")
            return

        print("Cleaning Deposit")
        filtered_deposit_df = clean_deposit_df(deposit_df)
        if filtered_deposit_df.empty:
            print("No Usable Deposits Found")
            return

        print("Merging Deposit and Bank Dataframes")
        merged = get_matched(filtered_deposit_df, bank_df)
        if merged.empty:
            print("No Match Found")
            return

        update_bank_data(merged)

    updating_OBD = update_OBD.expand(bank_acc=extract_bank_acc())

    auto_deposit_instance = auto_deposit()

    updating_OBD >> auto_deposit_instance


Auto_Deposit()
