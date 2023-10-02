from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import  datetime,timedelta

# Online Bank Data URLs
VTB_TMO_URL = 'https://api.louismmoo.com/api/viettinbank/transactions' 
BIDV_TMO_URL = 'https://api.louismmoo.com/api/bidv/get-transaction'
ACB_TMO_URL = 'https://api.louismmoo.com/api/acb/transactions-latest'
VCB_TMO_URL = 'https://api.louismmoo.com/api/vcb/transactions'
EXIM_TMO_URL = 'https://api.louismmoo.com/api/eximbank/transactions'
MBBANK_TMO_URL = 'https://api.louismmoo.com/api/mbbank/transactions'

# Providers
PROVIDER_NONE = 0
PROVIDER_TMO = 1
PROVIDER_CASSO = 2
PROVIDER_ATT = 3
PROVIDER_KMD = 4

# Bank Codes
VTB_CODE = 'VTB'
VCB_CODE = 'VCB'
ACB_CODE = 'ACB'
MBBANK_CODE = 'MBBANK'
TECH_CODE = 'TECH'
VPBANK_CODE = 'VPBANK'
BIDV_CODE = 'BIDV'
TIMO_CODE = 'TIMO'
EXIM_CODE = 'EXIM'

# Payment Types
PAYMENT_TYPE_DLBT = "DLBT"
PAYMENT_TYPE_DLBT60 = "DLBT60"

BANK_ACCOUNT_STATUS_ACTIVE = 1
BANK_ACC_AUTO_STATUS = 2

BANK_ACCOUNT_TABLE = 'bank_account'
ONLINE_BANK_ACCOUNT_TABLE = 'online_bank_data'  
DEPOSIT_TABLE = 'deposit'

DEPOSIT_LOG_TABLE ='deposit_log'

DEPOSIT_STATUS_PROCESSING = 1


dag = DAG(
    'auto_deposit_v1.0.0',
    description='Auto deposit proccessing',
    schedule_interval='*/1 * * * *', 
    start_date=datetime(2023, 1, 14), 
    catchup=False,
    max_active_runs=1
)


def extract_bank_acc():
    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')

    rawsql = f"""
        SELECT 
            ba.login_name as  username,
            ba.password,
            ba.account_no,
            ba.provider,
            ba.id as bank_account_id ,
            b.code as bank_code,
            b.id as bank_id
        FROM bank_account AS ba 
        LEFT JOIN bank AS b ON b.id = ba.bank_id 
        WHERE auto_status = '{BANK_ACC_AUTO_STATUS}' 
        AND status = '{BANK_ACCOUNT_STATUS_ACTIVE}'
    """
            
    bank_acc_df = conn_payment_pg_hook.get_pandas_df(rawsql)
      
    return bank_acc_df


def get_old_online_bank_df(date_from, date_to):
    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')

    begin_str = date_from.strftime("%m/%d/%Y")
    end_str = date_to.strftime("%m/%d/%Y")

    rawsql = f"""
        SELECT 
            hash_id
        FROM online_bank_data as d
        WHERE CAST (transaction_date AS DATE) >= '{begin_str}' 
        AND CAST (transaction_date AS DATE) <= '{end_str}' 
    """

    df = conn_payment_pg_hook.get_pandas_df(rawsql)

    return df


def compute_hash(row):
    from hashlib import sha1

    hash_input = (
        str( row['bank_reference'] ) +
        str( row['bank_description'] ) +
        str( row['net_amount'] ) +
        str( row['transaction_date'].day ) +
        str( row['transaction_date'].month ) +
        str( row['transaction_date'].year )
    )
    return sha1(hash_input.encode('utf-8')).hexdigest()   

def fetch_EXIM_TMO_data(payload):
    import requests
    import pandas as pd
    
    req = requests.post(
        EXIM_TMO_URL, 
        data = payload
    )

    result = req.json().get('data',{}).get('currentHistoryList', [])
    
    trans_df = pd.DataFrame.from_records(result)  

    if trans_df.empty:
        return trans_df

    new_bank_df = trans_df.loc[:, ['tranRef','tranDesc','amount','tranTimeNoFormat']]
    new_bank_df = new_bank_df.rename(columns={
        'tranRef': 'bank_reference',
        'tranDesc':'bank_description',
        'amount':'net_amount',
        'tranTimeNoFormat':'transaction_date'
    })
 
    new_bank_df['transaction_date'] = pd.to_datetime(new_bank_df['transaction_date'].astype(str), format='%Y%m%d%H%M%S')

    return new_bank_df

def fetch_VTB_TMO_data(payload):
    import requests
    import pandas as pd
    
    req =requests.post(
        VTB_TMO_URL, 
        data = payload
    )

    result = req.json().get('data',{}).get('transactions', [])
    
    trans_df = pd.DataFrame.from_records(result)  

    if trans_df.empty:
        return trans_df

    new_bank_df = trans_df.loc[:, ['trxId','remark','amount','processDate']]
    new_bank_df = new_bank_df.rename(columns={
        'trxId': 'bank_reference',
        'remark':'bank_description',
        'amount':'net_amount',
        'processDate':'transaction_date'
    })

    new_bank_df['transaction_date'] = pd.to_datetime(new_bank_df['transaction_date'], format='%d-%m-%Y %H:%M:%S')

    return new_bank_df


def update_online_bank_data(date_from,date_to):
    import pandas as pd
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')
    engine_payment = conn_payment_pg_hook.get_sqlalchemy_engine()

    begin_str = date_from.strftime("%d/%m/%Y")
    end_str = date_to.strftime("%d/%m/%Y")

    bank_acc_df = extract_bank_acc()

    old_bank_df = get_old_online_bank_df(date_from, date_to)
    
    for _, row in bank_acc_df.iterrows():
        page = 0
        while row['username']!= None and row['password']!= None and row['account_no']!= None and row['provider'] > PROVIDER_NONE:
            print( row['username'], row['password'], row['account_no'], begin_str, end_str, page)

            new_bank_df = pd.DataFrame()

            if row['provider'] == PROVIDER_TMO:
  
                payload = {
                    "username":row['username'],
                    "password":row['password'],
                    "accountNumber":row['account_no'],
                }
                
                if row['bank_code'] == VTB_CODE.lower():
                    print("Fetching VTB Data via TMO for page: ", page)

                    payload["begin"] = begin_str
                    payload["end"] = end_str
                    payload["page"] = page

                    print(payload)
                    trans_df = fetch_VTB_TMO_data(payload)
                    new_bank_df = trans_df

                elif row['bank_code'] == BIDV_CODE.lower():
                    print("Not Implemented") # TODO: Implement this

                elif row['bank_code'] == ACB_CODE.lower():
                    print("Not Implemented") # TODO: Implement this

                elif row['bank_code'] == VCB_CODE.lower():
                    print("Not Implemented") # TODO: Implement this

                elif row['bank_code'] == EXIM_CODE.lower():
                    print("Fetching EXIM Data via TMO")

                    payload["fromDate"] = begin_str
                    payload["toDate"] = end_str

                    print(payload)
                    trans_df = fetch_EXIM_TMO_data(payload)
                    new_bank_df = trans_df

                elif row['bank_code'] == MBBANK_CODE.lower():
                    print("Not Implemented") # TODO: Implement this

                # Not sure if TMO supports these
                elif row['bank_code'] == VPBANK_CODE.lower():
                    print("Not Implemented") # TODO: Implement this

                elif row['bank_code'] == TIMO_CODE.lower():
                    print("Not Implemented") # TODO: Implement this

                elif row['bank_code'] == TECH_CODE.lower():
                    print("Not Implemented") # TODO: Implement this


            if new_bank_df.empty:
                print("No Data Received")
                break

            new_bank_df['bank_account_id'] = row['bank_account_id']
            new_bank_df['bank_id'] = row['bank_id']

            new_bank_df['hash_id'] = new_bank_df.apply(compute_hash, axis=1)

            print('New Data Count: ', new_bank_df.shape[0])

            bank_df = new_bank_df[~new_bank_df['hash_id'].isin(old_bank_df['hash_id'])]
            if bank_df.empty:
                print("All New Data are found in DB: Terminating Fetching")
                break
            
            print("Inserting into DB: ", bank_df.shape[0])

            bank_df.to_sql(ONLINE_BANK_ACCOUNT_TABLE, con=engine_payment, if_exists='append', index=False)
            old_bank_df = pd.concat([old_bank_df, bank_df])

            if row['bank_code'] != VTB_CODE.lower():
                break

            page += 1


def get_online_bank_data(begin, end):
    import pandas as pd

    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')

    rawsql = f"""
        SELECT 
            id as online_bank_data_id,
            bank_account_id,
            bank_id,
            bank_reference,
            bank_description,
            net_amount as amount
        FROM online_bank_data as d
        WHERE deposit_id  = 0
        AND CAST (transaction_date as DATE) >= '{begin}'
        AND CAST (transaction_date as DATE) <= '{end}'
    """
    df = conn_payment_pg_hook.get_pandas_df(rawsql)

    df = df.rename(columns={'net_amount': 'amount'})
    df['amount'] = pd.to_numeric(df['amount']) 

    return df


def get_deposits():
    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')

    rawsql = f"""
        SELECT 
            id as deposit_id,
            bank_account_id,
            transaction_id,
            ref_code,
            net_amount as amount,
            payment_type_code,
            login_name,
            status
        FROM deposit 
        WHERE ( payment_type_code = '{PAYMENT_TYPE_DLBT}' OR payment_type_code = '{PAYMENT_TYPE_DLBT60}' )
        AND status = {DEPOSIT_STATUS_PROCESSING} 
    """

    df = conn_payment_pg_hook.get_pandas_df(rawsql)

    return df

def get_bank_accounts():
    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')

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
    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')

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

    merged_df = deposit_df.merge(bank_acc_df, 'left', 'bank_account_id')

    bank_df = get_banks()

    merge_df = merged_df.merge(bank_df, 'left', 'bank_id')

    return merge_df


def clean_deposit_df(deposit_df):
    deposit_df = deposit_df.dropna(subset=['bank_id'])
    deposit_df.drop_duplicates(subset=['ref_code', 'amount', 'bank_account_id', 'bank_code'], keep='first', inplace=True)

    return deposit_df

def match(word,string):
    import re
    match_string = r'\b' + word + r'\b'
    return bool(re.search(match_string, string))

def filter_vtb(x):
    cd1 = (x['bank_code'] == VTB_CODE.lower())
    cd2 = (x['amount_x'] == x['amount_y'])
    # cd3 = match(str(x['ref_code']),str(x['bank_reference']).replace('credit','').replace(',','')) & (len(x['ref_code']) >=12)
    cd3 = match(str(x['ref_code']),str(x['bank_description']))
    return cd1 & cd2 & cd3 

def filter_exim(x):
    cd1 = (x['bank_code'] == EXIM_CODE.lower())
    cd2 = (x['amount_x'] == x['amount_y'])
    # cd3 = match(str(x['ref_code']),str(x['bank_reference']).replace('credit','').replace(',','')) & (len(x['ref_code']) >=12)
    cd3 = match(str(x['ref_code']),str(x['bank_description']))
    return cd1 & cd2 & cd3 

def get_matched(deposit_df, bank_df):
    import pandas as pd

    deposit_df = deposit_df.rename(columns={'amount':'amount_x'})
    bank_df = bank_df.rename(columns={'amount':'amount_y'})
    bank_df['amount_y'] = round(bank_df['amount_y'].astype(float) * 0.001, 5)

    merged = pd.merge(deposit_df, bank_df, how='left', on=['bank_id', 'bank_account_id'])
    
    conditions = lambda x: filter_vtb(x) \
        | filter_exim(x)

    merged['result'] = merged.apply(lambda x: conditions(x), axis=1) 
    new_merged_df = merged[merged['result']]

    # Cleaning
    new_merged_df = new_merged_df.drop_duplicates(subset=['deposit_id'])
    new_merged_df['deposit_id'] = new_merged_df['deposit_id'].astype('int')
    new_merged_df['online_bank_data_id'] = new_merged_df['online_bank_data_id'].astype('int')

    return new_merged_df

def update_bank_data(merged_df):
    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')
    print("Updating Online Bank Data: ", merged_df.shape[0])
    sqls = []
    
    for _, row in merged_df.iterrows():
        update_online_bank_sql = f""" 
            UPDATE {ONLINE_BANK_ACCOUNT_TABLE} 
            SET deposit_id = '{row['deposit_id']}' ,
                transaction_id = '{row['transaction_id']}'
            WHERE id ='{row['online_bank_data_id']}' 
        """

        sqls.append(update_online_bank_sql)


    conn_payment_pg_hook.run(sqls)

def update_OBD(**context):
    try:
        date_to = datetime.utcnow()
        date_from = date_to - timedelta(hours=3)

        # Taking Optional Date Parameters
        date_format = '%Y-%m-%d %H:%M:%S'
        if 'date_from' in context['params']:
            date_from = datetime.strptime(context['params']['date_from'], date_format)
        
        if 'date_to' in context['params']:
            date_to = datetime.strptime(context['params']['date_to'], date_format)

        print("Updating Onlne Bank Data")
        update_online_bank_data(date_from, date_to)

    except Exception as error:
        print("An error occurred:", type(error).__name__, "-", error)
        exit(1)

def auto_deposit(**context):
    try: 
        date_to = datetime.utcnow()
        date_from = date_to - timedelta(hours=3)

        # Taking Optional Date Parameters
        date_format = '%Y-%m-%d %H:%M:%S'
        if 'date_from' in context['params']:
            date_from = datetime.strptime(context['params']['date_from'], date_format)
        
        if 'date_to' in context['params']:
            date_to = datetime.strptime(context['params']['date_to'], date_format)

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

    except Exception as error:
        print("An error occurred:", type(error).__name__, "-", error)
        exit(1)
    
update_OBD_operator = PythonOperator(
    task_id='update_online_bank_data',
    python_callable = update_OBD,
    dag=dag
)

auto_deposit_operator = PythonOperator(
    task_id='auto_deposit',
    python_callable=auto_deposit,
    dag=dag
)

update_OBD_operator >> auto_deposit_operator
