from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import  datetime,timedelta

VTB_TMO_URL = 'https://api.louismmoo.com/api/viettinbank/transactions' 
PROVIDER_TMO = 1

VIETINBANK_CODE = 'VTB'

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
    catchup=False
)


def extract_bank_acc():
    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')

    rawsql = f"""
        SELECT 
            ba.login_name as  username,
            ba.password,
            ba.account_no,
            ba.provider,
            ba.id  as bank_account_id ,
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


def fetch_VTB_TMO_data(username,password,accountNumber,begin,end, page):
    import requests
    import pandas as pd

    payload = {
        "username":username,
        "password":password ,
        "accountNumber":accountNumber,
        "begin":begin,
        "end":end,
        "page":page
    }
    
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

    return new_bank_df


def update_online_bank_data(date_from,date_to):
    import pandas as pd
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')
    engine_payment = conn_payment_pg_hook.get_sqlalchemy_engine()

    begin_str = date_from.strftime("%d/%m/%Y")
    end_str = date_to.strftime("%d/%m/%Y")

    bank_acc_df = extract_bank_acc()
    
    for index, row in bank_acc_df.iterrows():
        page = 0
        while row['username']!= None and row['password']!= None and row['account_no']!= None and row['provider']!= None:
            print("Fetching Data for Page ", page)
            print( row['username'], row['password'], row['account_no'], begin_str, end_str, page)

            new_bank_df = pd.DataFrame()

            if row['bank_code'] == VIETINBANK_CODE:
                if row['provider'] == PROVIDER_TMO:
                    trans_df = fetch_VTB_TMO_data(
                        row['username'], 
                        row['password'], 
                        row['account_no'],
                        begin_str, 
                        end_str,
                        page
                    )
                    new_bank_df = trans_df

            if new_bank_df.empty:
                break

            new_bank_df['bank_account_id'] = row['bank_account_id']
            new_bank_df['bank_id'] = row['bank_id']

            new_bank_df['transaction_date'] = pd.to_datetime(new_bank_df['transaction_date'])

            new_bank_df['hash_id'] = new_bank_df.apply(compute_hash, axis=1)

            old_bank_df = get_old_online_bank_df(date_from, date_to)
            print('New Data Count: ', new_bank_df.shape[0])

            bank_df = new_bank_df[~new_bank_df['hash_id'].isin(old_bank_df['hash_id'])]
            if bank_df.empty:
                print("All New Data are found in DB: Terminating Fetching")
                break
            
            print("Inserting into DB", bank_df.shape[0])

            bank_df.to_sql(ONLINE_BANK_ACCOUNT_TABLE, con=engine_payment, if_exists='append', index=False)

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


def get_deposit(begin,end):
    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')

    rawsql = f"""
          SELECT 
            d.id as deposit_id,
            b.id as bank_id,
            d.bank_account_id,
            b.code as bank_code,
            d.ref_code,
            d.net_amount as amount,
            d.payment_type_code,
            d.login_name,
            ba.account_no,
            d.status
        FROM deposit as d
        LEFT JOIN bank_account as ba ON ba.id = d.bank_account_id
        LEFT JOIN bank as b ON b.id = ba.bank_id  
        WHERE CAST (d.create_at as DATE) >= '{begin}'
        AND CAST (d.create_at as DATE) <= '{end}'
        AND ( d.payment_type_code = '{PAYMENT_TYPE_DLBT}' OR d.payment_type_code = '{PAYMENT_TYPE_DLBT60}' )
        AND d.status = {DEPOSIT_STATUS_PROCESSING} 
        AND ba.auto_status = {BANK_ACC_AUTO_STATUS}
    """

    df = conn_payment_pg_hook.get_pandas_df(rawsql)

    return df

def clean_deposit_df(deposit_df):
    deposit_df.drop_duplicates(subset=['ref_code', 'amount', 'bank_account_id', 'bank_code'], keep='first', inplace=True)

    ## Filter processing 
    deposit_df = deposit_df[deposit_df['status'] == DEPOSIT_STATUS_PROCESSING]

    return deposit_df

def match(word,string):
    import re
    match_string = r'\b' + word + r'\b'
    return bool(re.search(match_string, string))

def filter_vtb(x):
    cd1 = (x['bank_code'] == 'VTB')

    cd2 = (x['amount_x'] == x['amount_y'])

    cd3 = match(str(x['ref_code']),str(x['bank_reference']).replace('credit','').replace(',','')) & (len(x['ref_code']) >=12)
    cd4 = match(str(x['ref_code']),str(x['bank_description']))

    return cd1 & cd2 & ( cd3 | cd4 ) 

def get_matched(deposit_df, bank_df):
    import pandas as pd

    deposit_df = deposit_df.rename(columns={'amount':'amount_x'})
    bank_df = bank_df.rename(columns={'amount':'amount_y'})

    merged = pd.merge(deposit_df, bank_df, how='left', on=['bank_id', 'bank_account_id'])
    
    conditions = lambda x: filter_vtb(x)

    merged['result'] = merged.apply(lambda x: conditions(x), axis=1) 
    new_merged_df = merged[merged['result']]

    # Drop duplicates
    new_merged_df = new_merged_df.drop_duplicates(subset=['deposit_id'])

    return new_merged_df

def update_bank_data(merged_df):
    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')
    print("Updating Online Bank Data: ", merged_df.shape[0])
    sqls = []
    
    for index, row in merged_df.iterrows():
        update_online_bank_sql = f""" 
            UPDATE {ONLINE_BANK_ACCOUNT_TABLE} 
            SET deposit_id = '{row['deposit_id']}' 
            WHERE id ='{row['online_bank_data_id']}' 
        """

        sqls.append(update_online_bank_sql)


    conn_payment_pg_hook.run(sqls)


def auto_deposit(**context):
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

    print("Getting Online Bank Data")
    bank_df = get_online_bank_data(date_from, date_to)
    if bank_df.empty:
        print("No Bank Data Found")
        return

    print("Getting Deposits")
    deposit_df = get_deposit(date_from, date_to)
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

    
auto_deposit_operator = PythonOperator(
    task_id='auto_deposit',
    python_callable=auto_deposit,
    dag=dag
)

auto_deposit_operator	