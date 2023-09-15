from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import  datetime

TMO_URL = 'https://api.louismmoo.com/api/viettinbank/transactions'
PAYMENT_TYPE_DLBT = "DLBT"
PAYMENT_TYPE_DLBT60 = "DLBT60"

BANK_ACCOUNT_STATUS_ACTIVE = 1
BANK_ACC_AUTO_STATUS = 1

BANK_ACCOUNT_TABLE = 'bank_account'
ONLINE_BANK_ACCOUNT_TABLE = 'online_bank_data'  
DEPOSIT_TABLE = 'deposit'

DEPOSIT_LOG_TABLE ='deposit_log'

DEPOSIT_STATUS_PROCESSING = 1
DEPOSIT_STATUS_REVIEWED = 5


dag = DAG(
    'auto_deposit_v1.0.0',
    description='Auto deposit proccessing',
    schedule_interval='*/20 * * * *', 
    start_date=datetime(2021, 1, 14), 
    catchup=False
)


def extract_bank_acc():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')
    engine_payment = conn_payment_pg_hook.get_sqlalchemy_engine()

    rawsql = f"""
        SELECT 
            ba.login_name as  username,
            ba.password,
            ba.account_no,
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


def get_old_online_bank_df(begin_str, end_str):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')

    rawsql = f"""
        SELECT 
            hash_id
        FROM online_bank_data as d
        WHERE CAST (transaction_date AS DATE) >= '{begin_str}' 
        AND CAST (transaction_date AS DATE) <= '{end_str}' 
    """

    df = conn_payment_pg_hook.get_pandas_df(rawsql)

    return df


def fetch_online_bank_data(username,password,accountNumber,begin,end, page):
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
        TMO_URL, 
        data = payload
    )

    result = req.json().get('data',{}).get('transactions', [])
    
    df = pd.DataFrame.from_records(result)  
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

def update_online_bank_data(begin,given_day):
    import pandas as pd
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')
    engine_payment = conn_payment_pg_hook.get_sqlalchemy_engine()

    begin_str = begin.strftime("%d/%m/%Y")
    end_str = given_day.strftime("%d/%m/%Y")

    bank_acc_df = extract_bank_acc()
    
    for index, row in bank_acc_df.iterrows():
        page = 0
        while True:
            print("Fetching Data for Page ", page)
            print( row['username'], row['password'], row['account_no'], begin_str, end_str, page)
            trans_df = fetch_online_bank_data(
                row['username'], 
                row['password'], 
                row['account_no'],
                begin_str, 
                end_str,
                page
            )

            if trans_df.empty:
                break
            
            new_bank_df = trans_df.loc[:, ['trxId','remark','amount','processDate']]
            new_bank_df = new_bank_df.rename(columns={
                'trxId': 'bank_reference',
                'remark':'bank_description',
                'amount':'net_amount',
                'processDate':'transaction_date'
            })

            new_bank_df['bank_account_id'] = row['bank_account_id']
            new_bank_df['bank_id'] = row['bank_id']

            new_bank_df['transaction_date'] = pd.to_datetime(new_bank_df['transaction_date'])

            new_bank_df['hash_id'] = new_bank_df.apply(compute_hash, axis=1)

            old_bank_df = get_old_online_bank_df(begin_str, end_str)
            print('New Data Count: ', new_bank_df.shape[0])

            bank_df = new_bank_df[~new_bank_df['hash_id'].isin(old_bank_df['hash_id'])]
            if bank_df.empty:
                print("All New Data are found in DB: Terminating Fetching")
                break
            
            print("Inserting into DB", bank_df.shape[0])

            bank_df.to_sql(ONLINE_BANK_ACCOUNT_TABLE, con=engine_payment, if_exists='append', index=False)

            page += 1


def get_online_bank_data():
    import pandas as pd
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')

    rawsql = """
        SELECT 
            id as online_bank_data_id,
            bank_account_id,
            bank_id,
            bank_reference,
            bank_description,
            net_amount as amount
        FROM online_bank_data as d
        WHERE deposit_id  = 0 
    """
    df = conn_payment_pg_hook.get_pandas_df(rawsql)

    df = df.rename(columns={'net_amount': 'amount'})
    df['amount'] = pd.to_numeric(df['amount']) 

    return df


def get_deposit(begin,end):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')

    rawsql = f"""
          SELECT 
            d.id as deposit_id,
            b.id as bank_id,
            d.bank_account_id,
            b.code as bank_code,
            d.ref_code,
            d.net_amount as amount,
            d.login_name,
            ba.account_no
        FROM deposit as d
        LEFT JOIN bank_account as ba ON ba.id = d.bank_account_id
        LEFT JOIN bank as b ON b.id = ba.bank_id  
        WHERE d.create_at >= '{begin}'
        AND d.create_at <= '{end}'
        AND ( d.payment_type_code = '{PAYMENT_TYPE_DLBT}' OR d.payment_type_code = '{PAYMENT_TYPE_DLBT60}' )
        AND d.status = {DEPOSIT_STATUS_PROCESSING} 
        AND ba.auto_status = {BANK_ACC_AUTO_STATUS}
    """

    df = conn_payment_pg_hook.get_pandas_df(rawsql)

    df.drop_duplicates(subset=['ref_code', 'amount', 'bank_account_id', 'bank_code', 'status'], keep='first', inplace=True)

    return df

def update_deposit(merged_df):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    print("Approving Deposits: ", merged_df.shape[0])

    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    for index, row in merged_df.iterrows():
        select_matched_deposit_id_sql = f"""
            SELECT 1
            FROM online_bank_data
            WHERE deposit_id = {row['deposit_id']}
        """

        update_online_bank_sql = f""" 
            UPDATE {ONLINE_BANK_ACCOUNT_TABLE} 
            SET deposit_id = '{row['deposit_id']}',
                update_at = '{now}'
            WHERE id ='{row['online_bank_data_id']}' 
        """

        df = conn_payment_pg_hook.get_pandas_df(select_matched_deposit_id_sql)

        if df.empty:
            conn_payment_pg_hook.run(update_online_bank_sql)

def get_matched(deposit_df, bank_df):
    import pandas as pd

    deposit_df = deposit_df.rename(columns={'amount':'amount_x'})
    bank_df = bank_df.rename(columns={'amount':'amount_y'})

    merged = pd.merge(deposit_df, bank_df, how='left', on=['bank_id', 'bank_account_id'])

    # Filters
    cd1 = lambda x: (x['amount_x'] == x['amount_y'])
    cd2 = lambda x: (x['ref_code'] == x['bank_reference']) 
    
    merged['result'] = merged.apply(lambda x: cd1(x) & cd2(x), axis=1) 
    new_merged_df = merged[merged['result']]

    return new_merged_df


def auto_deposit(date_from, date_to):
    date_format = '%Y-%m-%d %H:%M:%S'

    given_day = datetime.strptime(date_to, date_format)

    begin = datetime.strptime(date_from, date_format)
    
    update_online_bank_data(begin, given_day)

    bank_df = get_online_bank_data()
    if bank_df.empty:
        print("No Bank Data Found")
        return

    deposit_df = get_deposit(begin, given_day)
    if deposit_df.empty:
        print("No Deposits Found")
        return

    merged = get_matched(deposit_df, bank_df)
    if merged.empty:
        print("No Match Found")
        return
    
    update_deposit(merged)

    
auto_deposit_operator = PythonOperator(
    task_id='auto_deposit', 
    python_callable=auto_deposit,     
    op_kwargs={
        "date_from": "{{ dag_run.conf['date_from']}}",
        "date_to": "{{ dag_run.conf['date_to']}}"
    },
    dag=dag
)

auto_deposit_operator	