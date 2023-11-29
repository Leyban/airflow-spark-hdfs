from airflow.providers.postgres.hooks.postgres import PostgresHook

def get_old_online_bank_df(bank_acc = None, date_from = None, date_to = None):

    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')

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
            amount
        FROM online_bank_data as d
        WHERE deposit_id  = 0
        AND CAST (transaction_date as DATE) >= '{begin}'
        AND CAST (transaction_date as DATE) <= '{end}'
    """
    df = conn_payment_pg_hook.get_pandas_df(rawsql)

    df['amount'] = pd.to_numeric(df['amount']) 

    return df


def get_deposits():
    from main_airflow.modules.auto_deposit import variables as var

    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')
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
        WHERE currency = '{var.DEFAULT_CURRENCY}'
        AND (payment_method_code = '{var.PAYMENT_METHOD_CODE_LBT}' OR payment_method_code = '{var.PAYMENT_METHOD_CODE_LBT60}') 
        AND status = {var.DEPOSIT_STATUS_PROCESSING}
    """
    df = conn_payment_pg_hook.get_pandas_df(rawsql)
    return df


def get_bank_accounts():
    from main_airflow.modules.auto_deposit import variables as var

    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')
    rawsql = f"""
        SELECT 
            id as bank_account_id,
            bank_id,
            account_no
        FROM bank_account
        WHERE auto_status = {var.BANK_ACC_AUTO_STATUS}
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


def update_bank_data(merged_df):
    from main_airflow.modules.auto_deposit import variables as var

    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')
    print("Updating Online Bank Data: ", merged_df.shape[0])
    sqls = []
    
    for _, row in merged_df.iterrows():
        update_online_bank_sql = f""" 
            UPDATE {var.ONLINE_BANK_ACCOUNT_TABLE} 
            SET deposit_id = '{row['deposit_id']}',
                transaction_id = '{row['transaction_id']}',
                deposit_amount = '{row['amount_x']}'
            WHERE id ='{row['online_bank_data_id']}' 
        """

        sqls.append(update_online_bank_sql)

    conn_payment_pg_hook.run(sqls)


def extract_bank_acc():
    from main_airflow.modules.auto_deposit import variables as var

    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')

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
        WHERE auto_status = '{var.BANK_ACC_AUTO_STATUS}' 
        AND status = '{var.BANK_ACCOUNT_STATUS_ACTIVE}'
    """
            
    bank_acc_df = conn_payment_pg_hook.get_pandas_df(rawsql)
        
    return bank_acc_df.to_dict('records')


def update_old_bank_data(new_bank_df, old_bank_df, row):
    import pandas as pd
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from main_airflow.modules.auto_deposit import variables as var
    from main_airflow.modules.auto_deposit import utils

    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')
    engine_payment = conn_payment_pg_hook.get_sqlalchemy_engine()

    new_bank_df['bank_account_id'] = row['bank_account_id']
    new_bank_df['bank_id'] = row['bank_id']

    new_bank_df['hash_id'] = new_bank_df.apply(utils.compute_hash, axis=1)

    print('New Data Count: ', new_bank_df.shape[0])

    bank_df = new_bank_df[~new_bank_df['hash_id'].isin(old_bank_df['hash_id'])]
    
    print("Inserting into DB: ", bank_df.shape[0])

    bank_df.to_sql(var.ONLINE_BANK_ACCOUNT_TABLE, con=engine_payment, if_exists='append', index=False)
    old_bank_df = pd.concat([old_bank_df, bank_df])

    return old_bank_df

