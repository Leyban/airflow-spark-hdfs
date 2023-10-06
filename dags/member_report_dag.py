from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime

import json


MEMBER_PAYMENT_ACCOUNT_TABLE ='member_payment_account'
MEMBER_ACCOUNT_TABLE ='member_account'
MEMBER_REPORT_TABLE ='member_report'

dag = DAG('calculate_member_report', description='member report processing',
           schedule_interval='*/2 * * * *', start_date=datetime(2023, 8, 30), catchup=False)


def member_report():
    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')

    query = """
        SELECT 
            ma.login_name,
            mpa.detail->>'account_no' as account_no ,
            mpa.detail->>'account_name' as account_name, 
			mpa.detail->>'bank_code'  as bank_code,
			mpa.verify_status as status
        FROM {0} as mpa LEFT JOIN {1} as ma ON mpa.member_id = ma.member_id
		
    """.format(MEMBER_PAYMENT_ACCOUNT_TABLE,MEMBER_ACCOUNT_TABLE)


    df = conn_payment_pg_hook.get_pandas_df(query)
    if not df.empty:
        columns_to_check = ['account_no']
        df['is_duplicate'] = df.duplicated(subset=columns_to_check, keep=False)
        save_member_report(df)

def save_member_report(df):
    conn_risk_pg_hook = PostgresHook(postgres_conn_id='risk_conn_id')
    connection = conn_risk_pg_hook.get_conn()
    cursor = connection.cursor()

    try:
        cursor.execute('BEGIN;')

        for _, row in df.iterrows():
            payment_account = [{
                'login_name': row['login_name'],
                'account_no': row['account_no'], 
                'account_name': row['account_name'],
                'bank_code':row['bank_code'],
                'status':row['status'],
                'is_duplicate':row['is_duplicate']}]

            sql =  """
                INSERT INTO {0} (login_name, payment_account)
                VALUES ('{1}', '{2}')
                ON CONFLICT (login_name) DO UPDATE
                SET payment_account = '{3}'
                WHERE member_report.login_name = '{4}'
            """.format(MEMBER_REPORT_TABLE, row['login_name'], json.dumps(payment_account), json.dumps(payment_account), row['login_name'])

            cursor.execute(sql)

    except Exception as e:
        print(f"Error occurred: {e}")
        connection.rollback() 
        raise AirflowException
    finally:
        cursor.close()
        connection.close()


download_pgsoft_operator = PythonOperator(
    task_id='calculate_member_report', python_callable=member_report, dag=dag)

download_pgsoft_operator
