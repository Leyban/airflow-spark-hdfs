from datetime import datetime, timedelta

# from airflow import DAG
# from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'leyban',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    default_args=default_args,
    dag_id='postgres_mapping_test',
    start_date=datetime(2023, 7, 23),
    schedule_interval="@daily",
    catchup=False
) 
def Postgres_Test():
    @task
    def print_stuff(x):
        print(x)

    @task
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
        """
                
        bank_acc_df = conn_payment_pg_hook.get_pandas_df(rawsql)
          
        return bank_acc_df.to_dict('records')

# printer = PythonOperator(
        # task_id = 'print stuff',
        # python_callable=print_stuff,
        # dag=dag
# )
            
    print_stuff.expand(x=extract_bank_acc())

Postgres_Test()
