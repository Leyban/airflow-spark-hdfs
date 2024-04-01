from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook

# Define your DAG
dag = DAG(
    'hive_hook_example_v2',
    schedule_interval=None,  # You can set your desired schedule interval here
    start_date=days_ago(1),  # Change this to your desired start date
    catchup=False,
)

# Define a Python function to execute Hive SQL using HiveServer2Hook
def execute_hive_sql():
    # Create a HiveServer2Hook
    hive_hook = HiveServer2Hook(
        hive_cli_conn_id='hiveserver2_default',  # Specify your Hive connection ID
    )
    
    # Your Hive SQL script
    hive_sql_scripts =[  "CREATE DATABASE IF NOT EXISTS wagers", """CREATE TABLE IF NOT EXISTS pgsoft(
            id INT64,
            bet_id INT64,
            parent_bet_id INT64,
            player_name STRING,
            currency STRING,
            game_id INT,
            platform INT,
            bet_type INT,
            transaction_type INT,
            bet_amount FLOAT,
            win_amount FLOAT,
            jackpot_rtp_contribution_amount FLOAT,
            jackpot_win_amount FLOAT,
            balance_before FLOAT,
            balance_after FLOAT,
            row_version INT64,
            bet_time STRING,
            create_at STRING,
            update_at STRING
        )"""]
    
    # Execute the Hive SQL
    for sql in hive_sql_scripts:
        hive_hook.run(
            sql=sql,
        )

    # Close the session
    hive_hook.close()

# Define a PythonOperator that calls the execute_hive_sql function
hive_task = PythonOperator(
    task_id='hive_hook_task',
    python_callable=execute_hive_sql,
    dag=dag,
)

if __name__ == "__main__":
    dag.cli()
