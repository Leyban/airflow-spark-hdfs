from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook

# Define your DAG
dag = DAG(
    'hive_hook_example',
    schedule_interval=None,  # You can set your desired schedule interval here
    start_date=days_ago(1),  # Change this to your desired start date
    catchup=False,
)

# Define a Python function to initialize wagers db and pgsoft table
def init_pgsoft_wager_table():
    HIVE_WAREHOUSE_LOCATION = "/opt/hive/data/warehouse/"
    # Create a HiveServer2Hook
    hive_hook = HiveServer2Hook(
        hive_cli_conn_id='hiveserver2_default',  
    )
    
    # Your Hive SQL script
    create_wagers_db_sql = f"CREATE DATABASE IF NOT EXISTS wagers LOCATION '{HIVE_WAREHOUSE_LOCATION}'"
    create_pgsoft_table_sql = """CREATE TABLE IF NOT EXISTS wagers.pgsoft(
            id BIGINT,
            bet_id BIGINT,
            parent_bet_id BIGINT,
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
            row_version INT,
            bet_time TIMESTAMP,
            create_at TIMESTAMP,
            update_at TIMESTAMP
        ) 
        PARTITIONED BY ( year INT, quarter INT )
        STORED AS PARQUET
        """

    # Execute the Hive SQL
    hive_hook.run(sql=create_wagers_db_sql)
    hive_hook.run(sql=create_pgsoft_table_sql)

# Define a PythonOperator that calls the execute_hive_sql function
hive_task = PythonOperator(
    task_id='hive_hook_task',
    python_callable=init_pgsoft_wager_table,
    dag=dag,
)

hive_task
# if __name__ == "__main__":
#     dag.cli()
