from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


@task
def create_deposits():
    import pandas as pd

    deposit_csv_path = "./data/monthly_commission/mock/deposit.csv"
    deposit_df = pd.read_csv(deposit_csv_path)

    conn_payment_pg_hook = PostgresHook(postgres_conn_id="payment_conn_id")
    engine_payment = conn_payment_pg_hook.get_sqlalchemy_engine()

    deposit_df.to_sql("deposit", engine_payment, if_exists="append", index=False)


@task
def create_withdrawals():
    import pandas as pd

    withdrawal_csv_path = "./data/monthly_commission/mock/withdrawal.csv"
    withdrawal_df = pd.read_csv(withdrawal_csv_path)

    conn_payment_pg_hook = PostgresHook(postgres_conn_id="payment_conn_id")
    engine_payment = conn_payment_pg_hook.get_sqlalchemy_engine()

    withdrawal_df.to_sql("withdrawal", engine_payment, if_exists="append", index=False)


@task
def create_adjustments():
    import pandas as pd

    adjustment_csv_path = "./data/monthly_commission/mock/adjustment.csv"
    adjustment_df = pd.read_csv(adjustment_csv_path)

    conn_payment_pg_hook = PostgresHook(postgres_conn_id="payment_conn_id")
    engine_payment = conn_payment_pg_hook.get_sqlalchemy_engine()

    adjustment_df.to_sql("adjustment", engine_payment, if_exists="append", index=False)


@dag(
    dag_id="mock_commission",
    description="Imports payment commission csv data",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=3,
)
def monthly_commission():
    create_deposits()
    create_withdrawals()
    create_adjustments()


monthly_commission()
