from airflow.decorators import dag, task
from datetime import datetime,timedelta

def update_online_bank_data(bank_acc, date_from, date_to, maxRows, **context):
    from airflow.exceptions import AirflowSkipException
    from main_airflow.modules.auto_deposit.providers import tmo
    from main_airflow.modules.auto_deposit.providers import att
    from main_airflow.modules.auto_deposit import variables as var
    from main_airflow.modules.auto_deposit.utils import notify_skype

    if bank_acc['username'] == None or bank_acc['password'] == None or bank_acc['account_no'] == None or bank_acc['provider'] <= var.PROVIDER_NONE: 
        print("Invalid Bank Account")
        print(bank_acc)

        error_details = {
            "Account Name": bank_acc['username'],
            "Account Number": bank_acc['account_no'],
            "Provider": var.Provider_Value[bank_acc['provider']]
        }

        notify_skype(f"{bank_acc['code'].upper()} | Invalid Bank Account", error_details, **context)
        raise AirflowSkipException

    print( f" '{bank_acc['username']}' '{bank_acc['password']}' '{bank_acc['account_no']}' '{bank_acc['bank_code']}' '{date_from}' '{date_to}' ")

    if bank_acc['provider'] == var.PROVIDER_TMO:

        if bank_acc['bank_code'].upper() == var.VTB_CODE:
            tmo.update_VTB_TMO(bank_acc, date_from, date_to, **context)

        elif bank_acc['bank_code'].upper() == var.ACB_CODE:
            tmo.update_ACB_TMO(bank_acc, maxRows, **context)

        elif bank_acc['bank_code'].upper() == var.EXIM_CODE:
            tmo.update_EXIM_TMO(bank_acc, date_from, date_to, **context)

        elif bank_acc['bank_code'].upper() == var.VCB_CODE:
            print("Not Implemented") # TODO: Implement this
            raise AirflowSkipException

        elif bank_acc['bank_code'].upper() == var.BIDV_CODE:
            print("Not Implemented") # TODO: Implement this
            raise AirflowSkipException

        elif bank_acc['bank_code'].upper() == var.MBB_CODE:
            print("Not Implemented") # TODO: Implement this
            raise AirflowSkipException

        # Not sure if TMO supports these
        elif bank_acc['bank_code'].upper() == var.VPBANK_CODE:
            print("Not Implemented") # TODO: Implement this
            raise AirflowSkipException

        elif bank_acc['bank_code'].upper() == var.TIMO_CODE:
            print("Not Implemented") # TODO: Implement this
            raise AirflowSkipException

        elif bank_acc['bank_code'].upper() == var.TECH_CODE:
            print("Not Implemented") # TODO: Implement this
            raise AirflowSkipException

        else:
            print("Invalid Bank Code: ", bank_acc['bank_code'])
            raise AirflowSkipException

    if bank_acc['provider'] == var.PROVIDER_ATT:

        if bank_acc['bank_code'].upper() == var.VCB_CODE:
            att.update_VCB_ATT(bank_acc, **context)


def get_valid_deposits():
    from main_airflow.modules.auto_deposit.payment_store import get_bank_accounts, get_banks, get_deposits

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


@dag(
    'auto_deposit_v1.0.0',
    description='Auto deposit proccessing',
    schedule_interval='*/1 * * * *', 
    start_date=datetime(2023, 1, 14), 
    catchup=False,
    max_active_runs=1
)
def Auto_Deposit():
    from main_airflow.modules.auto_deposit.payment_store import extract_bank_acc

    @task(max_active_tis_per_dag=1)
    def update_OBD(bank_acc, **context):
        date_to = datetime.utcnow()
        date_from = date_to - timedelta(days=60)
        maxRows = 200

        # Taking Optional Date Parameters
        date_format = '%Y-%m-%d %H:%M:%S'
        if 'date_from' in context['params']:
            date_from = datetime.strptime(context['params']['date_from'], date_format)
        
        if 'date_to' in context['params']:
            date_to = datetime.strptime(context['params']['date_to'], date_format)

        if 'max_rows' in context['params']:
            maxRows = context['params']['max_rows']

        print("Updating Onlne Bank Data")
        update_online_bank_data(bank_acc, date_from, date_to, maxRows, **context)


    @task(trigger_rule="all_done")
    def auto_deposit(**context):
        from main_airflow.modules.auto_deposit.payment_store import update_bank_data, get_online_bank_data
        from main_airflow.modules.auto_deposit.match import get_matched

        date_to = datetime.utcnow()
        date_from = date_to - timedelta(days=60)

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


    updating_OBD = update_OBD.expand(bank_acc=extract_bank_acc())

    auto_deposit_instance = auto_deposit()

    updating_OBD >> auto_deposit_instance

Auto_Deposit()