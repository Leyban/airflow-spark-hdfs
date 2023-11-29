from airflow.exceptions import AirflowSkipException

def fetch_EXIM_TMO_data(payload, bank_acc, **context):
    import requests
    import pandas as pd
    from main_airflow.modules.auto_deposit import variables as var
    from main_airflow.modules.auto_deposit.utils import notify_skype
    
    res = requests.post(
        var.EXIM_TMO_URL, 
        data = payload
    )

    if res.json().get('success') == False:
        print(f"An Error Occurred when fetching {bank_acc['bank_code'].upper()} api")
        print(res.json().get('message'))
        error_details = { "Account Name": bank_acc['username'], "Account Number": bank_acc['account_no'], "Provider": var.Provider_Value[bank_acc['provider']] }
        notify_skype(f"{bank_acc['code'].upper()} | {var.Provider_Value[bank_acc['provider']]} Fetching Failed", error_details, res.json().get('message'), **context)
        raise AirflowSkipException

    result = res.json().get('data',{}).get('currentHistoryList', [])
    
    trans_df = pd.DataFrame.from_records(result)  

    if trans_df.empty:
        return trans_df

    new_bank_df = trans_df.loc[:, ['tranRef','tranDesc','amount','tranTimeNoFormat']]
    new_bank_df = new_bank_df.rename(columns={
        'tranRef': 'bank_reference',
        'tranDesc':'bank_description',
        'tranTimeNoFormat':'transaction_date'
    })
 
    new_bank_df['transaction_date'] = pd.to_datetime(new_bank_df['transaction_date'].astype(str), format='%Y%m%d%H%M%S')

    return new_bank_df


def fetch_ACB_TMO_data(payload, bank_acc, **context):
    import requests
    import pandas as pd
    from main_airflow.modules.auto_deposit import variables as var
    from main_airflow.modules.auto_deposit.utils import notify_skype
    
    res =requests.post(
        var.ACB_TMO_URL, 
        data = payload
    )

    if res.json().get('success') == False:
        print(f"An Error Occurred when fetching {bank_acc['bank_code'].upper()} api")
        print(res.json().get('message'))
        error_details = { "Account Name": bank_acc['username'], "Account Number": bank_acc['account_no'], "Provider": var.Provider_Value[bank_acc['provider']] }
        notify_skype(f"{bank_acc['code'].upper()} | {var.Provider_Value[bank_acc['provider']]} Fetching Failed", error_details, res.json().get('message'), **context)
        raise AirflowSkipException

    result = res.json().get('transactions', [])
    
    trans_df = pd.DataFrame.from_records(result)

    if trans_df.empty:
        return trans_df

    new_bank_df = trans_df.loc[:, ['Reference','Description','Amount','TransactionDateFull']]
    new_bank_df = new_bank_df.rename(columns={
        'Reference': 'bank_reference',
        'Description':'bank_description',
        'Amount':'amount',
        'TransactionDateFull':'transaction_date'
    })

    new_bank_df['transaction_date'] = pd.to_datetime(new_bank_df['transaction_date'], format='%d/%m/%Y %H:%M:%S')
    new_bank_df['amount'] = new_bank_df['amount'].str.replace(',', '')

    return new_bank_df


def fetch_VTB_TMO_data(payload, bank_acc, **context):
    import requests
    import pandas as pd
    from main_airflow.modules.auto_deposit import variables as var
    from main_airflow.modules.auto_deposit.utils import notify_skype
    
    res =requests.post(
        var.VTB_TMO_URL, 
        data = payload
    )

    if res.json().get('success') == False:
        print(f"An Error Occurred when fetching {bank_acc['code'].upper()} api")
        print(res.json().get('message'))
        error_details = { "Account Name": bank_acc['username'], "Account Number": bank_acc['account_no'], "Provider": var.Provider_Value[bank_acc['provider']] }
        notify_skype(f"{bank_acc['code'].upper()} | {var.Provider_Value[bank_acc['provider']]} Fetching Failed", error_details, res.json().get('message'), **context)
        raise AirflowSkipException

    result = res.json().get('data',{}).get('transactions', [])
    
    trans_df = pd.DataFrame.from_records(result)

    if trans_df.empty:
        return trans_df

    new_bank_df = trans_df.loc[:, ['trxId','remark','amount','processDate']]
    new_bank_df = new_bank_df.rename(columns={
        'trxId': 'bank_reference',
        'remark':'bank_description',
        'amount':'amount',
        'processDate':'transaction_date'
    })

    new_bank_df['transaction_date'] = pd.to_datetime(new_bank_df['transaction_date'], format='%d-%m-%Y %H:%M:%S')

    return new_bank_df


def update_ACB_TMO(bank_acc, maxRows, **context):
    from main_airflow.modules.auto_deposit.payment_store import get_old_online_bank_df, update_old_bank_data

    print(f"Fetching {bank_acc['bank_code'].upper()} Data via TMO")

    payload = {
        "username":bank_acc['username'],
        "password":bank_acc['password'],
        "accountNumber":bank_acc['account_no'],
        "maxRows": maxRows
    }

    print(payload)
    trans_df = fetch_ACB_TMO_data(payload, bank_acc, **context)
    if trans_df.empty:
        print("No Data Received")
        return

    date_from = trans_df['transaction_date'].min()
    date_to = trans_df['transaction_date'].max()

    old_bank_df = get_old_online_bank_df(date_from=date_from, date_to=date_to)

    update_old_bank_data(trans_df, old_bank_df, bank_acc)


def update_VTB_TMO(bank_acc, date_from, date_to, **context):
    from main_airflow.modules.auto_deposit.payment_store import get_old_online_bank_df, update_old_bank_data

    print(f"Fetching {bank_acc['bank_code'].upper()} Data via TMO")

    old_bank_df = get_old_online_bank_df(date_from=date_from, date_to=date_to)

    begin_str = date_from.strftime("%d/%m/%Y")
    end_str = date_to.strftime("%d/%m/%Y")

    payload = {
        "username":bank_acc['username'],
        "password":bank_acc['password'],
        "accountNumber":bank_acc['account_no'],
        "begin": begin_str,
        "end": end_str
    }

    page = 0
    old_data_count = -1

    while old_data_count < old_bank_df.shape[0]:
        old_data_count = old_bank_df.shape[0]
        payload["page"] = page

        print(payload)
        trans_df = fetch_VTB_TMO_data(payload, bank_acc, **context)


        if trans_df.empty:
            print("No New Data Received")
            break

        old_bank_df = update_old_bank_data(trans_df, old_bank_df, bank_acc)
        page += 1


def update_EXIM_TMO(bank_acc, date_from, date_to, **context):
    from main_airflow.modules.auto_deposit.payment_store import get_old_online_bank_df, update_old_bank_data

    print(f"Fetching {bank_acc['bank_code'].upper()} Data via TMO")

    old_bank_df = get_old_online_bank_df(date_from=date_from, date_to=date_to)

    begin_str = date_from.strftime("%d/%m/%Y")
    end_str = date_to.strftime("%d/%m/%Y")

    payload = {
        "username":bank_acc['username'],
        "password":bank_acc['password'],
        "accountNumber":bank_acc['account_no'],
        "fromDate": begin_str,
        "toDate": end_str
    }

    print(payload)
    trans_df = fetch_EXIM_TMO_data(payload, bank_acc, **context)
    if trans_df.empty:
        print("No Data Received")
        return

    update_old_bank_data(trans_df, old_bank_df, bank_acc)

