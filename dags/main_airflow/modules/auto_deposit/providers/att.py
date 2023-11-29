from airflow.exceptions import AirflowSkipException

def fetch_VCB_ATT_data(payload, bank_acc, **context):
    import requests
    import pandas as pd
    from main_airflow.modules.auto_deposit import variables as var
    from main_airflow.modules.auto_deposit.utils import notify_skype
    
    res =requests.get(
        var.VCB_ATT_URL, 
        params = payload
    )

    if not res.ok:
        print(f"An Error Occurred when fetching {bank_acc['code'].upper()} api")
        print(res.content)
        error_details = { "Account Name": bank_acc['username'], "Account Number": bank_acc['account_no'], "Provider": var.Provider_Value[bank_acc['provider']] }
        notify_skype(f"{bank_acc['code'].upper()} | {var.Provider_Value[bank_acc['provider']]} Fetching Failed", error_details, res.content, **context)
        raise AirflowSkipException

    result = res.json().get('data',[])
    
    trans_df = pd.DataFrame.from_records(result)

    if trans_df.empty:
        return trans_df

    trans_df = trans_df[trans_df['type'] == "deposit"]

    new_bank_df = trans_df.loc[:, ['referenceNumber','memo','money','createdAt']]
    new_bank_df = new_bank_df.rename(columns={
        'referenceNumber': 'bank_reference',
        'memo':'bank_description',
        'money':'amount',
        'createdAt':'transaction_date'
    })

    new_bank_df['transaction_date'] = pd.to_datetime(new_bank_df['transaction_date'], unit='s')
    new_bank_df['amount'] = new_bank_df['amount'].astype(str).apply(lambda x: x.replace(',', ''))
    new_bank_df['amount'] = new_bank_df['amount'].astype(str).apply(lambda x: x.replace('+', ''))

    return new_bank_df


def update_VCB_ATT(bank_acc, **context):
    from main_airflow.modules.auto_deposit.payment_store import get_old_online_bank_df, update_old_bank_data

    from airflow.models import Variable
    
    print(f"Fetching {bank_acc['bank_code'].upper()} Data via ATT")

    accessToken = Variable.get("VCB_ATT_ACCESS_TOKEN")
    per_page = 100

    payload = {
        "accountNumber":bank_acc['account_no'],
        "accessToken": accessToken,
        "limit": per_page,
    }

    old_bank_df = get_old_online_bank_df(bank_acc=bank_acc)

    page = 0
    old_data_count = -1

    while old_data_count < old_bank_df.shape[0]:
        old_data_count = old_bank_df.shape[0]
        payload["offset"] = page * per_page

        print(payload)
        trans_df = fetch_VCB_ATT_data(payload, bank_acc, **context)

        if trans_df.empty:
            print("No New Data Received")
            break

        old_bank_df = update_old_bank_data(trans_df, old_bank_df, bank_acc)
        page += 1

