def get_matched(deposit_df, bank_df):
    import pandas as pd

    deposit_df = deposit_df.rename(columns={'amount':'amount_x'})
    bank_df = bank_df.rename(columns={'amount':'amount_y'})
    bank_df['amount_y'] = round(bank_df['amount_y'].astype(float) * 0.001, 5)

    merged = pd.merge(deposit_df, bank_df, how='left', on=['bank_id', 'bank_account_id'])
    
    conditions = lambda x: filter_vtb(x) \
        | filter_exim(x) \
        | filter_acb(x)  \
        | filter_vcb(x)

    merged['result'] = merged.apply(lambda x: conditions(x), axis=1) 
    new_merged_df = merged[merged['result']]

    # Cleaning
    new_merged_df = new_merged_df.drop_duplicates(subset=['deposit_id'])
    new_merged_df['deposit_id'] = new_merged_df['deposit_id'].astype('int')
    new_merged_df['online_bank_data_id'] = new_merged_df['online_bank_data_id'].astype('int')

    return new_merged_df


def filter_vcb(x):
    from main_airflow.modules.auto_deposit import variables as var

    cd1 = (x['bank_code'].upper() == var.VCB_CODE)
    cd2 = (x['amount_x'] == x['amount_y'])
    cd3 = match(str(x['ref_code']),str(x['bank_description']))
    return cd1 & cd2 & cd3 

def filter_acb(x):
    from main_airflow.modules.auto_deposit import variables as var

    cd1 = (x['bank_code'].upper() == var.ACB_CODE)
    cd2 = (x['amount_x'] == x['amount_y'])
    cd3 = match(str(x['ref_code']),str(x['bank_description']))
    return cd1 & cd2 & cd3 

def filter_vtb(x):
    from main_airflow.modules.auto_deposit import variables as var

    cd1 = (x['bank_code'].upper() == var.VTB_CODE)
    cd2 = (x['amount_x'] == x['amount_y'])
    # cd3 = match(str(x['ref_code']),str(x['bank_reference']).replace('credit','').replace(',','')) & (len(x['ref_code']) >=12)
    cd3 = match(str(x['ref_code']),str(x['bank_description']))
    return cd1 & cd2 & cd3 

def filter_exim(x):
    from main_airflow.modules.auto_deposit import variables as var

    cd1 = (x['bank_code'].upper() == var.EXIM_CODE)
    cd2 = (x['amount_x'] == x['amount_y'])
    cd3 = match(str(x['ref_code']),str(x['bank_description']))
    return cd1 & cd2 & cd3 


def match(word,string):
    import re
    match_string = r'\b' + word + r'\b'
    return bool(re.search(match_string, string))
