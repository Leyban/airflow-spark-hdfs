from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime
import pandas as pd
from requests.auth import HTTPBasicAuth
import requests
import json

dag = DAG(
    'migrate_member',
    description='migrate member proccessing',
    start_date=datetime(2023, 10, 26), 
    catchup=False,
     max_active_runs=1
    )

MEMBER_TABLE ='member'
MEMBER_ACCOUNT_TABLE ='member_account'
MEMBER_PAYMENT_ACCOUNT_TABLE ='member_payment_account'



def getListMemberIdentity():
    conn_identity_pg_hook = PostgresHook(postgres_conn_id='identity_conn_id')

    query = f"""
        SELECT  
            u.id as member_id,
            u.affiliate_id,
            u.login_name,
            u.currency, 
            u.status,
            u.email, 
            u.full_name, 
            u.phone,
            u.create_at, 
            u.update_at
        FROM {MEMBER_TABLE} as u
        """
    
    list_member_df = conn_identity_pg_hook.get_pandas_df(query)
    return list_member_df

def getListMemberAccPayment():
    conn_payment_pg_hook = PostgresHook(postgres_conn_id='payment_conn_id')

    query = f"""
        SELECT  
            ma.member_id,
            ma.payment_status,
            ma.risk_status,
            ma.balance,
            u.payment_type_code as payment_type,
            u.detail->>'account_no' as account_no,
            u.detail->>'account_name' as account_name,
            u.detail->>'bank_name' as bank_name,
            u.verify_status

        FROM {MEMBER_ACCOUNT_TABLE} as ma LEFT JOIN {MEMBER_PAYMENT_ACCOUNT_TABLE} as u
		ON ma.member_id =u.member_id
        """
    
    list_member_df = conn_payment_pg_hook.get_pandas_df(query)

    list_member_df['payment_accounts'] = list_member_df.apply(
    lambda row: {
        "payment_type": row['payment_type'],
            "account_no": row['account_no'],
            "account_name": row['account_name'],
            "bank_name": row['bank_name'],
            "verify_status": row['verify_status']
     } if not all(pd.isna(row[['payment_type', 'account_no', 'account_name', 'bank_name','verify_status']])) else None,
        axis=1)


    list_member_df = list_member_df.fillna('')
    combined_rows = []

    for _, group in list_member_df.groupby(['member_id','payment_status','risk_status','balance']):
        member_id, payment_status,risk_status,balance = _
        payment_accounts = group['payment_accounts'].tolist()
        combined_rows.append({
            "member_id": member_id,
            "payment_status": payment_status,
            "risk_status":risk_status,
            "balance":balance,
            "payment_accounts": payment_accounts
        })

    combined_result = pd.DataFrame(combined_rows)
    return combined_result

def getListMemberAccReward():
    conn_reward_pg_hook = PostgresHook(postgres_conn_id='reward_conn_id')

    query = f"""
        SELECT  
            member_id,
            level
        FROM {MEMBER_ACCOUNT_TABLE}
        """
    
    list_member_df = conn_reward_pg_hook.get_pandas_df(query)
    return list_member_df


def migrateMember():
   from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchSQLHook

   es_hook = ElasticsearchSQLHook(
           conn_name_attr='elasticsearch_conn_id',
           )
   es_conn = es_hook.get_conn()



   df_member = getListMemberIdentity()
   df_payment = getListMemberAccPayment()
   df_reward = getListMemberAccReward()

   merged_result = pd.merge(df_member, df_payment, on='member_id')
   merged_result = pd.merge(merged_result, df_reward, on='member_id')

   elasticsearch_host = Variable.get("elasticsearch_host")
   elasticsearch_port = Variable.get("elasticsearch_port")
#    elasticsearch_cert_path = Variable.get("elasticsearch_cert_path")
   elasticsearch_username = Variable.get("elasticsearch_username")
   elasticsearch_password = Variable.get("elasticsearch_password")
   elasticsearch_scheme =Variable.get("elasticsearch_scheme")
   elasticsearch_index ="member"
  

   cert_string = """
    -----BEGIN CERTIFICATE-----
    MIIDSTCCAjGgAwIBAgIULxRlJ5hwt+QC/I/TBFbca2zZue0wDQYJKoZIhvcNAQEL
    BQAwNDEyMDAGA1UEAxMpRWxhc3RpYyBDZXJ0aWZpY2F0ZSBUb29sIEF1dG9nZW5l
    cmF0ZWQgQ0EwHhcNMjMwODEyMDYzMDM5WhcNMjYwODExMDYzMDM5WjA0MTIwMAYD
    VQQDEylFbGFzdGljIENlcnRpZmljYXRlIFRvb2wgQXV0b2dlbmVyYXRlZCBDQTCC
    ASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAKYo29loBtsO4CPdP3mPKYXs
    bhrBJR2nDYkBtuC7lw89QTrr0qQ1NQujNqomIFvO0Xc7yDP4wZWxTFcEXoqBRB3b
    YgezKZV2iEQDRTaddmL2SHjfqq1DE72yoMGbvNEigN16Tv55L6oanoacdp/RUCpj
    eYHUkmBkX76nLUYXQ4VWKc7uxdhnkFOovf6uYTF0SLtHQhXe0QDxouLSZCZNcxSg
    qfmVpGgOv8O9GSOFPfUpXK5uIFEjZeUT5x+JY3tiSH8vF6mkTCW3Dz5OFTUXVlsU
    oLFoOrObydknpCej0YJxqclO1pmyMxQicV6tPybqWoF06si4DG2OINsN4EolqXUC
    AwEAAaNTMFEwHQYDVR0OBBYEFLFkTlVXB3ukLWX3vXaIb6IITf7CMB8GA1UdIwQY
    MBaAFLFkTlVXB3ukLWX3vXaIb6IITf7CMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZI
    hvcNAQELBQADggEBAIuXTsAwrkSBaS+5UzjxNSDh34QjQKufZNQLPjBfZGXa/GZ9
    gGWZ/gGAEU2piOAfRkcq3f+6YbUvHmothN9GZryV6JMms/c5MJBptu7Wui/bfSwo
    AQWhA7J6cI+whSynkaA9w3NiyQJXhP0sWS2DqS2Xe+Z0lRm5z1UQcQdNFD+9g42s
    01ozuNXq8EZKbVpEqpXSoHEK6Xme+4mDfNMUwh4JotiaAj6JKomfGV18kyuOW7dn
    yP5z2zU9mYvkEl2pgn+Bd/rBD6KEK6WYud0spVY6RA+GdYrtCSUgsA27fDQjadUB
    NxAf0E3oeG2ESF+uXVjaud+N/DuYlKdQ7J8IiPU=
    -----END CERTIFICATE-----
    """

   json_data = merged_result.to_json(orient='records', lines=True)
   bulk_data = []

   for line in json_data.split('\n'):
        if not line:
               continue
        document = json.loads(line)
        action = {
            "index": {
                "_index": elasticsearch_index,
                "_type": "_doc",
                "_id": str(document["member_id"])
            }
        }

        bulk_data.append(json.dumps(action))
        bulk_data.append(json.dumps(document))


   bulk_request_data = "\n".join(bulk_data) + "\n"

   elasticsearch_bulk_url = f"{elasticsearch_scheme}://{elasticsearch_host}:{elasticsearch_port}/_bulk"
   auth = HTTPBasicAuth(elasticsearch_username, elasticsearch_password)

   response = requests.post(
        elasticsearch_bulk_url,
        data=bulk_request_data,
        headers={"Content-Type": "application/x-ndjson"},
        auth=auth,  
        cert=(cert_string)
    )

   if response.status_code == 200:
        print("Bulk indexing completed successfully.")
   else:
        print("Failed to perform bulk indexing.")
        print(response.text)
    
migrate_member_operator = PythonOperator(
    task_id='migrate_member', python_callable=migrateMember, dag=dag)

migrate_member_operator
