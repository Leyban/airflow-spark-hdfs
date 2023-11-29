def compute_hash(row):
    from hashlib import sha1

    hash_input = (
        str( row['bank_reference'] ) +
        str( row['bank_description'] ) +
        str( row['amount'] ) +
        str( row['transaction_date'].day ) +
        str( row['transaction_date'].month ) +
        str( row['transaction_date'].year )
    )
    return sha1(hash_input.encode('utf-8')).hexdigest()   


def notify_skype(title, details=None, error_message=None, **context):
    from airflow.models import Variable
    from datetime import datetime
    from skpy import Skype

    username = Variable.get('SKYPE_USERNAME')
    password = Variable.get('SKYPE_PASSWORD')
    group_id = Variable.get('SKYPE_GROUP_ID')

    sk = Skype(username,password)
    ch = sk.chats[group_id]

    log_url = context.get("task_instance").log_url

    message = f"[{title}]({log_url}) -- {datetime.now()}"

    if details != None:
        for k in details:
            message += f"\n{k}: {details[k]}"
        if error_message != None:
            message = message + f"Error Message: {error_message}"
    
    ch.sendMsg(message)
