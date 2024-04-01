def fetch_pgsoft_wager(**context):
    import math
    import requests
    import pandas as pd
    from datetime import datetime, timedelta
    from airflow.models import Variable
    from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook
    from main_airflow.modules.utils import cassandra

    # Extract Variables
    pg_url = Variable.get('PG_URL')
    pg_secret_key = Variable.get('PG_SECRECT_KEY')
    pg_operator_token = Variable.get('PG_OPERATOR_TOKEN')

    # Calculate pgsoft_version
    day_begin = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=60)
    epoch = datetime.utcfromtimestamp(0)
    pgsoft_version = int( (day_begin - epoch).total_seconds() * 1000 )

    # Taking Optional Date Parameters
    if 'pgsoft_version' in context['params']:
        pgsoft_version = context['params']['pgsoft_version']

    history_api = '/v2/Bet/GetHistory'
    url = f"{pg_url}{history_api}" 
    
    # Fetch From API
    form_data = {
        "secret_key": pg_secret_key,
        "operator_token": pg_operator_token,
        "bet_type": "1",
        "row_version": pgsoft_version,
        "count": "5000"
        }
        
    try:
        print(f"Start download pg: row_version {pgsoft_version}")
        print(url)
        response = requests.post(url, data=form_data)
        response.raise_for_status() 

        # Create DF
        print(" Creating Pandas Dataframe ")
        res_obj = response.json().get('data',[])
        total_data_length = len(res_obj)
        print(f"Total {total_data_length}")

        if total_data_length == 0:
            print("No data Received ")
            print(response.json().get('error'))

        df = pd.DataFrame(res_obj)

        # Drop empty rows
        df = df.dropna()
        df = df.reset_index(drop=True)

        # Renaming for Consistency
        df = df.rename(columns={
            "betId":"bet_id",
            "parentBetId":"parent_bet_id",
            "playerName":"player_name",
            "gameId":"game_id",
            "betType":"bet_type",
            "transactionType":"transaction_type",
            "betAmount":"bet_amount",
            "winAmount":"win_amount",
            "jackpotRtpContributionAmount":"jackpot_rtp_contribution_amount",
            "jackpotWinAmount":"jackpot_win_amount",
            "balanceBefore":"balance_before",
            "balanceAfter":"balance_after",
            "rowVersion":"row_version",
            "betTime":"bet_time",
            "createAt":"create_at",
            "updateAt":"update_at",
            })

        # Partitioning
        df['bet_time'] = pd.to_datetime(df['bet_time'], unit='ms')
        df['year_month'] = df['bet_time'].apply(lambda x: cassandra.get_year_month(x))
        df['bet_id_range'] = df['bet_id'].apply(lambda x: math.ceil(x * 1e-13))

        # Type Corrections
        df['bet_time'] = df['bet_time'].apply(lambda x: x.to_pydatetime())
        df['bet_id'] = df['bet_id'].astype(int)
        df['parent_bet_id'] = df['parent_bet_id'].astype(int)
        df['game_id'] = df['game_id'].astype(int)
        df['platform'] = df['platform'].astype(int)
        df['bet_type'] = df['bet_type'].astype(int)
        df['transaction_type'] = df['transaction_type'].astype(int)
        df['row_version'] = df['row_version'].astype(int)
        
        print(" Saving to Cassandra ")

        # Create a Cassandra Hook
        cassandra_hook = CassandraHook( cassandra_conn_id='cassandra_conn_id' )
        conn = cassandra_hook.get_conn()

        tic = time.perf_counter()
        exists_df = cassandra.get_inserted_data(df, conn, 'wagers.pgsoft_by_id', 'bet_id_range', 'bet_id')
        toc = time.perf_counter()
        print(f"Time for fetching existing data: {toc - tic:0.4f} seconds")

        # Same bet_id different player
        if not exists_df.empty:
            df['duplicate'] = df.apply(lambda row: (row['bet_id'] in exists_df['bet_id'].values) and (row['player_name'] not in exists_df.loc[exists_df['bet_id'] == row['bet_id'], 'player_name'].values), axis=1)
            df = df[~df['duplicate']]

        if df.shape[0] == 0:
            print("No New Data Found")
            return

        # Insert new data
        tic = time.perf_counter()
        for _, row in df.iterrows():
            insert_into_pgsoft_by_id(row, conn)
        toc = time.perf_counter()
        print(f"Time for inserting data to pgsoft_by_id: {toc - tic:0.4f} seconds")

        tic = time.perf_counter()
        inserted_df = cassandra.get_inserted_data(df, conn, 'wagers.pgsoft_by_id', 'bet_id_range', 'bet_id')
        inserted_df['year_month'] = inserted_df['bet_time'].apply(lambda x: cassandra.get_year_month(x))
        toc = time.perf_counter()
        print(f"Time for retrieving inserted data: {toc - tic:0.4f} seconds")

        tic = time.perf_counter()
        for _, row in inserted_df.iterrows():
            insert_into_pgsoft_by_member(row, conn)
            insert_into_pgsoft_by_date(row, conn)
        toc = time.perf_counter()
        print(f"Time for inserting to pgsoft_by_member and pgsoft_by_date: {toc - tic:0.4f} seconds")
        conn.shutdown()

    except requests.exceptions.RequestException as err:
        print("Request error:", err)
        raise AirflowException

    except Exception as Argument:
        print(f"Error occurred: {Argument}")
        raise AirflowException
    

