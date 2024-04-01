create_keyspace = """
    CREATE KEYSPACE IF NOT EXISTS wagers 
    WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor' : 3} 
    AND durable_writes = true;
"""

create_table_pgsoft_by_id = """
    CREATE TABLE IF NOT EXISTS wagers.pgsoft_by_id (
        bet_id_range BIGINT,
        bet_id BIGINT,
        parent_bet_id BIGINT,
        player_name TEXT,
        currency TEXT,
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
        row_version BIGINT,
        bet_time TIMESTAMP,
        create_at TIMESTAMP,
        update_at TIMESTAMP,
        PRIMARY KEY (bet_id_range, bet_id)
    );
"""

create_table_pgosft_by_member = """
    CREATE TABLE IF NOT EXISTS wagers.pgsoft_by_member (
        player_name TEXT,
        bet_time TIMESTAMP,
        bet_id BIGINT,
        parent_bet_id BIGINT,
        currency TEXT,
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
        row_version BIGINT,
        create_at TIMESTAMP,
        update_at TIMESTAMP,
        PRIMARY KEY (player_name, bet_time, bet_id)
    );
"""

create_table_pgosft_by_date = """
    CREATE TABLE IF NOT EXISTS wagers.pgsoft_by_date (
        bet_id BIGINT,
        parent_bet_id BIGINT,
        player_name TEXT,
        currency TEXT,
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
        row_version BIGINT,
        bet_time TIMESTAMP,
        create_at TIMESTAMP,
        update_at TIMESTAMP,
        year_month INT,
        PRIMARY KEY (year_month, bet_time, bet_id)
    );
"""
