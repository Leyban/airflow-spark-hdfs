from airflow.decorators import dag, task
from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook
from datetime import  datetime


@dag(
    'scylla_test_dag',
    description='testing scylla with cassandra hook',
    schedule_interval='* 1 * * *', 
    start_date=datetime(2023, 1, 14), 
    catchup=False,
    max_active_runs=1
)
def scylla_dag():

    @task
    def create_keyspace():

        rawCQL = """
            CREATE KEYSPACE IF NOT EXISTS test_keyspace 
            WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor' : 3} 
            AND durable_writes = true
        """

        cassandra_hook = CassandraHook( cassandra_conn_id='cassandra_id' )
        conn = cassandra_hook.get_conn()

        conn.execute(rawCQL)

    @task
    def create_table():

        rawCQL = """
            CREATE TABLE IF NOT EXISTS test_keyspace.member (
                id BIGINT PRIMARY KEY,
                login_name VARCHAR,
                currency VARCHAR,
            )
        """

        cassandra_hook = CassandraHook( cassandra_conn_id='cassandra_id' )
        conn = cassandra_hook.get_conn()

        conn.execute(rawCQL)

    @task
    def insert_some():

        rawCQL = """
            INSERT INTO test_keyspace.member (
                id BIGINT PRIMARY KEY,
                login_name VARCHAR,
                currency VARCHAR,
            )
        """

        cassandra_hook = CassandraHook( cassandra_conn_id='cassandra_id' )
        conn = cassandra_hook.get_conn()

        conn.execute(rawCQL)

    create_keyspace()
    create_table()

scylla_dag()
