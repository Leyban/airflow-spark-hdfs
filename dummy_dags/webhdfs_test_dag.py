from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
import hdfs

import pyarrow as pa
import pyarrow.parquet as pq


from src.utils.logging import log_df


@task
def hadoopidoopy():
    client = WebHDFSHook(webhdfs_conn_id="webhdfs_conn_id")

    data = {"col1": [1, 2, 3], "col2": ["A", "B", "C"]}
    df = pd.DataFrame(data)

    parquet_bytes = df.to_parquet(index=False)

    hdfs_path = "/test/test.parquet"

    conn: hdfs.InsecureClient = client.get_conn()

    with conn.write(hdfs_path, overwrite=True) as writer:
        writer.write(parquet_bytes)

        # test

    with conn.read(
        hdfs_path,
    ) as reader:
        print(reader.read())

        pq_bytes = reader.read()

        # Nothing works beyond this point bruh

        reader = pa.BufferReader(pq_bytes)
        table = pq.read_table(reader)
        df_read = table.to_pandas()

        log_df(df_read, "Huzzah")


@dag(
    "Hadoopify_deez",
    description="Hadoop test baby",
    schedule="@daily",
    start_date=datetime(2023, 1, 14),
    catchup=False,
)
def hadoopify():
    hadoopidoopy()


hadoopify()
