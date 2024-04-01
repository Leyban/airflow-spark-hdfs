def get_year_month(x):
    year = x.strftime("%Y")
    month = x.strftime("%m")

    return int(f"{year}{month}")


def get_inserted_data(df, conn, table_name, partition_key, column_name):
    import pandas as pd

    inserted_df = pd.DataFrame()

    ranges = df[partition_key].unique()

    for range in ranges:

        range_df = df[df[partition_key] == range]
        range_df = range_df.reset_index(drop=True)

        rawCql = f"""
            SELECT *
            FROM {table_name}

            WHERE {partition_key} = ?
            AND {column_name} IN (?{",?" * int(range_df.shape[0] - 1)})
        """

        parameters = [range]
        for _, row in range_df.iterrows():
            parameters.append(row['bet_id'])

        prepared_query = conn.prepare(rawCql)
        result = conn.execute(prepared_query, parameters)

        result_df = pd.DataFrame(result)

        inserted_df = pd.concat([inserted_df, result_df])

    return inserted_df 


