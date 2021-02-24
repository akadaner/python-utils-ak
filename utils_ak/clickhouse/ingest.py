from clickhouse_driver import Client


def _create_clickhouse_table(cli, df, db, table, partition_by, order_by):
    types_map = {
        "int64": "UInt64",
        "float64": "Float64",
        "object": "String",
        "datetime64[ns]": "DateTime",
    }

    create_table_query = """CREATE TABLE {db}.{table} (
        {dtypes}
    ) ENGINE = MergeTree
    PARTITION BY {partition_by}
    ORDER BY {order_by}
    SETTINGS index_granularity = 8192;"""

    dtypes = ",\n".join(
        [
            "{} {}".format(df.columns[i], types_map[str(dtype)])
            for i, dtype in enumerate(df.dtypes)
        ]
    )

    query = create_table_query.format(
        table=table,
        db=db,
        dtypes=dtypes,
        partition_by=partition_by,
        order_by=order_by,
    )
    return cli.execute(query)


def _insert_df(cli, df, db, table):
    cli.execute(f"INSERT INTO {db}.{table} VALUES", df.to_dict("records"))


def ingest_table(df, host, db, table, partition_by, order_by):
    cli = Client(host=host)
    _create_clickhouse_table(cli, df, db, table, partition_by, order_by)
    _insert_df(cli, df, db, table)


def test():
    from utils_ak.clickhouse.config import load_config
    from datetime import datetime

    CONFIG = load_config("test")
    print(CONFIG)
    import pandas as pd

    df = pd.DataFrame([[1, 1.0, "a", datetime.now()]], columns=["a", "b", "c", "d"])

    table = "test13"
    ingest_table(
        df, CONFIG["clickhouse_url"], "datasets", table, df.columns[0], df.columns[0]
    )

    from pandahouse import read_clickhouse

    connection = {
        "host": f'http://{CONFIG["clickhouse_url"]}:8123',
        "database": "datasets",
    }
    df = read_clickhouse(
        f"SELECT * FROM datasets.{table}", index_col=None, connection=connection
    )
    print(df)


if __name__ == "__main__":
    test()
