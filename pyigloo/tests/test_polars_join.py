import polars as pl
from pyigloo import IglooClient

def test_join_parquet_iceberg():
    client = IglooClient()
    sql = """
    SELECT a.*, b.*
    FROM parquet_table a
    JOIN iceberg_table b ON a.id = b.id
    LIMIT 10
    """
    df = client.execute(sql, return_type="polars")
    assert isinstance(df, pl.DataFrame)
    print(df)
