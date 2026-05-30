import duckdb

result = duckdb.sql("""
    SELECT *
    FROM read_parquet('cotacoes/PETR4/*.parquet')
""").df()

print(result)
