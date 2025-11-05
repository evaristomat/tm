import sqlite3

import pandas as pd

conn = sqlite3.connect("bets.db")

print("Estrutura da tabela bets:")
schema = pd.read_sql_query("PRAGMA table_info(bets)", conn)
print(schema[["name", "type"]])

print("\nAmostra de dados:")
sample = pd.read_sql_query("SELECT * FROM bets LIMIT 3", conn)
print(sample.columns.tolist())

conn.close()
