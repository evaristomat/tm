import sqlite3
import pandas as pd

conn = sqlite3.connect("bets.db")
df = pd.read_sql_query(
    """
    SELECT b.* FROM bets b
    INNER JOIN telegram_sent_bets t ON b.id = t.bet_id
""",
    conn,
)
df.to_csv("bets_telegram.csv", index=False)
conn.close()
