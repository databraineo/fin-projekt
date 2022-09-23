import tools
import yfinance as yf
import pandas as pd

schema = "finance"

con = tools.connect_postgres(abschnitt="azure_postgres_testuser")

query = f"select * from {schema}.load_symbols"
df = pd.read_sql(query, con)
print(df)


for s in df["symbol"]:
    print(s)
    t = yf.Ticker(s)
    tools.insert_history(con, t, schema)
