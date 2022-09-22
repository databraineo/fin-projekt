import tools
import yfinance as yf

schema = "finance"
symbole = ["AAPL", "TSLA", "MSFT", "AMZN"]

con = tools.connect_postgres(abschnitt="azure_postgres")

for s in symbole:
    print(s)
    t = yf.Ticker(s)
    tools.insert_history(con, t)
