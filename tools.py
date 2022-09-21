import pandas as pd
import yfinance as yf
import sqlalchemy
import configparser


def connect_postgres(abschnitt="local_postgres"):
    "Verbindung zu lokaler Postgres-Datenbank"
    # Zugangsdaten aus der config-Datei einlesen
    config = configparser.ConfigParser()
    config.read("config.ini")
    user = config[abschnitt]["DB_USER"]
    pw = config[abschnitt]["DB_PW"]
    address = config[abschnitt]["DB_ADDRESS"]
    port = config[abschnitt]["DB_PORT"]
    name = config[abschnitt]["DB_NAME"]
    # Verbindung zu Datenbank aufbauen
    con_str = f"postgresql://{user}:{pw}@{address}:{port}/{name}"
    con = sqlalchemy.create_engine(con_str)
    return con


def insert_symbol(con, ticker):
    # Daten aus Ticker auslesen
    symbol = ticker.info["symbol"]
    name = ticker.info["longName"]
    country = ticker.info["country"]
    # Prüfen, ob Symbol schon vorhanden
    query = f"""select count(1) from finance.symbols 
                where symbol = '{symbol}';"""
    df = pd.read_sql(query, con)
    vorhanden = df.iloc[0, 0] == 1
    # Daten in Datenbank schreiben
    if not vorhanden:
        query = f"""insert into finance.symbols values 
            ('{symbol}', '{name}', '{country}');"""
        con.execute(query)
    else:
        print("Eintrag existiert schon")


def insert_earnings_quarter(con, ticker):
    # Tabelle aus Ticker holen
    df = ticker.quarterly_earnings
    # Symbol in Symbol-Tabelle eintragen (falls noch nicht vorhanden)
    insert_symbol(con, ticker)
    # Umformen, damit passend zu SQL-Tabelle
    symbol = ticker.info["symbol"]
    df["symbol"] = symbol
    df["quarter"] = df.index
    df["quarter"] = df["quarter"].str[2:] + "Q" + df["quarter"].str[0]
    df = df.rename(columns={"Revenue": "revenue", "Earnings": "earnings"})
    # schränke auf die Elemente ein, die noch nicht vorhanden sind
    query = f"""select quarter from finance.earnings_quarter
        where symbol = '{symbol}'"""
    df_vorhanden = pd.read_sql(query, con)
    df = df[~df["quarter"].isin(df_vorhanden["quarter"])]
    # in Datenbank hochladen
    df.to_sql(
        "earnings_quarter", con, schema="finance", if_exists="append", index=False
    )
