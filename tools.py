import pandas as pd
import yfinance as yf
import sqlalchemy
import configparser


def connect_postgres():
    "Verbindung zu lokaler Postgres-Datenbank"
    # Zugangsdaten aus der config-Datei einlesen
    config = configparser.ConfigParser()
    config.read("config.ini")
    user = config["local_postgres"]["DB_USER"]
    pw = config["local_postgres"]["DB_PW"]
    address = config["local_postgres"]["DB_ADDRESS"]
    port = config["local_postgres"]["DB_PORT"]
    name = config["local_postgres"]["DB_NAME"]
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
