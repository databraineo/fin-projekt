import pandas as pd
import yfinance as yf
import sqlalchemy
import configparser


def connect_postgres(dateiname="config.ini", abschnitt="local_postgres"):
    "Verbindung zu lokaler Postgres-Datenbank"
    # Zugangsdaten aus der config-Datei einlesen
    config = configparser.ConfigParser()
    config.read(dateiname)
    user = config[abschnitt]["DB_USER"]
    pw = config[abschnitt]["DB_PW"]
    address = config[abschnitt]["DB_ADDRESS"]
    port = config[abschnitt]["DB_PORT"]
    name = config[abschnitt]["DB_NAME"]
    # Verbindung zu Datenbank aufbauen
    con_str = f"postgresql://{user}:{pw}@{address}:{port}/{name}"
    con = sqlalchemy.create_engine(con_str)
    return con


def insert_symbol(con, ticker, schema="finance"):
    # Daten aus Ticker auslesen
    symbol = ticker.info["symbol"]
    name = ticker.info["longName"]
    country = ticker.info["country"]
    # Prüfen, ob Symbol schon vorhanden
    query = f"""select count(1) from {schema}.symbols 
                where symbol = '{symbol}';"""
    df = pd.read_sql(query, con)
    vorhanden = df.iloc[0, 0] == 1
    # Daten in Datenbank schreiben
    if not vorhanden:
        query = f"""insert into {schema}.symbols values 
            ('{symbol}', '{name}', '{country}');"""
        con.execute(query)
    else:
        print("Eintrag existiert schon")


def insert_earnings_quarter(con, ticker, schema="finance"):
    # Tabelle aus Ticker holen
    df = ticker.quarterly_earnings
    # Symbol in Symbol-Tabelle eintragen (falls noch nicht vorhanden)
    insert_symbol(con, ticker, schema)
    # Umformen, damit passend zu SQL-Tabelle
    symbol = ticker.info["symbol"]
    df["symbol"] = symbol
    df["quarter"] = df.index
    df["quarter"] = df["quarter"].str[2:] + "Q" + df["quarter"].str[0]
    df = df.rename(columns={"Revenue": "revenue", "Earnings": "earnings"})
    # schränke auf die Elemente ein, die noch nicht vorhanden sind
    query = f"""select quarter from {schema}.earnings_quarter
        where symbol = '{symbol}'"""
    df_vorhanden = pd.read_sql(query, con)
    df = df[~df["quarter"].isin(df_vorhanden["quarter"])]
    # in Datenbank hochladen
    df.to_sql("earnings_quarter", con, schema=schema, if_exists="append", index=False)


def insert_history(con, ticker, schema="finance"):
    # Symbol in Tabelle symbol schreiben, wenn noch nicht vorhanden
    insert_symbol(con, ticker, schema)
    symbol = ticker.info["symbol"]
    # Bestimme das maximale Datum in Tabelle history in der DB
    # +2, weil die history-Funktion die Daten von einem Tag vorher holt
    query = f"""select max(date)+2 from {schema}.history h 
        where symbol = '{symbol}'"""
    max_datum = pd.read_sql(query, con).iloc[0, 0]
    if max_datum is None:
        max_datum = "2000-01-02"
    df = ticker.history(start=max_datum)
    # Wenn die Zeilenanzahl größer als 0
    if df.shape[0] > 0:
        df["symbol"] = symbol
        del df["Dividends"]
        del df["Stock Splits"]
        df = df.reset_index()
        df.columns = df.columns.str.lower()
        df.to_sql("history", con=con, schema=schema, if_exists="append", index=False)
