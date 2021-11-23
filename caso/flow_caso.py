# Importación de bibliotecas y definición de entorno
import pandas as pd
from pandas.core.indexes import period
import numpy
import yfinance as yf
from datetime import date
import requests
from prefect import task, Flow


# *********************** ETL *********************** #

# --------------------- EXTRACT ---------------------
@task
def extract(tickers, today):
    # listado de los datos a consultar
    raw_dfs = {}

    # ----------- EXTRACCIÓN DE DATOS DESDE NASDAQ -----------

    for ticker in tickers:
        tk = yf.Ticker(ticker) # consulta del ticker correspondiente y se guarda en tk
        raw_df = pd.DataFrame(tk.history(period='1d')) # consulta historia de 1 dia
        raw_df.columns = raw_df.columns.str.lower() # pequeña transformación, convertir los nombres de col. a minúsculas
        raw_df = raw_df[['open','high','low','close']] # seleccion de variables relevantes
        raw_dfs[ticker] = raw_df # guardo en diccionario


    # ----------- EXTRACCIÓN DE DATOS DESDE COINBASE -----------

    # GET
    response = requests.get('https://api.coinbase.com/v2/prices/spot?currency=USD')
    btc_raw = response.json()
    btc_value = float(btc_raw['data']['amount']) # consulta solo del monto / se ha agregado float para convertir, puesto que estaba como string

    btc_dct = {'BTC_exc_usd': btc_value}

    btc_index = pd.to_datetime([today])
    btc_raw = pd.DataFrame(btc_dct, index=btc_index)

    raw_dfs['BTC_usd'] = btc_raw
    
    #return raw_dfs
    print(raw_dfs)



# --------------------- TRANSFORM ---------------------
@task
def transform(raw_dfs):
    pass



# --------------------- LOAD ---------------------
@task
def load():
    pass


# *********************** FLOW ***********************
with Flow("ETL Caso") as flow:
    #raw_dfs = extract()
    #tablon = transform(raw_dfs)
    #load(tablon)
    tickers = ['NVDA','TSLA','MSFT','AMZN','AMD','INTC']
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    extract(tickers, today)

flow.run()
