from pandas.core.indexes import period
import yfinance as yf
import pandas as pd
from datetime import date
import requests


# listado de los datos a consultar
tickers = ['NVDA','TSLA','MSFT','AMZN','AMD','INTC']
raw_dfs = {}

# ----------- EXTRACCIÓN DE DATOS DESDE NASDAQ -----------

for ticker in tickers:
    tk = yf.Ticker(ticker) # consulta del ticker correspondiente y se guarda en tk
    raw_df = pd.DataFrame(tk.history(period='1d')) # consulta historia de 1 dia
    raw_df.columns = raw_df.columns.str.lower() # pequeña transformación, convertir los nombres de col. a minúsculas
    raw_df = raw_df[['open','high','low','close']] # seleccion de variables relevantes

    raw_dfs[ticker] = raw_df # guardo en diccionario

print(raw_dfs)
#print(raw_dfs['NVDA']) # SI SOLO QUIERO DE UNA EMPRESA



# ----------- EXTRACCIÓN DE DATOS DESDE COINBASE -----------

# GET
response = requests.get('https://api.coinbase.com/v2/prices/spot?currency=USD')
btc_raw = response.json()
btc_value = float(btc_raw['data']['amount']) # consulta solo del monto / se ha agregado float para convertir, puesto que estaba como string

btc_dct = {'BTC_exc_usd': btc_value}
today = date.today()
today = today.strftime("%Y-%m-%d")

btc_index = pd.to_datetime([today])
btc_raw = pd.DataFrame(btc_dct, index=btc_index)

raw_dfs['BTC_usd'] = btc_raw
#return raw_dfs


print(btc_raw)
print(btc_index)
