import yfinance as yf
import pandas as pd
from datetime import date
import requests


# listado de los datos a consultar
tickers = ['NVDA','TSLA','MSFT','AMZN','AMD','INTC']
# diccionario para guardar lo consultado
raw_dfs = {}

# hay q considera q el precio de btc se puede consultar en cualquier momento pero no así el de nasdaq que solo opera de L-V
# por lo q se crea la variable today para trabajar esto
today = date.today()
#today = today.strptime("%Y-%m-%d")# es necesaria esta linea?
#print(today) # 2021-12-05
# ------------------------------------- EXTRACCIÓN DE DATOS DESDE NASDAQ -------------------------------------

for ticker in tickers:
    tk = yf.Ticker(ticker) # consulta del ticker correspondiente y se guarda en tk
    raw_df = pd.DataFrame(tk.history(period='1d')) # crea dataframe y consulta historia de 1 dia
    raw_df.columns = raw_df.columns.str.lower() # pequeña transformación, convertir los nombres de col. a minúsculas como buena práctica
    raw_df = raw_df[['open','high','low','close']] # seleccion de variables relevantes

    raw_dfs[ticker] = raw_df # guardo en diccionario
    #print(raw_dfs)
    #print(raw_dfs['NVDA']) # SI SOLO QUIERO DE UNA EMPRESA



# ------------------------------------- EXTRACCIÓN DE DATOS DESDE COINBASE -------------------------------------

# Se realiza un GET al sitio de coinbase
response = requests.get('https://api.coinbase.com/v2/prices/spot?currency=USD')
btc_raw = response.json() # se le da formato json
#print(btc_raw) # {'data': {'base': 'BTC', 'currency': 'USD', 'amount': '48667.57'}}

# consulta solo del monto / se ha agregado float para convertir, puesto que estaba como string
btc_raw = float(btc_raw['data']['amount']) 
#print(btc_raw)# 48766.1 (valor del momento)
""" 
today = date.today()
today = today.strftime("%Y-%m-%d")
 """
# le pasamos la fecha de hoy para convertirla en indice del dataframe
btc_index = pd.to_datetime([today])

# se crea diccionario y se le pasa el monto como valor
btc_dct = {'btc_usd': btc_raw}

# se crea un dataframe
btc_raw = pd.DataFrame(btc_dct, index=btc_index)

raw_dfs['btc_usd'] = btc_raw
#return raw_dfs
#print(btc_raw)
print(raw_dfs)
"""
{'NVDA':              open        high         low       close
Date                                                 
2021-12-03            320.0    321.290009  301.299988  306.929993, 

'TSLA':                    open         high          low        close
Date                                                          
2021-12-03             1084.790039  1090.579956  1000.210022  1014.969971, 

'MSFT':                  open        high         low      close
Date                                                    
2021-12-03             331.98999  332.700012  318.029999  323.01001, 

'AMZN':               open         high          low        close
Date                                                     
2021-12-03            3455.0  3469.870117  3338.600098  3389.790039, 

'AMD':                   open        high         low       close
Date                                                      
2021-12-03            151.649994  152.380005  140.720001  144.009995, 

'INTC':              open       high        low  close
Date                                          
2021-12-03          49.68  50.060001  48.759998  49.25, 

'btc_usd':          btc_usd
2021-12-05         49217.66}

"""
#print(btc_index)