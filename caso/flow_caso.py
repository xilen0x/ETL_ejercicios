# librerías
import yfinance as yf
import pandas as pd
import numpy as np
import requests
import pyodbc
from datetime import date
# librerias de prefect
from prefect import task, Flow
from prefect.tasks.secrets import PrefectSecret

#---------------------------------EXTRACT---------------------------------


@task()
def extract(tickers, today): 

    raw_dfs = {}

    # ------------------------------------- EXTRACCIÓN DE DATOS DESDE NASDAQ -------------------------------------

    for ticker in tickers:
        tk = yf.Ticker(ticker) 
        raw_df = pd.DataFrame(tk.history(period='1d')) 
        raw_df.columns = raw_df.columns.str.lower() 
        raw_df = raw_df[['open','high','low','close']] 

        raw_dfs[ticker] = raw_df 

    # ------------------------------------- EXTRACCIÓN DE DATOS DESDE COINBASE -------------------------------------

    # Se realiza un GET al sitio de coinbase
    response = requests.get('https://api.coinbase.com/v2/prices/spot?currency=USD')
    btc_raw = response.json() 
    #print(btc_raw) # {'data': {'base': 'BTC', 'currency': 'USD', 'amount': '48667.57'}}

    btc_raw = float(btc_raw['data']['amount']) 

    btc_index = pd.to_datetime([today])

    btc_dct = {'btc_usd': btc_raw}

    btc_raw = pd.DataFrame(btc_dct, index=btc_index)

    raw_dfs['btc_usd'] = btc_raw
    
    return raw_dfs
    #print(raw_dfs)

#---------------------------------TRANSFORM---------------------------------

@task()
def transform(raw_dfs, tickers, today):
    raw_dfs = raw_dfs.copy()# se hace una copia para evitar q se sobreescriba la otra raw_dfs
    
## Ingenieria de características
# Se crearan las siguientes columnas: dif_apert_cierre, rango_dia y signo_dia
#tickers = ['NVDA', 'TSLA', 'MSFT', 'AMZN', 'AMD', 'INTC']

    for ticker in tickers:
    #se obtiene el dataframe
        df = raw_dfs[ticker]
        #se crea variable en base a diferencia de estas otras dos
        df['dif_apert_cierre'] = df['open'] - df['close']
        df['rango_dia'] = df['high'] - df['low']#idem
        #se crea variable en base a condición(este np.where es como un ifelse pero en numpy)
        df['signo_dia'] = np.where(df['dif_apert_cierre'] > 0.0, "+",
        np.where(df['dif_apert_cierre'] < 0.0, "-",
        "0"))
        # se crea subconjunto con las variables con las que nos quedamos
        df = df[['close', 'dif_apert_cierre', 'rango_dia', 'signo_dia']]
        # se agrega prefijo al nombre de las columnas
        df.columns = list(map(lambda x: ticker +'_'+ x, df.columns.to_list()))
        
        raw_dfs[ticker] = df

    ## - Agregado
    dfs_list = raw_dfs.values()#con value, se obtienen solo los valores, sin el índice(osea una lista)
    # se crea el tablon o df final
    tablon = pd.concat(dfs_list, axis=1)
    """today = date.today()
    today = today.strftime("%Y-%m-%d")
    #tablon.to_csv("C:\\trace\\{}__post_tranform.csv".format(today))
    tablon.to_csv("{}__post_tranform.csv".format(today)) """

    return tablon
    #print(tablon)

#---------------------------------LOAD---------------------------------

@task()
def load(tablon, today):
    # 3.1 Validar la operación...
    num_rows_tablon = len(tablon.index)
    if num_rows_tablon != 1:
        print("conflicto en la actualización, probablemente NASDAQ no operó este día.")
        return

    # 3.2 Crear tabla si no existe.
    nombre_cols_sql = ['['+ col +']' for col in tablon.columns] # for q recorre las col del tablon y le agrega corchetes
    #query
    sql_create_btc_valores = """
        IF NOT EXISTS (SELECT name FROM sys.tables WHERE name = 'btcvalores')
            CREATE TABLE btcvalores (
                [fecha] DATE PRIMARY KEY,
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} VARCHAR,
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} VARCHAR,
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} VARCHAR,
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} VARCHAR,
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} VARCHAR,
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} DECIMAL (20, 2),
                {} VARCHAR,
                {} DECIMAL (20, 2)
        )
        """.format(*nombre_cols_sql)
    #print(sql_create_btc_valores)


    # CONEXION DB

    server = 'tcp:etlserver.database.windows.net' 
    database = 'casonasdaqbtc'
    username = 'admin_ude'
    password = '5iExA7AcpmCRgLU' 
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password)
    cursor = cnxn.cursor()
    cursor.execute(sql_create_btc_valores)
    cnxn.commit()


    # 3.3- Validar si existe el registro. Si existe descartamos los registros.

    sql_exists = "SELECT fecha FROM [dbo].[btcvalores] WHERE fecha = '{}'".format(str(today))
    cursor.execute(sql_exists)
    row = cursor.fetchone()
    if row:# si ya existe el registro
        print("Ya existe un registro para ese día.")
        return

    # 3.4 Inserción de los registros de la tabla.

    tablon.insert(0, 'fecha', today)
    tablon['fecha'] = tablon.index

    for index,row in tablon.iterrows():
        #print(row.tolist())
        cursor.execute('INSERT INTO dbo.btcvalores VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', row.tolist())
        cnxn.commit()

    cursor.close()
    cnxn.close()

#---------------------------------FLOW---------------------------------

with Flow("ETL BTC") as flow:
    tickers = ['NVDA','TSLA','MSFT','AMZN','AMD','INTC']
    today = date.today()
    #today = '2021-12-10'
    today = today.strftime("%Y-%m-%d")
    raw_dfs = extract(tickers, today)
    tablon = transform(raw_dfs, tickers, today)
    #secret = PrefectSecret("pwd_sql")
    load(tablon, today)

flow.run()