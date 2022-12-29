# desde extract.py se importan raw_dfs, tickers y today
from extract import raw_dfs, tickers, today
# librerias
import pandas as pd
import numpy as np
from datetime import date

# se hace una copia para evitar q se sobreescriba las otras variables
raw_dfs = raw_dfs.copy()
tickers = tickers.copy()# idem

## Ingenieria de características
# Se crearan las siguientes columnas: dif_apert_cierre, rango_dia y signo_dia

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
#print(raw_dfs)

## - Agregado
dfs_list = raw_dfs.values()#con value, se obtienen solo los valores de los diccionarios de dataframes, sin el índice(osea una lista)
# se crea el tablon o df final
tablon = pd.concat(dfs_list, axis=1) # axis=1 eje de las columnas

print(tablon)