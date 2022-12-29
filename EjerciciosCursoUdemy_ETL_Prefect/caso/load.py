from transform import tablon, today
import pyodbc

""" TODO:
3.1- Validar la operación en mercados durante el dia del flow.
      Descartar los registros del tablón en caso de que no se haya operado en nasdaq ese día.
3.2- Crear tabla si no existe.
3.3- Validar si existe el registro. si existe descartamos los registros.
3.4- Inserción de los registros de la tabla.
 """

# 3.1 Validar la operación...
num_rows_tablon = len(tablon.index)
if num_rows_tablon != 1:
    print("conflicto en la actualización, probablemente NASDAQ no operó este día.")
    #return

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
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+',1433;DATABASE='+database+';UID='+username+';PWD='+password)
cursor = cnxn.cursor()
cursor.execute(sql_create_btc_valores)
cnxn.commit()


# 3.3- Validar si existe el registro. Si existe descartamos los registros.

sql_exists = "SELECT fecha FROM [dbo].[btcvalores] WHERE fecha = '{}'".format(str(today))
cursor.execute(sql_exists)
row = cursor.fetchone()
if row:# si ya existe el registro
    print("Ya existe un registro para ese día.")
    #return

# 3.4 Inserción de los registros de la tabla.

tablon.insert(0, 'fecha', today)
tablon['fecha'] = tablon.index

for index,row in tablon.iterrows():
    #print(row.tolist())
    cursor.execute('INSERT INTO dbo.btcvalores VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', row.tolist())
    cnxn.commit()

cursor.close()
cnxn.close()