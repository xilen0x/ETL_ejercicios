# ETL Cryptocurrency Data
'''ETL that obtains data from the first 10 cryptocurrencies. 
It then generates a dataset to send a WhatsApp message through Twilio.'''

# Libraries
import requests
import time
import pandas as pd
from apikey import API_KEY
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from twilio.rest import Client
from twilio_config import TWILIO_ACCOUNT_SID,TWILIO_AUTH_TOKEN
from twilio_phone_number import MY_PHONE_NUMBER,TWILIO_PHONE_NUMBER

# ----------------------- Step I - Extract --------------------------

# Function to get the data(Extraction)
def getCriptoValue():
    '''
    Function that allows extract data of criptos from API of coinmarketcap
    by: Carlos Astorga
    '''
    headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': API_KEY,
    }

    params = {
    'start':'1',
    'limit':'10',
    'convert':'USD'
    }

    url = ' https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'

    try:
        json = requests.get(url, params=params, headers = headers).json()
        coins = json['data'] # data es el diccionario con la info q me interesa
        #print(json)

        print("Extrayendo datos...")
            
        for coin in coins:
            file = open("./data.csv", "a")# a = append - w = write - r = read
            file.write(coin['name'] + ',' + ' ')
            file.write(coin['symbol'] + ',' + ' ')
            file.write(coin['last_updated'] + ',' + ' ')
            file.write(str(coin['max_supply']) + ',' + ' ')
            file.write(str(coin['quote']['USD']['market_cap']) + ',' + ' ')
            file.write(str(round(coin['quote']['USD']['price'], 2)) + "\n")
            file.close()
            
        print("Datos extraidos con éxito!")        
        #time.sleep(60)
        
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)


# Function call
getCriptoValue()

# Reading the data
f = open("./data.csv", "r")
file = f.readlines()


for line in file:
    print(line, end="")

# ----------------------- Step II - Transform --------------------------

# Split each row by comma and save to list NewLine
newFile = []

for line in file:
    newFile.append(line.split(','))

# Viewing the result
for line in newFile:
    print(line)

# Creation of dataset
df = pd.DataFrame(data = newFile, columns=('Name','Symbol','Last_updated','Max_supply','Market_cap','Price'))

df = df[['Symbol','Price']]

# template twilio para el mensaje
template = '\nHola Carlos! \n\n Ranking 10 principales Criptos de hoy: \n\n '+ str(df)
print(template)

# ----------------------- Step III - Load --------------------------
from twilio.rest import Client 
 
account_sid = 'ACdd05e680da1a13fec131bcebcf59f155' 
auth_token = TWILIO_AUTH_TOKEN 
client = Client(account_sid, auth_token) 
 
message = client.messages.create( 
                              from_=TWILIO_PHONE_NUMBER,
                              body=template,
                              to=MY_PHONE_NUMBER 
                          ) 
 
print(message.sid)