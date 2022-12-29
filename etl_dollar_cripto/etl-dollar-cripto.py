import requests
from apikey import API_KEY
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects


def getCriptoValue():

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

getCriptoValue()