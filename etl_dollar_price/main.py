# Extract

import requests
from apikey2 import API_KEY2
import pandas as pd
import json

'''
    Script that allows extract data of dollar from API apilayer
    by: Carlos Astorga
'''

url = "https://api.apilayer.com/currency_data/live?source=&currencies=EUR"
    
payload = {}
headers= {
  "apikey": API_KEY2
}

response = requests.request("GET", url, headers=headers, data = payload)

status_code = response.status_code
result = response.text

# Transform

# string to a dictionary with json.loads() function
data = json.loads(result)

# Dataframe
df = pd.DataFrame(data = data)

# accesing the content
dollar = df.iloc[0]['quotes']

# Save to file
file = open("./dollar.txt", "w")
file.write(str(dollar))

print("Datos guardados con éxito!")