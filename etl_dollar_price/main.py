# ---------------------------------------- Extract

import requests
from apikey2 import API_KEY2
import pandas as pd
import json
#import os
from twilio.rest import Client
from twilio_config import TWILIO_ACCOUNT_SID,TWILIO_AUTH_TOKEN,PHONE_NUMBER,API_KEY_WAPI
import time


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

# ---------------------------------------- Transform

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

#--------------------------------------------------- TWILIO ---------------

# template para luego enviar al celular
template = '\nHola Carlos! \n\n El valor del Dollar para hoy es: \n\n '+ str(dollar)
print(template)


# ---------------------------------------- Load
time.sleep(2)
account_sid = TWILIO_ACCOUNT_SID
auth_token = TWILIO_AUTH_TOKEN

client = Client(account_sid, auth_token)

message = client.messages \
                .create(
                    body = template,
                    from_ = PHONE_NUMBER,
                    to = '+1 111 11 11 11'#your number
                )

print('El mensaje fue enviado con éxito!' + message.sid)
