import requests
from apikey import API_KEY2

url = "https://api.apilayer.com/currency_data/change?start_date=start_date&end_date=end_date"

payload = {}
headers= {
  "apikey": API_KEY2
}

response = requests.request("GET", url, headers=headers, data = payload)

status_code = response.status_code
result = response.text