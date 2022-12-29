import requests
from prefect import task, Flow

@task
def extract():
    response = requests.get("https://jsonplaceholder.typicode.com/posts")
    response = response.json()
    return response
@task
def load(response):
    output = response[0]['title']
    print("*****TITULO DEL PRIMER OBJETO DE LA API DE JSONPLACEHOLDER*****")
    print(str(output))

with Flow("P1.2 - JsonPlaceHolder I") as flow:
    raw = extract()
    load(raw)

flow.run()