from prefect import task, Flow

@task
def load():
    print("Teste")

with Flow("P1.1 - Hello world") as flow:
    load()


flow.run()