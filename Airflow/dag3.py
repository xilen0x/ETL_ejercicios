# Orquestando un DAG que se ejecutara mensualmente

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


with DAG(dag_id = "5.1-orquestacion",
        description = "testing",
        schedule_interval = "@montly",
        start_date = datetime(2022, 5, 1 ),
        end_date = datetime(2022, 6, 1),
        default_args = {"depends_on_past": True}) as dag:

    t1 = BashOperator(task_id = "tarea1",
                        bash_command = "sleep 2 && echo 'Tarea 1'")

    t2 = BashOperator(task_id = "tarea2",
                        bash_command = "sleep 2 && echo 'Tarea 2'")

    t3 = BashOperator(task_id = "tarea3",
                        bash_command = "sleep 2 && echo 'Tarea 3'")

    t4 = BashOperator(task_id = "tarea4",
                        bash_command = "sleep 2 && echo 'Tarea 4'")

    
    #dependencia de tareas
    t1 >> t2 >> t3 >> t4 # una depende de la anterior

    # t1 >> t2 >> [t3, t4] # t2 depende de t1, t3 y t4 dependen de t2 y se ejecutan al mismo tiempo