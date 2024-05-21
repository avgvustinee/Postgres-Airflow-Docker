
from datetime import datetime ,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args ={
    'owner': 'augustine',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}

with DAG(
    dag_id="first_dag",
    default_args = default_args,
    description = "This my first Directed Acyclic Graph experience ",
    start_date=datetime(2024, 5, 13),
    schedule='@daily',
    catchup=False,
    
) as dag:
    
    task1=BashOperator(
        task_id="first_task",
        bash_command='echo "hey augustine, are you ready to take over the world"'
    )
    task2 = BashOperator(
        task_id = "second_task",
        bash_command = 'echo "wake up augustine, are you ready for the world"'
    )
    task3 = BashOperator(
        task_id = "third_task",
        bash_command = 'echo "nah augustine, they are not ready for us "'
    )
    task1 >> task2 >> task3
