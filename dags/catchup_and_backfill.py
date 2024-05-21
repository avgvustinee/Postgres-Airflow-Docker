from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner':'augustine',
    'retries': 5,
    'retry_delay':timedelta(minutes= 5),
}

with DAG(
    dag_id ='catchup_and_backfill_v02',
    default_args = default_args,
    start_date = datetime(2024,5,1),
    schedule_interval = '@daily', 
    # catch up -> True
    # catch up -> False
    catchup = False,   
    
) as dag:
    task1 = BashOperator(
        task_id = 'task1',
        bash_command = 'echo This is a simple bash command'
    )
    
    task1