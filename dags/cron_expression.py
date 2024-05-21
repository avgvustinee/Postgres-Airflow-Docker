from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner':'deus',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

with DAG(
    dag_id = 'cron_expression_v03',
    default_args=default_args,
    start_date = datetime(year = 2024,month =5,day =1),
    schedule_interval = '0 3 * * Tue-Fri',
    
) as dag:
    task1 = BashOperator(
        task_id = 'task1',
        bash_command = 'echo "Hello World" , this is dag with cron expression'
    )
    
    task1