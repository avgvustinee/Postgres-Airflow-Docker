from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator
from airflow import DAG

default_args ={
    'owner': 'syre',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}

# functions
def greet(task_instance):
    first_name = task_instance.xcom_pull(task_ids='get_name',key = 'first_name')
    last_name = task_instance.xcom_pull(task_ids = 'get_name', key ='last_name')
    age = task_instance.xcom_pull(task_ids = 'get_age', key = 'age')
    print(f'"Hello World,i am {first_name} {last_name} and',
          f'i am {age} years old !! "')
# XCom 

def get_name(task_instance):
    task_instance.xcom_push(key='first_name',value='Augustine')
    task_instance.xcom_push(key='last_name', value='NoLastname')

def get_age(task_instance):
    task_instance.xcom_push(key='age',value = 24)
    
    
with DAG(
    dag_id="python_dag_v5",
    default_args = default_args,
    description = "Directed Acyclic Graph  ",
    start_date=datetime(2024, 5, 13),
    schedule='@daily',
    catchup=False,
    
) as dag:
    task1 = PythonOperator(
        task_id = "greet_matrix",
        python_callable = greet,
        # op_kwargs = {'age':24}
    )
    
    task2 = PythonOperator(
        task_id = "get_name",
        python_callable = get_name
    )
    task3 = PythonOperator(
        task_id = "get_age",
        python_callable = get_age,
    )
    
    [task3,task2 ]>> task1
   
    
    