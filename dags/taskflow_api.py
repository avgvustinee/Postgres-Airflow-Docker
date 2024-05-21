from datetime import datetime, timedelta
from airflow.decorators import dag, task


default_args ={
    "owner":"deus",
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}
# dag decorators
@dag(
    dag_id ='taskflow_api_v02',
    default_args = default_args,
    start_date = datetime(2024,5,18),
    schedule_interval = '@daily',
    )
def hello_world_etl():
    @task(multiple_outputs = True)
    def get_name():
        return {
            'firstname' : 'Augustine',
            'lastname': 'Shokane'    
            }
    
    @task()
    def get_age():
        return 24
    
    @task()
    def greet(firstname,lastname, age):
        print(f"Hello World : My name is {firstname} {lastname} and i am {age} years old !!")
    
    name_dict = get_name()
    age = get_age()
    greet(
        firstname = name_dict['firstname'],
        lastname=name_dict['lastname'] ,
        age=age
        )
    
greet_dag = hello_world_etl()