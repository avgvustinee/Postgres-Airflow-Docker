from datetime import datetime,timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner':'augustine',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

with DAG(
    dag_id = 'postgres_operator_v04',
    default_args = default_args,
    start_date = datetime(year=2024, month=5,day =17 ),
    schedule_interval = '0 0 * * *'
 ) as dag:
    task1 = PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id = 'postgres_localhost',
        sql= """ 
             CREATE TABLE IF NOT EXISTS dag_runs (
                 dt DATE,
                 dag_id  CHARACTER VARYING,
                 PRIMARY KEY (dt, dag_id)
             );
        """
    )
    
    task2 = PostgresOperator(
        task_id = 'insert_into_table',
        postgres_conn_id = 'postgres_localhost',
        sql = """
              INSERT INTO dag_runs(dt,dag_id) values('{{ ds }}' , '{{ dag.dag_id }}')
        
        """
    )
    task3 = PostgresOperator(
        task_id = 'delete_from_table',
        postgres_conn_id = 'postgres_localhost',
        sql = """
              DELETE FROM dag_runs where dt = '{{ ds }}' and dag_id = '{{ dag.dag_id }}';

        """ 
    )
    task1 >> task3 >> task2

