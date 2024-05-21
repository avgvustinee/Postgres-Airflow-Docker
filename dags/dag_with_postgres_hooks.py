import csv
import logging
from datetime import datetime,timedelta
from tempfile import  NamedTemporaryFile

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook



default_args = {
    'owner':'augustine',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}


def postgres_to_s3(data_interval_start, data_interval_end):
    ds_nodash = data_interval_start.format('YYYYMMDD')
    next_ds_nodash = data_interval_end.format('YYYYMMDD')
    
    # step 1: query data from postgresql db and save it into a text file
    hook = PostgresHook(postgres_conn_id="postgres_localhost")
    conn = hook.get_conn()
    cursor = conn.cursor()
    sql_query = "SELECT * FROM orders WHERE date >= %s AND date < %s"
    logging.info("Executing SQL query: %s", sql_query)
    cursor.execute(sql_query, (ds_nodash, next_ds_nodash))

    with NamedTemporaryFile(mode='w', suffix=f"{ds_nodash}") as f:
    #with open(f"dags/get_orders_{ds_nodash}.txt", "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
        f.flush()
        cursor.close()
        conn.close()
        logging.info("Saved orders data in text file: %s", f"dags/get_orders_{ds_nodash}.txt")
    
        # step 2: upload text file into S3 bucket
        # Add your S3 upload logic here
        s3_hook = S3Hook(aws_conn_id = "minio_conn")
        s3_hook.load_file(
            filename=f.name,
            key = f"orders/{ds_nodash}.txt",
            bucket_name = "airflow",
            replace = True
        )
        logging.info("Orders file %s has been  pushed to S3" , f.name)

with DAG(
    dag_id = 'dag_with_postgres_hooks_v04',
    default_args= default_args,
    start_date = datetime(year = 2022 , month = 5 , day = 20),
    schedule_interval = '@daily'

) as dag:
    task1 = PythonOperator(
        task_id = "postgres_to_s3",
        python_callable = postgres_to_s3
    )
    
    task1