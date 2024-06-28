from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow import DAG

from etl_functions.etl_fct import extract, transform, load, load_fact_table
   

with DAG('etl_pipeline',
         start_date=datetime(2024, 6, 9),
         schedule_interval='@daily',
         catchup=False) as dag:

    # Define extract task
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
        provide_context=True
    )

     # Define transform task
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
        provide_context=True
    )

    # Define load task
    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
        provide_context=True
    )

    load_fact_task = PythonOperator(
        task_id='load_fact',
        python_callable=load_fact_table,
        provide_context=True
    )

    extract_task >> transform_task >> load_task >> load_fact_task
