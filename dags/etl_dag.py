from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor

from etl_functions.etl_fct import extract, transform, load, load_fact_table

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Define the DAG
with DAG('etl_pipeline', default_args=default_args, schedule_interval=None) as dag:
    # ExternalTaskSensor: Waits for the scraping DAG to complete its 'end' task
    wait_for_scraping = ExternalTaskSensor(
        task_id='wait_for_scraping',
        external_dag_id='scrap_pipelinev2',  # DAG ID of the scraping DAG
        external_task_id='end',  # Task ID in the scraping DAG
        mode='poke',    # Use poke mode to check for task completion
        timeout=600,    # Timeout after 600 seconds if scraping DAG doesn't complete
        poke_interval=60,   # Check every 60 seconds for scraping DAG completion
        dag=dag,
    )

    # PythonOperator: Executes the 'extract' function
    extract_task = PythonOperator(
        task_id='extract',  # Define your 'extract' function here
        python_callable=extract,    # Pass Airflow context to the 'extract' function
        provide_context=True
    )

    # PythonOperator: Executes the 'transform' function
    transform_task = PythonOperator(
        task_id='transform',    # Define your 'transform' function here
        python_callable=transform,  # Pass Airflow context to the 'transform' function
        provide_context=True
    )

    # PythonOperator: Executes the 'load' function
    load_task = PythonOperator(
        task_id='load',  # Define your 'load' function here
        python_callable=load,   # Pass Airflow context to the 'load' function
        provide_context=True
    )

    # PythonOperator: Executes the 'load_fact_table' function
    load_fact_task = PythonOperator(
        task_id='load_fact',    # Define your 'load_fact_table' function here
        python_callable=load_fact_table,    # Pass Airflow context to the 'load_fact_table' function
        provide_context=True
    )

    # Define task dependencies
    wait_for_scraping >> extract_task >> transform_task >> load_task >> load_fact_task
