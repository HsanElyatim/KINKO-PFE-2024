from datetime import datetime, timedelta

from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy import DummyOperator
from scrapers.dictionary import DESTINATIONS
from scrapers.agoda_scrap import scrap as scrap_agoda
from scrapers.tunisiebooking_scrap import scrap as scrap_tunisiebooking
from scrapers.tunisiebooking_scrap import scrap as scrap_tunisiebooking
from scrapers.traveltodo_scrap import scrap as scrap_traveltodo
from scrapers.bookingcom_scrap import scrap as scrap_bookingcom
from scrapers.libertavoyages_scrap import scrap as scrap_libertavoyages

# List of destinations to scrape
DESTINATIONS = ["Hammamet", "Tunis", "Sousse"]

default_args = {
    'owner': 'hsan',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,  # Number of retries
    'retry_delay': timedelta(minutes=1),  # Delay between retries
}

# Define the DAG with its configurations
with DAG('scrap_pipelinev2',
         start_date=datetime(2024, 6, 9),
         schedule_interval='@daily',
         catchup=False,
) as dag:
    
    scrap_group_tasks = []   # List to hold TaskGroup instances for each destination

    # Loop through each destination to create a TaskGroup for scraping tasks
    for destination in DESTINATIONS:
        with TaskGroup(f"scrap_for_{destination}", tooltip=f"Scrap for {destination}") as scrap:
            # Define scraping tasks for each website or service
            scrap_tunisiebooking_task = PythonOperator(
                task_id=f'scrape_tunisiebooking_task_{destination}',
                python_callable=scrap_tunisiebooking,
                provide_context=True,
                op_kwargs={
                    'destination':destination, 
                    'check_in':datetime.today().date(), 
                    'check_out':datetime.today().date() + timedelta(days=1)
                    },
                retries=3,
                retry_delay=timedelta(minutes=2),
                trigger_rule=TriggerRule.ALL_DONE
            )

            scrap_traveltodo_task = PythonOperator(
                task_id=f'scrape_traveltodo_task_{destination}',
                python_callable=scrap_traveltodo,
                provide_context=True,
                trigger_rule=TriggerRule.ALL_DONE,
                op_kwargs={
                    'destination':destination, 
                    'check_in':datetime.today().date(), 
                    'check_out':datetime.today().date() + timedelta(days=1)
                    },
                retries=3,
                retry_delay=timedelta(minutes=2)
            )

            # scrap_bookingcom_task = PythonOperator(
            #     task_id=f'scrape_bookingcom_task_{destination}',
            #     python_callable=scrap_bookingcom,
            #     provide_context=True,
            #     trigger_rule=TriggerRule.ALL_DONE,
            #     op_kwargs={
            #         'destination':destination, 
            #         'check_in':datetime.today().date(), 
            #         'check_out':datetime.today().date() + timedelta(days=1)
            #         },
            #    retries=3,
            #    retry_delay=timedelta(minutes=2)
            # )

            scrap_libertavoyages_task = PythonOperator(
                task_id=f'scrape_libertavoyages_task_{destination}',
                python_callable=scrap_libertavoyages,
                provide_context=True,
                trigger_rule=TriggerRule.ALL_DONE,
                op_kwargs={
                    'destination':destination, 
                    'check_in':datetime.today().date(), 
                    'check_out':datetime.today().date() + timedelta(days=1)
                    },
                retries=3,
                retry_delay=timedelta(minutes=2)
            )
                
            scrap_agoda_task = PythonOperator(
                task_id=f'scrap_agoda_task_{destination}',
                python_callable=scrap_agoda,
                provide_context=True,
                trigger_rule=TriggerRule.ALL_DONE,
                op_kwargs={
                    'destination':destination,
                    'check_in':datetime.today().date(),
                    'check_out':datetime.today().date() + timedelta(days=1)
                    },
                retries=3,
                retry_delay=timedelta(minutes=2)
            )

            # Define dependencies within the TaskGroup
            scrap_libertavoyages_task >> scrap_tunisiebooking_task >> scrap_traveltodo_task >> scrap_agoda_task

        scrap_group_tasks.append(scrap) # Append each TaskGroup to the list

    end = DummyOperator(task_id='end')  # Dummy operator to mark the end of DAG execution

    # Define dependencies between TaskGroups
    for i in range(len(scrap_group_tasks)-1):
        if i == len(scrap_group_tasks)-2:
            scrap_group_tasks[i] >> scrap_group_tasks[i+1] >> end   # Link last TaskGroup to end DummyOperator

        scrap_group_tasks[i] >> scrap_group_tasks[i+1]  # Chain TaskGroups sequentially




