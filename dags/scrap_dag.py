from datetime import datetime, timedelta

from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from scrapers.dictionary import DESTINATIONS
from scrapers.agoda_scrap import scrap as scrap_agoda
from scrapers.tunisiebooking_scrap import scrap as scrap_tunisiebooking
from scrapers.tunisiebooking_scrap import scrap as scrap_tunisiebooking
from scrapers.traveltodo_scrap import scrap as scrap_traveltodo
from scrapers.bookingcom_scrap import scrap as scrap_bookingcom
from scrapers.libertavoyages_scrap import scrap as scrap_libertavoyages

DESTINATIONS = ["Hammamet", "Tunis", "Sousse"]
default_args = {
    'owner': 'hsan',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,  # Number of retries
    'retry_delay': timedelta(minutes=1),  # Delay between retries
}

with DAG('scrap_pipelinev2',
         start_date=datetime(2024, 6, 9),
         schedule_interval='@daily',
         catchup=False,
) as dag:
    
    scrap_group_tasks = []

    for destination in DESTINATIONS:
        with TaskGroup(f"scrap_for_{destination}", tooltip=f"Scrap for {destination}") as scrap:
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
            scrap_libertavoyages_task >> scrap_tunisiebooking_task >> scrap_traveltodo_task >> scrap_agoda_task

        scrap_group_tasks.append(scrap)

    trigger_etl_after_scraping = TriggerDagRunOperator(
        task_id='trigger_etl_after_scraping',
        trigger_dag_id='etl_pipeline',  # DAG ID of the ETL DAG
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    for i in range(len(scrap_group_tasks)-1):
        if i == len(scrap_group_tasks)-2:
            scrap_group_tasks[i] >> scrap_group_tasks[i+1] >> trigger_etl_after_scraping

        scrap_group_tasks[i] >> scrap_group_tasks[i+1]




