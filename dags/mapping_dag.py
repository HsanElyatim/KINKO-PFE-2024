from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from sqlalchemy import create_engine
from fuzzywuzzy import process
import re
import pandas as pd
import requests


db_name = Variable.get("DB_NAME")
TRIP_ADVISOR_API_KEY = Variable.get("TRIP_ADVISOR_API_KEY")


def clean_string(string):
    string = string.lower()
    string = re.sub(r'[^a-zA-Z0-9\s]', '', string)  # Remove special characters
    string = re.sub(r'\s+', ' ', string).strip()  # Remove extra whitespace
    return string


def create_column_mapping_table(engine, table_name, column_name, reference_list, threshold=90):
    query = f"SELECT * FROM {table_name};"
    df = pd.read_sql(query, engine)

    df[column_name] = df[column_name].apply(clean_string)
    df_unique = df.drop_duplicates(subset=[column_name])

    standardized_values = []
    for value in df_unique[column_name]:
        best_match = process.extractOne(value, reference_list)
        if best_match[1] > threshold:
            standardized_value = best_match[0]
        else:
            standardized_value = None  # If no good match, keep the original value

        standardized_values.append(standardized_value)

    df_unique[column_name + '_standardized'] = standardized_values

    df_unique[[column_name, f"{column_name}_standardized"]].to_sql(f'{table_name}_{column_name}_mapping', engine,
                                                                   if_exists='replace', index=False, chunksize=100000)


def create_column_mapping_tables():
    table_names = ['traveltodo_src', 'libertavoyages_src', 'tunisiebooking_src', 'agoda_src']

    conn = BaseHook.get_connection(db_name)
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')

    query = "SELECT * FROM traveltodo ;"
    traveltodo_df = pd.read_sql(query, engine)

    traveltodo_pensions_ref = list(traveltodo_df.pension.unique())
    traveltodo_room_types_ref = list(traveltodo_df.room_type.unique())

    for table_name in table_names:
        create_column_mapping_table(engine, table_name, 'room_type', traveltodo_room_types_ref, threshold=90)
        create_column_mapping_table(engine, table_name, "pension", traveltodo_pensions_ref, threshold=80)


def augment_hotel_info(row):
    res = search_by_hotel_name_and_destination(TRIP_ADVISOR_API_KEY, row['name'], row['destination'])
    if res is None:
        return None
    return res['ref_name']


def search_by_hotel_name_and_destination(API_KEY, name, destination):
    url = f"https://api.content.tripadvisor.com/api/v1/location/search?key={API_KEY}&searchQuery={name}&category=hotels&address={destination}&language=en"
    headers = {"accept": "application/json"}

    response = requests.get(url, headers=headers)
    if "data" not in response.json().keys() or len(response.json()['data']) == 0:
        return None

    response_data = response.json()['data'][0]

    return ({
        'ref_name': response_data['name'],
        'country': response_data['address_obj']['country'],
        'state': response_data['address_obj']['state'],
        'city': response_data['address_obj']['city']
    })


def create_hotel_name_mapping_table(table_name):
    conn = BaseHook.get_connection(db_name)
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')

    query = f"SELECT name, destination FROM {table_name};"
    df = pd.read_sql(query, engine)

    df_unique = df.drop_duplicates(subset=['name'])
    df_unique['hotel_name_ref(Trip Advisor)'] = df_unique.apply(augment_hotel_info, axis=1)
    df_unique.rename(columns={'name': f'{table_name}_hotel_name'}, inplace=True)
    df_unique[[f"{table_name}_hotel_name", "hotel_name_ref(Trip Advisor)"]].to_sql(f'{table_name}_hotel_name_mapping',
                                                                                   engine, if_exists='replace',
                                                                                   index=False, chunksize=100000)


def create_hotel_name_mapping_tables():
    table_names = ['libertavoyages_src', 'traveltodo_src', 'tunisiebooking_src', 'agoda_src']

    for table_name in table_names:
        create_hotel_name_mapping_table(table_name)


with DAG('mappers_pipeline',
         start_date=datetime(2024, 6, 9),
         schedule_interval='@daily',
         catchup=False) as dag:
    create_hotel_name_mapping_task = PythonOperator(
        task_id='create_hotel_name_mapping',
        python_callable=create_hotel_name_mapping_tables,
        provide_context=True
    )

    create_column_mapping_tables_task = PythonOperator(
        task_id='create_column_mapping_tables',
        python_callable=create_column_mapping_tables,
        provide_context=True
    )

    [create_hotel_name_mapping_task, create_column_mapping_tables_task]
