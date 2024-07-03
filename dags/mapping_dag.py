from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from sqlalchemy import create_engine, text
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


def get_src_tables():
    """
    Retrieves a list of table names from the database that end with '_src'.

    Returns:
    list: A list of table names that end with '_src'.
    """
    # Get the database connection details
    conn = BaseHook.get_connection(db_name)

    # Create the connection string for the PostgreSQL database
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')

    # Query the information schema to get table names ending with '_src'
    query = text("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name LIKE '%_src'
    """)

    # Execute the query and fetch the results
    with engine.connect() as connection:
        result = connection.execute(query)
        src_tables = [row['table_name'] for row in result.fetchall()]

    return src_tables


def create_column_mapping_table(engine, table_name, column_name, reference_list, threshold=90):
    """
        Creates a mapping table for a specific column in a database table, mapping values to standardized values
        based on a reference list using fuzzy matching.

        Parameters:
        - engine (sqlalchemy.engine.Engine): SQLAlchemy engine object for database connection.
        - table_name (str): Name of the source table from which data is read.
        - column_name (str): Name of the column in the source table to be standardized.
        - reference_list (list): List of reference values for fuzzy matching.
        - threshold (int, optional): Minimum score threshold (0-100) for fuzzy matching. Defaults to 90.

        Returns:
        None
    """
    # Query data from the source table
    query = f"SELECT * FROM {table_name};"
    df = pd.read_sql(query, engine)

    # Clean and drop duplicates from the column of interest
    df[column_name] = df[column_name].apply(clean_string)
    df_unique = df.drop_duplicates(subset=[column_name])

    # Perform fuzzy matching and create standardized values
    standardized_values = []
    for value in df_unique[column_name]:
        best_match = process.extractOne(value, reference_list)
        if best_match[1] > threshold:
            standardized_value = best_match[0]
        else:
            standardized_value = None  # If no good match, keep the original value

        standardized_values.append(standardized_value)

    # Create a DataFrame with original and standardized values
    df_unique[column_name + '_standardized'] = standardized_values

    df_unique[[column_name, f"{column_name}_standardized"]].to_sql(f'{table_name}_{column_name}_mapping', engine,
                                                                   if_exists='replace', index=False, chunksize=100000)


def create_column_mapping_tables():
    """
        Creates column mapping tables for 'room_type' and 'pension' columns across all source tables.

        Parameters:
        None

        Returns:
        None
    """
    table_names = get_src_tables()

    conn = BaseHook.get_connection(db_name)
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')

    query = "SELECT * FROM traveltodo_src ;"
    traveltodo_df = pd.read_sql(query, engine)

    traveltodo_pensions_ref = list(traveltodo_df.pension.unique())
    traveltodo_room_types_ref = list(traveltodo_df.room_type.unique())

    for table_name in table_names:
        create_column_mapping_table(engine, table_name, 'room_type', traveltodo_room_types_ref, threshold=90)
        create_column_mapping_table(engine, table_name, "pension", traveltodo_pensions_ref, threshold=80)


def augment_hotel_info(row):
    """
    Augments hotel information by fetching additional data using TripAdvisor API.

    Parameters:
    - row (pd.Series): Row containing 'name' and 'destination' of the hotel.

    Returns:
    - str: Reference name ('ref_name') from TripAdvisor API, or None if data not found.
    """
    res = search_by_hotel_name_and_destination(TRIP_ADVISOR_API_KEY, row['name'], row['destination'])
    if res is None:
        return None
    return res['ref_name']


def search_by_hotel_name_and_destination(API_KEY, name, destination):
    """
        Search for hotel information using the TripAdvisor API.

        Parameters:
        - API_KEY (str): Your TripAdvisor API key.
        - name (str): Name of the hotel to search.
        - destination (str): Destination where the hotel is located.

        Returns:
        - Dictionary containing additional information about the hotel, or None if not found.
    """
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
    table_names = get_src_tables()

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
