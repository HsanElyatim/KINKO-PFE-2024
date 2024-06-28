from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData, ForeignKey, select, insert
import pandas as pd
import re
from airflow.models import Variable
db_name = Variable.get("DB_NAME")


def clean_string(string):
    """
        Cleans the input string by performing the following operations:
        1. Converts all characters to lowercase.
        2. Removes special characters (anything that is not a letter, number, or whitespace).
        3. Replaces multiple whitespace characters with a single space.
        4. Strips leading and trailing whitespace.

        Parameters:
        string (str): The input string to be cleaned.

        Returns:
        str: The cleaned string.
    """
    string = string.lower()
    string = re.sub(r'[^a-zA-Z0-9\s]', '', string)  # Remove special characters
    string = re.sub(r'\s+', ' ', string).strip()  # Remove extra whitespace
    return string


def standardize_column(tbl_name, column_name, df, map_df):
    """
       Standardizes the values in a specified column of a DataFrame using a mapping DataFrame.

       Parameters:
       tbl_name (str): The name of the table (not used in this function, can be removed if unnecessary).
       column_name (str): The name of the column in `df` to be standardized.
       df (pd.DataFrame): The DataFrame containing the column to be standardized.
       map_df (pd.DataFrame): The DataFrame containing the mapping from original values to standardized values.
                              It should have a column with the same name as `column_name` and another column
                              named `{column_name}_standardized`.

       Returns:
       pd.DataFrame: The original DataFrame `df` with an additional column `standardized_{column_name}`
                     containing the standardized values.
    """
    standardized_values = []

    # Iterate over each value in the specified column of df
    for value in df[column_name]:
        # Find the standardized value from map_df
        standardized_value = map_df.loc[map_df[f"{column_name}"] == value, f'{column_name}_standardized'].values
        if len(standardized_value) > 0:
            standardized_values.append(standardized_value[0])
        else:
            standardized_values.append(None)

    # Add the standardized values as a new column to df
    df[f'standardized_{column_name}'] = standardized_values

    return df


def standardize_hotel_name_column(tbl_name, df, map_df):
    """
        Standardizes the hotel names in the 'name' column of a DataFrame using a mapping DataFrame.

        Parameters:
        tbl_name (str): The name of the table. It is used to identify the column in the mapping DataFrame.
        df (pd.DataFrame): The DataFrame containing the 'name' column to be standardized.
        map_df (pd.DataFrame): The DataFrame containing the mapping from original hotel names to standardized hotel names.
                               It should have a column named '{tbl_name}_hotel_name' and another column
                               named 'hotel_name_ref(Trip Advisor)'.

        Returns:
        pd.DataFrame: The original DataFrame `df` with an additional column 'standardized_name'
                      containing the standardized hotel names.
    """
    standardized_values = []

    # Iterate over each value in the 'name' column of df
    for value in df['name']:
        # Find the standardized value from map_df
        standardized_value = map_df.loc[
            map_df[f"{tbl_name}_hotel_name"] == value, 'hotel_name_ref(Trip Advisor)'].values
        if len(standardized_value) > 0:
            standardized_values.append(standardized_value[0])
        else:
            standardized_values.append(None)

    # Add the standardized values as a new column to df
    df['standardized_name'] = standardized_values

    return df


def extract_data_from_src_table(table):
    """
       Extracts all data from a specified source table in a PostgreSQL database.

       Parameters:
       table (str): The name of the table to extract data from.
       db_name (str): The name of the database connection to use.

       Returns:
       pd.DataFrame: A DataFrame containing all data from the specified table.
    """
    # Get the database connection details
    conn = BaseHook.get_connection(db_name)
    # Create the connection string for the PostgreSQL database
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    # Execute the SQL query to extract data from the table and return as a DataFrame
    return pd.read_sql(f"SELECT * FROM {table}", engine)


def extract(**kwargs):
    """
        Extracts data from multiple source tables and returns a dictionary of DataFrames.

        Parameters:
        **kwargs: Arbitrary keyword arguments. Not used in this function, but included to allow for compatibility with Airflow's PythonOperator.

        Returns:
        dict: A dictionary where the keys are table names and the values are DataFrames containing the extracted data from each table.
    """
    tables = ['traveltodo_src', 'libertavoyages_src', 'tunisiebooking_src', 'agoda_src']
    all_data = {}

    # Iterate over each table name
    for table in tables:
        # Extract data from the current table and store it in the dictionary
        all_data[table] = extract_data_from_src_table(table)
    return all_data


def transform_data(table, df):
    """
        Transforms the data in a DataFrame based on mappings retrieved from PostgreSQL tables.

        Parameters:
        table (str): The name of the source table.
        df (pd.DataFrame): The DataFrame containing the data to be transformed.
        db_name (str): The name of the database connection to use.

        Returns:
        pd.DataFrame: A DataFrame with transformed data including standardized hotel names, room types, and pensions,
                      and a new 'source' column indicating the source table.
    """
    # Get the database connection details
    conn = BaseHook.get_connection(db_name)

    # Create the connection string for the PostgreSQL database
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')

    # Read mapping DataFrames from PostgreSQL tables
    hotel_name_map = pd.read_sql(f"SELECT * FROM {table}_hotel_name_mapping", engine)
    room_type_map = pd.read_sql(f"SELECT * FROM {table}_room_type_mapping", engine)
    pension_map = pd.read_sql(f"SELECT * FROM {table}_pension_mapping", engine)

    print(f"Transforming DataFrame from table: {table}")

    # Clean and standardize data in the DataFrame
    df['room_type'] = df['room_type'].apply(clean_string)
    df['pension'] = df['pension'].apply(clean_string)

    stg_df = standardize_hotel_name_column(table, df, hotel_name_map)

    stg_df = standardize_column(table, "room_type", stg_df, room_type_map)

    stg_df = standardize_column(table, "pension", stg_df, pension_map)

    # Add a new column indicating the source table
    stg_df['source'] = table

    return stg_df


# Define transform function
def transform(**kwargs):
    """
        Transforms data extracted from multiple source tables using a transformation function.

        Parameters:
        **kwargs: Arbitrary keyword arguments. Expected to contain 'ti' as TaskInstance context.
                  'ti' should provide extracted data via XCom with task_id 'extract'.

        Returns:
        dict: A dictionary where the keys are table names and the values are DataFrames containing transformed data.
    """
    # Retrieve extracted data from XCom using TaskInstance 'ti'
    extracted_data = kwargs['ti'].xcom_pull(task_ids='extract')

    transformed_data = {}

    # Iterate over each table and its corresponding DataFrame from extracted_data
    for table, df in extracted_data.items():
        # Transform the data for the current table and store the result in transformed_data
        transformed_data[table] = transform_data(table, df)

    return transformed_data


# Define load function (assuming you have a load function)
def load(**kwargs):
    """
        Loads transformed data into a PostgreSQL database.

        Parameters:
        **kwargs: Arbitrary keyword arguments. Expected to contain 'ti' as TaskInstance context.
                  'ti' should provide transformed data via XCom with task_id 'transform'.

        Returns:
        None
    """
    # Get the database connection details
    conn = BaseHook.get_connection(db_name)
    # Create the connection string for the PostgreSQL database
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')

    # Retrieve transformed data from XCom using TaskInstance 'ti'
    transformed_data = kwargs['ti'].xcom_pull(task_ids='transform')

    # Merge all DataFrames into one DataFrame
    merged_df = pd.concat(transformed_data.values(), ignore_index=True)

    # Load merged DataFrame into the database table 'merged_df'
    merged_df.to_sql('merged_df', engine, if_exists='replace', index=False, chunksize=100000)


def get_or_create_dim_id(engine, table, value_column, value):
    """
        Retrieves or creates a dimension ID for a given value in a database table.

        Parameters:
        engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine instance for database connection.
        table (sqlalchemy.Table): The SQLAlchemy Table object representing the dimension table.
        value_column (str): The name of the column in the dimension table containing values.
        value (str): The value to retrieve or create in the dimension table.

        Returns:
        int: The ID of the dimension corresponding to the provided value.
    """
    with engine.connect() as connection:
        # Check if the value already exists in the dimension table
        select_stmt = select([table.c.id]).where(table.c[value_column] == value)
        result = connection.execute(select_stmt).fetchone()
        if result:
            return result[0]
        else:
            # Insert the new value and return the new ID
            insert_stmt = insert(table).values({value_column: value}).returning(table.c.id)
            result = connection.execute(insert_stmt).fetchone()
            return result[0]


def load_fact_table():
    conn = BaseHook.get_connection(db_name)
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')

    metadata = MetaData()

    check_in_dim = Table('check_in_dim', metadata, autoload_with=engine)
    extracted_at_dim = Table('extracted_at_dim', metadata, autoload_with=engine)
    location_dim = Table('location_dim', metadata, autoload_with=engine)
    source_dim = Table('source_dim', metadata, autoload_with=engine)
    hotel_dim = Table('hotel_dim', metadata, autoload_with=engine)
    room_dim = Table('room_dim', metadata, autoload_with=engine)
    pension_dim = Table('pension_dim', metadata, autoload_with=engine)
    fact_table = Table('fact_table', metadata, autoload_with=engine)

    staging_table = Table('stg_table', metadata, autoload_with=engine)

    # Extract data from the staging table
    with engine.connect() as connection:
        result = connection.execute(select([staging_table]))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

    # Map values to dimension IDs and create the fact table DataFrame
    fact_df = pd.DataFrame()
    fact_df['check_in_id'] = df['check_in'].apply(lambda x: get_or_create_dim_id(engine, check_in_dim, 'check_in', x))
    fact_df['extracted_at_id'] = df['extracted_at'].apply(
        lambda x: get_or_create_dim_id(engine, extracted_at_dim, 'extracted_at', x))
    fact_df['location_id'] = df['destination'].apply(
        lambda x: get_or_create_dim_id(engine, location_dim, 'destination', x))
    fact_df['source_id'] = df['name'].apply(lambda x: get_or_create_dim_id(engine, source_dim, 'name', x))
    fact_df['hotel_id'] = df['standardized_name'].apply(
        lambda x: get_or_create_dim_id(engine, hotel_dim, 'standardized_name', x))
    fact_df['room_id'] = df['standardized_room_type'].apply(
        lambda x: get_or_create_dim_id(engine, room_dim, 'standardized_room_type', x))
    fact_df['pension_id'] = df['standardized_pension'].apply(
        lambda x: get_or_create_dim_id(engine, pension_dim, 'standardized_pension', x))
    fact_df['price'] = df['price']
    fact_df['annulation'] = df['annulation']
    fact_df['availabilty'] = df['availabilty']

    # Load data into the fact table
    with engine.connect() as connection:
        connection.execute(insert(fact_table), fact_df.to_dict(orient='records'))

    print("Data successfully loaded into the data warehouse.")
