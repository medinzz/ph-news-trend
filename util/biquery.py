import os
from dotenv import load_dotenv
from google.cloud import bigquery
import pandas as pd
from typing import List, Dict, Any

# Local libraries
from util.tools import setup_logger

# load environment variables from .env file
load_dotenv()

# Get the value, returns None if not set
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')

client = bigquery.Client(project=GCP_PROJECT_ID)
logger = setup_logger()


def create_or_update_dataset(dataset_id: str, location: str = 'US'):
    dataset_ref = f'{GCP_PROJECT_ID}.{dataset_id}'
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = location

    try:
        client.create_dataset(dataset, exists_ok=True)
        logger.info(f'Dataset `{dataset_id}` ready.')
    except Exception as e:
        logger.error(f'Dataset creation failed: {e}')


def create_or_update_table(
    df: pd.DataFrame, dataset_id: str, table_name: str, mode: str = 'append'
):
    """
    Creates or updates a BigQuery table with the given DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to be loaded into the BigQuery table.
        dataset_id (str): The ID of the BigQuery dataset where the table resides.
        table_name (str): The name of the BigQuery table to create or update.
        mode (str, optional): The write mode for the operation. Defaults to 'append'.
            - 'append': Appends the data to the existing table.
            - 'replace': Replaces the existing table with the new data.
            - 'fail': Fails the operation if the table already exists.
    """
    table_id = f'{GCP_PROJECT_ID}.{dataset_id}.{table_name}'
    write_modes = {
        'append': bigquery.WriteDisposition.WRITE_APPEND,
        'replace': bigquery.WriteDisposition.WRITE_TRUNCATE,
        'fail': bigquery.WriteDisposition.WRITE_EMPTY,
    }

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_modes.get(mode, bigquery.WriteDisposition.WRITE_APPEND)
    )

    try:
        client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
        logger.info(
            f'Table `{table_name}` in dataset `{dataset_id}` updated with mode `{mode}`.'
        )
    except Exception as e:
        logger.error(f'Table update failed: {e}')


def insert_rows(dataset_id: str, table_name: str, rows: List[Dict[str, Any]]):
    """
    Inserts rows of data into a specified BigQuery table.

    Args:
        dataset_id (str): The ID of the dataset containing the target table.
        table_name (str): The name of the target table where rows will be inserted.
        rows (List[Dict[str, Any]]): A list of dictionaries, where each dictionary represents
            a row of data to be inserted.

    Logs:
        Logs an error message if there are insertion errors.
        Logs an info message indicating the number of rows successfully inserted.

    Raises:
        None: This function does not raise exceptions but logs errors if insertion fails.
    """
    table_id = f'{GCP_PROJECT_ID}.{dataset_id}.{table_name}'

    errors = client.insert_rows_json(table_id, rows)
    if errors:
        logger.error(f'Insertion errors: {errors}')
    else:
        logger.info(f'{len(rows)} row(s) inserted into `{table_name}`.')


def run_query(query: str):
    """
    Executes a SQL query using the BigQuery client and returns the results as a pandas DataFrame.

    Args:
        query (str): The SQL query to execute.

    Returns:
        pandas.DataFrame: A DataFrame containing the query results if successful.
        None: If the query execution fails.

    Logs:
        Logs an informational message if the query is executed successfully.
        Logs an error message if the query execution fails.
    """
    try:
        query_job = client.query(query)
        results = query_job.result()
        logger.info('Query executed.')
        return results.to_dataframe()
    except Exception as e:
        logger.error(f'Query failed: {e}')
        return None


def delete_records(dataset_id: str, table_name: str, condition: str):
    """
    Deletes records from a specified BigQuery table based on a given condition.

    Args:
        dataset_id (str): The ID of the dataset containing the table.
        table_name (str): The name of the table from which records will be deleted.
        condition (str): The SQL condition to determine which records to delete.

    Returns:
        None

    Side Effects:
        Executes a DELETE query on the specified BigQuery table and logs the operation.

    Raises:
        Any exceptions raised by the `run_query` function will propagate.
    """
    query = f'DELETE FROM `{GCP_PROJECT_ID}.{dataset_id}.{table_name}` WHERE {condition}'
    run_query(query)
    logger.info(f'Deleted records with condition: {condition}')


def update_records(dataset_id: str, table_name: str, set_clause: str, condition: str):
    """
    Updates records in a specified BigQuery table based on the given condition.

    Args:
        dataset_id (str): The ID of the dataset containing the table to update.
        table_name (str): The name of the table to update.
        set_clause (str): The SQL SET clause specifying the columns and values to update.
        condition (str): The SQL WHERE clause specifying the condition for the update.

    Returns:
        None

    Side Effects:
        Executes an SQL UPDATE query on the specified BigQuery table and logs the operation.

    Raises:
        Any exceptions raised by the `run_query` function will propagate to the caller.
    """
    query = f'UPDATE `{GCP_PROJECT_ID}.{dataset_id}.{table_name}` SET {set_clause} WHERE {condition}'
    run_query(query)
    logger.info(f'Updated records with condition: {condition}')