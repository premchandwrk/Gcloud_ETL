import pandas_gbq as gbq
import logging

def create_or_append_to_bigquery_tables(tables_dict, project_id, dataset_id):
    """
    Creates or appends data to BigQuery tables.

    Args:
    - tables_dict (dict): Dictionary containing table names as keys and corresponding DataFrames as values.
    - project_id (str): Google Cloud project ID.
    - dataset_id (str): BigQuery dataset ID.

    Returns:
    - None
    """
    for table_name, df in tables_dict.items():
        try:
            gbq.to_gbq(df, f"{dataset_id}.{table_name}", project_id=project_id, if_exists='replace')
        except Exception as e:
            logging.error(f"An error occurred while creating or appending to the BigQuery table {table_name}: {str(e)}")
