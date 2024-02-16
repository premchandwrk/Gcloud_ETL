import pandas_gbq as gbq

def create_or_append_to_bigquery_tables(tables_dict, project_id, dataset_id):
    """
    Create or append to BigQuery tables.
    """
    for table_name, df in tables_dict.items():
        try:
            gbq.to_gbq(df, f"{dataset_id}.{table_name}", project_id=project_id, if_exists='replace')
        except Exception as e:
            print(f"An error occurred while creating or appending to the BigQuery table {table_name}: {str(e)}")