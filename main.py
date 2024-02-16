import pandas as pd
import os
from google.cloud import bigquery
from DataNormalisation.datanormalisation import insered_data_into_bquery

# Path to your JSON key file
key_path = "key.json"

# Set environment variable to point to your key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

# Initialize BigQuery client
client = bigquery.Client()

project_id = 'project-414304'
dataset_id = 'onepage'
table_id = 'master_document_table'

if __name__ == "__main__":
    insered_data_into_bquery(project_id,dataset_id,table_id)
