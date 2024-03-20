import os
import logging
from DataNormalisation.datanormalisation import insert_data_into_bquery

# Set up logging
logging.basicConfig(filename='etl.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Path to your JSON key file
key_path = "key.json"

# Set environment variable to point to your key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

project_id = 'project-414304'
dataset_id = 'onepage'
table_id = 'master_document_table'

if __name__ == "__main__":
    insert_data_into_bquery(project_id, dataset_id, table_id)
