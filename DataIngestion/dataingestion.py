import re
import pandas as pd
from pandas_gbq import gbq
from google.cloud import bigquery
import logging

# Path to your JSON key file
key_path = "key.json"

# Set environment variable to point to your key
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

# Initialize BigQuery client
client = bigquery.Client()

# Now you can use the client object to interact with BigQuery

# Define master schema
MASTER_SCHEMA = [
    {'name': 'Title', 'type': 'STRING'},
    {'name': 'Name', 'type': 'STRING'},
    {'name': 'Filetype', 'type': 'STRING'},
    {'name': 'Modified', 'type': 'STRING'},
    {'name': 'Year', 'type': 'INTEGER'},
    {'name': 'Country', 'type': 'STRING'},
    {'name': 'Classification', 'type': 'STRING'},
    {'name': 'Category_for_one_page', 'type': 'STRING'},
    {'name': 'Application_type', 'type': 'STRING'},
    {'name': 'path', 'type': 'STRING'},
    {'name': 'filesize', 'type': 'INTEGER'},  
    {'name': 'Product_names', 'type': 'STRING'},
    {'name': 'Diseases', 'type': 'STRING'},
    {'name': 'pest', 'type': 'STRING'},
    {'name': 'crop', 'type': 'STRING'},
    {'name': 'efficacy', 'type': 'STRING'},
    {'name': 'selectivity', 'type': 'STRING'},
    {'name': 'Field_or_greenhouse_trial', 'type': 'STRING'},
    {'name': 'Trial_report', 'type': 'STRING'},
    {'name': 'sumamry', 'type': 'STRING'},  
    {'name': 'language', 'type': 'STRING'},
    {'name': 'created_at', 'type': 'TIMESTAMP'},
    {'name': 'updated_at', 'type': 'TIMESTAMP'}
    # Add more columns as needed
]

def read_excel_to_dataframe(excel_file, sheet_name):
    """
    Read data from Excel file into a Pandas DataFrame.
    
    Args:
    - excel_file (str): Path to the Excel file.
    - sheet_name (str or int): Name or index of the sheet to read.
    
    Returns:
    - DataFrame: The DataFrame containing the data from the specified sheet.
    """
    try:
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        # print(df.info())
        return df
    except FileNotFoundError:
        logging.error("Error: File not found. Please provide a valid file path.")
        return None
    except pd.errors.ParserError:
        logging.error("Error: File format is not valid. Please provide a valid Excel file.")
        return None

def create_or_append_to_bigquery_table(df, project_id, dataset_id, table_id, schema):
    """
    Truncates the BigQuery table and loads new data from DataFrame into it.
    Only inserts columns matching the schema.

    Args:
    - df (DataFrame): DataFrame containing data to be inserted into BigQuery.
    - project_id (str): Google Cloud project ID.
    - dataset_id (str): BigQuery dataset ID.
    - table_id (str): BigQuery table ID.
    - schema (list): List of dictionaries specifying the schema of the BigQuery table.

    Returns:
    - None
    """
    try:
        # Convert float columns to strings
        df["Modified"] = df["Modified"].astype(str)
        df["Trial_report"] = df["Trial_report"].astype(str)
            
        # Create table
        gbq.to_gbq(df,f"{dataset_id}.{table_id}", project_id=project_id, if_exists='replace', table_schema=schema)
        
        # Filter DataFrame columns based on master schema
        df_filtered = df[[col['name'] for col in schema if col['name'] in df.columns]]
        
        # Insert data into BigQuery
        df_filtered.to_gbq(destination_table=f"{dataset_id}.{table_id}",
                           project_id=project_id,
                           if_exists='replace',
                           table_schema=schema)  # enforce schema during insertion
    except Exception as e:
        logging.error(f"An error occurred while creating or appending to the BigQuery table: {str(e)}")

# Function to remove counts in parentheses
def remove_counts(text):
    """
    Removes counts in parentheses from a string.

    Args:
    - text (str): Input string containing counts.

    Returns:
    - str: String with counts removed.
    """
    # Remove counts in parentheses
    text = re.sub(r'\(\d+\)', '', text)
    # Remove extra whitespace characters around commas
    text = re.sub(r'\s*,\s*', ',', text)
    return text.strip()


def main():
    """
    Main function for data ingestion.
    """
    excel_file = 'random_data1.xlsx'
    sheet_name = 'Sheet1'
    project_id = 'project-414304'
    dataset_id = 'onepage'
    table_id = 'master_document_table'

    df = read_excel_to_dataframe(excel_file, sheet_name)
    # Apply the function to the 'Product_names' column
    df['Product_names'] = df['Product_names'].apply(remove_counts).str.strip()
    df['Diseases'] = df['Diseases'].apply(remove_counts)
    df['pest'] = df['pest'].apply(remove_counts)
    df['crop'] = df['crop'].apply(remove_counts)

    # df['Product_names'] = df['Product_names'].str.replace('Product 3', 'Product 10')

    if df is not None:
        create_or_append_to_bigquery_table(df, project_id, dataset_id, table_id, MASTER_SCHEMA)

if __name__ == "__main__":
    main()
