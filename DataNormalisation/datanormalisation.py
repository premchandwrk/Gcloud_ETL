import pandas as pd
from google.cloud import bigquery
import logging
from BQconnector.bqconnector import create_or_append_to_bigquery_tables


def normalize_data(df):
    """
    Normalizes the DataFrame into separate tables.

    Args:
    - df (DataFrame): Input DataFrame to be normalized.

    Returns:
    - tuple: Tuple containing normalized DataFrames for Products, Crops, Diseases, Pests, and Documents.
    """
    # Split Product, Crop, Disease, and Pest information
    product_df = df['Product_names'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).to_frame('Product_Name').drop_duplicates().reset_index(drop=True)
    product_df['Product_ID'] = product_df.index + 1

    crop_df = df['crop'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).to_frame('Crop_Name').drop_duplicates().reset_index(drop=True)
    crop_df['Crop_ID'] = crop_df.index + 1

    disease_df = df['Diseases'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).to_frame('Disease_Name').drop_duplicates().reset_index(drop=True)
    disease_df['Disease_ID'] = disease_df.index + 1

    pest_df = df['pest'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).to_frame('Pest_Name').drop_duplicates().reset_index(drop=True)
    pest_df['Pest_ID'] = pest_df.index + 1

    # Merge Document information with normalized tables
    document_df = df.drop(columns=['Product_names', 'crop', 'Diseases', 'pest'])
    
    document_df['Product_ID'] = df['Product_names'].apply(lambda x: ','.join(str(product_df.loc[product_df['Product_Name'] == p, 'Product_ID'].iloc[0]) for p in x.split(',')) if pd.notnull(x) else None)
    document_df['Crop_ID'] = df['crop'].apply(lambda x: ','.join(str(crop_df.loc[crop_df['Crop_Name'] == c, 'Crop_ID'].iloc[0]) for c in x.split(',')) if pd.notnull(x) else None)
    document_df['Disease_ID'] = df['Diseases'].apply(lambda x: ','.join(str(disease_df.loc[disease_df['Disease_Name'] == d, 'Disease_ID'].iloc[0]) for d in x.split(',')) if pd.notnull(x) else None)
    document_df['Pest_ID'] = df['pest'].apply(lambda x: ','.join(str(pest_df.loc[pest_df['Pest_Name'] == p, 'Pest_ID'].iloc[0]) for p in x.split(',')) if pd.notnull(x) else None)

    return product_df, crop_df, disease_df, pest_df, document_df

def extracting_data_as_dataframe(project_id,dataset_id,table_id):
    """
    Extracts data from BigQuery into a DataFrame.

    Args:
    - project_id (str): Google Cloud project ID.
    - dataset_id (str): BigQuery dataset ID.
    - table_id (str): BigQuery table ID.

    Returns:
    - DataFrame: DataFrame containing the extracted data.
    """
    client = bigquery.Client(project=project_id)
    query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"
    df = client.query(query).to_dataframe()
    return df


def insert_data_into_bquery(project_id,dataset_id,table_id):
    """
    Inserts data into BigQuery after normalization.

    Args:
    - project_id (str): Google Cloud project ID.
    - dataset_id (str): BigQuery dataset ID.
    - table_id (str): BigQuery table ID.

    Returns:
    - None
    """           
    df = extracting_data_as_dataframe(project_id,dataset_id,table_id)
    logging.info(f"Extracting data from BigQuery {project_id}.{dataset_id}.{table_id} is completed") 

    if not df.empty:
        # Perform data normalization
        df['Modified'] = df['Modified'].astype(str)
        df['Trial_report'] = df['Trial_report'].astype(str)
        product_df, crop_df, disease_df, pest_df, document_df = normalize_data(df)
        logging.info("Normalize data is completed")
        # print(document_df.info())

        # Define tables dictionary
        tables_dict = {
            'Product': product_df,
            'Crop': crop_df,
            'Disease': disease_df,
            'Pest': pest_df,
            'Document': document_df,
        }

        # Create or append to BigQuery tables
        create_or_append_to_bigquery_tables(tables_dict, project_id, dataset_id)
        logging.info("Inserted data is completed")
