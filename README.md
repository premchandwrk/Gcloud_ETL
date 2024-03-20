# ETL Project README

## Overview
This project implements an ETL (Extract, Transform, Load) pipeline for ingesting data from Excel files, normalizing it, and loading it into BigQuery tables.

## File Structure
- `BQconnector`: Contains functions for interacting with BigQuery.
- `DataIngestion`: Contains functions for reading Excel data and ingesting it into BigQuery.
- `DataNormalisation`: Contains functions for normalizing data and loading it into BigQuery.
- `main.py`: Main script to execute the ETL pipeline.

## Setup
1. Install required dependencies:

    ```sh
    pip install pandas pandas-gbq google-cloud-bigquery
    ```
    
2. Set up Google Cloud credentials by providing the path to your JSON key file in the `key.json` file.
3. Ensure that the Google Cloud SDK is installed and configured properly.

## Usage
1. Place your Excel file(s) in the appropriate directory.
2. Update the `key.json` file with your Google Cloud service account key.
3. Execute the main script `main.py` to run the ETL pipeline.

## Logging
- The ETL process logs are written to a file named `etl.log`.
- Log messages include timestamps, log levels, and details of any errors encountered during the execution.

## Contributing
Feel free to contribute to this project by submitting pull requests or reporting issues.

## License
This project is licensed under the MIT License - see the `LICENSE` file for details.
