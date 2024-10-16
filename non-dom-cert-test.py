#%%
import requests
from requests.auth import HTTPBasicAuth
import get_ca_data as get_ca
import zipfile
from pathlib import Path
import logging
import json
import polars as pl
#%%
del epc_creds


#%%
def download_zip_files(years: list[int], folder: str = 'data/epc_csv') -> str:
    base_url = "https://epc.opendatacommunities.org/api/v1/files/non-domestic-{}.zip"
    epc_creds = get_ca.load_config('../config.yml').get('epc')

    auth = HTTPBasicAuth(epc_creds.get('email'), epc_creds.get('apikey'))
    
    for year in years:
        url = base_url.format(year)
        response = requests.get(url, auth=auth)
        filepath = f"{folder}/non-domestic-{year}.zip"
        if response.status_code == 200:
            with open(filepath, 'wb') as file:
                file.write(response.content)
            print(f"Downloaded: non-domestic-{year}.zip")
            
        else:
            print(f"Failed to download: {url}, Status Code: {response.status_code}")
#%%
# Example usage: download zip files for the years 2021, 2022, and 2023
non_dom_path = download_zip_files([2021])
# %%
def extract_and_rename_csv(zip_path):
    try:
        # Convert the path to a Path object
        zip_file_path = Path(zip_path)

        # Check if the zip file exists
        if not zip_file_path.exists():
            logging.error(f"File {zip_file_path} does not exist.")
            return f"Error: File {zip_file_path} does not exist."

        # Open the zip file
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            # Check if certificates.csv is in the zip file
            if 'certificates.csv' not in zip_ref.namelist():
                logging.error("certificates.csv not found in the zip file.")
                return "Error: certificates.csv not found in the zip file."

            # Extract certificates.csv
            zip_ref.extract('certificates.csv', '.')

            # Create new filename
            base_name = zip_file_path.parent / zip_file_path.stem
            new_filename = f"{base_name}-certificates.csv"
            new_file_path = Path(new_filename)

            # Rename the extracted file
            Path('certificates.csv').rename(new_file_path)

        # Delete the zip file
        zip_file_path.unlink()
        logging.info(f"Extraction successful: {new_file_path}")
        return f"Extraction successful: {new_file_path}"

    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return f"Error: {str(e)}"
#%%
# Example usage
result = extract_and_rename_csv('data/non-domestic-2021.zip')
print(result)
# %%
def get_csv_schema(json_path):
    try:
        # Convert the path to a Path object
        json_file_path = Path(json_path)

        # Check if the JSON file exists
        if not json_file_path.exists():
            logging.error(f"File {json_file_path} does not exist.")
            return None

        # Open and parse the JSON file
        with json_file_path.open('r', encoding='utf-8') as file:
            schema_data = json.load(file)

        # Extract the schema for certificates.csv
        for table in schema_data.get('tables', []):
            if table.get('url') == 'certificates.csv':
                schema = {}
                for column in table['tableSchema']['columns']:
                    name = column['name']
                    datatype = column['datatype']
                    # Map JSON datatypes to Polars datatypes
                    polars_type = map_datatype(datatype)
                    schema[name] = polars_type
                logging.info("Schema successfully extracted.")
                return schema

        logging.error("certificates.csv schema not found.")
        return None

    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return None
#%%
def map_datatype(datatype):
    # Map JSON datatypes to Polars datatypes
    type_mapping = {
        "string": pl.Utf8,
        "integer": pl.Int64,
        "decimal": pl.Float64,
        "date": pl.Date
    }
    return type_mapping.get(datatype, pl.Utf8)
#%%
# Example usage
schema = get_csv_schema('data/schema.json')
print(schema)
# %%
sc = pl.Schema(schema)

#%%

nondom_cert_test_df = pl.read_csv('data/non-domestic-2021-certificates.csv', schema=schema)