#%%
import requests
from requests.auth import HTTPBasicAuth
import get_ca_data as get_ca
import zipfile
from pathlib import Path
import logging
import json
import polars as pl
import re
from typing import Dict, Optional

#%%
def download_epc_nondom_zip_files(years: Optional[list[int]],
                                  all: bool = True,
                                  folder: str = 'data/epc_csv') -> None:
    if years is None and not all:
        logging.error("No years provided and 'all' is set to False.")
        return
    base_url = "https://epc.opendatacommunities.org/api/v1/files/non-domestic-{}.zip"
    all_bulk_url = "https://epc.opendatacommunities.org/files/all-non-domestic-certificates.zip"
    epc_creds = get_ca.load_config('../config.yml').get('epc')

    auth = HTTPBasicAuth(epc_creds.get('email'), epc_creds.get('apikey'))
    
    if not all:
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
    else:
        response = requests.get(all_bulk_url, auth=auth)
        filepath = f"{folder}/all-non-domestic-certificates.zip"
        if response.status_code == 200:
            with open(filepath, 'wb') as file:
                file.write(response.content)
            print(f"Downloaded: all-non-domestic-certificates.zip")
        else:
            print(f"Failed to download: {all_bulk_url}, Status Code: {response.status_code}")
    return None
#%%
# Example usage: download zip files for the years 2021, 2022, and 2023

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
def extract_nondom_schema(zip_path: str) -> str:
    """
    Extracts the schema.json file from the zip file and renames it to schema.json
    Args:
        zip_path (str): Path to the zip file
    Returns:
        str: Path to the extracted schema.json file
    """
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
            if 'schema.json' not in zip_ref.namelist():
                logging.error("schema.json not found in the zip file.")
                return "Error: schema.json not found in the zip file."

            # Extract certificates.csv
            zip_ref.extract('schema.json', '.')

            # Create new filename
            new_filename = f"{str(zip_file_path.parent)}/schema.json"
            new_file_path = Path(new_filename)

            # Rename the extracted file
            Path('schema.json').rename(new_file_path)

        # Delete the zip file
        zip_file_path.unlink()
        logging.info(f"Extraction successful: {new_file_path}")
        return new_file_path

    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return f"Error: {str(e)}"
    
#%%
   
schema_path = extract_nondom_schema('data/non-domestic-2021.zip')
#%%
# %%
def get_csv_schema(json_path: str) -> dict:
    """
    Extracts the schema for certificates.csv from the JSON file
    Args:
        json_path (str): Path to the JSON file
    Returns:
        dict: Schema for certificates.csv
        in the form of names: polars datatypes
        For import into Polars DataFrame
    """
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


#%%

def list_files_matching_pattern(folder: str, pattern: str) -> list:
    folder_path = Path(folder)
    if not folder_path.exists():
        logging.error(f"Folder {folder_path} does not exist.")
        return []

    matching_files = [str(file) for file in folder_path.iterdir() if re.match(pattern, file.name)]
    return matching_files

# Example usage
epc_dl_folder = 'data/epc_csv'
#%%

#%%
download_epc_nondom_zip_files(list(range(2008,2025)), epc_dl_folder)
zip_files = list_files_matching_pattern(epc_dl_folder, r"non-domestic-\d{4}\.zip")
#%%
schema_path = extract_nondom_schema(zip_files[0])

[extract_and_rename_csv(file) for file in zip_files]

#%%
schema = get_csv_schema(schema_path)
print(schema)
# %%
#%%
non_dom_csv_file_list = list_files_matching_pattern(
    epc_dl_folder,
    r"non-domestic-\d{4}-certificates.csv")
# %%
pldf_non_dom_list = [pl.read_csv(file, schema=schema) for file in non_dom_csv_file_list]
#%%
nondom_cert_df = pl.concat(pldf_non_dom_list)