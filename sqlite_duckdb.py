#%%

import polars as pl
import duckdb # version 1.1.1
import requests
import shutil
from pathlib import Path
import zipfile
import logging

#%%
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # You can also add logging to a file, if necessary
        logging.FileHandler('etl.log')  # Log to a file named app.log
    ]
)

#%%
base_url_pc_centroids_zip = "https://www.arcgis.com/sharing/rest/content/items/3700342d3d184b0d92eae99a78d9c7a3/data"

#%%

def download_zip(url: str, directory: str = "data", filename: str = None) -> str:
    """
    Downloads a zip file from the given URL and saves it to the specified directory with an optional custom filename.
    
    Args:
        url (str): The URL of the zip file to download.
        directory (str): The directory where the zip file will be saved. Defaults to "data".
        filename (str, optional): The name to save the zip file as. If not provided, the name is extracted from the URL.
    
    Returns:
        str: The full path to the downloaded file.
    """
    logging.info(f"Starting download from URL: {url}")
    # Create a Path object for the directory
    directory_path = Path(directory)
    
    # Ensure the directory exists, create it if it doesn't
    directory_path.mkdir(parents=True, exist_ok=True)
    
    # Use the provided filename or extract the filename from the URL
    if filename is None:
        filename = url.split("/")[-1]
    
    # Create the full file path
    file_path = directory_path / filename
    logging.info(f"Saving zip file to: {file_path}")
    # Stream the download for efficiency
    with requests.get(url, stream=True) as r:
        r.raise_for_status()  # Check if the request was successful
        with file_path.open('wb') as f:
            shutil.copyfileobj(r.raw, f)

    logging.info(f"Download completed: {file_path}")

    return str(file_path)
#%%

def extract_csv_from_zip(zip_file_path: str) -> str:
    """
    Extracts the CSV file from the immediate 'Data' folder inside the given zip file
    and saves it directly to the same folder where the zip file is located, without
    retaining the 'Data/' folder structure.
    
    Args:
        zip_file_path (str): The path to the zip file.
    
    Returns:
        str: The path to the extracted CSV file.
    
    Raises:
        FileNotFoundError: If no CSV file is found in the 'Data' folder inside the zip.
        ValueError: If multiple or no CSV files are found in the immediate 'Data' folder.
    """
    logging.info(f"Extracting CSV from zip file: {zip_file_path}")

    # Create Path object for the zip file and get the directory where it is located
    zip_file = Path(zip_file_path)
    extract_path = zip_file.parent  # Extract to the same directory as the zip file

    # Ensure the zip file exists
    if not zip_file.exists():
        logging.error(f"Zip file '{zip_file}' does not exist.")
        raise FileNotFoundError(f"Zip file '{zip_file}' does not exist.")

    # Open the zip file
    with zipfile.ZipFile(zip_file, 'r') as z:
        # List all files in the zip archive
        all_files = z.namelist()

        # Filter for CSV files in the immediate 'Data/' folder (no subfolders)
        csv_files = [f for f in all_files if f.startswith('Data/') and f.count('/') == 1 and f.endswith('.csv')]

        # Ensure there's exactly one CSV file in the immediate 'Data' folder
        if len(csv_files) == 0:
            logging.error("No CSV file found in the immediate 'Data' folder.")
            raise FileNotFoundError("No CSV file found in the immediate 'Data' folder inside the zip.")
        elif len(csv_files) > 1:
            logging.error("Multiple CSV files found in the 'Data' folder.")
            raise ValueError("Multiple CSV files found in the immediate 'Data' folder. Only one expected.")

        # Extract the CSV file without the 'Data/' folder structure
        csv_file = csv_files[0]
        csv_filename = Path(csv_file).name  # Get only the file name, ignoring the folder
        extracted_csv_path = extract_path / csv_filename

        logging.info(f"Extracting CSV to: {extracted_csv_path}")

        # Extract the file, but rename it to remove the folder structure
        with z.open(csv_file) as source, extracted_csv_path.open('wb') as target:
            shutil.copyfileobj(source, target)
        
        logging.info(f"Extraction completed: {extracted_csv_path}")

        return str(extracted_csv_path)

#%%

def delete_zip_file(zip_file_path: str):
    """
    Deletes the specified zip file if it exists.
    
    Args:
        zip_file_path (str): The path to the zip file that should be deleted.
    
    Raises:
        FileNotFoundError: If the zip file does not exist.
    """
    # Create a Path object for the zip file
    zip_file = Path(zip_file_path)

    # Check if the file exists
    if zip_file.exists() and zip_file.is_file():
        # Delete the file
        logging.info(f"Deleting zip file: {zip_file}")
        zip_file.unlink()
        print(f"Deleted zip file: {zip_file}")
        logging.info(f"Deleted zip file: {zip_file}")
    else:
        logging.error(f"Zip file '{zip_file}' does not exist or is not a file.")
        raise FileNotFoundError(f"Zip file '{zip_file}' does not exist or is not a file.")
#%%
zipped_file_path = download_zip(url = base_url_pc_centroids_zip,
directory="data",
filename="postcode_centroids.zip")
#%%
csv_file = extract_csv_from_zip(zip_file_path = zipped_file_path)
#%%
delete_zip_file(zip_file_path = zipped_file_path)
#%%
pl.read_csv(csv_file, n_rows=1000).glimpse()

#%%
csv_file = "data/ONSPD_NOV_2023_UK.csv"

#%%
schema_columns = {
    'OBJECTID': 'BIGINT',
    'PCD': 'VARCHAR',
    'PCD2': 'VARCHAR',
    'PCDS': 'VARCHAR',
    'DOINTR': 'BIGINT',
    'DOTERM': 'BIGINT',
    'OSCTY': 'VARCHAR',
    'CED': 'VARCHAR',
    'OSLAUA': 'VARCHAR',
    'OSWARD': 'VARCHAR',
    'PARISH': 'VARCHAR',
    'USERTYPE': 'BIGINT',
    'OSEAST1M': 'BIGINT',
    'OSNRTH1M': 'BIGINT',
    'OSEAST100M': 'BIGINT',
    'OSNRTH100M': 'BIGINT',
    'OSGRDIND': 'BIGINT',
    'OSHLTHAU': 'VARCHAR',
    'HRO': 'VARCHAR',
    'CTRY': 'VARCHAR',
    'GENIND': 'VARCHAR',
    'PAFIND': 'BIGINT',
    'RGN': 'VARCHAR',
    'STREG': 'BIGINT',
    'PCON': 'VARCHAR',
    'EER': 'VARCHAR',
    'TECLEC': 'VARCHAR',
    'TTWA': 'VARCHAR',
    'PCT': 'VARCHAR',
    'ITL': 'VARCHAR',
    'PSED': 'VARCHAR',
    'CENED': 'VARCHAR',
    'EDIND': 'BIGINT',
    'ADDRCT': 'BIGINT',
    'DPCT': 'BIGINT',
    'MOCT': 'BIGINT',
    'SMLBUSCT': 'BIGINT',
    'OSHAPREV': 'VARCHAR',
    'LEA': 'VARCHAR',
    'OLDHA': 'VARCHAR',
    'WARDC91': 'VARCHAR',
    'WARDO91': 'VARCHAR',
    'WARD98': 'VARCHAR',
    'STATSWARD': 'VARCHAR',
    'OA01': 'VARCHAR',
    'OAIND': 'BIGINT',
    'CASWARD': 'VARCHAR',
    'PARK': 'VARCHAR',
    'LSOA01': 'VARCHAR',
    'MSOA01': 'VARCHAR',
    'UR01IND': 'VARCHAR',
    'OAC01': 'VARCHAR',
    'OLDPCT': 'VARCHAR',
    'OLDHRO': 'VARCHAR',
    'CANREG': 'VARCHAR',
    'CANNET': 'VARCHAR',
    'OA11': 'VARCHAR',
    'LSOA11': 'VARCHAR',
    'MSOA11': 'VARCHAR',
    'SICBL': 'VARCHAR',
    'RU11IND': 'VARCHAR',
    'OAC11': 'VARCHAR',
    'WZ11': 'VARCHAR',
    'BUA11': 'VARCHAR',
    'BUASD11': 'VARCHAR',
    'LAT': 'DOUBLE',
    'LONG': 'DOUBLE',
    'LEP1': 'VARCHAR',
    'LEP2': 'VARCHAR',
    'PFA': 'VARCHAR',
    'IMD': 'BIGINT',
    'CALNCV': 'VARCHAR',
    'NHSER': 'VARCHAR',
    'ICB': 'VARCHAR',
    'OA21': 'VARCHAR',
    'LSOA21': 'VARCHAR',
    'MSOA21': 'VARCHAR',
    'GlobalID': 'VARCHAR',
    'x': 'BIGINT',
    'y': 'BIGINT'
}
#%%
schema_columns = {
    'pcd': 'VARCHAR',
    'pcd2': 'VARCHAR',
    'pcds': 'VARCHAR',
    'dointr': 'BIGINT',
    'doterm': 'BIGINT',
    'oscty': 'VARCHAR',
    'ced': 'VARCHAR',
    'oslaua': 'VARCHAR',
    'osward': 'VARCHAR',
    'parish': 'VARCHAR',
    'usertype': 'BIGINT',
    'oseast1m': 'BIGINT',
    'osnrth1m': 'BIGINT',
    'osgrdind': 'BIGINT',
    'oshlthau': 'VARCHAR',
    'nhser': 'VARCHAR',
    'ctry': 'VARCHAR',
    'rgn': 'VARCHAR',
    'streg': 'VARCHAR',
    'pcon': 'VARCHAR',
    'eer': 'VARCHAR',
    'teclec': 'VARCHAR',
    'ttwa': 'VARCHAR',
    'pct': 'VARCHAR',
    'itl': 'VARCHAR',
    'statsward': 'VARCHAR',
    'oa01': 'VARCHAR',
    'casward': 'VARCHAR',
    'npark': 'VARCHAR',
    'lsoa01': 'VARCHAR',
    'msoa01': 'VARCHAR',
    'ur01ind': 'VARCHAR',
    'oac01': 'VARCHAR',
    'oa11': 'VARCHAR',
    'lsoa11': 'VARCHAR',
    'msoa11': 'VARCHAR',
    'wz11': 'VARCHAR',
    'sicbl': 'VARCHAR',
    'bua11': 'VARCHAR',
    'buasd11': 'VARCHAR',
    'ru11ind': 'VARCHAR',
    'oac11': 'VARCHAR',
    'lat': 'DOUBLE',
    'long': 'DOUBLE',
    'lep1': 'VARCHAR',
    'lep2': 'VARCHAR',
    'pfa': 'VARCHAR',
    'imd': 'BIGINT',
    'calncv': 'VARCHAR',
    'icb': 'VARCHAR',
    'oa21': 'VARCHAR',
    'lsoa21': 'VARCHAR',
    'msoa21': 'VARCHAR'
}
#%%
con = duckdb.connect(database='data/postcodes.duckdb', read_only=False)

#%%
query_create_postcodes_centroids = f"""
CREATE OR REPLACE TABLE postcode_centroids_tbl AS 
SELECT * 
FROM read_csv(
'{csv_file}',
dtypes={schema_columns},
header=True,      
null_padding=True);
"""

#%%

con.execute(query_create_postcodes_centroids)

#%%

con.query("SELECT COUNT(*) FROM postcode_centroids_tbl;")

#%%
query = f"""
CREATE TABLE postcode_centroids_cauth AS 
SELECT PCDS,OSLAUA,OSWARD,OSEAST1M,OSNRTH1M,PCON,LSOA11,MSOA11,LAT,LONG,LEP1,LEP2,IMD,LSOA21,MSOA21 
FROM read_csv(
'data/postcode_centroids.csv',
dtypes={schema_columns},
header=True,      
null_padding=True)
WHERE LSOA21 IN {lsoa_in_cauths};
"""
print(query)
#%%
con.sql(query)
    
#%%

con.sql("SHOW TABLES;")
#%%
con.close()
#%%

con.sql("SELECT COUNT(PCDS) FROM postcode_centroids_cauth;")

# %%

#%%
#%%
# polars schema
schema = {
    "OBJECTID": pl.Int64,
    "PCD": pl.Utf8,
    "PCD2": pl.Utf8,
    "PCDS": pl.Utf8,
    "DOINTR": pl.Int64,
    "DOTERM": pl.Int64,
    "OSCTY": pl.Utf8,
    "CED": pl.Utf8,
    "OSLAUA": pl.Utf8,
    "OSWARD": pl.Utf8,
    "PARISH": pl.Utf8,
    "USERTYPE": pl.Int64,
    "OSEAST1M": pl.Int64,
    "OSNRTH1M": pl.Int64,
    "OSEAST100M": pl.Int64,
    "OSNRTH100M": pl.Int64,
    "OSGRDIND": pl.Int64,
    "OSHLTHAU": pl.Utf8,
    "HRO": pl.Utf8,
    "CTRY": pl.Utf8,
    "GENIND": pl.Utf8,  # Nullable
    "PAFIND": pl.Int64,  # Nullable
    "RGN": pl.Utf8,
    "STREG": pl.Int64,
    "PCON": pl.Utf8,
    "EER": pl.Utf8,
    "TECLEC": pl.Utf8,
    "TTWA": pl.Utf8,
    "PCT": pl.Utf8,
    "ITL": pl.Utf8,
    "PSED": pl.Utf8,
    "CENED": pl.Utf8,
    "EDIND": pl.Int64,
    "ADDRCT": pl.Int64,  # Nullable
    "DPCT": pl.Int64,    # Nullable
    "MOCT": pl.Int64,    # Nullable
    "SMLBUSCT": pl.Int64,  # Nullable
    "OSHAPREV": pl.Utf8,
    "LEA": pl.Utf8,
    "OLDHA": pl.Utf8,
    "WARDC91": pl.Utf8,
    "WARDO91": pl.Utf8,
    "WARD98": pl.Utf8,
    "STATSWARD": pl.Utf8,
    "OA01": pl.Utf8,
    "OAIND": pl.Int64,
    "CASWARD": pl.Utf8,
    "PARK": pl.Utf8,
    "LSOA01": pl.Utf8,
    "MSOA01": pl.Utf8,
    "UR01IND": pl.Int64,
    "OAC01": pl.Utf8,
    "OLDPCT": pl.Utf8,
    "OLDHRO": pl.Utf8,
    "CANREG": pl.Utf8,
    "CANNET": pl.Utf8,
    "OA11": pl.Utf8,
    "LSOA11": pl.Utf8,
    "MSOA11": pl.Utf8,
    "SICBL": pl.Utf8,
    "RU11IND": pl.Int64,
    "OAC11": pl.Utf8,
    "WZ11": pl.Utf8,
    "BUA11": pl.Utf8,    # Nullable
    "BUASD11": pl.Utf8,
    "LAT": pl.Float64,
    "LONG": pl.Float64,
    "LEP1": pl.Utf8,
    "LEP2": pl.Utf8,    # Nullable
    "PFA": pl.Utf8,
    "IMD": pl.Int64,
    "CALNCV": pl.Utf8,
    "NHSER": pl.Utf8,
    "ICB": pl.Utf8,
    "OA21": pl.Utf8,
    "LSOA21": pl.Utf8,   # Nullable
    "MSOA21": pl.Utf8,   # Nullable
    "GlobalID": pl.Utf8,
    "x": pl.Int64,
    "y": pl.Int64
}
#%%
# just the columns we need
cols = ['PCDS','OSLAUA','OSWARD','OSEAST1M',
'OSNRTH1M','PCON','LSOA11','MSOA11','LAT',
'LONG','LEP1','LEP2','IMD','LSOA21','MSOA21']
print(cols)
#%%

pc_df_qry = pl.scan_csv('data/postcode_centroids.csv', schema = schema)
# %%

lsoa_df = pl.read_parquet('data/lsoa_poly/lsoa_poly_cauth_2024.parquet')

#%%
lsoa_in_cauths = (lsoa_df
             .select('LSOA21CD')
             .to_series()
             .to_list()
             )

#%%
postcodes_in_cauths = (pc_df_qry
                       .filter(pl.col('LSOA21')
                               .is_in(lsoa_in_cauths))
                        .select(pl.col(cols)
                            ).collect()
)

#%%

postcodes_in_cauths.glimpse()


















#%%
duckdb.execute("INSTALL sqlite;")
duckdb.execute("LOAD sqlite;")
#%%
duckdb.execute("INSTALL spatial;")
duckdb.execute("LOAD spatial;")

# %%
duckdb.execute("ATTACH 'data/postcode_centroids.geodatabase' (TYPE SQLITE);")

#%%
duckdb.execute("USE 'postcode_centroids';")


#%%

duckdb.execute("FROM information_schema.tables;")
# %%
