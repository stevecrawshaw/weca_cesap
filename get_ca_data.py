
from datetime import datetime
import requests
import polars as pl
import os
import duckdb
import json
import pathlib
import zipfile
from pyproj import Transformer
from pathlib import Path

"""
Functions to get geographies and EPC data for combined authorities
These are used in the notebook cesap_epc_load_duckdb.ipynb
To populate a duckdb database which holds base data for analyses
supporting the CESAP indicators. Plotting and further analysis are done in
R scripts in this project folder.
"""

def remove_numbers(input_string: str) -> str:
    """
    For an input string remove all numbers and return as a string
    """
    # rename columns
    # Create a translation table that maps each digit to None
    lowercase_string = input_string.lower()
    translation_table = str.maketrans("", "", "0123456789")
    # Use the translation table to remove all numbers from the input string
    result_string = lowercase_string.translate(translation_table)
    return result_string

def wrangle_epc(certs_df: pl.DataFrame, remove_numbers = remove_numbers) -> pl.DataFrame:
    certs_df_names = get_rename_dict(certs_df, remove_numbers)
    """
    Creates date artefacts and changes data types
    """
    wrangled_df = (        
    certs_df
    .with_columns([pl.col('LODGEMENT_DATETIME')
                   .dt.date()
                   .alias('date')])
    .with_columns([pl.col('date').dt.year().alias('year'),
                   pl.col('date').dt.month().cast(pl.Int16).alias('month'),
                   pl.col('date').cast(pl.Utf8)])
    .rename(certs_df_names)
    .filter(~pl.col('uprn').is_duplicated()) # some nulls and duplicates (~134) so remove
    .drop('lodgement_datetime'))
    return wrangled_df

def get_ca_la_df(year: int,
                 baseurl: str = 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/',
                 inc_ns: bool = True,
                 remove_numbers = remove_numbers) -> pl.DataFrame:

    """
    Download the lookup table for Combined and Local Authorities
    From ArcGIS ONS Geography portal
    Different versions for year - boundary changes
    Rename columns by removing numerals
    """
    c_year = (datetime.now().year) + 1
    start_year = c_year - 3
    years = list(range(start_year, c_year))    

    try:
        assert year in years
        year_suffix = str(year)[2:4]
        lad_cauth_portion = f'LAD{year_suffix}_CAUTH{year_suffix}'
        url_suffix = '_EN_LU/FeatureServer/0/query'
        url = f'{baseurl}{lad_cauth_portion}{url_suffix}'
        params = {
        "where":"1=1", # maps to True
        "outFields":"*", # all
        "SR":"4326", # WGS84
        "f":"json"
        }

        r = requests.get(url = url, params = params)
        if r.status_code != 200:
            raise Exception(f'API call failed {r.status_code}')
    except AssertionError:
        print(f'API call failed {r.status_code}')
    except:
        print(r.status_code)

    else:
        response = r.json()
        attrs = response.get('features')
        rows = [attr.get('attributes') for attr in attrs]
        ca_la_df = pl.DataFrame(rows).select(pl.exclude('ObjectId'))

        old_names = ca_la_df.columns
        new_names = [remove_numbers(colstring) for colstring in old_names]
        rename_dict = dict(zip(old_names, new_names))

        clean_ca_la_df = (ca_la_df
                        .rename(rename_dict))
        
        ns_line_df = pl.DataFrame(
            {'ladcd':'E06000024',
             'ladnm': 'North Somerset',
             'cauthcd': 'E47000009',
             'cauthnm': 'West of England'}
             )
        
        if inc_ns: # if North Somerset to be included add a line to the df
            return clean_ca_la_df.vstack(ns_line_df)
        else:
            return clean_ca_la_df
    
def get_rename_dict(df: pl.DataFrame, remove_numbers, rm_numbers = False) -> dict:
    old = df.columns
    if rm_numbers == False:
        new = [colstring.lower() for colstring in df.columns]
    else:
        new = [remove_numbers(colstring).lower() for colstring in df.columns]
    return dict(zip(old, new))
        
def get_zipped_csv_file(url = "https://www.arcgis.com/sharing/rest/content/items/3770c5e8b0c24f1dbe6d2fc6b46a0b18/data",
                      file_folder_name = "postcode_lookup"):
    """
    Delete any existing files in the folder
    Download and unzip a CSV lookup file for the UK
    Return the path of the downloaded file
    """
    destination_directory = f'data//{file_folder_name}'
    if not os.path.exists(destination_directory):
        os.makedirs(destination_directory)

    files = [f for f in os.listdir(destination_directory) if os.path.isfile(os.path.join(destination_directory, f))]

    if files:
        for file in files:
            file_path = os.path.join(destination_directory, file)
            os.remove(file_path)
    else:
        print("No files found in the directory.")

    # Download the file
    response = requests.get(url)
    if response.status_code != 200:
            raise Exception(f'API call failed {response.status_code}')
    with open("file_folder_name.zip", "wb") as f:
        f.write(response.content)

    # Unzip the file
    with zipfile.ZipFile("file_folder_name.zip", "r") as zip_ref:
        zip_ref.extractall(destination_directory)

    files_list =  [os.path.join(destination_directory, file) for file in os.listdir(destination_directory)]
    if len(files_list) == 1:
        file_path = files_list[0]
    else:
         print("More than one file present")
    return file_path


def get_ca_la_codes(ca_la_df: pl.DataFrame) -> list:
    """
    Return a list of the LA codes which comprise each Combined Authority
    """
    return (ca_la_df
            .select(pl.col('ladcd'))
            .to_series()
            .to_list()
            )
    
def get_postcode_df(postcode_file: str, ca_la_codes: list) -> pl.DataFrame:
    """
    Read the postcode file and filter the df 
    to return only those postcodes within Combined authorities
    """

    old = ['pcds', 'lsoa21cd', 'msoa21cd', 'ladcd', 'ladnm']
    new = ['postcode', 'lsoacd', 'msoacd', 'ladcd', 'ladnm']
    rename_dict = dict(zip(old, new))

    postcodes_q = (
        pl.scan_csv(postcode_file)
        .select(pl.col(old))
        .filter(pl.col('ladcd').is_in(ca_la_codes))
        .rename(rename_dict)
        )
    pcdf = postcodes_q.collect()
    return pcdf

def get_geojson(url = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LLSOA_Dec_2021_PWC_for_England_and_Wales_2022/FeatureServer/0/query",
                      destination_directory = "data\\geojson"):
    """
    Download geoJSON from ESRI ONS OG
    Return the path of the downloaded file
    THIS ONLY DOWNLOADS 2000 RECORDS DO NOT USE - DOWNLOAD MANUALLY
    """
    
    files = [f for f in os.listdir(destination_directory) if os.path.isfile(os.path.join(destination_directory, f))]

    if files:
        for file in files:
            file_path = os.path.join(destination_directory, file)
            os.remove(file_path)
    else:
        print("No files found in the directory.")

    params = {
         'outFields': '*',
         'where': '1=1',
         'f': 'geojson'

    }
    # Download the file
    response = requests.get(url, params)
    if response.status_code != 200:
            raise Exception(f'API call failed {response.status_code}')
    with open(os.path.join(destination_directory, "esri.geojson"), "wb") as f:
        f.write(response.content)

    files_list =  [os.path.join(destination_directory, file) for file in os.listdir(destination_directory)]
    if len(files_list) == 1:
        path = files_list[0]
    else:
         print("More than one file present")
    return path

def get_ca_lsoa_codes(postcodes_df: pl.DataFrame) -> list:
    """
    Get all the LSOA codes within combined authorities
    """
    return (
        postcodes_df
        .select(pl.col('lsoacd'))
        .unique()
        .to_series()
        .to_list()
    )

def filter_geojson(input_file: str, output_file: str, property_name: str, ca_lsoa_codes: list) -> str:
    
    """
    Filter the LSOA population weighted centroid geoJSON file for 
    just those LSOA's within CA's
    and save the resulting file as geoJSON
    return the path of the output file - this can be directly loaded into datasette
    
    """
    # Load GeoJSON file
    with open(input_file, 'r') as f:
        geojson_data = json.load(f)

    # Filter features based on the specified property and values
    filtered_features = [
        feature for feature in geojson_data['features']
        if feature['properties'].get(property_name) in ca_lsoa_codes
    ]

    # Create new GeoJSON structure with filtered features
    filtered_geojson = {
        'type': 'FeatureCollection',
        'features': filtered_features
    }

    # Save filtered GeoJSON to output file
    with open(output_file, 'w') as f:
        json.dump(filtered_geojson, f)

    return output_file

def reproject(input_bng_file: str, output_wgs84_file: str, lsoa_code: str = 'LSOA21CD') -> str:
    """
    The filtered geojson file for pop weighted lsoa centroids in CA's
    is projected to BNG. This function reprojects it to WGS 84 to enable visualisation in datasette
    """
    # Create a transformer
    transformer = Transformer.from_crs('epsg:27700', 'epsg:4326', always_xy=True)

    # Load your GeoJSON file
    with open(input_bng_file, 'r') as f:
        data = json.load(f)

    # Reproject each feature
    for feature in data['features']:
        if feature['geometry']['type'] == 'Point':
            x, y = feature['geometry']['coordinates']
            lon, lat = transformer.transform(x, y)
            feature['geometry']['coordinates'] = [lon, lat]

        if lsoa_code in feature['properties']:
            feature['properties']['lsoacd'] = feature['properties'].pop(lsoa_code)

    # Save the reprojected GeoJSON
    with open(output_wgs84_file, 'w') as f:
        json.dump(data, f)
    
    return(output_wgs84_file)

def ingest_certs(la_list: list, cols_schema: dict, root_dir: str) -> pl.DataFrame:
    """
    Loop through all folders in a root directory
    if the folder name matches an item in a list of folder names
    us an optimised polars query to ingest a subset of columns and do 
    some transformations to create a single large DF of EPC data
    """
    all_dataframes = []
    cols_select_list = list(cols_schema.keys())
    for item in la_list:
        for folder_name in os.listdir(root_dir):
            # Check if the folder name matches an item in la_list
            if item in folder_name:
                file_path = os.path.join(root_dir, folder_name, "certificates.csv")
                # Check if certificates.csv actually exists inside the folder
                if os.path.exists(file_path):
                    # Optimised query which implements predicate pushdown for each file
                    # Polars optimises the query to make it fast and efficient
                    q = (
                    pl.scan_csv(file_path,
                    dtypes = cols_schema,
                    # infer_schema_length=0
                    ) #all as strings
                    .select(pl.col(cols_select_list))
                    .with_columns(pl.col('LODGEMENT_DATETIME').str.to_datetime(format='%Y-%m-%d %H:%M:%S', strict=False))
                    .sort(pl.col(['UPRN', 'LODGEMENT_DATETIME']))
                    .group_by('UPRN').last()
                    )
                    # The query is collected for each file
                    df = q.collect()
                    # the collected dataframe is appended to the list
                    all_dataframes.append(df)
    # Concatenate list of dataframes into one consolidated DF                
    certs_df = pl.concat(all_dataframes)                
    return certs_df



def clean_lsoa_geojson(file_path: str = 'data/geojson/for_rename.geojson',
                       lsoacd: str = 'LSOA21CD'):
    """
    First download the population weighted centroids from ONS open geography portal
    LLSOA_Dec_2021_PWC_for_England_and_Wales_2022_-7534040603619445107
    This function removes redundant fields and renames the LSOA codes to lower case no numbers
    And outputs geojson ready for import to the duckdb. Returns the file path of the new file.
    """
    try:
        with open(file_path, 'r') as file:
            geojson_data = json.load(file)

        # Check if the file is a valid GeoJSON format
        if 'features' not in geojson_data:
            raise ValueError("Invalid GeoJSON format. 'features' key not found.")

        # Rename the keys from 'LSOA21CD' to 'lsoacd'
        for feature in geojson_data['features']:
            if 'properties' in feature:
                feature['properties'].pop('GlobalID', None)  # Remove 'GlobalID' if exists
                feature['properties'].pop('FID', None)       # Remove 'FID' if exists
                if lsoacd in feature['properties']:
                    feature['properties']['lsoacd'] = feature['properties'].pop(lsoacd)

        success = True
    except Exception as e:
        error_message = str(e)
        success = False
    # construct new file path for saved renamed json  
    if success:  
        new_stem = f'{pathlib.PurePath(file_path).stem}_{datetime.now().date().isoformat()}'
        new_path = pathlib.PurePath(file_path).with_stem(new_stem)
        out_file = open(new_path, "w")
        json.dump(geojson_data, out_file)
        out_file.close()
    return new_path if success else False

def load_data(command_list: list, db_path: str = 'data/ca_epc.duckdb', overwrite = True):
    if os.path.isfile(db_path):
        if overwrite:
            os.remove(db_path)
        else:
            os.rename(db_path, 'data/old_db.duckdb')
    
    con = duckdb.connect(db_path)
    success = True
    for command in command_list:
        try:
            con.execute(command)
        except:
            success = False
            print(f'{command} failed')
    con.close()
    return db_path if success else success

def get_ca_la_dft_lookup(dft_csv_path: str, la_list: list) -> pl.DataFrame:
    """
    Read the DFT annual traffic data, get the most recent year's data and return just the ONS la codes (ladcd)
    And the corresponding DFT ID which will be used in the R script to retrieve detailed link data. Year retained for context.
    Filter for LA's in CA's (la_list). Not all LA's in the CA's are within this set.
    """        
    ca_la_dft_lookup_df = (pl.read_csv(dft_csv_path)
    .filter(pl.col('year') == pl.col('year').max())
    .select([pl.col('Local_authority_id').alias('dft_la_id'),
                pl.col('ONS_code').alias('ladcd'),
                pl.col('year')])
    .filter(pl.col('ladcd').is_in(la_list))
                )
                
    return ca_la_dft_lookup_df

def populate_sqlite(dfs_dict: dict, db_path: str = 'data/sqlite/ca_epc.db', overwrite: bool = True):

    if os.path.isfile(db_path):
        if overwrite:
            os.remove(db_path)
        else:
            os.rename(db_path, f'{db_path}_old')

    uri = Path("sqlite:///" + db_path).as_posix()
    
    for table_name, pldf in dfs_dict.items():
        (pldf
         .write_database(
             table_name = table_name, 
             if_exists = 'replace',
             connection = uri,
             engine = 'adbc'
            )
            )
        

