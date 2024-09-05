
from datetime import datetime
import requests
import polars as pl
import os
import glob
import duckdb
import json
import pathlib
import zipfile
from pyproj import Transformer
from pathlib import Path
import yaml

"""
Functions to get geographies and EPC data for combined authorities
These are used in the notebook cesap_epc_load_duckdb.ipynb
To populate a duckdb database which holds base data for analyses
supporting the CESAP indicators. Plotting and further analysis are done in
R scripts in this project folder.
"""

def delete_all_csv_files(folder_path):
    # Get the list of all CSV files in the folder
    csv_files = glob.glob(os.path.join(folder_path, '*.csv'))
    
    # Loop through the list and remove each file
    for file in csv_files:
        os.remove(file)
        print(f"Deleted: {file}")

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

def wrangle_epc(certs_df: pl.DataFrame) -> pl.DataFrame:
  
    """
    Creates date artefacts and changes data types
    """
    wrangled_df = (        
    certs_df
    .rename(lambda col: col.lower())
    .with_columns([pl.col('lodgement_datetime')
                   .dt.date()
                   .alias('date')])
    .with_columns([pl.col('date').dt.year().alias('year'),
                   pl.col('date').dt.month().cast(pl.Int16).alias('month'),
                   pl.col('date').cast(pl.Utf8)])
    # .rename(certs_df_names)
    .filter(~pl.col('uprn').is_duplicated()) # some nulls and duplicates (~134) so remove
    .drop('lodgement_datetime')
    )
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
    counts = {}
    if not rm_numbers:
        new = [colstring.lower() for colstring in df.columns]
    else:
        new = [remove_numbers(colstring).lower() for colstring in df.columns]
    
    for i, item in enumerate(new):
        if new.count(item) > 1:
            counts[item] = counts.get(item, 0) + 1
            new[i] = f"{item}_{counts[item]}"

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

def get_chunk_range(base_url: str, params_base: dict, max_records: int = 2000) -> list:
    '''
    Get the range of offsets to query the ArcGIS API based on the record count
    '''
    param_rt = {'returnCountOnly': 'true'}
    # combine base and return count parameters
    params = {**params_base, **param_rt}
    record_count = requests.get(base_url, params = params).json().get('count')
    chunk_size = round(record_count / math.ceil(record_count / max_records))
    chunk_range = list(range(0, record_count, chunk_size))
    return chunk_range

def get_gis_data(offset: int, params_base: dict, base_url: str) -> pl.DataFrame:
    '''
    Get the data from the ArcGIS API based on the offset
    '''
    with requests.get(base_url,
                      params = {**params_base, **{'resultOffset': offset}},
                      stream = True) as r:
        r.raise_for_status()
        features = r.json().get('features')
        features_df = (
            pl.DataFrame(features)
            .unnest('attributes')
            .drop('GlobalID')
            .unnest('geometry')
            )
    return features_df

def make_lsoa_df(base_url: str, params_base: dict, max_records: int = 2000) -> pl.DataFrame:
    '''
    Make a polars DataFrame of the LSOA data from the ArcGIS API
    by calling the get_chunk_range and get_data functions
    concatenated and sorted by the FID
    '''
    chunk_range = get_chunk_range(base_url, params_base, max_records)
    df_list = []
    for offset in chunk_range:
        df_list.append(get_data(offset, params_base, base_url))
    lsoa_df = pl.concat(df_list).unique().sort('FID')
    return lsoa_df



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
    ca_lsoa_set = set(ca_lsoa_codes) 
        # Filter features based on the specified property and values
    filtered_features = [
        feature for feature in geojson_data['features']
        if feature['properties'].get(property_name) in ca_lsoa_set
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
    THIS IS NOW DEPRECATED REPLACED BY ingest_dom_certs_csv 
    Loop through all folders in a root directory
    if the folder name matches an item in a list of folder names
    us an optimised polars query to ingest a subset of columns and do 
    some transformations to create a single large DF of EPC data
    """
    all_lazyframes = []
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
                    )
                    .select(pl.col(cols_select_list))
                    .with_columns(pl.col('LODGEMENT_DATETIME').str.to_datetime(format='%Y-%m-%d %H:%M:%S', strict=False))
                    .sort(pl.col(['UPRN', 'LODGEMENT_DATETIME']))
                    .group_by('UPRN').last()
                    )
                    all_lazyframes.append(q)
    # Concatenate list of lazyframes into one consolidated DF, then collect all at once - FAST
    certs_df = pl.concat(pl.collect_all(all_lazyframes))   # faster than intermediate step             
    return certs_df

def ingest_dom_certs_csv(la_list: list, cols_schema: dict) -> pl.DataFrame:
    """
    Loop through all csv files in the epc_csv 
    folder and ingest them into a single DF. Use an optimised polars query to ingest a subset of columns and do some transformations to create a single large DF of EPC data
    """
    all_lazyframes = []
    # rename columns to replace hyphens with underscores
    cols_select_list = list(cols_schema.keys())
    new_cols_names = [col.replace('-', '_') for col in cols_select_list]
    renamed_cols_dict = dict(zip(cols_select_list, new_cols_names))


    for item in la_list:

        file_path = f'data/epc_csv/epc_{item}.csv'

        if os.path.exists(file_path):
            # Optimised query which implements predicate pushdown for each file
            # Polars optimises the query to make it fast and efficient
            q = (
            pl.scan_csv(file_path,
            dtypes = cols_schema,
            encoding='utf8-lossy', # or trips up with odd characters
            ignore_errors=True
            )
            # grouping the data by uprn and selecting the last lodgement date
            # to remove old duplicates
            .select(pl.col(cols_select_list))
            .with_columns(pl.col('lodgement-datetime').str.to_datetime(format='%Y-%m-%d %H:%M:%S', strict=False))
            .sort(pl.col(['uprn', 'lodgement-datetime']))
            .group_by('uprn').last()
            .rename(renamed_cols_dict)
            )
            all_lazyframes.append(q)
    # Concatenate list of lazyframes into one consolidated DF, then collect all at once - FAST
    certs_df = pl.concat(pl.collect_all(all_lazyframes))   # faster than intermediate step             
    return certs_df

def get_epc_csv(la: str):
    '''
    Uses the opendatacommunities API to get EPC data for a given local authority and writes it to a CSV file
    the epc auth_token is in the config.yml file in parent directory
    This function uses pagination cribbed from https://epc.opendatacommunities.org/docs/api/domestic#domestic-pagination
    '''
    # Load the EPC auth token from the config file
    epc_key = yaml.safe_load(open('../config.yml'))['epc']['auth_token']
    # Page size (max 5000)
    query_size = 5000
    # Output file name
    output_file = f'data/epc_csv/epc_{la}.csv'

    # Base url and example query parameters
    base_url = 'https://epc.opendatacommunities.org/api/v1/domestic/search'
    query_params = {'size': query_size, 'local-authority': la}

    # Set up authentication
    headers = {
        'Accept': 'text/csv',
        'Authorization': f'Basic {epc_key}'
    }

    # Keep track of whether we have made at least one request for CSV headers and search-after
    first_request = True
    # Keep track of search-after from previous request
    search_after = None
    # Loop over entries in query blocks of up to 5000 to write all the data into a file

    with open(output_file, 'w') as file:
        # Perform at least one request; if there's no search_after, there are no further results
        while search_after is not None or first_request:
            # Only set search-after if this isn't the first request
            if not first_request:
                query_params["search-after"] = search_after

            # Make the request
            response = requests.get(base_url, headers=headers, params=query_params)
            response.raise_for_status()
            body = response.text
            search_after = response.headers.get('X-Next-Search-After')

            # For CSV data, only keep the header row from the first response
            if not first_request and body:
                body = body.split("\n", 1)[1]

            # Write received data
            file.write(body)
            print(f"Written {len(body)} bytes to {output_file}")

            first_request = False

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
        print(error_message)
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
        except duckdb.DatabaseError as e:
            success = False
            print(f'{command} failed {e}')
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
    .select([pl.col('local_authority_id').alias('dft_la_id'),
                pl.col('local_authority_code').alias('ladcd'),
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
def clean_tenure(expr: pl.Expr, new_colname: str) -> pl.Expr:
    ''''
    Function for cleaning the tenure column
    The column is piped into this function
    In a .with_columns context

    '''
    return (pl.when(expr.str.contains('wner'))
    .then(pl.lit('Owner occupied'))
    .when(expr.str.contains('rivate'))
    .then(pl.lit('Private rented'))
    .when(expr.str.contains('ocial'))
    .then(pl.lit('Social rented'))
    .otherwise(pl.lit('Unknown'))
    .alias(new_colname))

def make_n_construction_age(df: pl.DataFrame, new_colname: str) -> pl.DataFrame:
    '''
    Take a dataframe and create a new column with the nominal construction date
    Creating temporary columns and then dropping them
    Return the dataframe with the new column
    Done in polars for speed
    '''

    return  (df
    .with_columns(pl.col('construction_age_band')
                .str.extract_all(r'[0-9]')
                .list.join('')
                .alias('age_char'))
    .with_columns(pl.col('age_char').str.len_chars().gt(4).alias('_8_chars'))
    
    .with_columns(pl.when((pl.col('_8_chars').ne(True)) & (pl.col('age_char') == "1900"))
                .then(pl.lit(1899))
                .otherwise(pl.col('age_char').str.slice(0, 4))
                .alias('lower')
                .cast(pl.Int16, strict=False)
                )
    .with_columns(pl.when(pl.col('_8_chars'))
                .then(pl.col('age_char').str.slice(4, 8))
                .otherwise(None)
                .alias('upper')
                .cast(pl.Int16, strict=False)
                )
    .with_columns(pl.when(pl.col('upper').is_not_null())
                .then(((pl.col('upper') - pl.col('lower')) // 2) + pl.col('lower'))
                .when(pl.col('lower').is_nan())
                .then(None)
                .otherwise(pl.col('lower'))
                .alias(new_colname))

    .drop('age_int', '_8_chars', 'age_char', 'lower', 'upper')
    )
        
def get_imd_df(path: str = 'data/IMD2019.csv') -> pl.DataFrame:
    '''
    Read the IMD CSV file and return a polars dataframe
    with the IMD rank and decile for all LSOA's in UK
    Pivot the data to get the rank and decile as columns
    '''
    
    url = '''
    https://opendatacommunities.org/resource?uri=http%3A%2F%2Fopendatacommunities.org%2Fdata%2Fsocietal-wellbeing%2Fimd2019%2Findices
    '''
    try:
        file_path = Path(path)

    # Check if the file exists
        if file_path.exists():
            imd_df_lazy = pl.scan_csv(file_path,
                        infer_schema_length=0)
            metrics = pl.Series('metrics', ['Rank', 'Decile ']) # with a space :|

        return ((
            imd_df_lazy
            .rename(lambda col: col.replace(' ', '_').lower())
            .filter(pl.col('indices_of_deprivation').is_in(['a. Index of Multiple Deprivation (IMD)']))
            .filter(pl.col('measurement').is_in(metrics))
            .select(pl.col(['featurecode', 'measurement', 'value']))
            .with_columns(pl.col('value').cast(pl.Int64).alias('value'))
            
        ).collect()
        .pivot(values='value', index='featurecode', columns='measurement')
        .rename({'featurecode': 'lsoacd',
                'Rank': 'imd_rank',
                'Decile ': 'imd_decile'})
        )

    # Print result or URL if the file does not exist
    except FileNotFoundError as e:
        print(e)
        print("You can download the file from the following URL:")
        url = "https://opendatacommunities.org/resource?uri=http%3A%2F%2Fopendatacommunities.org%2Fdata%2Fsocietal-wellbeing%2Fimd2019%2Findices"
        print(url)
        return None
