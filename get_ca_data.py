
from datetime import datetime
import requests
import polars as pl
from pathlib import Path
from urllib.parse import urlencode, urlunparse
import yaml
import math
import logging
from typing import Dict, Optional
from dateutil import parser
from dateutil.relativedelta import relativedelta

"""
Functions to get geographies and EPC data for combined authorities
These are used in the notebook cesap_epc_load_duckdb.ipynb
To populate a duckdb database which holds base data for analyses
supporting the CESAP indicators. Plotting and further analysis are done in
R scripts in this project folder.
"""

def load_config(config_path: str) -> Dict:
    """
    Function to retrieve credentials from a config file.
    """
    try:
        with open(config_path, 'r') as config_file:
            return yaml.safe_load(config_file)
    except FileNotFoundError:
        logging.error(f"Config file not found: {config_path}")
        raise

def delete_all_csv_files(folder_path):
    # Convert the folder path to a Path object
    folder = Path(folder_path)
    
    # Get the list of all CSV files in the folder
    csv_files = folder.glob('*.csv')
    
    # Loop through the list and remove each file
    for file in csv_files:
        file.unlink()
        print(f"Deleted: {file}")


def delete_file(file_path):
    """
    Delete a file if it exists using pathlib.
    """
    path = Path(file_path)
    if path.exists():
        path.unlink()
        print(f"Deleted: {file_path}")
    else:
        print(f"File not found: {file_path}")

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
        
def get_chunk_list(base_url: str, params_base: dict, max_records: int = 2000) -> list:
    """
    Get a list of offsets to query the ArcGIS API based on the record count limit
    Sometimes the limit is 1000, sometimes 2000
    Required due to pagination of API
    """
    params_rt = {'returnCountOnly': 'true', 'where': '1=1'}
    # combine base and return count parameters
    params = {**params_base, **params_rt}
    record_count = (requests
                    .get(base_url, params = params)
                    .json()
                    .get('count'))
    chunk_size = round(record_count / math.ceil(record_count / max_records))
    chunk_list = list(range(0, record_count, chunk_size))
    return chunk_list

def get_gis_data(offset: int,
                 params_base: dict,
                 params_other: dict,
                 base_url: str) -> pl.DataFrame:
    """
    Get a dataset containing geometry from the ArcGIS API based on the offset
    """
    with requests.get(base_url,
                      params = {**params_base,
                                **{'resultOffset': offset},
                                **params_other},
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

def get_flat_data(offset: int,
                  params_base: dict,
                  params_other: dict,
                  base_url: str) -> pl.DataFrame:
    """
    Get the data from the ArcGIS API based on the offset
    This is for data without a geometry field
    """
    with requests.get(base_url,
                      params = {**params_base,
                                **{'resultOffset': offset},
                                **params_other},
                                stream = True) as r:
        r.raise_for_status()
        features = r.json().get('features')
        features_df = (
            pl.DataFrame(features)
            .unnest('attributes')
            .drop('GlobalID')
            )
    return features_df

def make_poly_url(base_url: str,
                  params_base: dict,
                  lsoa_code_list: list,
                  lsoa_code_name: str) -> list[str]:
    """
    Make a url to retrieve lsoa polygons given a list of lsoa codes
    """
    lsoa_in_clause = str(tuple(list(lsoa_code_list)))
    where_params = {'where': f"{lsoa_code_name} IN {lsoa_in_clause}"}
    params = {**params_base, **where_params}
    # use urlencode to avoid making an actual call to get the urls
    query_string = urlencode(params)
    url = urlunparse(('','', base_url, '', query_string, ''))
    return url

def make_lsoa_pwc_df(base_url: str,
                     params_base: dict,
                     params_other: dict,
                     max_records: int = 2000) -> pl.DataFrame:
    """
    Make a polars DataFrame of the LSOA data from the ArcGIS API
    by calling the get_chunk_range and get_data functions
    concatenated and sorted by the FID
    """
    chunk_range = get_chunk_list(base_url, params_base, max_records)
    df_list = []
    for offset in chunk_range:
        df_list.append(get_gis_data(offset,
                                    params_base,
                                    params_other,
                                    base_url))
    lsoa_df = pl.concat(df_list).unique()
    return lsoa_df
   
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

def ingest_dom_certs_csv(la_list: list, cols_schema: dict) -> pl.DataFrame:
    """
    Loop through all csv files in the epc_csv 
    folder and ingest them into a single DF. 
    Use an optimised polars query to ingest a subset of columns
    and do some transformations to create a single large DF of EPC data
    """
    all_lazyframes = []
    # rename columns to replace hyphens with underscores
    cols_select_list = list(cols_schema.keys())
    new_cols_names = [col.replace('-', '_') for col in cols_select_list]
    renamed_cols_dict = dict(zip(cols_select_list, new_cols_names))


    for item in la_list:

        file_path = Path('data/epc_csv') / f'epc_{item}.csv'

        if file_path.exists():
            # Optimised query which implements predicate pushdown for each file
            # Polars optimises the query to make it fast and efficient
            q = (
            pl.scan_csv(str(file_path),
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

def get_epc_from_date() -> Dict[str, int]:
    """
    Get the last date of the EPC data on the open data portal.

    Returns:
        dict: A dictionary containing the year and month of the last date.
              Format: {'year': int, 'month': int}

    Raises:
        requests.RequestException: If there's an error with the API request.
        ValueError: If there's an error parsing the response or the date.
    """
    base_url = 'https://opendata.westofengland-ca.gov.uk/api/explore/v2.1'
    endpoint = 'catalog/datasets'
    dataset = 'lep-epc-domestic-point'
    call_type = 'records'
    query_param = {
        'select': 'max(date) as max_date',
        'limit': 1,
        'offset': 0,
        'timezone': 'UTC',
        'include_links': 'false',
        'include_app_metas': 'false'
    }

    url = f'{base_url}/{endpoint}/{dataset}/{call_type}'

    try:
        with requests.Session() as session:
            response = session.get(url, params=query_param)
            response.raise_for_status()
            data = response.json()

        max_date = data.get('results', [{}])[0].get('max_date')
        if not max_date:
            raise ValueError("No max_date found in the API response")

        parsed_date = parser.parse(max_date) + relativedelta(months=1)
        return {'year': parsed_date.year, 'month': parsed_date.month}

    except requests.RequestException as e:
        raise requests.RequestException(f"Error fetching data from API: {e}")
    except (ValueError, KeyError, IndexError) as e:
        raise ValueError(f"Error parsing API response: {e}")

def get_epc_csv(la: str,
                from_date: Dict[str, int],
                to_date: Optional[Dict[str, int]] = None) -> None:
    """
    Uses the opendatacommunities API to get EPC data for a given local authority and 
    time period. It writes it to a CSV file.

    Args:
        la (str): Local authority code.
        from_date (Dict[str, int]): Start date with 'year' and 'month' keys.
        to_date (Optional[Dict[str, int]]): End date with 'year' and 'month' keys. 
                                            If None, uses current date.

    Raises:
        requests.RequestException: If there's an error with the API request.
        IOError: If there's an error writing to the file.
    """
    # Set up logging
    logging.basicConfig(level=logging.INFO)

    # Load the EPC auth token from the config file
    config = load_config('../config.yml')
    epc_key = config['epc']['auth_token']

    # Deal with data parameters
    from_year = from_date.get('year')
    from_month = from_date.get('month')
    if to_date == None:
        to_month = datetime.now().month - 1 or 12
        to_year = datetime.now().year if to_month != 12 else datetime.now().year - 1
    else:
        to_year = to_date.get('year')
        to_month = to_date.get('month')

    # Page size (max 5000)
    query_size = 5000
    # Output file name
    output_file = f'data/epc_csv/epc_{la}.csv'

    # Base url and example query parameters
    base_url = 'https://epc.opendatacommunities.org/api/v1/domestic/search'
    query_params = {'size': query_size,
                    'local-authority': la,
                    'from-month': from_month,
                    'from-year': from_year,
                    'to-month': to_month,
                    'to-year': to_year
                    }

    # Set up authentication
    headers = {
        'Accept': 'text/csv',
        'Authorization': f'Basic {epc_key}'
    }

    output_file = f'data/epc_csv/epc_{la}.csv'
    
    try:
        with open(output_file, 'w') as file:
            first_request = True
            search_after = None

            while search_after is not None or first_request:
                if not first_request:
                    query_params["search-after"] = search_after

                response = requests.get(base_url, headers=headers, params=query_params)
                response.raise_for_status()
                
                body = response.text
                search_after = response.headers.get('X-Next-Search-After')

                if not first_request and body:
                    body = body.split("\n", 1)[1]

                file.write(body)
                logging.info(f"Written {len(body)} bytes to {output_file}")

                first_request = False

    except requests.RequestException as e:
        logging.error(f"API request error: {e}")
        raise
    except IOError as e:
        logging.error(f"File writing error: {e}")
        raise

    logging.info(f"EPC data successfully written to {output_file}")

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

def clean_tenure(expr: pl.Expr, new_colname: str) -> pl.Expr:
    """
    Function for cleaning the tenure column
    The column is piped into this function
    In a .with_columns context

    """
    return (pl.when(expr.str.contains('wner'))
    .then(pl.lit('Owner occupied'))
    .when(expr.str.contains('rivate'))
    .then(pl.lit('Private rented'))
    .when(expr.str.contains('ocial'))
    .then(pl.lit('Social rented'))
    .otherwise(pl.lit('Unknown'))
    .alias(new_colname))

def make_n_construction_age(df: pl.DataFrame, new_colname: str) -> pl.DataFrame:
    """
    Take a dataframe and create a new column with the nominal construction date
    Creating temporary columns and then dropping them
    Return the dataframe with the new column
    Done in polars for speed
    """

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