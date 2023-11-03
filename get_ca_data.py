
from datetime import datetime
import requests
import polars as pl
import os
import json
import zipfile
from pyproj import Transformer

def remove_numbers(input_string):
    # rename columns
    # Create a translation table that maps each digit to None
    lowercase_string = input_string.lower()
    translation_table = str.maketrans("", "", "0123456789")
    # Use the translation table to remove all numbers from the input string
    result_string = lowercase_string.translate(translation_table)
    return result_string

def get_ca_la_df(year: int, baseurl: str = 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/') -> pl.DataFrame:

    """
    Download the lookup table for Combined and Local Authorities
    From ArcGIS ONS Geography portal
    Different versions for year - boundary changes
    Rename columns by removing numerals
    """
    c_year = (datetime.now().year) + 1
    start_year = c_year - 3
    years = list(range(start_year, c_year))
    
    def remove_numbers(input_string):
        # rename columns
        # Create a translation table that maps each digit to None
        lowercase_string = input_string.lower()
        translation_table = str.maketrans("", "", "0123456789")
        # Use the translation table to remove all numbers from the input string
        result_string = lowercase_string.translate(translation_table)
        return result_string

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
    new = ['pcds', 'lsoacd', 'msoacd', 'ladcd', 'ladnm']
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

def reproject(input_bng_file: str, output_wgs84_file: str) -> str:
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

    # Save the reprojected GeoJSON
    with open(output_wgs84_file, 'w') as f:
        json.dump(data, f)
    
    return(output_wgs84_file)
