#%%
import requests
import math
import polars as pl
import duckdb
import geojson
import glob
import os
import json
from pathlib import Path
import geojson

#%%
base_url = f'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LLSOA_Dec_2021_PWC_for_England_and_Wales_2022/FeatureServer/0/query?'
base_url = 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LSOA_Dec_2001_EW_BFC_2022/FeatureServer/0/query?'
params_base = {
    'where': '1=1',
    'outFields': '*',
    'outSR': 4326,
    'f': 'json'
}

# %%

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

#%%
chunk_range = get_chunk_range(base_url, params_base, max_records = 2000)
#%%
def get_gis_data(offset: int, params_base: dict, base_url: str) -> pl.DataFrame:
    '''
    Get the data from the ArcGIS API based on the offset
    '''
    with requests.get(base_url,
                      params = {**params_base,
                                **{'resultOffset': offset}},
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


#%%

def get_write_poly_data(offset: int, params_base: dict, base_url: str) -> bool:
    '''
    Get the data from the ArcGIS API based on the offset
    '''
    with requests.get(base_url,
                      params = {**params_base, **{'resultOffset': offset}},
                      stream = True) as r:
        r.raise_for_status()
        features = r.json()
        api_status = r.status_code == 200

        with open (f'data/lsoa_poly/arcgis_poly_{offset}.geojson', 'w') as f:
            json.dump(features, f, indent=4)

    return api_status
#%%
def write_all_poly_geojson(base_url: str, params_base: dict, max_records: int = 2000) -> pl.DataFrame:
    '''
    Make a polars DataFrame of the LSOA data from the ArcGIS API
    by calling the get_chunk_range and get_data functions
    concatenated and sorted by the FID
    '''
    chunk_range = get_chunk_range(base_url, params_base, max_records)
    responses = []
    for offset in chunk_range:
        responses.append(get_write_poly_data(offset, params_base, base_url))
    return all(responses)
#get_write_poly_data(0, params_base, base_url)

#%%
write_all_poly_geojson(base_url, params_base, max_records = 2000)
#%%
folder_path = r'data\lsoa_poly'

collection = []

for filename in os.listdir(folder_path):
    if filename.endswith('.geojson'):
        file_path = os.path.join(folder_path, filename)
        with open(file_path, 'r') as file:
            geojson_data = geojson.load(file)
            collection.append(geojson_data)

geo_collection = geojson.GeometryCollection(collection)

with open(os.path.join(folder_path,'test_collection.geojson'), 'w') as f:
    geojson.dump(geo_collection, f)
#%%
write_all_poly_geojson(base_url, params_base, max_records = 2000)

#%%

def make_lsoa_pwc_df(base_url: str, params_base: dict, max_records: int = 2000) -> pl.DataFrame:
    '''
    Make a polars DataFrame of the LSOA data from the ArcGIS API
    by calling the get_chunk_range and get_data functions
    concatenated and sorted by the FID
    '''
    chunk_range = get_chunk_range(base_url, params_base, max_records)
    df_list = []
    for offset in chunk_range:
        df_list.append(get_gis_data(offset, params_base, base_url))
    lsoa_df = pl.concat(df_list).unique()
    return lsoa_df

#%%
lsoa_pwc_df = make_lsoa_df(base_url = base_url,
                           params_base = params_base, 
                           max_records = 2000)

#%%

con = duckdb.connect(database='data/test_arcgis_2000.duckdb', read_only=False)

#%%
con.install_extension('spatial')
con.load_extension('spatial')
#%%
con.sql("CREATE TABLE lsoa_pwc_df AS SELECT * FROM lsoa_pwc_df")
#%%
con.sql("ALTER TABLE lsoa_pwc_df ADD COLUMN geom GEOMETRY")
#%%
con.sql("UPDATE lsoa_pwc_df SET geom = ST_Point(x, y)")
#%%
con.sql("FROM lsoa_pwc_df")

#%%
# crs conversion
con.sql("UPDATE lsoa_pwc_df SET geom = ST_Transform(geom, 'EPSG:4326', 'EPSG:27700')")

#%%
con.sql("FROM lsoa_pwc_df")

#%%

con.sql("UPDATE lsoa_pwc_df SET geom = ST_Transform(geom, 'EPSG:27700', 'EPSG:4326')")
#%%
con.close