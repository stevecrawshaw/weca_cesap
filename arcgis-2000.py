#%%
import requests
import math
import polars as pl
import duckdb
import geopandas as gpd
import pandas as pd
import get_ca_data as get_ca
from urllib.parse import urlencode, urlunparse

#%%
base_url_centroids = f'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LLSOA_Dec_2021_PWC_for_England_and_Wales_2022/FeatureServer/0/query?'
base_url_lsoa_polys = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW_BFC_V10/FeatureServer/0/query"
base_url_lsoa_lookups = 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LSOA21_WD24_LAD24_EW_LU/FeatureServer/0/query?'
params_base = {
    'outFields': '*',
    'outSR': 4326,
    'f': 'json'
}

# %%

def get_chunk_list(base_url: str, params_base: dict, max_records: int = 2000) -> list:
    '''
    Get the range of offsets to query the ArcGIS API based on the record count
    '''
    params_rt = {'returnCountOnly': 'true', 'where': '1=1'}
    params_other = {'where': '1=1'}
    # combine base and return count parameters
    params = {**params_base, **params_rt, **params_other}
    record_count = requests.get(base_url, params = params).json().get('count')
    chunk_size = round(record_count / math.ceil(record_count / max_records))
    chunk_range = list(range(0, record_count, chunk_size))
    return chunk_range

#%%
centroind_chunk_list = get_chunk_list(base_url_centroids, params_base, max_records = 2000)
#%%
def get_gis_data(offset: int,
                 params_base: dict,
                 params_other: dict,
                 base_url: str) -> pl.DataFrame:
    '''
    Get the data from the ArcGIS API based on the offset
    '''
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

#%%

def get_flat_data(offset: int,
                  params_base: dict,
                  params_other: dict,
                  base_url: str) -> pl.DataFrame:
    '''
    Get the data from the ArcGIS API based on the offset
    This is for data without a geometry field
    '''
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

#%%
ca_la_df = get_ca.get_ca_la_df(year = 2023)

#%%
ladcds_in_cauths = get_ca.get_ca_la_codes(ca_la_df)

#%%

lookups_chunk_list = get_chunk_list(base_url_lsoa_lookups,
                                      params_base,
                                      max_records = 1000)


#%%
# list of pl.dataframes of the lookups data in cauths
lookups_pldf_list = [get_flat_data(chunk,
                                   params_base,
                                   params_other = {'where':'1=1'},
                                   base_url = base_url_lsoa_lookups)
                     for chunk in lookups_chunk_list]

#%%

lookups_pldf = pl.concat(lookups_pldf_list, how='vertical_relaxed')

#%%

lsoas_in_cauths_iter = (lookups_pldf
                        .filter(pl.col('LAD24CD').is_in(ladcds_in_cauths))
                        .select(pl.col('LSOA21CD'))
                        .to_series())
# get 100 lsoas at a time
chunk_size = 100
lsoas_in_cauths_chunks = [lsoas_in_cauths_iter[i:i + chunk_size]
                          for i in range(0,
                                         len(lsoas_in_cauths_iter),
                                         chunk_size)]

#%%

def make_poly_url(base_url: str,
                  params_base: dict,
                  lsoa_code_list: list) -> list[str]:
    '''
    Make a url to retrieve the lsoa polygons given a list of lsoa codes

    '''
    lsoa_in_clause = str(tuple(list(lsoa_code_list)))
    params = {**params_base, **{'where': f"LSOA21CD IN {lsoa_in_clause}"}}
    # use urlencode to avoid making an actual call
    query_string = urlencode(params)
    url = urlunparse(('','', base_url, '', query_string, ''))
    return url
#%%
# list of urls to get the lsoa polygons in the combined authorities
poly_url_list = [make_poly_url(base_url_lsoa_polys, params_base, lsoas)
                  for lsoas in
                  lsoas_in_cauths_chunks]

#%%
# a list of geopandas dataframes to hold all lsoa polygons in the combined authorities
lsoa_gdf_list = [gpd.read_file(polys_url) for polys_url in poly_url_list]

#%%

lsoa_gdf = gpd.GeoDataFrame(pd.concat(lsoa_gdf_list,
                                      ignore_index=True))

#%%

lsoa_gdf.to_parquet('data/lsoa_poly/lsoa_poly_cauth_2024.parquet')

#%%

def make_lsoa_pwc_df(base_url: str,
                     params_base: dict,
                     params_other: dict,
                     max_records: int = 2000) -> pl.DataFrame:
    '''
    Make a polars DataFrame of the LSOA data from the ArcGIS API
    by calling the get_chunk_range and get_data functions
    concatenated and sorted by the FID
    '''
    chunk_range = get_chunk_list(base_url, params_base, max_records)
    df_list = []
    for offset in chunk_range:
        df_list.append(get_gis_data(offset,
                                    params_base,
                                    params_other,
                                    base_url))
    lsoa_df = pl.concat(df_list).unique()
    return lsoa_df

#%%
lsoa_pwc_df = make_lsoa_pwc_df(base_url = base_url_centroids,
                           params_base = params_base, 
                           params_other = {'where': '1=1'},
                           max_records = 2000)
#%%

lsoa_pwc_cauth_df = (lsoa_pwc_df
                     .filter(pl.col('LSOA21CD').is_in(lsoas_in_cauths_iter))
                     )
#%%
# Indices of Multiple Deprivation Data by LSOA
lsoa_imd_df = pl.read_csv('https://github.com/humaniverse/IMD/raw/master/data-raw/imd_england_lsoa.csv')

#%%

lsoa_imd_df.glimpse()

#%%
lsoa_pwc_df.glimpse()
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

qry_parq = """
CREATE OR REPLACE TABLE 
lsoa_poly_cauth_df AS 
SELECT * EXCLUDE geometry,
ST_GeomFROMWKB(geometry) AS geom
FROM 'data/lsoa_poly/lsoa_poly_cauth_2024.parquet'
"""

con.sql(qry_parq)

#%%

con.sql("FROM lsoa_poly_cauth_df")

#%%
con.close