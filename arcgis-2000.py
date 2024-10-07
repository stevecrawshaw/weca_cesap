# %% [markdown]
# Load libraries
#%%
import requests
import math
import polars as pl
import duckdb
import geopandas as gpd
import pandas as pd
import get_ca_data as get_ca
from urllib.parse import urlencode, urlunparse
# %% [markdown]
# Define the base urls for the ArcGIS API and some parameters
#%%
base_url_lsoa_2021_centroids = f'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LLSOA_Dec_2021_PWC_for_England_and_Wales_2022/FeatureServer/0/query?'
base_url_2021_lsoa_polys = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW_BFC_V10/FeatureServer/0/query"
base_url_2011_lsoa_polys = 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LSOA_Dec_2011_Boundaries_Generalised_Clipped_BGC_EW_V3/FeatureServer/0/query'
base_url_lsoa_2021_lookups = 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LSOA21_WD24_LAD24_EW_LU/FeatureServer/0/query?'
base_url_lsoa_2011_lookups = 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LSOA01_LSOA11_LAD11_EW_LU_ddfe1cd1c2784c9b991cded95bc915a9/FeatureServer/0/query'
imd_data_path = 'https://github.com/humaniverse/IMD/raw/master/data-raw/imd_england_lsoa.csv'
params_base = {
    'outFields': '*',
    'outSR': 4326,
    'f': 'json'
}
# %% [markdown]
# Define functions to get data from the ArcGIS API
# %%
def get_chunk_list(base_url: str, params_base: dict, max_records: int = 2000) -> list:
    '''
    Get a list of offsets to query the ArcGIS API based on the record count limit
    Sometimes the limit is 1000, sometimes 2000
    Required due to pagination of API
    '''
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

#%%
def get_gis_data(offset: int,
                 params_base: dict,
                 params_other: dict,
                 base_url: str) -> pl.DataFrame:
    '''
    Get a dataset containing geometry from the ArcGIS API based on the offset
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
def make_poly_url(base_url: str,
                  params_base: dict,
                  lsoa_code_list: list,
                  lsoa_code_name: str) -> list[str]:
    '''
    Make a url to retrieve lsoa polygons given a list of lsoa codes

    '''
    lsoa_in_clause = str(tuple(list(lsoa_code_list)))
    where_params = {'where': f"{lsoa_code_name} IN {lsoa_in_clause}"}
    params = {**params_base, **where_params}
    # use urlencode to avoid making an actual call to get the urls
    query_string = urlencode(params)
    url = urlunparse(('','', base_url, '', query_string, ''))
    return url
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
ca_la_df = get_ca.get_ca_la_df(year = 2023)

#%%
ladcds_in_cauths = get_ca.get_ca_la_codes(ca_la_df)
# %% [markdown]
# Retrieve the 2021 LSOA polygon data for the combined authorities
#%%
lookups_2021_chunk_list = get_chunk_list(base_url_lsoa_2021_lookups,
                                      params_base,
                                      max_records = 1000)
#%%
# list of pl.dataframes of the lookups data in cauths
lookups_2021_pldf_list = [get_flat_data(chunk,
                                   params_base,
                                   params_other = {'where':'1=1'},
                                   base_url = base_url_lsoa_2021_lookups)
                     for chunk
                     in lookups_2021_chunk_list]

#%%
lookups_2021_pldf = pl.concat(lookups_2021_pldf_list, how='vertical_relaxed')
#%%
#%%
lsoas_in_cauths_iter = (lookups_2021_pldf
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
# list of urls to get the lsoa polygons in the combined authorities
lsoa_2021_poly_url_list = [make_poly_url(base_url_2021_lsoa_polys,
                               params_base,
                               lsoas,
                               lsoa_code_name='LSOA21CD')
                  for lsoas in
                  lsoas_in_cauths_chunks]

#%%
# a list of geopandas dataframes to hold all lsoa polygons in the combined authorities
lsoa_2021_gdf_list = [gpd.read_file(polys_url) for polys_url in lsoa_2021_poly_url_list]

#%%
lsoa_2021_gdf = gpd.GeoDataFrame(pd.concat(lsoa_2021_gdf_list,
                                      ignore_index=True))
#%%

lsoa_2021_gdf.to_parquet('data/lsoa_poly/lsoa_poly_cauth_2024.parquet')

# %% [markdown]
# Retrieve the 2021 LSOA points (population weighted centroids)
#%%
#%%
lsoa_2021_pwc_df = make_lsoa_pwc_df(base_url = base_url_lsoa_2021_centroids,
                           params_base = params_base, 
                           params_other = {'where': '1=1'},
                           max_records = 2000)
#%%
lsoa_2021_pwc_df.glimpse()
#%%

lsoa_2021_pwc_cauth_df = (lsoa_2021_pwc_df
                     .filter(pl.col('LSOA21CD').is_in(lsoas_in_cauths_iter))
                     .rename(lambda x: x.lower())
                     )
# %% [markdown]
# Retrieve the 2011 LSOA polygon data for the LEP - for joining with IMD
# The latest IMD data availoable is for 2019
# %%
lookups_2011_chunk_list = get_chunk_list(base_url_lsoa_2011_lookups,
                                         params_base, 
                                            max_records = 1000)

#%%
lookups_2011_pldf_list = [get_flat_data(chunk,
                                      params_base,
                                      params_other = {'where':'1=1'},
                                      base_url = base_url_lsoa_2011_lookups)
                        for chunk
                        in lookups_2011_chunk_list]
#%%
lookups_2011_pldf = (pl
                     .concat(lookups_2011_pldf_list,
                             how='vertical_relaxed')
                             .rename(lambda x: x.lower())
                             )



#%%
lookups_2011_pldf.glimpse()

# %%
lsoacd_2011_in_cauths_iter = (lookups_2011_pldf
                              .filter(pl.col('lad11cd')
                                      .is_in(ladcds_in_cauths))
                                      .select(pl.col('lsoa11cd'))
                                      .unique()
                                      .to_series()
                                      )
# %%
lsoa_2011_chunk_list = get_chunk_list(base_url_2011_lsoa_polys, params_base, max_records = 2000)

# get 100 lsoas at a time
lsoa_2011_in_cauths_chunks = [lsoacd_2011_in_cauths_iter[i:i + chunk_size]
                          for i in range(0,
                                         len(lsoacd_2011_in_cauths_iter),
                                         chunk_size)]

# %%
lsoa_2011_poly_url_list = [make_poly_url(base_url_2011_lsoa_polys,
                               params_base,
                               lsoas,
                               lsoa_code_name='LSOA11CD')
                  for lsoas in
                  lsoa_2011_in_cauths_chunks]

# %%
lsoa_2011_gdf_list = [gpd.read_file(polys_url) for polys_url in lsoa_2011_poly_url_list]

# %%
lsoa_2011_gdf = gpd.GeoDataFrame(pd.concat(lsoa_2011_gdf_list,  ignore_index=True))

# %%
lsoa_2011_gdf.to_parquet('data/lsoa_poly/lsoa_poly_cauth_2011.parquet')
#%%
# Indices of Multiple Deprivation Data by LSOA (2011)
lsoa_imd_df = (pl.read_csv(imd_data_path)
.rename(lambda x: x.lower()))

#%%

lsoa_imd_df.glimpse()

# %% [markdown]
# Write the data to a duckdb database
#%%
con = duckdb.connect(database='data/test_arcgis_2000.duckdb', read_only=False)

#%%
con.install_extension('spatial')
con.load_extension('spatial')
con.install_extension('httpfs')
con.load_extension('httpfs')

#%%
con.sql("CREATE OR REPLACE TABLE lsoa_2021_pwc_tbl AS SELECT * FROM lsoa_2021_pwc_df")
#%%
con.sql("ALTER TABLE lsoa_2021_pwc_tbl ADD COLUMN geom GEOMETRY")
#%%
con.sql("UPDATE lsoa_2021_pwc_tbl SET geom = ST_Point(x, y)")
#%%
con.sql("FROM lsoa_2021_pwc_tbl")

#%%
# crs conversion
con.sql("UPDATE lsoa_2021_pwc_tbl SET geom = ST_Transform(geom, 'EPSG:4326', 'EPSG:27700')")

#%%
con.sql("FROM lsoa_2021_pwc_tbl")

#%%

qry_parq_2021 = """
CREATE OR REPLACE TABLE 
lsoa_poly_2021_cauth_tbl AS 
SELECT * EXCLUDE geometry,
ST_GeomFROMWKB(geometry) AS geom
FROM 'data/lsoa_poly/lsoa_poly_cauth_2024.parquet'
"""

con.sql(qry_parq_2021)

#%%
qry_parq_2011 = """
CREATE OR REPLACE TABLE 
lsoa_poly_2011_cauth_tbl AS 
SELECT * EXCLUDE geometry,
ST_GeomFROMWKB(geometry) AS geom
FROM 'data/lsoa_poly/lsoa_poly_cauth_2011.parquet'
"""

con.sql(qry_parq_2011)

#%%

con.sql("CREATE OR REPLACE TABLE lsoa_imd_tbl AS SELECT * FROM lsoa_imd_df")
#%%
con.close()
# %%
