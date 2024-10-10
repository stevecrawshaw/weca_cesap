# %%
# use .venv - make sure to restart jupyter kernel.
import polars as pl
import duckdb
import get_ca_data as get_ca # functions for retrieving CA \ common data
import geopandas as gpd
import pandas as pd
import requests

download_epc = True
download_lsoa = True

# %% [markdown]
# This notebook retrieves all the base data needed for comparison analysis with other Combined Authorities and loads it into a duckdb database.
# SQLite was tried, but it is slow, not directly compatible with polars and does not work well with datasette because of the size of data.

# %% [markdown]
## Define the base urls for the ArcGIS API and some parameters
# we get the 2011 LSOA data as these match to the IMD lsoa codes
#%%
base_url_lsoa_2021_centroids = f'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LLSOA_Dec_2021_PWC_for_England_and_Wales_2022/FeatureServer/0/query?'
base_url_postcode_centroids = 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/ONSPD_Online_Latest_Centroids/FeatureServer/0/query'
base_url_2021_lsoa_polys = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW_BFC_V10/FeatureServer/0/query"
base_url_2011_lsoa_polys = 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LSOA_Dec_2011_Boundaries_Generalised_Clipped_BGC_EW_V3/FeatureServer/0/query'
base_url_lsoa_2021_lookups = 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LSOA21_WD24_LAD24_EW_LU/FeatureServer/0/query?'
base_url_lsoa_2011_lookups = 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LSOA01_LSOA11_LAD11_EW_LU_ddfe1cd1c2784c9b991cded95bc915a9/FeatureServer/0/query'
imd_data_path = 'https://github.com/humaniverse/IMD/raw/master/data-raw/imd_england_lsoa.csv'
path_2011_poly_parquet = 'data/all_cas_lsoa_poly_2011.parquet'
path_2021_poly_parquet = 'data/all_cas_lsoa_poly_2021.parquet'
chunk_size = 100 # this is used in a a where clause to set the number of lsoa polys per api call

chunk_size = 1
params_base = {
    'outFields': '*',
    'outSR': 4326,
    'f': 'json'
}

# %%
ca_la_df = get_ca.get_ca_la_df(2023, inc_ns = True) # include NS
ca_la_codes = get_ca.get_ca_la_codes(ca_la_df)
ladcds_in_cauths = ca_la_codes # this is the same as ca_la_codes RATIONALISE
# ca_la_df.glimpse()

# %%
la_list = (ca_la_df['ladcd']) #includes north somerset
ladnm = tuple(ca_la_df['ladnm'].to_list())
f'There are {str(la_list.shape)[1:3]} Local Authorities in Combined Authorities'

# %% [markdown]
# Get the lookup table that relates DFT Local authority ID's in the Combined authorities to ONS LA codes

# %%
ca_la_dft_lookup_df = get_ca.get_ca_la_dft_lookup(
    dft_csv_path = 'https://storage.googleapis.com/dft-statistics/road-traffic/downloads/data-gov-uk/local_authority_traffic.csv',
    la_list = la_list)
# ca_la_dft_lookup_df.glimpse()

# %%

# postcode_file = get_ca.get_zipped_csv_file(url = "https://www.arcgis.com/sharing/rest/content/items/3770c5e8b0c24f1dbe6d2fc6b46a0b18/data",
#                       file_folder_name = "postcode_lookup")
# postcodes_df = get_ca.get_postcode_df(postcode_file, ca_la_codes)

# %%
# this is hit by the 2000 record limit too so use DL file
# input_file = get_ca.get_geojson(url = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LLSOA_Dec_2021_PWC_for_England_and_Wales_2022/FeatureServer/0/query",
#                       destination_directory = "data\\geojson")

# %%
#ca_lsoa_codes = get_ca.get_ca_lsoa_codes(postcodes_df)

# %% [markdown]
# run cell below if not needing to update LSOA geodata (its expensive and crashes)

# %%
# if not download_lsoa:
#     reproject_path = 'data/geojson/ca_lsoa_pwc_wgs84.geojson'
#     reproject_lsoa_poly_path = 'data/geojson/ca_lsoa_poly_wgs84.geojson'

# %%
# if download_lsoa:
lookups_2021_chunk_list = get_ca.get_chunk_list(base_url_lsoa_2021_lookups,
                                    params_base,
                                    max_records = 1000)
# list of pl.dataframes of the lookups data in cauths
lookups_2021_pldf_list = [get_ca.get_flat_data(chunk,
                                params_base,
                                params_other = {'where':'1=1'},
                                base_url = base_url_lsoa_2021_lookups)
                    for chunk
                    in lookups_2021_chunk_list]

lookups_2021_pldf = pl.concat(lookups_2021_pldf_list, how='vertical_relaxed')
#%%
lsoas_in_cauths_iter = (lookups_2021_pldf
                        .filter(pl.col('LAD24CD').is_in(ladcds_in_cauths))
                        .select(pl.col('LSOA21CD'))
                        .to_series())

lsoas_in_cauths_chunks = [lsoas_in_cauths_iter[i:i + chunk_size]
                        for i in range(0,
                                        len(lsoas_in_cauths_iter),
                                        chunk_size)]
#%%
# list of urls to get the lsoa polygons in the combined authorities
lsoa_2021_poly_url_list = [get_ca.make_poly_url(base_url_2021_lsoa_polys,
                            params_base,
                            lsoas,
                            lsoa_code_name='LSOA21CD')
                for lsoas in
                lsoas_in_cauths_chunks]

# a list of geopandas dataframes to hold all lsoa polygons in the combined authorities
lsoa_2021_gdf_list = [gpd.read_file(polys_url) for polys_url in lsoa_2021_poly_url_list]

lsoa_2021_gdf = gpd.GeoDataFrame(pd.concat(lsoa_2021_gdf_list,
                                    ignore_index=True))
# parquet export to import to duckdb
lsoa_2021_gdf.to_parquet('data/all_cas_lsoa_poly_2021.parquet')

# Retrieve the 2021 LSOA points (population weighted centroids)
lsoa_2021_pwc_df = get_ca.make_lsoa_pwc_df(base_url = base_url_lsoa_2021_centroids,
                        params_base = params_base, 
                        params_other = {'where': '1=1'},
                        max_records = 2000)

lsoa_2021_pwc_cauth_df = (lsoa_2021_pwc_df
                    .filter(pl.col('LSOA21CD').is_in(lsoas_in_cauths_iter))
                    .rename(lambda x: x.lower())
                    )
# Retrieve the 2011 LSOA polygon data for the LEP - for joining with IMD
# The latest IMD data available is for 2019
lookups_2011_chunk_list = get_ca.get_chunk_list(base_url_lsoa_2011_lookups,
                                        params_base, 
                                            max_records = 1000)

lookups_2011_pldf_list = [get_ca.get_flat_data(chunk,
                                    params_base,
                                    params_other = {'where':'1=1'},
                                    base_url = base_url_lsoa_2011_lookups)
                        for chunk
                        in lookups_2011_chunk_list]

lookups_2011_pldf = (pl
                    .concat(lookups_2011_pldf_list,
                            how='vertical_relaxed')
                            .rename(lambda x: x.lower())
                            )

lsoacd_2011_in_cauths_iter = (lookups_2011_pldf
                            .filter(pl.col('lad11cd')
                                    .is_in(ladcds_in_cauths))
                                    .select(pl.col('lsoa11cd'))
                                    .unique()
                                    .to_series()
                                    )

lsoa_2011_in_cauths_chunks = [lsoacd_2011_in_cauths_iter[i:i + chunk_size]
                        for i in range(0,
                                        len(lsoacd_2011_in_cauths_iter),
                                        chunk_size)]

lsoa_2011_poly_url_list = [get_ca.make_poly_url(base_url_2011_lsoa_polys,
                            params_base,
                            lsoas,
                            lsoa_code_name='LSOA11CD')
                for lsoas in
                lsoa_2011_in_cauths_chunks]

lsoa_2011_gdf_list = [gpd.read_file(polys_url) for polys_url in lsoa_2011_poly_url_list]

lsoa_2011_gdf = gpd.GeoDataFrame(pd.concat(lsoa_2011_gdf_list,  ignore_index=True))
# parquet export to import to duckdb
lsoa_2011_gdf.to_parquet('data/all_cas_lsoa_poly_2011.parquet')

lsoa_imd_df = (pl.read_csv(imd_data_path)
                .rename(lambda x: x.lower()))
#%%


postcode_url_list = [get_ca.make_poly_url(base_url_postcode_centroids,
                                params_base,
                                lsoas,
                                lsoa_code_name='LSOA21')
                    for lsoas in
                    lsoas_in_cauths_chunks]


#%%
test_url = postcode_url_list[0]

#%%
print(test_url)
#%%

with requests.get(test_url,
                      params = {},
                      stream = True) as r:
        r.raise_for_status()
        features = r.json().get('features')
        features_df = (
            pl.DataFrame(features)
            .unnest('attributes')
            .drop('GlobalID')
            )










# %% [markdown]
    # Now for the EPC data
# %%
cols_schema_nondom = {
    'LMK_KEY': pl.Utf8,
    'POSTCODE': pl.Utf8,
    'BUILDING_REFERENCE_NUMBER': pl.Int64,
    'ASSET_RATING': pl.Int64,
    'ASSET_RATING_BAND': pl.Utf8,
    'PROPERTY_TYPE': pl.Utf8,
    'LOCAL_AUTHORITY': pl.Utf8,
    'CONSTITUENCY': pl.Utf8,
    'TRANSACTION_TYPE': pl.Utf8,
    'STANDARD_EMISSIONS': pl.Float64,
    'TYPICAL_EMISSIONS': pl.Float64,
    'TARGET_EMISSIONS': pl.Float64,
    'BUILDING_EMISSIONS': pl.Float64,
    'BUILDING_LEVEL': pl.Int64,
    'RENEWABLE_SOURCES': pl.Utf8,
    'LODGEMENT_DATETIME': pl.Utf8,
    'UPRN': pl.Utf8
    }

# %% [markdown]
## download from https://epc.opendatacommunities.org/downloads/non-domestic
# %%
epc_non_domestic = (pl.scan_csv(
    'data/all-non-domestic-certificates-single-file/certificates.csv',
    schema = cols_schema_nondom,

)
.filter(pl.col('LOCAL_AUTHORITY').is_in(la_list.to_list()))
.with_columns(pl.col('LODGEMENT_DATETIME').str.to_datetime(format='%Y-%m-%d %H:%M:%S', strict=False))
                    .sort(pl.col(['UPRN', 'LODGEMENT_DATETIME']))
                    .group_by('UPRN').last()
).collect()

# %%
# cols_schema_adjusted this schema is for the csv files retrieved using the epc API
cols_schema_adjusted = {
 'lmk-key': pl.Utf8,
 'postcode': pl.Utf8,
 'local-authority': pl.Utf8,
 'property-type': pl.Utf8,
 'lodgement-datetime': pl.Utf8,
 'transaction-type': pl.Utf8,
 'tenure': pl.Utf8,
 'mains-gas-flag': pl.Utf8,
 'hot-water-energy-eff': pl.Utf8,
 'windows-description': pl.Utf8,
 'windows-energy-eff': pl.Utf8,
 'walls-description': pl.Utf8,
 'walls-energy-eff': pl.Utf8,
 'roof-description': pl.Utf8,
 'roof-energy-eff': pl.Utf8,
 'mainheat-description': pl.Utf8,
 'mainheat-energy-eff': pl.Utf8,
 'mainheat-env-eff': pl.Utf8,
 'main-heating-controls': pl.Utf8,
 'mainheatcont-description': pl.Utf8,
 'mainheatc-energy-eff': pl.Utf8,
 'main-fuel': pl.Utf8,
 'solar-water-heating-flag': pl.Utf8,
 'construction-age-band': pl.Utf8,
 'current-energy-rating': pl.Utf8,
 'potential-energy-rating': pl.Utf8,
 'current-energy-efficiency': pl.Utf8,
 'potential-energy-efficiency': pl.Utf8,
 'built-form': pl.Utf8,
 'constituency': pl.Utf8,
 'floor-description': pl.Utf8,
 'environment-impact-current': pl.Int64,
 'environment-impact-potential': pl.Int64,
 'energy-consumption-current': pl.Int64,
 'energy-consumption-potential': pl.Int64,
 'co2-emiss-curr-per-floor-area': pl.Int64,
 'co2-emissions-current': pl.Float64,
 'co2-emissions-potential': pl.Float64,
 'lighting-cost-current': pl.Int64,
 'lighting-cost-potential': pl.Int64,
 'heating-cost-current': pl.Int64,
 'heating-cost-potential': pl.Int64,
 'hot-water-cost-current': pl.Int64,
 'hot-water-cost-potential': pl.Int64,
 'total-floor-area': pl.Float64,
 'number-habitable-rooms': pl.Int64,
 'number-heated-rooms': pl.Int64,
 'photo-supply': pl.Float64,
 'uprn': pl.Int64,
 'building-reference-number': pl.Int64}

# %%
# Only run this if you are updating the EPC data from the opendatacommunities API
# THIS WILL TAKE AT LEAST 2 HOURS TO RUN!!!
# could do with tqdm in here
from_date_dict = get_ca.get_epc_from_date()
if not download_epc:
    print('Not downloading EPC data')
else:
    get_ca.delete_all_csv_files('data/epc_csv')
    [get_ca.get_epc_csv(la,
                        from_date = from_date_dict)
                        for la 
                        in la_list]


# %%
epc_domestic = get_ca.ingest_dom_certs_csv(la_list, cols_schema_adjusted)
epc_domestic.glimpse()
# %%
epc_domestic_df = ((get_ca.wrangle_epc(certs_df = epc_domestic)
                   .with_columns(pl.col('tenure')
                                 .pipe(get_ca.clean_tenure, 'tenure_epc')))
                   .pipe(get_ca.make_n_construction_age, 'n_nominal_construction_date')
                   )
epc_domestic_df.glimpse()

# %%
epc_non_domestic_df = get_ca.wrangle_epc(epc_non_domestic)
epc_non_domestic_df.glimpse()

# %%
del epc_domestic, epc_non_domestic

# %% [markdown]
# Collect centroids - here to stop crash

# %%
pc_centroids_q = pl.scan_csv('data/postcode_centroids.csv',
                             dtypes={
                                 'RU11IND': pl.Utf8,
                                 'x': pl.Float64,
                                 'y': pl.Float64
                                 })
# pc_centroids_df.head()

# %%
pc_min_df = pc_centroids_q.head(1).collect()

# %%
pc_rename_dict = get_ca.get_rename_dict(pc_min_df, get_ca.remove_numbers, rm_numbers=False)

# %%
pc_centroids_df = (pc_centroids_q
                   .filter(pl.col('LAUA').is_in(la_list))
                   .rename(mapping=pc_rename_dict)).collect()
# pc_centroids_df.glimpse()

# %% [markdown]
# Tenure - ts054 from NOMIS - slightly cleaned - remove csv header 

# %%
ca_tenure_lsoa = (pl.scan_csv('data/ts054_tenure_nomis.csv')
                  .select(pl.all().name.map(lambda col_name: col_name.replace(' ', '_')))
                  .select(pl.all().name.to_lowercase())
                  .select(pl.all().name.map(lambda col_name: col_name.replace(':', '')))
                  .with_columns(pl.col('lsoa').str.slice(0, 9).alias('lsoacd'))
                  .filter(pl.col('lsoacd').is_in(ca_lsoa_codes))
                  ).collect()

# %%
ca_tenure_lsoa.glimpse()

# %%
del postcodes_df

# %% [markdown]
# Load the data into a duckDB data base

# %%
con = duckdb.connect('data/ca_epc.duckdb')

# %%
try:
    con.execute("BEGIN TRANSACTION;")
    con.execute('INSTALL spatial;')
    con.execute('LOAD spatial;')
    con.execute("CREATE OR REPLACE TABLE lsoa_2021_pwc_tbl AS SELECT * FROM lsoa_2021_pwc_df")
    con.execute("ALTER TABLE lsoa_2021_pwc_tbl ADD COLUMN geom GEOMETRY")
    con.execute("UPDATE lsoa_2021_pwc_tbl SET geom = ST_Point(x, y)")
    con.execute("CREATE OR REPLACE TABLE imd_lsoa_tbl AS SELECT * FROM lsoa_imd_df")
    con.execute(f'CREATE OR REPLACE TABLE lsoa_poly_2021_tbl AS SELECT * FROM ST_Read("{path_2021_poly_parquet}")')
    con.execute(f'CREATE OR REPLACE TABLE lsoa_poly_2011_tbl AS SELECT * FROM ST_Read("{path_2011_poly_parquet}")')
    con.execute('CREATE UNIQUE INDEX lsoacd_poly_idx ON lsoa_poly_tbl (lsoacd)')
    con.execute('CREATE UNIQUE INDEX lsoacd_pwc_idx ON lsoa_pwc_tbl (lsoacd)')
    con.execute('CREATE OR REPLACE TABLE ca_tenure_lsoa_tbl AS SELECT * FROM ca_tenure_lsoa')
    con.execute('CREATE UNIQUE INDEX lsoacd_tenure_idx ON ca_tenure_lsoa_tbl (lsoacd)')
    con.execute('CREATE OR REPLACE TABLE ca_la_tbl AS SELECT * FROM ca_la_df')
    con.execute('CREATE OR REPLACE TABLE imd_tbl AS SELECT * FROM imd_df')
    # con.execute('CREATE OR REPLACE TABLE postcodes_tbl AS SELECT * FROM postcodes_df')
    # con.execute('CREATE UNIQUE INDEX postcode_idx ON postcodes_tbl (postcode)')
    con.execute('CREATE OR REPLACE TABLE postcode_centroids_tbl AS SELECT * FROM pc_centroids_df')
    con.execute('CREATE UNIQUE INDEX postcode_centroids_idx ON postcode_centroids_tbl (PCDS)')
    con.execute('CREATE OR REPLACE TABLE epc_non_domestic_tbl AS SELECT * FROM epc_non_domestic_df')
    con.execute('CREATE UNIQUE INDEX uprn_nondom_idx ON epc_non_domestic_tbl (uprn)')
    con.execute('CREATE OR REPLACE TABLE ca_la_dft_lookup_tbl AS SELECT * FROM ca_la_dft_lookup_df')
    con.execute('CREATE UNIQUE INDEX ca_la_dft_lookup_idx ON ca_la_dft_lookup_tbl (ladcd)')
    con.execute("COMMIT;")
except Exception as e:
    # If an error occurs, rollback the transaction
    con.execute("ROLLBACK;")
    print(f"Transaction rolled back due to an error: {e}")

# %% [markdown]
# Have to do the domestic epc's outside the transaction block otherwise memory fail

# %%
del epc_non_domestic_df, pc_centroids_df
#%%
# the query to create the view for epc_lep_domestic_ods_vw
create_epc_domestic_view_qry = '''
CREATE OR REPLACE VIEW epc_lep_domestic_ods_vw AS

SELECT   ROW_NUMBER() OVER (ORDER BY lmk_key) AS rowname,
         lmk_key,
         local_authority,
         property_type,
         transaction_type,
         tenure_epc as tenure,
         walls_description,
         roof_description,
         walls_energy_eff,
         roof_energy_eff,
         mainheat_description,
         mainheat_energy_eff,
         mainheat_env_eff,
         main_fuel,
         solar_water_heating_flag,
         construction_age_band,
         current_energy_rating,
         potential_energy_rating,
         co2_emissions_current,
         co2_emissions_potential,
         co2_emiss_curr_per_floor_area,
         number_habitable_rooms,
         number_heated_rooms,
         photo_supply,
         total_floor_area,
         building_reference_number,
         built_form,
         lsoa21,
         msoa21,
         lat,
         long,
         imd,
         total,
         owned,
         social_rented,
         private_rented,
         date,
         year,
         month,
         imd_decile as n_imd_decile,
         n_nominal_construction_date,
         CASE WHEN n_nominal_construction_date < 1900 THEN 'Before 1900'
         WHEN (n_nominal_construction_date >= 1900) AND (n_nominal_construction_date <= 1930) THEN '1900 - 1930'
         WHEN n_nominal_construction_date > 1930 THEN '1930 to present'
         ELSE 'Unknown' END AS construction_epoch,
         ca_la_tbl.ladnm as ladnm,
        FROM epc_domestic_tbl

        LEFT JOIN postcode_centroids_tbl ON epc_domestic_tbl.postcode = postcode_centroids_tbl.pcds

        LEFT JOIN ca_tenure_lsoa_tbl ON postcode_centroids_tbl.lsoa21 = ca_tenure_lsoa_tbl.lsoacd

        LEFT JOIN ca_la_tbl 
        ON ca_la_tbl.ladcd = epc_domestic_tbl.local_authority

        LEFT JOIN imd_tbl
        ON ca_tenure_lsoa_tbl.lsoacd = imd_tbl.lsoacd

        WHERE local_authority IN 
        (SELECT ladcd
        FROM ca_la_tbl
        WHERE cauthnm = \'West of England\')
'''

#%%
create_epc_non_domestic_view_qry = """
 CREATE OR REPLACE VIEW epc_non_domestic_ods_vw AS
        SELECT
                epc_non_domestic_tbl.* EXCLUDE postcode,
                ca_la_tbl.*,
                p.lsoa21,
                p.lat,
                p.long
        FROM epc_non_domestic_tbl

INNER JOIN
        ca_la_tbl 
        ON epc_non_domestic_tbl.local_authority = ca_la_tbl.ladcd
INNER JOIN 
        
        (SELECT pcds, lsoa21, lat, long 
        FROM postcode_centroids_tbl) as p
        ON epc_non_domestic_tbl.postcode = p.pcds
WHERE ca_la_tbl.ladnm 
        IN ('Bristol, City of',
        'Bath and North East Somerset',
        'North Somerset',
        'South Gloucestershire');
"""
# %%
con.execute('CREATE OR REPLACE TABLE epc_domestic_tbl AS SELECT * FROM epc_domestic_df')
con.execute('CREATE UNIQUE INDEX uprn_idx ON epc_domestic_tbl (uprn)')
# make the domestic and non domestic views for export to ODS
con.execute(create_epc_domestic_view_qry)
con.execute(create_epc_non_domestic_view_qry)
# %%
del epc_domestic_df, ca_tenure_lsoa, ca_lsoa_codes
# %%
# temporary files to delete
get_ca.delete_file(path_2011_poly_parquet)
get_ca.delete_file(path_2021_poly_parquet)

# %%
con.execute("EXPORT DATABASE 'data/db_export' (FORMAT PARQUET);")

# %%
con.close()

# %% [markdown]
# Introspect Database
# 

# %%
con = duckdb.connect('data/ca_epc.duckdb')

# %%
con.sql("SHOW ALL TABLES;")

# %%
con.sql('DESCRIBE epc_domestic_tbl')

# %%
con.sql('DESCRIBE epc_non_domestic_tbl')

# %%
con.sql('SELECT COUNT(*) num_rows FROM lsoa_pwc_tbl')

# %%


# %%
con.close()
