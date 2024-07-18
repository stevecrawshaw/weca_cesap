# %%
# use .venv - make sure to restart jupyter kernel. duckdb need python 3.10
import polars as pl
import duckdb
import get_ca_data as get_ca # functions for retrieving CA \ common data
download_epc = False
download_lsoa = False

# %% [markdown]
# This notebook retrieves all the base data needed for comparison analysis with other Combined Authorities and loads it into a duckdb database.
# SQLite was tried, but it is slow, not directly compatible with polars and does not work well with datasette because of the size of data.

# %%
ca_la_df = get_ca.get_ca_la_df(2023, inc_ns = True) # include NS
# ca_la_df.glimpse()


# %%
la_list = (ca_la_df['ladcd']) #includes north somerset
f'There are {str(la_list.shape)[1:3]} Local Authorities in Combined Authorities'
ladnm = tuple(ca_la_df['ladnm'].to_list())

# %% [markdown]
# Get the lookup table that relates DFT Local authority ID's in the Combined authorities to ONS LA codes

# %%
ca_la_dft_lookup_df = get_ca.get_ca_la_dft_lookup(
    dft_csv_path = 'https://storage.googleapis.com/dft-statistics/road-traffic/downloads/data-gov-uk/local_authority_traffic.csv',
    la_list = la_list)
# ca_la_dft_lookup_df.glimpse()

# %%
ca_la_codes = get_ca.get_ca_la_codes(ca_la_df)
postcode_file = get_ca.get_zipped_csv_file(url = "https://www.arcgis.com/sharing/rest/content/items/3770c5e8b0c24f1dbe6d2fc6b46a0b18/data",
                      file_folder_name = "postcode_lookup")
postcodes_df = get_ca.get_postcode_df(postcode_file, ca_la_codes)

# %%
# this is hit by the 2000 record limit too so use DL file
# input_file = get_ca.get_geojson(url = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LLSOA_Dec_2021_PWC_for_England_and_Wales_2022/FeatureServer/0/query",
#                       destination_directory = "data\\geojson")

# %%
ca_lsoa_codes = get_ca.get_ca_lsoa_codes(postcodes_df)

# %% [markdown]
# run cell below if not needing to update LSOA geodata (its expensive and crashes)

# %%
if not download_lsoa:
    reproject_path = 'data/geojson/ca_lsoa_pwc_wgs84.geojson'
    reproject_lsoa_poly_path = 'data/geojson/ca_lsoa_poly_wgs84.geojson'

# %%
# rename the LSOA features and return the path
if download_lsoa:
    cleaned_lsoa_pwc_path = get_ca.clean_lsoa_geojson('data/LLSOA_Dec_2021_PWC_for_England_and_Wales_2022_-7534040603619445107.geojson',
                                                    lsoacd='LSOA21CD')
    # filter for just the LSOA's in Combined Authorities
    ca_lsoa_pwc_path = get_ca.filter_geojson(input_file = cleaned_lsoa_pwc_path,
                                            output_file='data/geojson/ca_lsoa_pwc.geojson',
                                            property_name ='lsoacd',
                                            ca_lsoa_codes = ca_lsoa_codes)

    cleaned_lsoa_poly_path = get_ca.clean_lsoa_geojson('data/geojson/Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW_BFE_V10_1289561450475266465.geojson',
                                                    lsoacd='LSOA21CD')
    # reproject to WGS84:4326 as default from ONS is 27700
    reproject_path = get_ca.reproject(ca_lsoa_pwc_path, output_wgs84_file='data/geojson/ca_lsoa_pwc_wgs84.geojson', lsoa_code = 'lsoacd')

    # filter for just the LSOA's polys in Combined Authorities
    ca_lsoa_poly_path = get_ca.filter_geojson(input_file = cleaned_lsoa_poly_path,
                                            output_file='data/geojson/ca_lsoa_poly.geojson',
                                            property_name ='lsoacd',
                                            ca_lsoa_codes = ca_lsoa_codes)

    reproject_lsoa_poly_path = get_ca.reproject(ca_lsoa_poly_path,
                                                output_wgs84_file='data/geojson/ca_lsoa_poly_wgs84.geojson',
                                                lsoa_code = 'lsoacd')
#%%
imd_df = get_ca.get_imd_df(path = 'data/imd2019lsoa.csv')
# %%
# https://geoportal.statistics.gov.uk/datasets/lsoa-dec-2021-pwc-for-england-and-wales/explore

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
if not download_epc:
    print('Not downloading EPC data')
else:
    get_ca.delete_all_csv_files('data/epc_csv')
    [get_ca.get_epc_csv(la) for la in la_list]
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
    con.execute(f'CREATE OR REPLACE TABLE lsoa_pwc_tbl AS SELECT * FROM ST_Read("{reproject_path}")')
    con.execute(f'CREATE OR REPLACE TABLE lsoa_poly_tbl AS SELECT * FROM ST_Read("{reproject_lsoa_poly_path}")')
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
create_view_qry = '''
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
# %%
con.execute('CREATE OR REPLACE TABLE epc_domestic_tbl AS SELECT * FROM epc_domestic_df')
con.execute('CREATE UNIQUE INDEX uprn_idx ON epc_domestic_tbl (uprn)')
# make the domestic epc view for export to ODS
con.execute(create_view_qry)
# %%
del epc_domestic_df, ca_tenure_lsoa

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
