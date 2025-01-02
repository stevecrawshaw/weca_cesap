# %%
import polars as pl
import duckdb
import json
import get_ca_data as get_ca # functions for retrieving CA \ common data
import geopandas as gpd
import pandas as pd
from janitor.polars import clean_names
import glob

download_epc = False
download_lsoa = True
download_postcodes = False

# %% [markdown]
# This notebook retrieves all the base data needed for comparison analysis with other Combined Authorities and loads it into a duckdb database.

# %% [markdown]
## Define the base urls for the ArcGIS API and some parameters
# we get the 2011 LSOA data as these match to the IMD lsoa codes

#%%
esri_fs_base_url = 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/'
esri_fs_tail_url = 'FeatureServer/0/query'

#%% [markdown]
# Esri Featureserver paths
# %%
# this one is a zip file for all postcode centroids.
base_url_pc_centroids_zip = "https://www.arcgis.com/sharing/rest/content/items/3700342d3d184b0d92eae99a78d9c7a3/data"

base_url_lsoa_2021_centroids = get_ca.make_esri_fs_url(esri_fs_base_url,
                                                "LLSOA_Dec_2021_PWC_for_England_and_Wales_2022/",
                                                esri_fs_tail_url)

base_url_2021_lsoa_polys = get_ca.make_esri_fs_url(esri_fs_base_url,
                                            "Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW_BFC_V10/",
                                            esri_fs_tail_url)

base_url_2011_lsoa_polys = get_ca.make_esri_fs_url(esri_fs_base_url,
                                            "LSOA_Dec_2011_Boundaries_Generalised_Clipped_BGC_EW_V3/",
                                            esri_fs_tail_url)

base_url_lsoa_2021_lookups = get_ca.make_esri_fs_url(esri_fs_base_url,
                                            "LSOA21_WD24_LAD24_EW_LU/",
                                            esri_fs_tail_url)

base_url_lsoa_2011_lookups = get_ca.make_esri_fs_url(esri_fs_base_url,
                                            "LSOA01_LSOA11_LAD11_EW_LU_ddfe1cd1c2784c9b991cded95bc915a9/",
                                            esri_fs_tail_url)

base_url_lsoa_2011_lookups = get_ca.make_esri_fs_url(esri_fs_base_url,
                                            "LSOA01_LSOA11_LAD11_EW_LU_ddfe1cd1c2784c9b991cded95bc915a9/",
                                            esri_fs_tail_url)
#%% [markdown]
# Other paths

#%%
imd_data_path = 'https://github.com/humaniverse/IMD/raw/master/data-raw/imd_england_lsoa.csv'
path_2011_poly_parquet = 'data/all_cas_lsoa_poly_2011.parquet'
path_2021_poly_parquet = 'data/all_cas_lsoa_poly_2021.parquet'
chunk_size = 100 # this is used in a a where clause to set the number of lsoa polys per api call
nomis_ts054_url = "https://www.nomisweb.co.uk/api/v01/dataset/NM_2072_1.data.csv"
postcode_directory = "data/postcode_centroids"
dft_csv_path = 'https://storage.googleapis.com/dft-statistics/road-traffic/downloads/data-gov-uk/local_authority_traffic.csv'
#%% [markdown]
# Nomis parameters for the tenure data
#%%
ts054_params = {'date': ['latest'],
                'c2021_tenure_9': ['0,1001...1004,8,9996,9997'],
                'measures': ['20100'],
                'geography': ['TYPE151'],
                'select': ['GEOGRAPHY_NAME,GEOGRAPHY_CODE,C2021_TENURE_9_NAME,C2021_TENURE_9_SORTORDER,OBS_VALUE']
                }
nomis_creds = get_ca.load_config('../config.yml').get('nomis')
# %% [markdown]
# ESRI FeatureServer parameters
#%%
params_base = {
    'outFields': '*',
    'outSR': 4326,
    'f': 'json'
}

# %%
ca_la_df = get_ca.get_ca_la_df(2024, inc_ns = True) # include NS
ca_la_codes = get_ca.get_ca_la_codes(ca_la_df)
ladcds_in_cauths = ca_la_codes # this is the same as ca_la_codes RATIONALISE
# ca_la_df.glimpse()
# %%
la_list = (ca_la_df['ladcd']) #includes north somerset
ladnm = tuple(ca_la_df['ladnm'].to_list())
f'There are {str(la_list.shape)[1:3]} Local Authorities in Combined Authorities'
#%%
# %% [markdown]
# Get the lookup table that relates DFT Local authority ID's in the Combined authorities to ONS LA codes

# %%
ca_la_dft_lookup_df = get_ca.get_ca_la_dft_lookup(
    dft_csv_path,
    la_list = la_list)
# ca_la_dft_lookup_df.glimpse()

# %%
# if download_lsoa:
lookups_2021_chunk_list = get_ca.get_chunk_list(base_url_lsoa_2021_lookups,
                                    params_base,
                                    max_records = 1000)
#%%
#x = get_ca.get_flat_data(200, params_base, params_other={'where':'1=1'}, base_url = base_url_lsoa_2021_lookups)
print(base_url_lsoa_2021_lookups)
#%%
# list of pl.dataframes of the lookups data in cauths
lookups_2021_pldf_list = [get_ca.get_flat_data(chunk,
                                params_base,
                                params_other = {'where':'1=1'},
                                base_url = base_url_lsoa_2021_lookups)
                    for chunk
                    in lookups_2021_chunk_list]

lookups_2021_pldf = (pl.concat(lookups_2021_pldf_list, how='vertical_relaxed')
                                                 .rename(lambda x: x.lower())
                                                 .unique())
#%%

lookups_2011_chunk_list = get_ca.get_chunk_list(base_url_lsoa_2011_lookups,
                                        params_base, 
                                            max_records = 2000)

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
                            .unique()
                            )

#%%
lsoas_in_cauths_iter = (lookups_2021_pldf
                        .filter(pl.col('lad24cd').is_in(ladcds_in_cauths))
                        .select(pl.col('lsoa21cd'))
                        .to_series())

lsoas_in_cauths_chunks = [lsoas_in_cauths_iter[i:i + chunk_size]
                        for i in range(0,
                                        len(lsoas_in_cauths_iter),
                                        chunk_size)]

#%%
lsoas_in_cauths_iter_list = list(lsoas_in_cauths_iter)
with open("data/lsoas_in_cauths_iter.json", 'w') as f:
    # indent=2 is not needed but makes the file human-readable 
    # if the data is nested
    json.dump(lsoas_in_cauths_iter_list, f, indent=2) 
# %%
with open("data/lsoas_in_cauths_iter.json", 'r') as f:
    lsoas_in_cauths_iter = json.load(f)



#%%
# list of urls to get the lsoa polygons in the combined authorities
lsoa_2021_poly_url_list = [get_ca.make_poly_url(base_url_2021_lsoa_polys,
                            params_base,
                            lsoas,
                            lsoa_code_name='lsoa21cd')
                for lsoas in
                lsoas_in_cauths_chunks]
#%%
# a list of geopandas dataframes to hold all lsoa polygons in the combined authorities
lsoa_2021_gdf_list = [gpd.read_file(polys_url)
                      for polys_url 
                      in lsoa_2021_poly_url_list]
#%%
lsoa_2021_gdf = (gpd.GeoDataFrame(pd.concat(lsoa_2021_gdf_list,
                                    ignore_index=True))
                                    .drop_duplicates(subset='LSOA21CD')
)

#%%
# parquet export to import to duckdb
lsoa_2021_gdf.to_parquet('data/all_cas_lsoa_poly_2021.parquet')
#%%
# Retrieve the 2021 LSOA points (population weighted centroids)
lsoa_2021_pwc_df = get_ca.make_lsoa_pwc_df(base_url = base_url_lsoa_2021_centroids,
                        params_base = params_base, 
                        params_other = {'where': '1=1'},
                        max_records = 2000)
#%%
lsoa_2021_pwc_cauth_df = (lsoa_2021_pwc_df
                    .filter(pl.col('lsoa21cd').is_in(lsoas_in_cauths_iter))
                    .rename(lambda x: x.lower())
                    )
#%%
# Retrieve the 2011 LSOA polygon data - for joining with IMD
# The latest IMD data available is for 2019


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

lsoa_2011_gdf = gpd.GeoDataFrame(pd.concat(lsoa_2011_gdf_list,
                                           ignore_index=True))
# parquet export to import to duckdb
lsoa_2011_gdf.to_parquet('data/all_cas_lsoa_poly_2011.parquet')
#%%
lsoa_imd_df = (pl.read_csv(imd_data_path)
                .rename(lambda x: x.lower())
                .rename({'lsoa_code': 'lsoa11cd'}))
#%% [markdown]
# Read the POSTCODES DATA

#%%
if download_postcodes:
    zipped_file_path = get_ca.download_zip(url = base_url_pc_centroids_zip,
    directory=postcode_directory,
    filename = "postcodes.zip")
    # TODO: build import routine for direct import to duckdb
    postcodes_csv_file = get_ca.extract_csv_from_zip(zip_file_path = zipped_file_path)
    get_ca.delete_zip_file(zip_file_path = zipped_file_path)


#%%
# make a list of urls, download the zip files and extract the csv files
# for domestic and non - domestic EPC data
# the csv's are ingested directly into the duckdb database without significant processing
# the EPC data is cleaned and processed in the database creating views for export to the ODS
if download_epc:
    la_zipfile_nondom_list = get_ca.make_zipfile_list(ca_la_df, type = "non-domestic")
    get_ca.dl_bulk_epc_zip(la_zipfile_nondom_list, path = "data/epc_bulk_nondom_zips")
    get_ca.extract_and_rename_csv_from_zips("data/epc_bulk_nondom_zips")
    la_zipfile_list = get_ca.make_zipfile_list(ca_la_df, type = "domestic")
    get_ca.dl_bulk_epc_zip(la_zipfile_list, path = "data/epc_bulk_zips")
    get_ca.extract_and_rename_csv_from_zips("data/epc_bulk_zips")


# %%
# SECTION BELOW IS FOR THE UPDATE ROUTINE ONLY TESTING
    # - PROBABLY IMPLEMENT IN SEPARATE SCRIPT
if not download_epc:
    from_date_dict = get_ca.get_epc_from_date()
    epc_update_pldf = get_ca.make_epc_update_pldf(la_list, from_date_dict)
#%%
# TENURE
# Load all tenure LSOA data into the db and create views for 
# subsets e.g. West of England and all cauths
tenure_raw_df = get_ca.get_nomis_data(nomis_ts054_url, ts054_params, nomis_creds)

#%%
tenure_raw_df.glimpse()
#%%
tenure_df = (tenure_raw_df
             .pivot('C2021_TENURE_9_NAME',
                    index = ['GEOGRAPHY_NAME', 'GEOGRAPHY_CODE'],
                    values = 'OBS_VALUE')
             .clean_names() # uses pyjanitor.polars
             .rename({'geography_name': 'lsoa_name',
                      'geography_code': 'lsoa21cd'})
)

#%%
tenure_df.glimpse()

# %% [markdown]
# Load the data into a duckDB data base
# %%
con = duckdb.connect('data/ca_epc.duckdb')
#%%
con.close()
# %% [markdown]
### Load the EPC data into the duckdb database
    # It's not done in the main commit block because multiple csv files are inported
    # rather than a single dataframe
#%%

#%%
# now domestic certs
get_ca.load_csv_duckdb(con = con,
                       csv_path="data/epc_bulk_zips",
                       schema_file="certificates_schema.sql",
                       table_name="epc_domestic_tbl",
                       schema_cols=get_ca.cols_schema_domestic)
#%%

get_ca.load_csv_duckdb(con = con,
                       csv_path="data/epc_bulk_nondom_zips",
                       schema_file="nondom_certificates_schema.sql",
                       table_name="epc_nondom_tbl",
                       schema_cols=get_ca.cols_schema_nondom)
#%% [markdown]
# load the postcode data into the database

get_ca.load_csv_duckdb(con = con,
                       csv_path=postcode_directory,
                       schema_file="postcodes_schema.sql",
                       table_name="postcodes_tbl",
                       schema_cols = get_ca.postcodes_schema)

# %%
try:
    con.execute("BEGIN TRANSACTION;")
    con.execute('INSTALL spatial;')
    con.execute('LOAD spatial;')
    # LSOA PWC
    con.execute("CREATE OR REPLACE TABLE lsoa_2021_pwc_tbl AS SELECT * FROM lsoa_2021_pwc_df")
    con.execute("ALTER TABLE lsoa_2021_pwc_tbl ADD COLUMN geom GEOMETRY")
    con.execute("UPDATE lsoa_2021_pwc_tbl SET geom = ST_Point(x, y)")
    con.execute('CREATE UNIQUE INDEX lsoacd_pwc_idx ON lsoa_2021_pwc_tbl (lsoa21cd)')
    # LOOKUPS
    con.execute("CREATE OR REPLACE TABLE lsoa_2021_lookup_tbl AS SELECT * FROM lookups_2021_pldf")
    con.execute("CREATE UNIQUE INDEX lsoa21cd_lookup_idx ON lsoa_2021_lookup_tbl (lsoa21cd)")
    con.execute("CREATE OR REPLACE TABLE lsoa_2011_lookup_tbl AS SELECT * FROM lookups_2011_pldf")
    # no unique index for 2011 lookups as there are duplicates
    # IMD
    con.execute("CREATE OR REPLACE TABLE imd_lsoa_tbl AS SELECT * FROM lsoa_imd_df")
    con.execute("CREATE UNIQUE INDEX lsoa11cd_imd_idx ON imd_lsoa_tbl (lsoa11cd)")
    # LSOA POLYS
    
    con.execute(f"""
                CREATE OR REPLACE TABLE
                lsoa_poly_2021_tbl AS
                SELECT *,
                    ST_GeomFromText(geometry::VARCHAR) as geom
                FROM read_parquet("{path_2021_poly_parquet}");
                """)

    con.execute(f"""
                CREATE OR REPLACE TABLE
                lsoa_poly_2011_tbl AS
                SELECT *,
                    ST_GeomFromText(geometry::VARCHAR) as geom
                FROM read_parquet("{path_2011_poly_parquet}");
                """)
    # INDEXES
    con.sql("CREATE INDEX lsoa21cd_geom_idx ON lsoa_poly_2021_tbl (geom)")
    con.sql("CREATE INDEX lsoa11cd_geom_idx ON lsoa_poly_2011_tbl (geom)")

    con.execute('CREATE UNIQUE INDEX lsoa21cd_poly_idx ON lsoa_poly_2021_tbl (lsoa21cd)')
    con.execute('CREATE UNIQUE INDEX lsoa11cd_poly_idx ON lsoa_poly_2011_tbl (lsoa11cd)')
    # TENURE
    con.execute('CREATE OR REPLACE TABLE tenure_tbl AS SELECT * FROM tenure_df')
    con.execute('CREATE UNIQUE INDEX lsoacd_tenure_idx ON tenure_tbl (lsoa21cd)')
    # POSTCODES (Index only as import outside this block)
    con.execute('CREATE UNIQUE INDEX postcode_centroids_idx ON postcodes_tbl (pcds)')
    con.execute("ALTER TABLE postcodes_tbl ADD COLUMN geom GEOMETRY")
    con.execute("UPDATE postcodes_tbl SET geom = ST_Point(long, lat)")
    # CA LA lookups
    con.execute('CREATE OR REPLACE TABLE ca_la_tbl AS SELECT * FROM ca_la_df')
    # NEED TO CREATE INDEXES FOR EPC TABLES
    con.execute('CREATE UNIQUE INDEX lmk_key_dom_idx ON epc_domestic_tbl (LMK_KEY)')
    con.execute('CREATE UNIQUE INDEX lmk_key_nondom_idx ON epc_nondom_tbl (LMK_KEY)')
    con.execute('CREATE OR REPLACE TABLE ca_la_dft_lookup_tbl AS SELECT * FROM ca_la_dft_lookup_df')
    con.execute('CREATE UNIQUE INDEX ca_la_dft_lookup_idx ON ca_la_dft_lookup_tbl (ladcd)')
    con.execute("COMMIT;")
except Exception as e:
    # If an error occurs duckdb rolls back automatically
    print(f"Transaction rolled back due to an error: {e}")

#%%

#%%
con.sql("SHOW TABLES;")
#%%
con.sql("DESCRIBE postcodes_tbl;")

#%%

con.sql("DESCRIBE epc_domestic_tbl;")
#%%
con.sql("EXPORT DATABASE 'data/db_export' (FORMAT PARQUET);")
#%%
con.close()
#%%

bulk_files = glob.glob('data/epc_bulk_zips/*.*')

for file in bulk_files:
    get_ca.delete_file(file)
#%%
    
con.sql("SHOW TABLES;")
#%%
#%%
# query to partially create the epc domestic
# view which removes duplicates
# and extracts the lodgement date parts 
# and the nominal construction year

qry_create_epc_domestic_vw = """
CREATE OR REPLACE VIEW epc_domestic_vw AS
SELECT 
    c.*,
    -- Clean the construction age band to produce a nominal construction year
    CAST(
        CASE 
            WHEN CONSTRUCTION_AGE_BAND IS NULL THEN NULL
            WHEN REGEXP_REPLACE(CONSTRUCTION_AGE_BAND, '[A-Za-z\\s:!]', '', 'g') = '' THEN NULL
            WHEN LOWER(CONSTRUCTION_AGE_BAND) LIKE '%before%' THEN 1899
            WHEN REGEXP_REPLACE(CONSTRUCTION_AGE_BAND, '[A-Za-z\\s:!]', '', 'g') LIKE '%-%' 
            THEN (
                CAST(SPLIT_PART(REGEXP_REPLACE(CONSTRUCTION_AGE_BAND, '[A-Za-z\\s:!]', '', 'g'), '-', 1) AS INTEGER) +
                CAST(SPLIT_PART(REGEXP_REPLACE(CONSTRUCTION_AGE_BAND, '[A-Za-z\\s:!]', '', 'g'), '-', 2) AS INTEGER)
            ) / 2
            ELSE CAST(REGEXP_REPLACE(CONSTRUCTION_AGE_BAND, '[A-Za-z\\s:!]', '', 'g') AS INTEGER)
        END AS INTEGER
    ) AS NOMINAL_CONSTRUCTION_YEAR,
    -- Clean the construction age band to produce a construction epoch
    CASE WHEN CAST(NOMINAL_CONSTRUCTION_YEAR AS INTEGER) < 1900 THEN 'Before 1900'
         WHEN (CAST(NOMINAL_CONSTRUCTION_YEAR AS INTEGER) >= 1900) AND (CAST(NOMINAL_CONSTRUCTION_YEAR AS INTEGER) <= 1930) THEN '1900 - 1930'
         WHEN CAST(NOMINAL_CONSTRUCTION_YEAR AS INTEGER) > 1930 THEN '1930 to present'
         ELSE 'Unknown' END AS CONSTRUCTION_EPOCH,
    -- Extract the day, month, and year from the lodgement datetime
    date_part('day', c.LODGEMENT_DATETIME)
        AS LODGEMENT_DAY,
    date_part('month', c.LODGEMENT_DATETIME)
        AS LODGEMENT_MONTH,
    date_part('year', c.LODGEMENT_DATETIME)
        AS LODGEMENT_YEAR,
        -- Select the postcodes table columns
    postcodes_tbl.lsoa21,
    postcodes_tbl.msoa21,
    postcodes_tbl.lsoa11,
    postcodes_tbl.msoa11,
    postcodes_tbl.lat,
    postcodes_tbl.long,
    postcodes_tbl.osnrth1m,
    postcodes_tbl.oseast1m,
    ST_AsText(postcodes_tbl.geom) as geom_text,
    postcodes_tbl.imd,
    postcodes_tbl.pcds,
    tenure_tbl.*,
    imd_lsoa_tbl.*
FROM epc_domestic_tbl c
-- Join the certificates table with the latest certificates for each UPRN
-- This is to ensure that we only have the latest certificate for each UPRN
INNER JOIN (
    SELECT UPRN, MAX(LODGEMENT_DATETIME) as max_date
    FROM epc_domestic_tbl
    GROUP BY UPRN
) latest ON c.UPRN = latest.UPRN 
    AND c.LODGEMENT_DATETIME = latest.max_date

LEFT JOIN postcodes_tbl ON c.POSTCODE = postcodes_tbl.pcds

LEFT JOIN tenure_tbl ON postcodes_tbl.lsoa21 = tenure_tbl.lsoa21cd

LEFT JOIN imd_lsoa_tbl ON postcodes_tbl.lsoa11 = imd_lsoa_tbl.lsoa11cd;
"""
con.execute("INSTALL SPATIAL;")
con.execute("LOAD SPATIAL;")
con.sql(qry_create_epc_domestic_vw)
#%%

# query to create the epc domestic view for export to ODS
# this view subsets the data to the West of England Combined Authority

qry_create_epc_domestic_lep_vw = """
CREATE OR REPLACE VIEW epc_domestic_lep_vw AS
SELECT 
    c.*,
    -- Clean the construction age band to produce a nominal construction year
    CAST(
        CASE 
            WHEN CONSTRUCTION_AGE_BAND IS NULL THEN NULL
            WHEN REGEXP_REPLACE(CONSTRUCTION_AGE_BAND, '[A-Za-z\\s:!]', '', 'g') = '' THEN NULL
            WHEN LOWER(CONSTRUCTION_AGE_BAND) LIKE '%before%' THEN 1899
            WHEN REGEXP_REPLACE(CONSTRUCTION_AGE_BAND, '[A-Za-z\\s:!]', '', 'g') LIKE '%-%' 
            THEN (
                CAST(SPLIT_PART(REGEXP_REPLACE(CONSTRUCTION_AGE_BAND, '[A-Za-z\\s:!]', '', 'g'), '-', 1) AS INTEGER) +
                CAST(SPLIT_PART(REGEXP_REPLACE(CONSTRUCTION_AGE_BAND, '[A-Za-z\\s:!]', '', 'g'), '-', 2) AS INTEGER)
            ) / 2
            ELSE CAST(REGEXP_REPLACE(CONSTRUCTION_AGE_BAND, '[A-Za-z\\s:!]', '', 'g') AS INTEGER)
        END AS INTEGER
    ) AS NOMINAL_CONSTRUCTION_YEAR,
    -- Clean the construction age band to produce a construction epoch
    CASE WHEN CAST(NOMINAL_CONSTRUCTION_YEAR AS INTEGER) < 1900 THEN 'Before 1900'
         WHEN (CAST(NOMINAL_CONSTRUCTION_YEAR AS INTEGER) >= 1900) AND (CAST(NOMINAL_CONSTRUCTION_YEAR AS INTEGER) <= 1930) THEN '1900 - 1930'
         WHEN CAST(NOMINAL_CONSTRUCTION_YEAR AS INTEGER) > 1930 THEN '1930 to present'
         ELSE 'Unknown' END AS CONSTRUCTION_EPOCH,
    -- Extract the day, month, and year from the lodgement datetime
    date_part('day', c.LODGEMENT_DATETIME)
        AS LODGEMENT_DAY,
    date_part('month', c.LODGEMENT_DATETIME)
        AS LODGEMENT_MONTH,
    date_part('year', c.LODGEMENT_DATETIME)
        AS LODGEMENT_YEAR,
        -- Select the postcodes table columns
    postcodes_tbl.lsoa21,
    postcodes_tbl.msoa21,
    postcodes_tbl.lsoa11,
    postcodes_tbl.msoa11,
    postcodes_tbl.lat,
    postcodes_tbl.long,
    postcodes_tbl.osnrth1m,
    postcodes_tbl.oseast1m,
    ST_AsText(postcodes_tbl.geom) as geom_text,
    postcodes_tbl.imd,
    postcodes_tbl.pcds,
    tenure_tbl.*,
    imd_lsoa_tbl.*,
    ca_la_tbl.ladnm,
    ca_la_tbl.cauthnm,
    ca_la_tbl.ladcd
FROM epc_domestic_tbl c
-- Join the certificates table with the latest certificates for each UPRN
-- This is to ensure that we only have the latest certificate for each UPRN
INNER JOIN (
    SELECT UPRN, MAX(LODGEMENT_DATETIME) as max_date
    FROM epc_domestic_tbl
    GROUP BY UPRN
) latest ON c.UPRN = latest.UPRN 
    AND c.LODGEMENT_DATETIME = latest.max_date

LEFT JOIN postcodes_tbl ON c.POSTCODE = postcodes_tbl.pcds

LEFT JOIN tenure_tbl ON postcodes_tbl.lsoa21 = tenure_tbl.lsoa21cd

LEFT JOIN imd_lsoa_tbl ON postcodes_tbl.lsoa11 = imd_lsoa_tbl.lsoa11cd

LEFT JOIN ca_la_tbl 
        ON ca_la_tbl.ladcd = c.LOCAL_AUTHORITY

 WHERE local_authority IN 
        (SELECT ladcd
        FROM ca_la_tbl
        WHERE cauthnm = \'West of England\');
"""
con.execute("INSTALL SPATIAL;")
con.execute("LOAD SPATIAL;")

con.sql(qry_create_epc_domestic_lep_vw)

#%%


#%%
con.sql("SUMMARIZE epc_domestic_lep_vw;")

#%%

con.sql("SHOW TABLES;")

#%%
con.sql("SELECT COUNT(*) FROM epc_nondom_tbl;")

#%%
con.sql("FROM postcodes_tbl LIMIT 10;").pl().glimpse()

#%%
con.sql("DESCRIBE epc_nondom_tbl;")
#%%
create_epc_non_domestic_view_qry = """
 CREATE OR REPLACE VIEW epc_non_domestic_ods_vw AS
        SELECT
                c.*,
                ca_la_tbl.*,
                p.lsoa21,
                p.lat,
                p.long,
    date_part('day', c.LODGEMENT_DATETIME)
        AS LODGEMENT_DAY,
    date_part('month', c.LODGEMENT_DATETIME)
        AS LODGEMENT_MONTH,
    date_part('year', c.LODGEMENT_DATETIME)
        AS LODGEMENT_YEAR
        FROM epc_nondom_tbl c
-- Join the certificates table with the latest certificates for each UPRN
-- This is to ensure that we only have the latest certificate for each UPRN
INNER JOIN (
    SELECT UPRN, MAX(LODGEMENT_DATETIME) as max_date
    FROM epc_nondom_tbl
    GROUP BY UPRN
) latest ON c.UPRN = latest.UPRN 
    AND c.LODGEMENT_DATETIME = latest.max_date

INNER JOIN
        ca_la_tbl 
        ON c.LOCAL_AUTHORITY = ca_la_tbl.ladcd
INNER JOIN 
        
        (SELECT pcds, lsoa21, lat, long 
        FROM postcodes_tbl) as p
        ON c.POSTCODE = p.pcds

WHERE c.LOCAL_AUTHORITY IN 
        (SELECT ladcd
        FROM ca_la_tbl
        WHERE ca_la_tbl.cauthnm = \'West of England\');
"""
con.sql(create_epc_non_domestic_view_qry)
# %%
con.sql("SHOW TABLES;")
# %%
con.sql("FROM epc_non_domestic_ods_vw LIMIT 5;")

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

# %%
