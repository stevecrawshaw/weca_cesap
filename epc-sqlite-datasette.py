
# %% 

# This is residual code for loading the epc database in to sqlite from duckdb
# Section below is to load data into SQLite.

# %%
# dictionary of all the dfs to be imported to sqlite - for datasette
dfs_dict = {
    'ca_la_tbl':ca_la_df,
    'imd_tbl':imd_df,
    'postcodes_tbl':postcodes_df,
    'epc_domestic_tbl':epc_domestic_df,
    'epc_non_domestic_tbl': epc_non_domestic_df,
    'ca_la_dft_lookup_tbl':ca_la_dft_lookup_df

}

# %%
# %pip install adbc_driver_sqlite

# %%
[df.write_csv(f'data/holding/{table_name}.csv') for table_name, df in dfs_dict.items()]

# %%
def import_dfs(folder_path: str = 'data/holding'):
    # Looping through each CSV file in the folder
    for file in Path(folder_path).glob('*.csv'):
        # Getting the stem (file name without extension) of the file
        stem = file.stem

        # Reading the CSV file into a DataFrame
        df = pl.read_csv(file)

        # Storing the DataFrame in the dictionary with the stem as the key
        globals()[stem] = df
    # return dataframes

# %%
import_dfs() # to save running the import routines

# %%


# %%
get_ca.populate_sqlite(dfs_dict, db_path='data/sqlite/ca_epc.db', overwrite=True)

# %%
get_ca.populate_sqlite(tables_dict, uri)



lsoa_bng_file_path = get_ca.filter_geojson('data/geojson/LLSOA_Dec_2021_PWC_for_England_and_Wales_2022_-7534040603619445107.geojson',
 output_file='data/geojson/ca_lsoa_pwc.geojson',
 property_name = 'LSOA21CD',
 ca_lsoa_codes = ca_lsoa_codes)

# %%
get_ca.reproject(input_bng_file='data/geojson/ca_lsoa_pwc.geojson',
                  output_wgs84_file='data/geojson/ca_lsoa_pwc_wgs84.geojson',
                    lsoa_code='LSOA21CD')

# %%

db = Database('data/ca_epc.db', recreate = True)
db.close()

# %%
# %pip install geojson-to-sqlite

# %%
# import the lsoa PWC file as geojson to the DB setting the primary key to lsoacd
!geojson-to-sqlite data/sqlite/ca_epc.db lsoa_pwc_tbl data/geojson/ca_lsoa_pwc_wgs84.geojson --pk=lsoacd


# %%
db = Database('data/ca_epc.db', recreate = False)
print(db.schema)

# %%
db['imd_tbl'].create(
{
 'lsoacd': str,
 'lsoanm': str,
 'ladcd': str,
 'ladnm': str,
 'imd': int
 },
 pk = 'lsoacd'
)

# %%
db['ca_la_tbl'].create(
{
 'ladcd': str,
 'ladnm': str,
 'cauthcd': str,
 'cauthnm': str
 },
 pk = 'ladcd'
)

# %%
db['postcodes_tbl'].create(
    {"pcds": str,
     "lsoacd": str,
     "msoacd": str,
     "ladcd": str,
     "ladnm": str},
     pk = 'pcds'
     )

# %%
print(db.schema)

# %%
# db['epc_clean_tbl'].add_foreign_key('postcode', 'postcodes_tbl', 'pcds') # too big causes crash
db['postcodes_tbl'].add_foreign_key('lsoacd', 'imd_tbl', 'lsoacd')
db['postcodes_tbl'].add_foreign_key('ladcd', 'ca_la_tbl', 'ladcd')
db['postcodes_tbl'].add_foreign_key('lsoacd', 'lsoa_pwc_tbl', 'lsoacd')
db['imd_tbl'].add_foreign_key('lsoacd', 'lsoa_pwc_tbl', 'lsoacd')

# %%
db['ca_la_tbl'].insert_all(ca_la_tbl_payload)
# db['epc_clean_tbl'].insert_all(epc_clean_tbl_payload)
db['postcodes_tbl'].insert_all(postcodes_tbl_payload)
db['imd_tbl'].insert_all(imd_tbl_payload)


# %%
db.close()


