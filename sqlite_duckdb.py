#%%
import polars as pl
import duckdb
import sqlite3
#%%

duckdb.sql("SELECT * FROM 'data/postcode_centroids.csv' LIMIT 10")

#%%

pc_df_qry = pl.scan_csv('data/postcode_centroids.csv', ignore_errors=True)

# %%

lsoa_df = pl.read_parquet('data/lsoa_poly/lsoa_poly_cauth_2024.parquet')

#%%
lsoa_in_cauths = (lsoa_df
             .select('LSOA21CD')
             .to_series()
             )

#%%
postcodes_in_cauths = (pc_df_qry
                       .filter(pl.col('LSOA21')
                               .is_in(lsoa_in_cauths))
                        .select(pl.col(
                            ['PCDS','OSLAUA','OSWARD','OSEAST1M','OSNRTH1M','PCON','LSOA11','MSOA11','LAT','LONG','LEP1','LEP2','IMD','LSOA21','MSOA21'])
                            ).collect()
)

#%%

postcodes_in_cauths.glimpse()


















#%%
duckdb.execute("INSTALL sqlite;")
duckdb.execute("LOAD sqlite;")
#%%
duckdb.execute("INSTALL spatial;")
duckdb.execute("LOAD spatial;")

# %%
duckdb.execute("ATTACH 'data/postcode_centroids.geodatabase' (TYPE SQLITE);")

#%%
duckdb.execute("USE 'postcode_centroids';")


#%%

duckdb.execute("FROM information_schema.tables;")
# %%
