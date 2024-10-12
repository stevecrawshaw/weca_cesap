#%%
import polars as pl
import duckdb
#%%
schema_columns = {
    'OBJECTID': 'BIGINT',
    'PCD': 'VARCHAR',
    'PCD2': 'VARCHAR',
    'PCDS': 'VARCHAR',
    'DOINTR': 'BIGINT',
    'DOTERM': 'BIGINT',
    'OSCTY': 'VARCHAR',
    'CED': 'VARCHAR',
    'OSLAUA': 'VARCHAR',
    'OSWARD': 'VARCHAR',
    'PARISH': 'VARCHAR',
    'USERTYPE': 'BIGINT',
    'OSEAST1M': 'BIGINT',
    'OSNRTH1M': 'BIGINT',
    'OSEAST100M': 'BIGINT',
    'OSNRTH100M': 'BIGINT',
    'OSGRDIND': 'BIGINT',
    'OSHLTHAU': 'VARCHAR',
    'HRO': 'VARCHAR',
    'CTRY': 'VARCHAR',
    'GENIND': 'VARCHAR',
    'PAFIND': 'BIGINT',
    'RGN': 'VARCHAR',
    'STREG': 'BIGINT',
    'PCON': 'VARCHAR',
    'EER': 'VARCHAR',
    'TECLEC': 'VARCHAR',
    'TTWA': 'VARCHAR',
    'PCT': 'VARCHAR',
    'ITL': 'VARCHAR',
    'PSED': 'VARCHAR',
    'CENED': 'VARCHAR',
    'EDIND': 'BIGINT',
    'ADDRCT': 'BIGINT',
    'DPCT': 'BIGINT',
    'MOCT': 'BIGINT',
    'SMLBUSCT': 'BIGINT',
    'OSHAPREV': 'VARCHAR',
    'LEA': 'VARCHAR',
    'OLDHA': 'VARCHAR',
    'WARDC91': 'VARCHAR',
    'WARDO91': 'VARCHAR',
    'WARD98': 'VARCHAR',
    'STATSWARD': 'VARCHAR',
    'OA01': 'VARCHAR',
    'OAIND': 'BIGINT',
    'CASWARD': 'VARCHAR',
    'PARK': 'VARCHAR',
    'LSOA01': 'VARCHAR',
    'MSOA01': 'VARCHAR',
    'UR01IND': 'BIGINT',
    'OAC01': 'VARCHAR',
    'OLDPCT': 'VARCHAR',
    'OLDHRO': 'VARCHAR',
    'CANREG': 'VARCHAR',
    'CANNET': 'VARCHAR',
    'OA11': 'VARCHAR',
    'LSOA11': 'VARCHAR',
    'MSOA11': 'VARCHAR',
    'SICBL': 'VARCHAR',
    'RU11IND': 'BIGINT',
    'OAC11': 'VARCHAR',
    'WZ11': 'VARCHAR',
    'BUA11': 'VARCHAR',
    'BUASD11': 'VARCHAR',
    'LAT': 'DOUBLE',
    'LONG': 'DOUBLE',
    'LEP1': 'VARCHAR',
    'LEP2': 'VARCHAR',
    'PFA': 'VARCHAR',
    'IMD': 'BIGINT',
    'CALNCV': 'VARCHAR',
    'NHSER': 'VARCHAR',
    'ICB': 'VARCHAR',
    'OA21': 'VARCHAR',
    'LSOA21': 'VARCHAR',
    'MSOA21': 'VARCHAR',
    'GlobalID': 'VARCHAR',
    'x': 'BIGINT',
    'y': 'BIGINT'
}
#%%
conn = duckdb.connect(database=':memory:', read_only=False)
#%%
qry = f"""
SELECT *
    FROM read_csv(
        'data/postcode_centroids.csv',
        columns = {schema_columns}
    )
"""


#%%
df = conn.execute(qry).pl()
#%%

duckdb.read_csv("data/postcode_centroids.csv", dtype = schema_columns)

#%%

test_df = pl.read_csv('data/postcode_centroids.csv', n_rows=10)

test_df.glimpse()
#%%

schema = {
    "PCDS": pl.Utf8,
    "OSLAUA": pl.Utf8,
    "OSWARD": pl.Utf8,
    "OSEAST1M": pl.Int64,
    "OSNRTH1M": pl.Int64,
    "PCON": pl.Utf8,
    "LSOA11": pl.Utf8,
    "MSOA11": pl.Utf8,
    "LAT": pl.Float64,
    "LONG": pl.Float64,
    "LEP1": pl.Utf8,
    "LEP2": pl.Utf8,  # None is represented as Null in Polars, so Utf8 is appropriate
    "IMD": pl.Int64,
    "LSOA21": pl.Utf8,
    "MSOA21": pl.Utf8,
}

cols = list(schema.keys())
print(cols)
#%%

pc_df_qry = pl.read_csv('data/postcode_centroids.csv',columns = cols)
pc_df_qry.glimpse()
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
