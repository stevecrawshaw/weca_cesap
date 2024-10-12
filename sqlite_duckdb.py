#%%

import polars as pl
import duckdb # version 1.1.1
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
    'UR01IND': 'VARCHAR',
    'OAC01': 'VARCHAR',
    'OLDPCT': 'VARCHAR',
    'OLDHRO': 'VARCHAR',
    'CANREG': 'VARCHAR',
    'CANNET': 'VARCHAR',
    'OA11': 'VARCHAR',
    'LSOA11': 'VARCHAR',
    'MSOA11': 'VARCHAR',
    'SICBL': 'VARCHAR',
    'RU11IND': 'VARCHAR',
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

con = duckdb.connect(database='data/postcodes.duckdb', read_only=False)

#%%

query = f"""
CREATE TABLE postcode_centroids_cauth AS 
SELECT PCDS,OSLAUA,OSWARD,OSEAST1M,OSNRTH1M,PCON,LSOA11,MSOA11,LAT,LONG,LEP1,LEP2,IMD,LSOA21,MSOA21 
FROM read_csv(
'data/postcode_centroids.csv',
dtypes={schema_columns},
header=True,      
null_padding=True)
WHERE LSOA21 IN {lsoa_in_cauths};
"""
print(query)
#%%
con.sql(query)
    
#%%

con.sql("SHOW TABLES;")

#%%

con.sql("SELECT COUNT(PCDS) FROM postcode_centroids_cauth;")

# %%

#%%
#%%
# polars schema
schema = {
    "OBJECTID": pl.Int64,
    "PCD": pl.Utf8,
    "PCD2": pl.Utf8,
    "PCDS": pl.Utf8,
    "DOINTR": pl.Int64,
    "DOTERM": pl.Int64,
    "OSCTY": pl.Utf8,
    "CED": pl.Utf8,
    "OSLAUA": pl.Utf8,
    "OSWARD": pl.Utf8,
    "PARISH": pl.Utf8,
    "USERTYPE": pl.Int64,
    "OSEAST1M": pl.Int64,
    "OSNRTH1M": pl.Int64,
    "OSEAST100M": pl.Int64,
    "OSNRTH100M": pl.Int64,
    "OSGRDIND": pl.Int64,
    "OSHLTHAU": pl.Utf8,
    "HRO": pl.Utf8,
    "CTRY": pl.Utf8,
    "GENIND": pl.Utf8,  # Nullable
    "PAFIND": pl.Int64,  # Nullable
    "RGN": pl.Utf8,
    "STREG": pl.Int64,
    "PCON": pl.Utf8,
    "EER": pl.Utf8,
    "TECLEC": pl.Utf8,
    "TTWA": pl.Utf8,
    "PCT": pl.Utf8,
    "ITL": pl.Utf8,
    "PSED": pl.Utf8,
    "CENED": pl.Utf8,
    "EDIND": pl.Int64,
    "ADDRCT": pl.Int64,  # Nullable
    "DPCT": pl.Int64,    # Nullable
    "MOCT": pl.Int64,    # Nullable
    "SMLBUSCT": pl.Int64,  # Nullable
    "OSHAPREV": pl.Utf8,
    "LEA": pl.Utf8,
    "OLDHA": pl.Utf8,
    "WARDC91": pl.Utf8,
    "WARDO91": pl.Utf8,
    "WARD98": pl.Utf8,
    "STATSWARD": pl.Utf8,
    "OA01": pl.Utf8,
    "OAIND": pl.Int64,
    "CASWARD": pl.Utf8,
    "PARK": pl.Utf8,
    "LSOA01": pl.Utf8,
    "MSOA01": pl.Utf8,
    "UR01IND": pl.Int64,
    "OAC01": pl.Utf8,
    "OLDPCT": pl.Utf8,
    "OLDHRO": pl.Utf8,
    "CANREG": pl.Utf8,
    "CANNET": pl.Utf8,
    "OA11": pl.Utf8,
    "LSOA11": pl.Utf8,
    "MSOA11": pl.Utf8,
    "SICBL": pl.Utf8,
    "RU11IND": pl.Int64,
    "OAC11": pl.Utf8,
    "WZ11": pl.Utf8,
    "BUA11": pl.Utf8,    # Nullable
    "BUASD11": pl.Utf8,
    "LAT": pl.Float64,
    "LONG": pl.Float64,
    "LEP1": pl.Utf8,
    "LEP2": pl.Utf8,    # Nullable
    "PFA": pl.Utf8,
    "IMD": pl.Int64,
    "CALNCV": pl.Utf8,
    "NHSER": pl.Utf8,
    "ICB": pl.Utf8,
    "OA21": pl.Utf8,
    "LSOA21": pl.Utf8,   # Nullable
    "MSOA21": pl.Utf8,   # Nullable
    "GlobalID": pl.Utf8,
    "x": pl.Int64,
    "y": pl.Int64
}
#%%
# just the columns we need
cols = ['PCDS','OSLAUA','OSWARD','OSEAST1M',
'OSNRTH1M','PCON','LSOA11','MSOA11','LAT',
'LONG','LEP1','LEP2','IMD','LSOA21','MSOA21']
print(cols)
#%%

pc_df_qry = pl.scan_csv('data/postcode_centroids.csv', schema = schema)
# %%

lsoa_df = pl.read_parquet('data/lsoa_poly/lsoa_poly_cauth_2024.parquet')

#%%
lsoa_in_cauths = (lsoa_df
             .select('LSOA21CD')
             .to_series()
             .to_list()
             )

#%%
postcodes_in_cauths = (pc_df_qry
                       .filter(pl.col('LSOA21')
                               .is_in(lsoa_in_cauths))
                        .select(pl.col(cols)
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
