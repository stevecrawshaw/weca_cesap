#%%

import polars as pl
import pyarrow
import duckdb

#%%
con = duckdb.connect('data/epc_dom.duckdb')
#%%

cols_df = pl.read_csv('data/all-domestic-certificates-single-file/columns.csv', has_header=True, low_memory=False)


domestic_schema = (
    cols_df
    .filter(pl.col('filename') == 'certificates.csv')
    .select(pl.col(['column', 'datatype']))
    .with_columns(
        pl.when(pl.col('datatype') == 'string')
        .then(pl.lit('VARCHAR'))
        .when(pl.col('datatype') == 'date')
        .then(pl.lit('DATE'))
        .when(pl.col('datatype') == 'float')
        .then(pl.lit('DOUBLE'))
        .when(pl.col('datatype') == 'decimal')
        .then(pl.lit('DOUBLE'))
        .when(pl.col('datatype') == 'datetime')
        .then(pl.lit('TIMESTAMP'))
        .when(pl.col('datatype') == 'integer')
        .then(pl.lit('BIGINT'))
        .alias('duck_datatype')
    )
    .drop('datatype')
)

#%%
dtype_dict = {row['column']: row['duck_datatype'] for row in domestic_schema.to_dicts()}

#%%
print(dtype_dict)

#%%
domestic_certificates_path = "data/all-domestic-certificates-single-file/certificates.csv"

#%%
con.execute(f'CREATE OR REPLACE TABLE epc_domestic_all AS SELECT * FROM read_csv("{domestic_certificates_path}", columns={dtype_dict})')
# %%
con.close()