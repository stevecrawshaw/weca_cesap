# %%
# use .venv - make sure to restart jupyter kernel.
import polars as pl
import duckdb
from epc_schema import postcodes_schema

# %%
con = duckdb.connect("data/ca_epc.duckdb")

#%%
postcode_local_path = "data/ONSPD_NOV_2023_UK.csv"

#%%

ps_pldf = pl.read_csv(postcode_local_path,
                      schema_overrides={"ru11ind": pl.Utf8,
                                        'ur01ind': pl.Utf8})
#%%
ps_pldf.schema.to_python()
# stick it into clause to get schema

#%%

with open("postcodes_schema.sql", "r") as f:
    con.execute(f.read())


#%%
table = "postcodes_tbl"
schema_cols = postcodes_schema

#%%
con.execute(f"""
INSERT INTO {table}
SELECT * FROM read_csv(?,
                        header=true,
                        auto_detect=false,
                        columns= ?,
                        parallel=true)""",
     [postcode_local_path, schema_cols])


#%%
con.sql("SHOW TABLES;")

#%%
con.sql("SUMMARIZE TABLE postcodes_tbl;")
#%%
con.close()

#%%
