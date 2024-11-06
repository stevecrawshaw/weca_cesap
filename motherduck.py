#%%
import duckdb
import yaml
#%%
with open('../config.yml') as f:
    motherduck_token = yaml.load(f, Loader=yaml.FullLoader)['motherduck']['token']
#%%
con = duckdb.connect(f'md:?motherduck_token={motherduck_token}')
#%%
con.sql("SHOW TABLES;")
#%%
con.close()
#%%