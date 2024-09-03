#%%
from datetime import datetime
import requests
import polars as pl
import duckdb
import json
from pyproj import Transformer
from pathlib import Path
import yaml

#%%
#https://machine.domain.com/webadaptor/rest/services/USA/MapServer/3/query?where=STATE_NAME='California'&outFields=Name,Population&returnGeometry=false&resultOffset=5&resultRecordCount=10&orderByFields=Population&f=pjson
#%%

base_url = f'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LLSOA_Dec_2021_PWC_for_England_and_Wales_2022/FeatureServer/0/query?'
params_base = {
    'where': '1=1',
    'outFields': '*',
    'outSR': 4326,
    'f': 'json'
}

param_rt = {'returnCountOnly': 'true'}

#%%
params = {**params_base, **param_rt}
print(params)
#%%


record_count = requests.get(base_url, params = params).json().get('count')
# %%
range_count = range(0, record_count, 2000)
# %%
range_count = list(range(0, record_count + 1, 2000))
list(range_count)
# %%

# %%
