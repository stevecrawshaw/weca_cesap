# %%

# use .venv - make sure to restart jupyter kernel. duckdb need python 3.10
import polars as pl
import requests
import yaml
import pyarrow
import duckdb
import get_ca_data as get_ca # functions for retrieving CA \ common data
from pathlib import Path
import os

# %%
def read_api_key():
    with open(os.path.join(os.pardir, 'config.yml'), 'r') as file:
        config = yaml.safe_load(file)
        return config['epc']['auth_token']

# %%

ca_la_df = get_ca.get_ca_la_df(2023, inc_ns=True) # include NS
# ca_la_df.glimpse()

# %%
# retrieve the epc certs from the source csv s
la_list = (ca_la_df['ladcd']) #includes north somerset
f'There are {str(la_list.shape)[1:3]} Local Authorities in Combined Authorities'
# %%
base_zip_url = 'https://epc.opendatacommunities.org/files/domestic-'

# E06000023-Bristol-City-of.zip
#%%

zipfiles_list = [base_zip_url + row['ladcd'] + '-' + row['ladnm'] + '.zip' for row in ca_la_df.iter_rows(named = True)]
zipfiles_list
# %%
def replace_with_hyphen(text):
    return text.replace(' ', '-').replace(',', '')

# %%
zipfiles_urls = [replace_with_hyphen(zipfile) for zipfile in zipfiles_list]

# %%
token = read_api_key()
zipfiles_urls

# %%
token
# %%

# %%
headers = {
    'Accept': 'application/zip',
    'Authorization': f'Basic {token}'
        }

# %%

response = requests.get(zipfiles_urls[0], headers = headers)

response.status_code

# %%
def download_files(url_list, token = token):
 
    headers = {
    'Accept': 'application/zip',
    'Authorization': f'Basic {token}'
        }
    for url in url_list:
        response = requests.get(url, headers = headers)
        filename = 'data/' + url.split("/")[-1]

        with open(filename, 'wb') as file:
            file.write(response.content)

# %%

download_files(zipfiles_urls[0:2])
# %%
