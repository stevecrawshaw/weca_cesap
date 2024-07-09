#%%
import polars as pl
import get_ca_data as get_ca # functions for retrieving CA \ common data
from pathlib import Path
import requests
import os
import urllib.request
from urllib.parse import urlencode
import yaml


#%%

epc_key = yaml.safe_load(open('../config.yml'))['epc']['auth_token']

#%%

ca_la_df = get_ca.get_ca_la_df(2023, inc_ns=True) # include NS
la_list = (ca_la_df['ladcd'])

test_la = la_list[0]

#%%
# Page size (max 5000)
query_size = 5000
# Output file name
output_file = 'output.csv'

# Base url and example query parameters
base_url = 'https://epc.opendatacommunities.org/api/v1/domestic/search'
query_params = {'size': query_size, 'local-authority': test_la}

#%%

# Set up authentication
headers = {
	'Accept': 'text/csv',
	'Authorization': f'Basic {epc_key}'
}
headers['Authorization']
#%%
# Keep track of whether we have made at least one request for CSV headers and search-after
first_request = True
# Keep track of search-after from previous request
search_after = None

#%%
# Loop over entries in query blocks of up to 5000 to write all the data into a file
with open(output_file, 'w') as file:
    # Perform at least one request; if there's no search_after, there are no further results
    while search_after != None or first_request:
        # Only set search-after if this isn't the first request
        if not first_request:
            query_params["search-after"] = search_after

        # Set parameters for this query
        encoded_params = urlencode(query_params)
        full_url = f"{base_url}?{encoded_params}"

        # Now make request and extract the data and next search_after
        with urllib.request.urlopen(urllib.request.Request(full_url, headers=headers)) as response:
            response_body = response.read()
            body = response_body.decode()
            search_after = response.getheader('X-Next-Search-After')

        # For CSV data, only keep the header row from the first response
        if not first_request and body != "":
            body = body.split("\n", 1)[1]

        # Write received data
        file.write(body)

        first_request = False



#%%

headers['Authorization']


#%%

