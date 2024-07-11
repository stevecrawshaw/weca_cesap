#%%
import polars as pl
import get_ca_data as get_ca # functions for retrieving CA \ common data
from pathlib import Path
import requests
import os
import yaml
from tqdm import tqdm
import requests

#%%

epc_key = yaml.safe_load(open('../config.yml'))['epc']['auth_token']

#%%

ca_la_df = get_ca.get_ca_la_df(2023, inc_ns=True) # include NS
la_list = (ca_la_df['ladcd'])


# %%
def get_epc_csv(la: str, epc_key: str):
    '''
    Uses the opendatacommunities API to get EPC data for a given local authority and writes it to a CSV file
    the epc auth_token is in the config.yml file in parent directory
    This function uses pagination cribbed from https://epc.opendatacommunities.org/docs/api/domestic#domestic-pagination
    '''
    # Page size (max 5000)
    query_size = 5000
    # Output file name
    output_file = f'data/epc_csv/epc_{la}.csv'

    # Base url and example query parameters
    base_url = 'https://epc.opendatacommunities.org/api/v1/domestic/search'
    query_params = {'size': query_size, 'local-authority': la}

    # Set up authentication
    headers = {
        'Accept': 'text/csv',
        'Authorization': f'Basic {epc_key}'
    }

    # Keep track of whether we have made at least one request for CSV headers and search-after
    first_request = True
    # Keep track of search-after from previous request
    search_after = None
    # Loop over entries in query blocks of up to 5000 to write all the data into a file

    with open(output_file, 'w') as file:
        # Perform at least one request; if there's no search_after, there are no further results
        while search_after != None or first_request:
            # Only set search-after if this isn't the first request
            if not first_request:
                query_params["search-after"] = search_after

            # Make the request
            response = requests.get(base_url, headers=headers, params=query_params)
            response.raise_for_status()
            body = response.text
            search_after = response.headers.get('X-Next-Search-After')

            # For CSV data, only keep the header row from the first response
            if not first_request and body:
                body = body.split("\n", 1)[1]

            # Write received data
            file.write(body)
            print(f"Written {len(body)} bytes to {output_file}")

            first_request = False
#%%

# for la in la_list:
#     get_epc_csv(la, epc_key)

#%%
# schema for the EPC data
cols_schema_adjusted = {
 'lmk-key': pl.Utf8,
 'postcode': pl.Utf8,
 'local-authority': pl.Utf8,
 'property-type': pl.Utf8,
 'lodgement-datetime': pl.Utf8,
 'transaction-type': pl.Utf8,
 'tenure': pl.Utf8,
 'mains-gas-flag': pl.Utf8,
 'hot-water-energy-eff': pl.Utf8,
 'windows-description': pl.Utf8,
 'windows-energy-eff': pl.Utf8,
 'walls-description': pl.Utf8,
 'walls-energy-eff': pl.Utf8,
 'roof-description': pl.Utf8,
 'roof-energy-eff': pl.Utf8,
 'mainheat-description': pl.Utf8,
 'mainheat-energy-eff': pl.Utf8,
 'mainheat-env-eff': pl.Utf8,
 'main-heating-controls': pl.Utf8,
 'mainheatcont-description': pl.Utf8,
 'mainheatc-energy-eff': pl.Utf8,
 'main-fuel': pl.Utf8,
 'solar-water-heating-flag': pl.Utf8,
 'construction-age-band': pl.Utf8,
 'current-energy-rating': pl.Utf8,
 'potential-energy-rating': pl.Utf8,
 'current-energy-efficiency': pl.Utf8,
 'potential-energy-efficiency': pl.Utf8,
 'built-form': pl.Utf8,
 'constituency': pl.Utf8,
 'floor-description': pl.Utf8,
 'environment-impact-current': pl.Int64,
 'environment-impact-potential': pl.Int64,
 'energy-consumption-current': pl.Int64,
 'energy-consumption-potential': pl.Int64,
 'co2-emiss-curr-per-floor-area': pl.Int64,
 'co2-emissions-current': pl.Float64,
 'co2-emissions-potential': pl.Float64,
 'lighting-cost-current': pl.Int64,
 'lighting-cost-potential': pl.Int64,
 'heating-cost-current': pl.Int64,
 'heating-cost-potential': pl.Int64,
 'hot-water-cost-current': pl.Int64,
 'hot-water-cost-potential': pl.Int64,
 'total-floor-area': pl.Float64,
 'number-habitable-rooms': pl.Int64,
 'number-heated-rooms': pl.Int64,
 'photo-supply': pl.Float64,
 'uprn': pl.Int64,
 'building-reference-number': pl.Int64}


#%%

def ingest_dom_certs_csv(la_list: list, cols_schema: dict) -> pl.DataFrame:
    """
    Loop through all csv files in the epc_csv folder and ingest them into a single DF. Use an optimised polars query to ingest a subset of columns and do some transformations to create a single large DF of EPC data
    """
    all_lazyframes = []
    # rename columns to replace hyphens with underscores
    cols_select_list = list(cols_schema.keys())
    new_cols_names = [col.replace('-', '_') for col in cols_select_list]
    renamed_cols_dict = dict(zip(cols_select_list, new_cols_names))


    for item in la_list:

        file_path = f'data/epc_csv/epc_{item}.csv'

        if os.path.exists(file_path):
            # Optimised query which implements predicate pushdown for each file
            # Polars optimises the query to make it fast and efficient
            q = (
            pl.scan_csv(file_path,
            dtypes = cols_schema,
            encoding='utf8-lossy', # or trips up with odd characters
            ignore_errors=True
            )
            # grouping the data by uprn and selecting the last lodgement date
            # to remove old duplicates
            .select(pl.col(cols_select_list))
            .with_columns(pl.col('lodgement-datetime').str.to_datetime(format='%Y-%m-%d %H:%M:%S', strict=False))
            .sort(pl.col(['uprn', 'lodgement-datetime']))
            .group_by('uprn').last()
            .rename(renamed_cols_dict)
            )
            all_lazyframes.append(q)
    # Concatenate list of lazyframes into one consolidated DF, then collect all at once - FAST
    certs_df = pl.concat(pl.collect_all(all_lazyframes))   # faster than intermediate step             
    return certs_df

#%%
all_dom_epc_raw = ingest_dom_certs_csv(la_list, cols_schema_adjusted)

#%%
all_dom_epc.write_parquet('data/all_domestic_epc.parquet')
#%%
all_dom_epc_raw.glimpse()


#%%
def wrangle_epc(certs_df: pl.DataFrame) -> pl.DataFrame:
  
    """
    Creates date artefacts and changes data types
    """
    wrangled_df = (        
    certs_df
    .with_columns([pl.col('lodgement_datetime')
                   .dt.date()
                   .alias('date')])
    .with_columns([pl.col('date').dt.year().alias('year'),
                   pl.col('date').dt.month().cast(pl.Int16).alias('month'),
                   pl.col('date').cast(pl.Utf8)])
    # .rename(certs_df_names)
    .filter(~pl.col('uprn').is_duplicated()) # some nulls and duplicates (~134) so remove
    .drop('lodgement_datetime'))
    return wrangled_df
# %%

wrangled_certs = wrangle_epc(all_dom_epc_raw)

#%%

wrangled_certs.glimpse()
#%%
lep_codes = (ca_la_df
 .filter(pl.col('ladnm').is_in(['Bristol, City of', 'South Gloucestershire', 'North Somerset', 'Bath and North East Somerset']))
 .select('ladcd')
 ).to_series()

#%%

#check expected number of certificates
(wrangled_certs
 .filter(pl.col('local_authority').is_in(lep_codes))
).shape