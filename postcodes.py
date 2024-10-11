#%%
import polars as pl
import niquests
import json
from urllib.parse import urlencode, urlunparse
import asyncio
import nest_asyncio
from niquests import AsyncSession, Response, exceptions
#%%
# base_url_lsoa_2021_centroids = f'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LLSOA_Dec_2021_PWC_for_England_and_Wales_2022/FeatureServer/0/query?'
base_url_postcode_centroids = 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/ONSPD_Online_Latest_Centroids/FeatureServer/0/query'
# base_url_2021_lsoa_polys = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW_BFC_V10/FeatureServer/0/query"
# base_url_2011_lsoa_polys = 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LSOA_Dec_2011_Boundaries_Generalised_Clipped_BGC_EW_V3/FeatureServer/0/query'
# base_url_lsoa_2021_lookups = 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LSOA21_WD24_LAD24_EW_LU/FeatureServer/0/query?'
# base_url_lsoa_2011_lookups = 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LSOA01_LSOA11_LAD11_EW_LU_ddfe1cd1c2784c9b991cded95bc915a9/FeatureServer/0/query'
# imd_data_path = 'https://github.com/humaniverse/IMD/raw/master/data-raw/imd_england_lsoa.csv'
# path_2011_poly_parquet = 'data/all_cas_lsoa_poly_2011.parquet'
# path_2021_poly_parquet = 'data/all_cas_lsoa_poly_2021.parquet'
chunk_size = 100 # this is used in a a where clause to set the number of lsoa polys per api call

chunk_size = 1
params_base = {
    'outFields': '*',
    'outSR': 4326,
    'f': 'json'
}

#%%

def get_postcode_pldf(base_url: str)-> pl.DataFrame:
        with niquests.get(base_url,
                          stream = True) as r:
            r.raise_for_status()
            features = r.json().get('features')
            features_df = (
                pl.DataFrame(features)
                .unnest('attributes')
                .drop('GlobalID')
                )
            return features_df


#%%
def make_postcode_lsoa_url(base_url: str,
                  lsoa_code: str,
                  lsoa_code_name: str) -> str:
    """
    Make a url to retrieve postcodes given a lsoa code
    """
    where_params = {'where': f"{lsoa_code_name}='{lsoa_code}'"}
    params_default = {'outFields': '*', 'f': 'json'}
    params = {**params_default, **where_params}
    # use urlencode to avoid making an actual call to get the urls
    query_string = urlencode(params)
    url = urlunparse(('','', base_url, '', query_string, ''))
    return url

#%%

test_lsoa = 'E01012049'

with open("data/lsoas_in_cauths_iter.json", 'r') as f:
    lsoas_in_cauths_iter = json.load(f)

lsoas_in_cauths_iter[0:9]
# %%

postcode_url_list = [make_postcode_lsoa_url(base_url_postcode_centroids,
                                                  lsoa,
                                                  lsoa_code_name='LSOA21')
                                                  for lsoa
                                                  in lsoas_in_cauths_iter[0:1000]]

print(postcode_url_list[0])
                    
#%%
                                         
postcode_pldf_list = [get_postcode_pldf(url) for url in postcode_url_list]

#%%

postcode_pldf = pl.concat(postcode_pldf_list, how='vertical_relaxed')


# %%

# Function to fetch data from the API for a given LSOA code
async def fetch_lsoa_data(session, lsoa_code, timeout=14, retries=3):
    url = (
        "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
        "ONSPD_Online_Latest_Centroids/FeatureServer/0/query"
    )
    params = {
        "outFields": "PCDS,OSLAUA,OSWARD,OSEAST1M,OSNRTH1M,PCON,LSOA11,MSOA11,LAT,LONG,LEP1,LEP2,IMD,LSOA21,MSOA21",
        "f": "json",
        "where": f"LSOA21='{lsoa_code}'"
    }

    attempt = 0
    while attempt < retries:
        try:
            # Make the request and set a timeout
            response = await session.get(url, params=params, stream=True, timeout=timeout)
            
            # Await the JSON parsing as it's an async function
            json_response = await response.json()
            
            # Extract the features from the response
            features = json_response['features']
            
            # Convert the features into a Polars DataFrame
            features_df = (
                pl.DataFrame(features)
                .unnest('attributes')  # Unnest the 'attributes' column
                .drop('GlobalID')      # Drop 'GlobalID' if it's not needed
            )
            
            return features_df
        
        except exceptions.Timeout:
            attempt += 1
            print(f"Request for LSOA {lsoa_code} timed out. Retrying {attempt}/{retries}...")
            if attempt == retries:
                print(f"Request for LSOA {lsoa_code} failed after {retries} attempts.")
                return None
        except Exception as e:
            print(f"An error occurred for LSOA {lsoa_code}: {e}")
            return None
# %%
# Main function to handle multiple requests concurrently
async def fetch_multiple_lsoa_data(lsoa_codes):
    async with AsyncSession(multiplexed=True) as session:
        tasks = [fetch_lsoa_data(session, lsoa_code) for lsoa_code in lsoa_codes]
        
        # Use asyncio.gather with return_exceptions=True to handle exceptions
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out None results (in case of timeouts or errors)
        valid_results = [res for res in results if res is not None]
        
        return valid_results
# %%
# Example usage with multiple LSOA codes

chunk_size = 1000
lsoas_in_cauths_chunks = [lsoas_in_cauths_iter[i:i + chunk_size]
                        for i in range(0,
                                        len(lsoas_in_cauths_iter),
                                        chunk_size)]
# %%

nest_asyncio.apply()

results = [asyncio.run(fetch_multiple_lsoa_data(lsoa_codes)) 
           for lsoa_codes 
           in lsoas_in_cauths_chunks]
    

# %%
l = []
for r_list in results:
    l.append(pl.concat(r_list, how='vertical_relaxed'))
#%%
postcode_pldf = pl.concat(l, how='vertical_relaxed')


# %%
postcode_pldf = pl.concat(results, how='vertical_relaxed')

#%%

postcode_pldf.glimpse()

#%%

postcode_pldf.write_parquet('data/postcode_centroids.parquet')

#%%

results[0].glimpse()