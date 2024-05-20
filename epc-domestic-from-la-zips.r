pacman::p_load(tidyverse,
               janitor,
               config,
               httr2,
               glue, jsonlite)

token = config::get("auth_token", config = "epc", file = "../config.yml")

headers = list(
  'Accept' =  'application/zip',
  'Authorization' =  glue('Basic {token}')
)


# combined and local authorities

ons_la_ca_url <- "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LAD23_CAUTH23_EN_LU/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json"

ca_la_list <- jsonlite::read_json(ons_la_ca_url)

ca_la_tbl <-  ca_la_list %>% 
  pluck("features") %>% 
  enframe() %>% 
  unnest_wider(value) %>% 
  unnest_wider(attributes) %>% 
  rename_with(tolower)


base_zip_url = 'https://epc.opendatacommunities.org/files/domestic-'

# E06000023-Bristol-City-of.zip
#%%

zip_urls <- ca_la_tbl %>% 
  mutate(zip_url = glue("{base_zip_url}{lad23cd}-{lad23nm}.zip") %>% 
           str_remove_all( ",") %>% 
           str_replace_all(" ", "-")) %>% 
  pull(zip_url)

test_bris <- zip_urls[map_lgl(zip_urls, ~grepl("Bristol", .x))]

r <- request(test_bris) %>% 
  req_headers('Accept' =  'application/zip',
              'Authorization' =  glue('Basic {token}')) %>% 
  req_perform()

lobstr::obj_size(r)

resp_status(r)

resp_body_string(r)


download.file(test_bris, destfile = "data/test_bris.zip")
