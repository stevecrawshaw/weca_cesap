pacman::p_load(tidyverse, # Data wrangling
               glue, # Manipulating strings
               janitor, # general data cleaning
               httr2, # API wrangling
               jsonlite, # handling API responses
               sf, # Spatial data frames and ops
               config # securely store API parameters
) 

cf <- "../config.yml"
apikey <- config::get(value = 'apikey', cf, config = 'epc')
email <-  config::get(value = 'email', cf, config = 'epc')
email
domsearch_endpoint <-  config::get(value = 'domsearch_endpoint', cf, config = 'epc')
pc_url <- config::get(value = 'pc_url', cf, config = 'postcode')

# Testing Functions ----

ca_la_tbl <- read_csv('data/ca_la_tbl.csv') %>% 
  clean_names()

lep_vec <- ca_la_tbl$lad22cd

epc_domestic_tbl <- make_param_tbl(start_year = 2008, end_year = 2023, lep_vec = lep_vec) %>% 
  mutate(endpoint = domsearch_endpoint) %>% 
  pmap(.f = read_epc_api,
       .progress = TRUE) %>% 
  make_epc_table(col_types_domestic_fnc())

# check table meets our expectations
epc_domestic_tbl %>% glimpse()