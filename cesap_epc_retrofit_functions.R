pacman::p_load(tidyverse,
               fastverse,
               janitor,
               glue,
               config,
               gghighlight,
               paletteer,
               toOrdinal,
               ggtext)

# compare the energy efficiency of the housing stock
# between CA's
# using IMD by some measure of deprivation
# (because we are aiming for a just transition)
epc_tbl <- read_csv('data/clean_epc.csv')


make_postcodes_list <- function(epc_tbl, chunksize = 100){
  epc_tbl %>%
    distinct(postcode) %>% 
    pull() %>% 
    split(ceiling(seq_along(.)/chunksize))
  
}

# get a response for a vector of postcodes
get_postcode_response <- function(pcs, pc_url){
  
  response_pc <- request(pc_url) %>% 
    req_headers(Accept = "application/json") %>% 
    req_body_json(list(postcodes = pcs)) %>% 
    req_perform()
  
  return(response_pc)
  Sys.sleep(0.05)
  
}

get_postcode_result <- function(response_pc){
  
  pc_out <- resp_body_string(response_pc) %>%
    fromJSON() %>%
    pluck("result", "result") %>%
    as_tibble()
  
  return(pc_out)
}

join_epc_postcode_tbl <- function(epc_tbl, postcodes_tbl){
  # joins EPC table and postcodes table, converting to a sf object
  epc_tbl %>% 
    left_join(postcodes_tbl, by = join_by(postcode == postcode)) %>% 
    filter(!is.na(latitude) | !is.na(longitude)) %>% 
    st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>% 
    return()
  
}