pacman::p_load(tidyverse, # Data wrangling
               fastverse,
               glue, # Manipulating strings
               janitor, # general data cleaning
               duckdb,
               sf,
               fs,
               DBI
               
)

con <- DBI::dbConnect(duckdb(), dbdir = "data/ca_epc.duckdb")

props = function(data, ...) {
  data %>%
    count(...) %>%
    mutate(prop = n / sum(n), .keep="unused", by=...)
}

freq_table <- function(x, 
                       ...) {
  x %>% 
    group_by(...) %>% 
    summarise(n = n()) %>% 
    mutate(freq = n /sum(n)) %>% 
    ungroup()
}

dbListTables(con)

lep_codes <- tbl(con, "ca_la_tbl") %>% 
  filter(cauthnm == "West of England") %>% 
  collect() %>% 
  pull(ladcd)

lep_epc_domestic_tbl <- tbl(con, "epc_domestic_tbl") %>% 
  filter(local_authority %in% lep_codes) %>% 
  left_join(tbl(con, "postcodes_tbl"), 
            by = join_by(postcode == postcode)) %>%
  left_join(tbl(con, "imd_tbl"), by = join_by(lsoacd == lsoacd)) %>% 
  left_join(tbl(con, "ca_tenure_lsoa_tbl"), by = join_by(lsoacd == lsoacd)) %>% 
  collect()

lep_epc_domestic_tbl %>% 
  freq_table(lsoacd, property_type, tenure, imd) 


lep_epc_domestic_tbl %>% glimpse()


