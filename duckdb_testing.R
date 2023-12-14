pacman::p_load(tidyverse,
               fastverse,
               janitor,
               glue,
               duckdb
               )

con <- dbConnect(duckdb(), dbdir = "data/ca_epc.duckdb")

con %>% 
  dbListTables()

ca_la_tbl <- con %>% 
  tbl("ca_la_tbl") %>% 
  filter(cauthnm == "West of England") %>% 
  collect()

imd_tbl <- con %>% 
  tbl("imd_tbl") %>% 
  glimpse()

woe_las <- ca_la_tbl$ladcd # includes north somerset

woe_epc_clean_tbl <- con %>% 
  tbl("epc_clean_tbl") %>% 
  filter(local_authority %in% woe_las) #%>% # you can't include a function or
                                            # subset column as filter criteria

woe_postcodes <- con %>% 
  tbl("postcodes_tbl") %>% 
  filter(ladcd %in% woe_las) 

woe_postcodes$ladnm %>% unique()


epc_imd_tbl <- woe_epc_clean_tbl %>% 
  inner_join(woe_postcodes, by = join_by(postcode == pcds)) %>% 
  inner_join(imd_tbl, by = join_by(lsoacd == lsoacd)) %>% 
  collect()


con %>% dbDisconnect()
con
