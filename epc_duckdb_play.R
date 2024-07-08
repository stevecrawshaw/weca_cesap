pacman::p_load(duckdb, tidyverse, janitor, glue, arrow)


con <- dbConnect(duckdb(), dbdir = "data/ca_epc.duckdb")
dbListTables(con)

epc_tbl <- con %>% 
  tbl("epc_domestic_tbl") %>%
  colnames()

epc_tbl %>% 
  xfun::raw_string()

epc_tbl %>% 
  collect() %>% 
  glimpse()


epc_from_parquet <- arrow::read_parquet("data/db_export/epc_domestic_tbl.parquet")



