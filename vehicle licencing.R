pacman::p_load(fastverse, tidyverse, janitor, glue, duckdb, timetk, arrow)
# cols ----
cols <- cols(
  LSOA11CD = col_character(),
  LSOA11NM = col_character(),
  BodyType = col_character(),
  Keepership = col_character(),
  LicenceStatus = col_character(),
  `2023 Q4` = col_integer(),
  `2023 Q3` = col_integer(),
  `2023 Q2` = col_integer(),
  `2023 Q1` = col_integer(),
  `2022 Q4` = col_integer(),
  `2022 Q3` = col_integer(),
  `2022 Q2` = col_integer(),
  `2022 Q1` = col_integer(),
  `2021 Q4` = col_integer(),
  `2021 Q3` = col_integer(),
  `2021 Q2` = col_integer(),
  `2021 Q1` = col_integer(),
  `2020 Q4` = col_integer(),
  `2020 Q3` = col_integer(),
  `2020 Q2` = col_integer(),
  `2020 Q1` = col_integer(),
  `2019 Q4` = col_integer(),
  `2019 Q3` = col_integer(),
  `2019 Q2` = col_integer(),
  `2019 Q1` = col_integer(),
  `2018 Q4` = col_integer(),
  `2018 Q3` = col_integer(),
  `2018 Q2` = col_integer(),
  `2018 Q1` = col_integer(),
  `2017 Q4` = col_integer(),
  `2017 Q3` = col_integer(),
  `2017 Q2` = col_integer(),
  `2017 Q1` = col_integer(),
  `2016 Q4` = col_integer(),
  `2016 Q3` = col_integer(),
  `2016 Q2` = col_integer(),
  `2016 Q1` = col_integer(),
  `2015 Q4` = col_integer(),
  `2015 Q3` = col_integer(),
  `2015 Q2` = col_integer(),
  `2015 Q1` = col_integer(),
  `2014 Q4` = col_integer(),
  `2014 Q3` = col_integer(),
  `2014 Q2` = col_integer(),
  `2014 Q1` = col_integer(),
  `2013 Q4` = col_integer(),
  `2013 Q3` = col_integer(),
  `2013 Q2` = col_integer(),
  `2013 Q1` = col_integer(),
  `2012 Q4` = col_integer(),
  `2012 Q3` = col_integer(),
  `2012 Q2` = col_integer(),
  `2012 Q1` = col_integer(),
  `2011 Q4` = col_integer(),
  `2011 Q3` = col_integer(),
  `2011 Q2` = col_integer(),
  `2011 Q1` = col_integer(),
  `2010 Q4` = col_integer(),
  `2010 Q3` = col_integer(),
  `2010 Q2` = col_integer(),
  `2010 Q1` = col_integer(),
  `2009 Q4` = col_integer()
)

# ingest ----

# read in the vehicle data with integer values
veh0125_raw_tbl <- read_csv("data/df_veh0125.csv", col_types = cols)

veh0125_raw_tbl %>% glimpse()
# get the la's in ca's
con <- dbConnect(duckdb(), dbdir = "data/ca_epc.duckdb")
dbListTables(con)

ca_la_tbl <- tbl(con, "ca_la_tbl") %>% collect()
ca_ladcds <- ca_la_tbl$ladcd

ca_lsoa_tbl <- tbl(con, "postcode_centroids_tbl") %>% 
  filter(laua %in% ca_ladcds) %>% 
  distinct(lsoa11, ladcd = laua) %>%
  collect() 

weca_ladcds <- ca_la_tbl %>%
  filter(cauthnm == "West of England") %>% 
  pull(ladcd)

weca_lsoa_cds <- ca_lsoa_tbl %>%
  filter(ladcd %in% weca_ladcds) %>% 
  pull(lsoa11)

all_ca_lsoa_long_tbl <- veh0125_raw_tbl %>% 
  filter(LSOA11CD %in% ca_lsoa_tbl$lsoa11) %>% 
  rename_with(\(x) str_replace(x, "Q", "")) %>% 
  pivot_longer(cols = starts_with("2"), values_to =  "count") %>%
  mutate(date = parse_date_time(name, orders = "Y q"),
         name = NULL) %>%
  glimpse()
  
weca_lsoa_long_tbl <- all_ca_lsoa_long_tbl %>%
  filter(LSOA11CD %in% weca_lsoa_cds,
         date >= as.Date("2019-01-01"))
  
weca_lsoa_long_tbl %>% write_csv("data/veh_licence_weca_0125.csv")

weca_lsoa_long_tbl %>% write_parquet("data/veh_licence_weca_0125.parquet")

all_ca_lsoa_long_tbl %>% write_parquet("data/veh_licence_all_ca_0125.parquet")

test <- read_parquet("data/veh_licence_weca_0125.parquet")


identical(weca_lsoa_long_tbl, test)

