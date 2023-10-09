pacman::p_load(tidyverse,
               fastverse,
               janitor,
               glue,
               config,
               sf,
               naniar)

# get the lookup for CA to LA

ca_la_tbl <- read_csv('data/ca_la_tbl.csv') %>% 
  clean_names()

# lookup for LSO to postcode
pc_lsoa_lookup_raw <- read_csv('data/PCD_OA_LSOA_MSOA_LAD_FEB19_UK_LU.csv')

# filter just CAs

ca_pc_lsoa_tbl <- pc_lsoa_lookup_raw %>% 
  inner_join(ca_la_tbl, by = join_by(ladnm == lad22nm))

unique(ca_pc_lsoa_tbl$ladnm)

# get all IMD in LSOA for all CAs
# https://geoportal.statistics.gov.uk/datasets/index-of-multiple-deprivation-dec-2019-lookup-in-england/explore
imd_tbl <- read_csv('data/Index_of_Multiple_Deprivation_(Dec_2019)_Lookup_in_England.csv') %>% 
  clean_names()

# the rank of the least deprived LSOA: most_deprived = 1
max_imd <- nrow(imd_tbl)

# postcode lsoa lookup for all CA's: each postcode now has an IMD rank
pc_lsoa_imd_ca_tbl <- ca_pc_lsoa_tbl %>% 
  left_join(imd_tbl, by = join_by(lsoa11cd == lsoa11cd), keep = FALSE) %>% 
  select(pcds, lsoa11cd, msoa11cd, ladcd, ladnm, starts_with("cauth"), imd19)

pc_lsoa_imd_ca_tbl %>% 
  write_csv('data/pc_lsoa_imd_ca_tbl.csv')

hist(pc_lsoa_imd_ca_tbl$imd19)

ca_lsoa_tbl <- pc_lsoa_imd_ca_tbl %>% 
  distinct(lsoa11cd, imd19)

# using 2011 LSO boundaries
lsoa_bound <- st_read("data/LSOA_2011_Boundaries_Super_Generalised_Clipped.geojson") %>% 
  inner_join(ca_lsoa_tbl, by = join_by(LSOA11CD == lsoa11cd))
# check the lsoas visually
lsoa_bound %>% 
  select(imd19) %>% 
  plot()

ca_cert_subset_tbl <- read_rds("data/ca_cert_subset_tbl.rds")

ca_epc_imd_tbl <- ca_cert_subset_tbl %>% 
  inner_join(pc_lsoa_imd_ca_tbl %>% 
               select(pcds, imd19),
             by = join_by(POSTCODE == pcds))


naniar::pct_miss(ca_epc_imd_tbl$imd19)



