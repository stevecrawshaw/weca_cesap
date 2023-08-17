pacman::p_load(tidyverse,
               rvest,
               janitor,
               glue,
               jsonlite,
               readODS)

source("../airquality_GIT/gg_themes.R")



# using the query endpoint for this dataset
# https://geoportal.statistics.gov.uk/datasets/86b7c99d0fe042a2975880ff9ec51c1c_0/api
# ArcGIS urgghhh

ca_url <-  "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LAD22_CAUTH22_EN_LU/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json"

ca_tbl <- fromJSON(ca_url) %>% 
  pluck('features', 'attributes') %>% 
  as_tibble()


charging_raw_tbl <- read_ods('data/electric-vehicle-charging-device-statistics-july-2023.ods', sheet = "1a",
                             col_names = TRUE,
                             skip = 2,
                             .name_repair = janitor::make_clean_names)


m_date <- function(colname){
  # browser()
  m <- str_sub(colname, 1, 3) %>% str_to_title()
  y <- str_extract_all(str_sub(colname, 4, 7), pattern = '\\d+')[[1]]
  datestring <- glue("01 {m} {y}")
  outdate <- strptime(datestring, format = "%d %b %y")
  return(outdate)
  
}


names(charging_raw_tbl)


charging_ca_long <- charging_raw_tbl %>% 
  inner_join(ca_tbl %>% select(-ObjectId),
             by = join_by(local_authority_region_code == LAD22CD),
             keep = FALSE) %>% 
  pivot_longer(cols = matches("devices|population")) %>% 
  mutate(
    end_date = map(name, m_date) %>% pluck(1),
         variable = if_else(str_detect(name, "charging"),
                            "total_charging_devices",
                            "per_100k_pop"),
         value = as.numeric(value),
         name = NULL,
         local_authority_region_name = NULL)

charging_ca_long %>% glimpse()

charging_ca_long %>% 
  filter(variable == "per_100k_pop") %>% 
  # group_by(CAUTH22NM) %>% 
  ggplot() +
  geom_col(aes(x = end_date, y = value)) +
  facet_wrap(~ CAUTH22NM)
  
  
  
  
  
  
