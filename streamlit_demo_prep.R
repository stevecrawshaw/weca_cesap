library(tidyverse)

certs_df <- read_csv("data/certificates.csv")

pcds <- read_csv("data/PCD_OA21_LSOA21_MSOA21_LAD_MAY23_UK_LU.csv")


unique_certs <- certs_df %>% 
  select(UPRN, POSTCODE, LODGEMENT_DATETIME, ENERGY_CONSUMPTION_CURRENT, ENVIRONMENT_IMPACT_CURRENT, CURRENT_ENERGY_RATING) %>% 
  group_by(UPRN) %>% 
  slice_max(order_by = LODGEMENT_DATETIME, n= 1)
  
joined_data <- pcds %>% 
  select(pcds, msoa21nm, ladnm) %>% 
  filter(ladnm == "Bristol, City of") %>% 
  right_join(unique_certs, by = join_by(pcds == POSTCODE))
  
joined_data %>% 
  write_csv("joined_data.csv")
