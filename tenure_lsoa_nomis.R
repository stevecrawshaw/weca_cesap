pacman::p_load(tidyverse)

# the url is too long for a string so has to be ingested vi sourcing a script!
source("lep_tenure_url.r")

nchar(lep_tenure_url)

lep_tenure_tbl <- lep_tenure_url %>% 
  read_csv()

lep_tenure_tbl %>% glimpse()
