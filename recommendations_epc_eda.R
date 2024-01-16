pacman::p_load(tidyverse,
               fastverse,
               glue,
               janitor)

path <- "data/all-domestic-certificates/domestic-E06000023-Bristol-City-of/recommendations.csv"

bris_recs_raw_tbl <- read_csv(path)

bris_recs_raw_tbl %>% glimpse()

clean_bris_recs <- 
  bris_recs_raw_tbl %>% 
  clean_names()

clean_bris_recs %>% glimpse()
cols = c("improvement_id", "improvement_id_text")

clean_bris_recs %>%# glimpse()
  funique(cols, ) %>% 
  fselect(cols) %>% 
  view()

x <- clean_bris_recs$indicative_cost[1:100]
x %>% 
  str_replace_all("£|,", "") %>% 
  str_split_fixed(" - ", 2)

clean_bris_recs %>% 
  na.omit(indicative_cost) %>% 
  mutate(range_low = indicative_cost %>%
           str_replace_all("£|,", "") %>% 
           str_split_i(" - ", 1) %>% 
           as.integer(),
         range_high = indicative_cost %>%
           str_replace_all("£|,", "") %>% 
           str_split_i(" - ", 2) %>% 
           as.integer()
           ) %>% 
  rowwise() %>% 
  mutate(mean_cost = mean(c(range_low, range_high))) %>% 
  group_by(improvement_id_text) %>% 
  summarise(mean_cost_of_improvement = mean(mean_cost, na.rm = TRUE),
            count = n()) %>% 
  arrange(desc(count)) %>% 
  view()
  

