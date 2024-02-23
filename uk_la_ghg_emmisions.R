pacman::p_load(tidyverse, # Data wrangling
               fastverse,
               glue, # Manipulating strings
               janitor, # general data cleaning
               duckdb,
               config
)

# Get the UK LA greenhouse gas emissions and create files for charting in 
# state of the region powerpoint ppt

colours <- config::get(config = "colours", file = "../config.yml")

raw_ghg <- read_csv("data/2005-21-local-authority-ghg-emissions-csv-dataset-update-060723.csv") %>% 
  clean_names()

ua_vec <- c("Bristol, City of",
            "South Gloucestershire",
            "Bath and North East Somerset",
            "North Somerset")

ghg_la_tbl <- raw_ghg %>% 
  filter(local_authority %in% ua_vec) %>% 
  select(local_authority,
         local_authority_code,
         calendar_year,
         la_ghg_sector,
         greenhouse_gas,
         territorial_emissions_kt_co2e,
         mid_year_population_thousands,
         area_km2) %>% 
  group_by(local_authority, calendar_year, la_ghg_sector) %>% 
  summarise(emissions_sum = sum(territorial_emissions_kt_co2e))


ghg_la_tbl %>% glimpse()

make_chart_tbl <- function(ghg_la_tbl,
                           year_start = 2005,
                           sectors = c("Commercial", "Domestic", "Transport"),
                           include_ns = TRUE){
  
if(!include_ns){
  interim_tbl <- ghg_la_tbl %>% 
    filter(!local_authority %in% "North Somerset")
} else {
  interim_tbl <- ghg_la_tbl
}
  
next_tbl <- interim_tbl %>% 
  ungroup() %>% 
  mutate(group = if_else(la_ghg_sector %in% sectors,
                          la_ghg_sector, "Other")) %>% 
  group_by(calendar_year, group) %>% 
  summarise(
    emissions_by_sector = sum(emissions_sum)) %>% 
  filter(calendar_year >= year_start)

return(next_tbl)

}


weca_tbl <- make_chart_tbl(ghg_la_tbl,
                           year_start = 2005,
                           sectors = c("Commercial", "Domestic", "Transport"),
                           include_ns = FALSE)

lep_tbl <- make_chart_tbl(ghg_la_tbl,
                          year_start = 2005,
                          sectors = c("Commercial", "Domestic", "Transport"),
                          include_ns = TRUE)

weca_tbl %>% glimpse()

weca_tbl %>% 
  pivot_wider(id_cols = calendar_year, names_from = group, values_from = emissions_by_sector) %>% 
  write_csv("data/weca_ghg_chart_data.csv")

lep_tbl %>% 
  pivot_wider(id_cols = calendar_year, names_from = group, values_from = emissions_by_sector) %>% 
  write_csv("data/lep_ghg_chart_data.csv")

# calculate % drop in emissions from start to end of dataset
emission_drop <- function(chart_tbl){

min_year = min(chart_tbl$calendar_year)
max_year = max(chart_tbl$calendar_year)

chart_tbl %>% 
  filter(calendar_year %in% c(min_year, max_year)) %>% 
  group_by(calendar_year) %>% 
  summarise(sum_emissions = sum(emissions_by_sector)) %>% 
  pivot_wider(names_from = calendar_year, names_prefix = "year_", values_from = sum_emissions) %>%
  rename(min_year = 1, max_year = 2) %>% 
  mutate(percent_drop = (((min_year - max_year) * 100)/ min_year) %>%
           round(1)) %>% 
  pull(percent_drop )

}


lep_tbl %>% emission_drop()

sectors <- unique(ghg_la_tbl$la_ghg_sector)

specsec <- c("Commercial", "Domestic", "Transport") 

base::setdiff(sectors, specsec)


weca_tbl %>%
  ggplot(aes(x = calendar_year, y = emissions_by_sector, colour = group)) +
  geom_line(linewidth = 2)


