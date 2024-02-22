pacman::p_load(tidyverse, # Data wrangling
               fastverse,
               glue, # Manipulating strings
               janitor, # general data cleaning
               duckdb,
               config
)

colours <- config::get(config = "colours", file = "../config.yml")

colours

raw_ghg <- read_csv("data/2005-21-local-authority-ghg-emissions-csv-dataset-update-060723.csv") %>% 
  clean_names()

ua_vec <- c("Bristol, City of",
            "South Gloucestershire",
            "Bath and North East Somerset",
            "North Somerset")

names(raw_ghg)

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
  summarise(sum(territorial_emissions_kt_co2e))


ghg_la_tbl %>% glimpse()


make_chart_tbl <- function(ghg_la_tbl,
                           year_start = 2005,
                           sectors = c("Commercial", "Domestic", "Transport"),
                           include_ns = TRUE){
  
  interim_tbl <- 
  
  
}


weca_tbl <- ghg_la_tbl %>% 
  filter(!local_authority %in% "North Somerset") %>% 
  ungroup() %>% 
  group_by(calendar_year, la_ghg_sector) %>% 
  summarise(emissions_by_sector = sum(`sum(territorial_emissions_kt_co2e)`))

lep_tbl <- ghg_la_tbl %>% 
  ungroup() %>% 
  group_by(calendar_year, la_ghg_sector) %>% 
  summarise(emissions_by_sector = sum(`sum(territorial_emissions_kt_co2e)`)) %>% 
  filter(la_ghg_sector %in% c("Commercial", "Domestic", "Transport"),
         calendar_year >= 2005)





weca_tbl %>%
  filter(la_ghg_sector %in% c("Commercial", "Domestic", "Transport"),
         calendar_year >= 2005) %>% 
  ggplot(aes(x = calendar_year, y = emissions_by_sector, colour = la_ghg_sector)) +
  geom_line(linewidth = 2)


