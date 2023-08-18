pacman::p_load(tidyverse,
               rvest,
               janitor,
               glue,
               jsonlite,
               readODS,
               config)

source("../airquality_GIT/gg_themes.R")

weca_colours <- config::get(file = '../config.yml',
                            config = "colours")


get_ca_tbl <- function(){
# using the query endpoint for this dataset
# https://geoportal.statistics.gov.uk/datasets/86b7c99d0fe042a2975880ff9ec51c1c_0/api
# ArcGIS urgghhh
"https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LAD22_CAUTH22_EN_LU/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json" %>% 
    fromJSON() %>% 
    pluck('features', 'attributes') %>% 
    as_tibble() %>% 
    return()
}

get_chargers_tbl <- function(ods_file_path = 'data/electric-vehicle-charging-device-statistics-july-2023.ods'){
  # no good open data source for this
  ods_file_path %>% 
    read_ods(sheet = "1a",
             col_names = TRUE,
             skip = 2,
             .name_repair = janitor::make_clean_names) %>% 
    return()
}

make_charging_long_tbl <- function(charging_raw_tbl, ca_tbl){
# join tables, pivot, make dates from column names, clean
charging_raw_tbl %>% 
  inner_join(ca_tbl %>% select(-ObjectId),
             by = join_by(local_authority_region_code == LAD22CD),
             keep = FALSE) %>% 
  pivot_longer(cols = matches("devices|population")) %>% 
    mutate(end_date = make_date(year = 2000 + str_extract(name, "\\d+") %>% as.integer(),
                                month = str_sub(name, 1, 3) %>%
                                                str_to_title() %>% 
                                  match(month.abb),
                                day = 1),
    variable = if_else(str_detect(name, "charging"),
                       "total_charging_devices",
                       "per_100k_population"),
    value = as.numeric(value),
    name = NULL,
    local_authority_region_name = NULL) %>% 
    return()
}

plot_chargers <- function(charging_long_tbl,
                          metric = c("per_100k_population", "total_charging_devices"),
                          caption = "https://www.gov.uk/government/statistics/electric-vehicle-charging-device-statistics-july-2023/"){
  
  stopifnot("incorrect metric" = metric %in% c("per_100k_population", "total_charging_devices"))
  
  charging_long_tbl %>%
    filter(variable == metric) %>%
    ggplot() +
    geom_col(aes(x = end_date, y = value), fill = weca_colours$blue) +
    facet_wrap( ~ CAUTH22NM, ncol = 2) +
    theme_report_facet() +
    labs(
      title = "EV Charging Points per Combined Authority",
      subtitle = metric %>% str_replace_all("_", " ") %>% str_to_title(),
      caption = caption,
      x = "Date (quarter ending)",
      y = "Chargers"
    )
  
  
  }
  

ca_tbl <- get_ca_tbl()

charging_raw_tbl <- get_chargers_tbl()

charging_long_tbl <- make_charging_long_tbl(charging_raw_tbl, ca_tbl)

plot_chargers(charging_long_tbl, metric = "total_charging_devices")

