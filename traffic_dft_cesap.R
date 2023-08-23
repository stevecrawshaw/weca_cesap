pacman::p_load(fastverse,
               tidyverse,
               janitor,
               glue,
               config,
               paletteer)

source("../airquality_GIT/gg_themes.R")

get_la_dft_lookup <- function(){
# get the relationship between ONS codes and the dft LA code
la_dft_lookup <- read_csv('https://storage.googleapis.com/dft-statistics/road-traffic/downloads/data-gov-uk/local_authority_traffic.csv', lazy = TRUE) %>% 
  filter(year == max(year)) %>% 
  select(Local_authority_id, name, ONS_code, year)

return(la_dft_lookup)

}

get_ca_tbl <- function(){
  read_csv('https://raw.githubusercontent.com/stevecrawshaw/weca_cesap/main/data/ca_la_tbl.csv') %>% 
    return()
}

get_weca_colours <- function(){

weca_colours <- config::get(config = "colours",
                            file = '../weca_config.yml',
                            'core') %>% 
  map(1)
return(weca_colours)
}

get_authority_list <- function(){
authorities <- config::get(config = "names_codes",
                           file = '../weca_config.yml',
                           'ua')
return(authorities)
}

make_authorities_tbl <- function(authority_list){
authorities_tbl <- tibble(
'authority' = names(map(authority_list, 1)),
'dft_code' = map(authority_list, 2) %>% unlist(),
'la_code' = map(authority_list, 1) %>% unlist()
)
return(authorities_tbl)
}


get_dft_data_tbl <- function(authorities_tbl, auth_grouping = c('weca', 'lep')){

  stopifnot('incorrect code - should be weca or lep' = (auth_grouping %in% c('weca', 'lep')))
  
  if (auth_grouping == 'weca'){
    codes <- authorities_tbl$dft_code[authorities_tbl$authority != 'North Somerset']
  } else if (auth_grouping == 'lep'){
    codes <- authorities_tbl$dft_code
  }

dft_url_root <- "https://storage.googleapis.com/dft-statistics/road-traffic/downloads/traffic/local_authority_id/dft_traffic_local_authority_id_"

urls <- paste0(dft_url_root, codes, '.csv')

dft_data_tbl <- map(urls, read_csv) %>% 
  bind_rows() %>% 
  mutate(cars_all_ratio = (cars_and_taxis * 100) / all_motor_vehicles) 
return(dft_data_tbl)
}



weca_colours <- get_weca_colours()

authority_list <- get_authority_list()

authorities_tbl <- make_authorities_tbl(authority_list)

dft_data_tbl <- get_dft_data_tbl(authorities_tbl, auth_grouping = "weca")

# much of CPCA doesn't have an equivalent dft code \ data
ca_dft_lookup <- ca_tbl %>% 
  inner_join(la_dft_lookup, by = join_by(LAD22CD == ONS_code))


get_dft_tbl <- function(dft_la_id){
  dft_url_root <- "https://storage.googleapis.com/dft-statistics/road-traffic/downloads/traffic/local_authority_id/dft_traffic_local_authority_id_"
  
  url <- paste0(dft_url_root, dft_la_id, '.csv')
  
  dft_data_tbl <- read_csv(url, col_types = cols(
    local_authority_id = col_double(),
    local_authority_name = col_character(),
    year = col_double(),
    link_length_km = col_double(),
    link_length_miles = col_double(),
    cars_and_taxis = col_double(),
    all_motor_vehicles = col_double()
  )) %>%
    mutate(cars_all_ratio = (cars_and_taxis * 100) / all_motor_vehicles) 
    
  return(dft_data_tbl)
}


dft_data_tbl <- ca_dft_lookup$Local_authority_id %>% 
  map(get_dft_tbl) %>% 
  bind_rows() %>% 
  inner_join(ca_dft_lookup, by = join_by(local_authority_id == Local_authority_id))

dft_data_tbl %>% 
  group_by(CAUTH22NM, year.x) %>% 
  summarise(mean_ratio = mean(cars_all_ratio), .groups = 'drop') %>% 
  mutate(lty = if_else(CAUTH22NM == "West of England", 1, 3)) %>% 
  ggplot(aes(x = year.x, y = mean_ratio, group = CAUTH22NM, colour = CAUTH22NM, lty = lty)
         ) +
  scale_linetype_binned(guide = 'none') +
  geom_line(linewidth = 1) + 
  # scale_colour_brewer(palette = 'Spectral') +
  # scale_color_paletteer_d('ggthemes::hc_darkunica') +
  # scale_linewidth(range = c(0.1, 1), guide = 'none') +
  labs(x = "Year",
       y = str_wrap("Mean %", width = 12),
       colour = "CA name",
       title = "Proportion of cars to all motorised traffic",
       caption = "https://roadtraffic.dft.gov.uk/local-authorities") +
  theme_minimal(base_size = 18) +
  theme(axis.title.y = element_text(angle = 0, hjust = 0, vjust = 0.5))

dft_data_tbl %>% 
  ggplot(aes(x = year,
             y = cars_all_ratio,
             group = local_authority_name,
             colour = local_authority_name)) +
  geom_line() +
  theme_weca_clean()


plot_dft_data_tbl <- function(dft_data_tbl){
  dft_data_tbl %>% 
  group_by(year) %>% 
  summarise(lep_ratio = mean(cars_all_ratio)) %>% 
  ggplot(aes(x = year, y = lep_ratio)) +
  geom_line()
  
}