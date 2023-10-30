pacman::p_load(
  tidyverse,
  janitor,
  glue,
  jsonlite,
  readODS,
  config,
  httr2,
  gghighlight,
  paletteer,
  toOrdinal,
  ggtext
)

source("../airquality_GIT/gg_themes.R")

# Generic CA functions ----

get_ca_tbl <- function(year = 2023) {
  # using the query endpoint for this dataset
  # https://geoportal.statistics.gov.uk/datasets/86b7c99d0fe042a2975880ff9ec51c1c_0/api
  # ArcGIS urgghhh
  this_year <- lubridate::year(Sys.Date()) %>% as.integer()
  stopifnot("year should be an integer before current year" =
              year %in%  seq.int(this_year - 3, this_year))
  year = 2023
  year_suffix <- (year - 2000) %>% as.character()
  url <-
    glue(
      "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LAD{year_suffix}_CAUTH{year_suffix}_EN_LU/FeatureServer/0/query/"
    )
  
  req <-  request(url) %>%
    req_url_query(
      where = "1=1",
      outFields = "*",
      outSR = 4326,
      f = "json"
    )
  resp <- req_perform(req)
  
  
  resp %>%
    resp_body_string() %>%
    fromJSON() %>%
    pluck('features', 'attributes') %>%
    as_tibble() %>%
    rename_with(.fn = ~ str_remove(.x, year_suffix))
}
#
# get_ca_tbl <- function(){
#   read_csv('https://raw.githubusercontent.com/stevecrawshaw/weca_cesap/main/data/ca_la_tbl.csv')
# }

get_weca_colours <- function() {
  config::get(config = "colours",
              file = '../weca_config.yml',
              'core') %>%
    map(1)
}

get_authority_list <- function() {
  config::get(config = "names_codes",
              file = '../weca_config.yml',
              'ua')
}


make_authorities_tbl <- function(authority_list) {
  tibble(
    'authority' = names(map(authority_list, 1)),
    'dft_code' = map(authority_list, 2) %>% unlist(),
    'la_code' = map(authority_list, 1) %>% unlist()
  )
}

# Charging Points ----

get_chargers_tbl <-
  function(ods_file_path = 'data/electric-vehicle-charging-device-statistics-july-2023.ods') {
    # no good open data source for this
    ods_file_path %>%
      read_ods(
        sheet = "1a",
        col_names = TRUE,
        skip = 2,
        .name_repair = janitor::make_clean_names
      )
  }

make_charging_long_tbl <- function(charging_raw_tbl, ca_tbl) {
  # join tables, pivot, make dates from column names, clean
  charging_raw_tbl %>%
    inner_join(
      ca_tbl %>% select(-ObjectId),
      by = join_by(local_authority_region_code == LADCD),
      keep = FALSE
    ) %>%
    pivot_longer(cols = matches("devices|population")) %>%
    mutate(
      end_date = make_date(
        year = 2000 + str_extract(name, "\\d+") %>%
          as.integer(),
        month = str_sub(name, 1, 3) %>%
          str_to_title() %>%
          match(month.abb),
        day = 1
      ),
      variable = if_else(
        str_detect(name, "charging"),
        "total_charging_devices",
        "per_100k_population"
      ),
      value = as.numeric(value),
      name = NULL,
      local_authority_region_name = NULL
    ) %>%
    return()
}

plot_chargers <- function(charging_long_tbl,
                          metric = c("per_100k_population", "total_charging_devices"),
                          caption = "https://www.gov.uk/government/statistics/electric-vehicle-charging-device-statistics-july-2023/") {
  stopifnot("incorrect metric" = metric %in% c("per_100k_population", "total_charging_devices"))
  
  charging_long_tbl %>%
    filter(variable == metric) %>%
    ggplot() +
    geom_col(aes(x = end_date, y = value), fill = weca_colours$blue) +
    facet_wrap(~ CAUTHNM, ncol = 2) +
    theme_report_facet() +
    labs(
      title = "EV Charging Points per Combined Authority",
      subtitle = metric %>%
        str_replace_all("_", " ") %>%
        str_to_title(),
      caption = caption,
      x = "Date (quarter ending)",
      y = "Chargers"
    )
}

# Traffic Modes ----

get_la_dft_lookup <- function() {
  # get the relationship between ONS codes and the dft LA code
  read_csv(
    'https://storage.googleapis.com/dft-statistics/road-traffic/downloads/data-gov-uk/local_authority_traffic.csv',
    lazy = TRUE
  ) %>%
    filter(year == max(year)) %>%
    select(Local_authority_id, name, ONS_code)
  
}

get_local_dft_data_tbl <-
  function(authorities_tbl,
           auth_grouping = c('weca', 'lep')) {
    # get the traffic data for WECA or LEP UA's
    stopifnot('incorrect code - should be weca or lep' = (auth_grouping %in% c('weca', 'lep')))
    
    if (auth_grouping == 'weca') {
      codes <-
        authorities_tbl$dft_code[authorities_tbl$authority != 'North Somerset']
    } else if (auth_grouping == 'lep') {
      codes <- authorities_tbl$dft_code
    }
    
    dft_url_root <-
      "https://storage.googleapis.com/dft-statistics/road-traffic/downloads/traffic/local_authority_id/dft_traffic_local_authority_id_"
    
    urls <- paste0(dft_url_root, codes, '.csv')
    map(urls, read_csv) %>%
      bind_rows() %>%
      mutate(cars_all_ratio = (cars_and_taxis * 100) / all_motor_vehicles)
  }

# much of CPCA doesn't have an equivalent dft code \ data
get_ca_dft_lookup <- function(ca_tbl, la_dft_lookup) {
  # relate the counties in each CA to a DFT ID
  ca_tbl %>%
    inner_join(la_dft_lookup, by = join_by(LADCD == ONS_code))
}

get_dft_traffic_tbl <- function(dft_la_id) {
  # function to get traffic flow data given a specific DfT LA ID
  # mapped over a vector of LA DfT ID's later
  dft_url_root <-
    "https://storage.googleapis.com/dft-statistics/road-traffic/downloads/traffic/local_authority_id/dft_traffic_local_authority_id_"
  
  url <- paste0(dft_url_root, dft_la_id, '.csv')
  
  read_csv(
    url,
    col_types = cols(
      local_authority_id = col_double(),
      local_authority_name = col_character(),
      year = col_double(),
      link_length_km = col_double(),
      link_length_miles = col_double(),
      cars_and_taxis = col_double(),
      all_motor_vehicles = col_double()
    )
  ) %>%
    mutate(cars_all_ratio = (cars_and_taxis * 100) / all_motor_vehicles)
  
}

get_all_ca_traffic_data_tbl <- function(ca_dft_lookup) {
  ca_dft_lookup$Local_authority_id %>%
    map(get_dft_traffic_tbl) %>%
    bind_rows() %>%
    inner_join(ca_dft_lookup,
               by = join_by(local_authority_id == Local_authority_id))
  
}

rank_woe_ordinal <- function(tbl, year_col, meas_col) {
  # get the rank of the west of england for a numeric column (meas_col)
  # for a given year (year_col)
  # expressed as an ordinal text value
  tbl %>%
    filter({
      {
        year_col
      }
    } == max({
      {
        year_col
      }
    })) %>%
    group_by(CAUTHNM) %>%
    summarise(mean_meas_col = mean({
      {
        meas_col
      }
    })) %>%
    mutate(rank = rank(mean_meas_col)) %>%
    subset(CAUTHNM == "West of England" |
             CAUTHNM == "West of England (inc NS)",
           rank) %>%
    pull() %>%
    toOrdinal()
  
}

plot_car_use_ca <- function(all_ca_traffic_data_tbl, meas_col) {
  # plot a time series of the ratio of car use to other road transport
  # highlighting the West of England
  # and giving an ordinal rank for the most recent year
  all_ca_traffic_data_tbl %>%
    group_by(CAUTHNM, year) %>%
    summarise(mean_ratio = mean(cars_all_ratio),
              .groups = 'drop') %>%
    ggplot(aes(
      x = year,
      y = mean_ratio,
      group = CAUTHNM,
      colour = CAUTHNM
    )) +
    geom_line(linewidth = 2) +
    scale_color_discrete() +
    gghighlight(
      CAUTHNM == "West of England",
      unhighlighted_params = list(
        colour = NULL,
        alpha = 0.4,
        linewidth = 1
      ),
      keep_scales = TRUE,
      use_direct_label = FALSE
    ) +
    # nice diverging scale
    paletteer::scale_color_paletteer_d('ggthemes::calc') +
    scale_linewidth(range = c(0.1, 1), guide = 'none') +
    labs(
      x = "Year",
      y = str_wrap("Mean %", width = 12),
      colour = "Combined\nAuthority",
      title = "Proportion of cars to all motorised traffic",
      subtitle = glue(
        "The West of England ranks {rank_woe_ordinal(all_ca_traffic_data_tbl, year_col = 'year', meas_col = {{meas_col}})} in car use. Lower is better"
      ),
      caption = "https://roadtraffic.dft.gov.uk/local-authorities"
    ) +
    theme_minimal(base_size = 18) +
    theme(axis.title.y = element_text(
      angle = 0,
      hjust = 0,
      vjust = 0.5
    ))
  
}

make_ca_emissions_list <-
  function(uk_emissions_raw_tbl,
           ca_dft_lookup,
           sub_sectors =  c("Road Transport (Minor roads)",
                            "Road Transport (A roads)"),
           auth_grouping = c('weca', 'lep')) {
  # make a list which gives a per_cap_emissions tbl to give territorial CO2 emissions per capita.
  # and retains the sub sectors and definition for WoE for use in the plot
  # It groups the raw emissions data by the combined authorities, year
  # and the sub sectors supplied
  # and defines the constituents of WoE as either LEP or WECA
  
  stopifnot(
    'incorrect code - should be weca or lep' = (auth_grouping %in% c('weca',
                                                                     'lep')),
    "sub sector(s) not in emissions table" = (
      sub_sectors %in% unique(uk_emissions_raw_tbl$la_ghg_sub_sector)
    )
  )
  
  if (auth_grouping == "lep") {
    # if north somerset to be included
    # add extra row to ca lookup table
    
    ns_row_tbl <- tibble(
      LADCD = "E06000024",
      LADNM = "North Somerset",
      CAUTHCD = "E47000009",
      CAUTHNM = "West of England",
      ObjectId = 54L,
      Local_authority_Id = 183,
      name = "North Somerset"
    )
    ca_dft_lookup <- bind_rows(ca_dft_lookup, ns_row_tbl) %>%
      mutate(CAUTHNM = if_else(
        CAUTHNM == "West of England",
        "West of England (inc NS)",
        CAUTHNM
      ))
    
  }
  
  subset_tbl <- uk_emissions_raw_tbl %>%
    right_join(ca_dft_lookup,
               by = join_by(local_authority_code == LADCD)) %>%
    filter(la_ghg_sub_sector %in% sub_sectors)
  
  pop_la_year_tbl <- subset_tbl %>%
    summarise(
      la_pop = mean(mid_year_population_thousands) * 1000,
      .by = c(local_authority_code, calendar_year)
    )
  
  co2_la_year_tbl <- subset_tbl %>%
    group_by(calendar_year, CAUTHNM, local_authority_code) %>%
    summarise(kt_CO2e = sum(territorial_emissions_kt_co2e))
  
  per_cap_annual_sub_sector_emissions_tbl <- pop_la_year_tbl %>%
    inner_join(
      co2_la_year_tbl,
      by = join_by(
        local_authority_code == local_authority_code,
        calendar_year == calendar_year
      )
    ) %>%
    group_by(CAUTHNM, calendar_year) %>%
    summarise(
      ca_pop = sum(la_pop),
      ca_kt_CO2e = sum(kt_CO2e),
      ca_t_CO2e_per_cap = (ca_kt_CO2e * 1e3) / ca_pop,
      .groups = "keep"
    )
  
  list(
    per_cap_annual_sub_sector_emissions_tbl = per_cap_annual_sub_sector_emissions_tbl,
    auth_grouping = auth_grouping,
    sub_sectors = sub_sectors
  )
}


plot_ca_emissions <- function(ca_emissions_list) {
  # plot trend of ca emissions, highlighting the WoE
  # summarise WoE rank of mean emissions for latest year per cap
  # and the sub sector(s) analysed
  if (ca_emissions_list$auth_grouping == "lep") {
    woe_name <- "West of England (inc NS)"
  } else {
    woe_name <- "West of England"
  }
  
  # get rank of Woe compared to others
  rank_woe <-
    rank_woe_ordinal(
      ca_emissions_list$per_cap_annual_sub_sector_emissions_tbl,
      year_col = calendar_year,
      meas_col = ca_t_CO2e_per_cap
    )
  # per cap emissions for latest year
  co2e_per_cap_woe_latest <-
    ca_emissions_list$per_cap_annual_sub_sector_emissions_tbl %>%
    ungroup() %>%
    filter(str_detect(CAUTHNM, "West of England"),
           calendar_year == max(calendar_year)) %>%
    pull(ca_t_CO2e_per_cap)
  
  # plot
  ca_emissions_list$per_cap_annual_sub_sector_emissions_tbl %>%
    ggplot() +
    geom_line(
      aes(
        x = calendar_year,
        y = ca_t_CO2e_per_cap,
        colour = CAUTHNM,
        group = CAUTHNM
      ),
      linewidth = 2
    ) +
    gghighlight(
      CAUTHNM == "West of England" |
        CAUTHNM == "West of England (inc NS)",
      unhighlighted_params = list(colour = NULL, alpha = 0.2),
      keep_scales = TRUE,
      use_direct_label = FALSE
    ) +
    scale_colour_brewer(palette = 'Spectral') +
    theme_minimal(base_size = 18) +
    theme(axis.title.y = element_text(
      angle = 0,
      hjust = 0,
      vjust = 0.5
    )) +
    labs(
      x = "Year",
      y = "Tonnes\nCO2e",
      title = "Per Capita CO2e Emissions by Combined Authority",
      subtitle = paste(ca_emissions_list$sub_sectors, collapse = " and "),
      caption = glue(
        "The {woe_name} is {rank_woe} \nlowest in CO<sub>2</sub> emissions at {round(co2e_per_cap_woe_latest, 2)} tonnes per person",
        .trim = FALSE
      ),
      colour = "Combined\nAuthority"
    ) +
    theme(plot.title = element_markdown()) +
    theme(plot.caption = element_markdown(hjust = 0))
  
}
