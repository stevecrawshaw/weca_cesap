ca_tbl <- get_ca_tbl()
ca_tbl <- get_ca_source_tbl()
weca_colours <- get_weca_colours()
authority_list <- get_authority_list()
authorities_tbl <- make_authorities_tbl(authority_list)
# charging
charging_raw_tbl <- get_chargers_tbl()
charging_long_tbl <- make_charging_long_tbl(charging_raw_tbl, ca_tbl)
charger_plot <- plot_chargers(charging_long_tbl, metric = "per_100k_population")
charger_plot
# traffic
la_dft_lookup <- get_la_dft_lookup()
local_dft_data_tbl <- get_local_dft_data_tbl(authorities_tbl, auth_grouping = "lep")
ca_dft_lookup <- get_ca_dft_lookup(ca_tbl, la_dft_lookup)
all_ca_traffic_data_tbl <- get_all_ca_traffic_data_tbl(ca_dft_lookup)
ca_traffic_plot <- plot_car_use_ca(all_ca_traffic_data_tbl,
                                   year_col = year.x,
                                   meas_col = cars_all_ratio)
ca_traffic_plot

uk_emissions_raw_tbl <- read_csv('data/uk-local-authority-ghg-emissions-2020-dataset.csv') %>% 
  clean_names()

ca_emissions_list <- make_ca_emissions_list(
  uk_emissions_raw_tbl,
  ca_dft_lookup,
  sub_sectors = c("Road Transport (Minor roads)", 
                  "Road Transport (A roads)"),
  auth_grouping = "lep")

ca_emissions_transport_plot <- plot_ca_emissions(ca_emissions_list)
ca_emissions_transport_plot


ggsave("plots/car_traffic.png", ca_traffic_plot, bg = "white", width = 10, height = 8)
ggsave("plots/transport_emissions.png", ca_emissions_transport_plot, bg = "white", width = 10, height = 8)
ggsave("plots/ev_charger_plot.png", charger_plot, bg = "white")
