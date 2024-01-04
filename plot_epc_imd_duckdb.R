pacman::p_load(tidyverse, # Data wrangling
               fastverse,
               glue, # Manipulating strings
               janitor, # general data cleaning
               gghighlight,
               toOrdinal,
               ggtext,
               duckdb
)

is_full_year <- function(epc_domestic_tbl){
# checks last date in the epc datasets and returns true if 31/12/YYYY  
last_epc_date <- epc_domestic_tbl %>%
  filter(year == max(year)) %>% 
  transmute(dt = as.Date(date)) %>%
  slice_max(dt, n = 1,  with_ties = FALSE) %>% 
  collect() %>% 
  pull(dt)

month(last_epc_date) == 12 && lubridate::day(last_epc_date) == 31

}

get_caption <- function(plot_tbl, max_year){
# get ordinal rank of WoE
place_woe <- plot_tbl %>% 
  filter(year == max_year) %>% 
  ungroup() %>% 
  mutate(place = (10 - rank(w_mean_env_imd)) + 1 ) %>% 
  filter(cauthnm == "West of England")

place <-  toOrdinal(place_woe$place)

caption <- if(place_woe$place == 1L){
  glue("West of England is ranked first in {max_year}")
  } else {
  glue("West of England is ranked {place} best in {max_year}")
  }
caption
}

# Get data from duckdb ----

con <- dbConnect(duckdb(), dbdir = "data/ca_epc.duckdb")
# The database is built in a python notebook using polars
# in this project folder
# as the EPC data is so big

dbListTables(con)

ca_la_tbl <- con %>% 
  tbl("ca_la_tbl")

epc_domestic_tbl <- con %>% 
  tbl("epc_domestic_tbl")

epc_non_domestic_tbl <- con %>% 
  tbl("epc_non_domestic_tbl")
  
imd_tbl <- con %>% 
  tbl("imd_tbl")

postcodes_tbl <- con %>% 
  tbl("postcodes_tbl")
# Join and group data ----



source_tbl <- epc_domestic_tbl %>% 
  inner_join(postcodes_tbl, by = join_by(postcode == postcode)) %>% 
  inner_join(imd_tbl, by = join_by(lsoacd == lsoacd)) %>% 
  inner_join(ca_la_tbl, by = join_by(ladcd.x == ladcd)) %>% 
  group_by(lsoacd,
           current_energy_rating,
           property_type,
           transaction_type,
           cauthnm,
           year) %>% 
  summarise(n = n(),
            imd = mean(imd),
            score = mean(environment_impact_current), .groups = "drop") %>% 
  collect()

full_year <- is_full_year(epc_domestic_tbl = epc_domestic_tbl)

con %>% dbDisconnect()

plot_tbl <- source_tbl %>% 
  group_by(cauthnm, year) %>% 
  summarise(w_mean_env_imd = weighted.mean(score, w = 1 / imd)) %>% 
  arrange(w_mean_env_imd) 

# Plot data ----

plot_tbl %>% glimpse()

max_year <- if(full_year){
  max(plot_tbl$year)
} else {
  max(plot_tbl$year) - 1
}

caption <- get_caption(plot_tbl, max_year)

epc_score_ts_plot <- plot_tbl %>% 
  filter(year >= (max_year - 10),
         year <= max_year) %>% 
    ggplot(aes(
    x = as_factor(year),
    y = w_mean_env_imd,
    colour = cauthnm,
    group = cauthnm
  )) +
  geom_line(linewidth = 2) +
    gghighlight(
    cauthnm == "West of England",
    unhighlighted_params = list(
      colour = NULL,
      alpha = 0.4,
      linewidth = 0.7
    ),
    keep_scales = TRUE,
    use_direct_label = FALSE,
    use_group_by = FALSE
  ) +
  # nice diverging scale
  paletteer::scale_color_paletteer_d('ggthemes::calc') +
  expand_limits(y = c(50, 70)) +
  theme_minimal(base_size = 18) +
  labs(
    title = "Annual Environmental Impact Scores: Domestic Properties",
    subtitle = "Weighted by Indices of Multiple Deprivation Rank",
    colour = "Combined\nAuthority",
    x = "Year",
    y = "EPC\nScore",
    caption = caption
  ) +
  theme(axis.title.y = element_text(angle = 360,
                                    vjust = 0.5)
        ) +
  theme(plot.caption = element_markdown(hjust = 0))

epc_score_ts_plot

ggsave(glue("plots/epc_ca_score_imd_weighted_{max_year}.png"),
       plot = epc_score_ts_plot,
       bg = "white",
       height = 8,
       width = 12)

# ---------
epc_nd_sample %>% glimpse()
epc_nd_sample <- epc_non_domestic_tbl %>% 
  inner_join(postcodes_tbl, by = join_by(postcode == postcode)) %>% 
  inner_join(ca_la_tbl, by = join_by(ladcd == ladcd)) %>% 
  mutate(rating_cat = if_else(str_detect(asset_rating_band, "A"),
                              "A", "B_G")) %>% 
  group_by(cauthnm, rating_cat) %>%
  summarise(count = n()) %>%
  pivot_wider(names_from = rating_cat, values_from = count) %>% 
  mutate(prop_A = round(A * 100/ B_G, 1)) %>% 
  collect()
  epc_nd_sample %>%  glimpse()

# need proportion of A or A+ EPC's annually
epc_nd_sample %>% 
  ggplot(aes(x = year, y = prop_A)) +
  geom_line() +
  facet_wrap(~ cauthnm)










