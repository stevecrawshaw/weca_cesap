pacman::p_load(tidyverse, # Data wrangling
               fastverse,
               glue, # Manipulating strings
               janitor, # general data cleaning
               gghighlight,
               toOrdinal,
               ggtext
)

ca_cert_subset_tbl <- read_rds("data/ca_cert_subset_tbl.rds") # epc data for CA w/ postcodes
pc_lsoa_imd_ca_tbl <- fread("data/pc_lsoa_imd_ca_tbl.csv") # imd data
# inner join
# to give property level data with an IMD rank, env score and lsoa ID
# this is the latest EPC cert data for the property
pc_lsoa_imd_ca_epc_tbl <- ca_cert_subset_tbl[pc_lsoa_imd_ca_tbl,
                                             on = .(POSTCODE = pcds),
                                             nomatch = NULL]
pc_lsoa_imd_ca_epc_tbl %>% glimpse()

# below gives the IMD, and env score grouped by LSOA, CA, year and some property
# parameters in case we need to assess the EPC banding

lsoa_imd_ca_epc_grp_tbl <-
  pc_lsoa_imd_ca_epc_tbl[,
                         .(.N,
                           imd19 = mean(imd19),
                           score = mean(ENVIRONMENT_IMPACT_CURRENT)),
                         by = .(lsoa11cd,
                                CURRENT_ENERGY_RATING,
                                PROPERTY_TYPE,
                                TRANSACTION_TYPE,
                                cauth22nm,
                                year = as.integer(year(LODGEMENT_DATE)))
                         ]

lsoa_imd_ca_epc_grp_tbl %>% glimpse()
# total weighted mean by inverse of imd rank by CA
env_score_annual_tbl <-
  lsoa_imd_ca_epc_grp_tbl[,  .(w_mean_env_imd = weighted.mean(score, w = 1 /
                                                                imd19)),
                          by = .(cauth22nm, year)][order(w_mean_env_imd)]

# get ordinal rank of WoE
place_woe <- env_score_annual_tbl[year == max(year)][, place := (10 - rank(w_mean_env_imd)) + 1][cauth22nm == "West of England"]

place <-  toOrdinal(place_woe$place)
year <- place_woe$year

epc_score_ts_plot <- env_score_annual_tbl[year >= 2013] %>%
  ggplot(aes(
    x = as_factor(year),
    y = w_mean_env_imd,
    colour = cauth22nm,
    group = cauth22nm
  )) +
  geom_line(linewidth = 2) +
    gghighlight(
    cauth22nm == "West of England",
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
    title = "Annual Environmental Impact Scores",
    subtitle = "Weighted by Indices of Multiple Deprivation Rank",
    colour = "Combined\nAuthority",
    x = "Year",
    y = "EPC\nScore",
    caption = glue("West of England is {place} best in {year}")
  ) +
  theme(axis.title.y = element_text(angle = 360,
                                    vjust = 0.5)
        ) +
  theme(plot.caption = element_markdown(hjust = 0))

epc_score_ts_plot

ggsave("plots/epc_ca_score_imd_weighted.png",
       plot = epc_score_ts_plot,
       bg = "white",
       height = 8,
       width = 12)



