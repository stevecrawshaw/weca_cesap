pacman::p_load(tidyverse,
               janitor,
               glue,
               ggtext,
               gghighlight,
               paletteer,
               sf)

get_ca_area_tbl <- function() {
  st_read(
    "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Combined_Authorities_December_2022_EN_BFC/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
  ) %>%
    clean_names() %>%
    mutate(km2 = shape_area / 1e6) %>%
    st_drop_geometry() %>%
    select(CAUTH22CD = cauth22cd, km2)
}

re_generation_la_tbl <- read_csv("data/re_generation_la_tbl.csv")
ca_la_tbl <- read_csv("data/ca_la_tbl.csv")
ca_area_tbl <- get_ca_area_tbl()

make_re_plot_tbl <- function(re_generation_la_tbl, ca_la_tbl, ca_area_tbl){
  
  re_generation_la_tbl %>%
  inner_join(ca_la_tbl, by = join_by(la_code == LAD22CD)) %>%
  group_by(CAUTH22NM, CAUTH22CD, year) %>%
  summarise(total_mwh_ca = sum(total_mwh)) %>%
  mutate(total_mwh_cum_ca = cumsum(total_mwh_ca),
         CAUTH22NM = str_wrap(CAUTH22NM, 19)) %>% 
  inner_join(ca_area_tbl, by = join_by(CAUTH22CD == CAUTH22CD)) %>% 
  mutate(cum_gwh_ca_km2 = total_mwh_cum_ca / km2 / 1000)
}

re_plot_tbl <- make_re_plot_tbl(re_generation_la_tbl, ca_la_tbl, ca_area_tbl)

re_plot <- 
re_plot_tbl %>% 
ggplot(aes(x = year, y = cum_gwh_ca_km2, colour = CAUTH22NM)) +
  geom_line(linewidth = 2) +
  gghighlight(
    CAUTH22NM == "West of England",
    unhighlighted_params = list(
      colour = NULL,
      alpha = 0.4,
      linewidth = 0.7
    ),
    keep_scales = TRUE,
    use_direct_label = FALSE,
    use_group_by = FALSE
  ) +
  paletteer::scale_color_paletteer_d('ggthemes::calc') +
  theme_minimal(base_size = 20) +
  labs(title = "Cumulative Renewable Electricity Generation",
       x = "Year",
       y = "GWh<br>Km<sup>-2</sup>",
       colour = "Combined\nAuthority",
       caption = "Source: https://www.gov.uk/government/statistics/regional-renewable-statistics") +
  theme(axis.title.y = element_markdown(angle = 360, vjust = 0.5),
        plot.caption = element_text(size = 10))

re_plot

ggsave("plots/re_gen_ca.png",
       plot = re_plot,
       bg = "white",
       height = 8,
       width = 12)

