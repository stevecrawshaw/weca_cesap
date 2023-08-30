pacman::p_load(fastverse,
               tidyverse,
               janitor,
               glue,
               config,
               jsonlite,
               scales)


cols <- read_json('../WECABITheme2021.json')

scales::show_col(cols$dataColors %>% as.character())
show_col(cols$tableAccent)
