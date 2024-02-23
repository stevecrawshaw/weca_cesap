pacman::p_load(tidyverse,
               janitor,
               glue,
               readxl,
               gt,
               gtExtras,
               rlist)

# RELOAD FROM SHAREPOINT
path = "data/CESAP Action Tracker 2024_2025.xlsx"
sheets_vec <-
  readxl::excel_sheets(path)

make_list_from_sheets <- function(
    path = "data/CESAP Action Tracker 2024_2025.xlsx",
    sheets_vec) {
  # read all the sheets and store in a list with each element a sheet
  sheets_vec %>%
    map(~ read_xlsx(path, .x)) %>%
    set_names((sheets_vec))  %>%
    map(.f = clean_names)
}

char_it <- function(tbl){ 
  mutate(tbl, across(everything(), as.character))
}

sheets_list_raw <- make_list_from_sheets(path, sheets_vec)

sheets_list_clean <- sheets_list_raw %>% 
  list.remove(range = c("instructions", "workings"))
  
tracker_raw_tbl <- map(sheets_list_clean, char_it) %>% 
  bind_rows(.id = "cesap_pillar")

tracker_clean_tbl <- tracker_raw_tbl %>% 
  filter(!is.na(action)) %>% 
  select(cesap_pillar, action_areas, id, action, update_for_march_2024, impact, mca_control) %>% 
  mutate(action_areas = if_else(
    cesap_pillar == "Climate Resilience ",
    "Climate Resilience",
    action_areas)) %>% 
  group_by(cesap_pillar, action_areas) 

tracker_clean_tbl %>% 
  write_rds("data/tracker_clean_tbl.rds")

tracker_clean_tbl <- read_rds("data/tracker_clean_tbl.rds")

# make the GT and format
tracker_clean_gt <- tracker_clean_tbl %>% 
  gt() %>% 
  tab_style(style = cell_text(weight = "bold"),
            locations = cells_row_groups()) %>% 
  cols_label(id ~ "ID",
             action ~ "Action",
             update_for_march_2024 ~ "Update: March 2024",
             impact ~ "Impact",
             mca_control ~ "Control") %>% 
  tab_style(style = cell_text(size = "large",
                              weight = "bold"),
            locations = cells_column_labels())

# to get rid of NA's
# sub_missing(
#   data,
#   columns = everything(),
#   rows = everything(),
#   missing_text = "---"
# )


gtsave(tracker_clean_gt, "data/tracker_clean.html")
# then print as pdf from browser




