pacman::p_load(tidyverse,
               janitor,
               glue,
               tidyxl,
               readxl)

contents <-
  xlsx_cells("data/Renewable_electricity_by_local_authority_2014_2022.xlsx",
                     sheets = "Cover sheet")

get_year_range <- function(contents) {
  contents %>%
    filter(address == "A1") %>%
    pull(character) %>%
    str_extract_all(pattern = "\\d{4}") %>%
    pluck(1) %>%
    map_int( ~ as.integer(.x))
  
}

# year_range <- get_year_range(contents)

get_sheet_names <- function(year_range) {
  year_vec <- as.character(seq.int(year_range[1], year_range[2]))
  
  paste0("LA - Generation, ", year_vec)
  
}

# sheet_names <- get_sheet_names(year_range)

get_sheet <- function(sheet_name, path = "data/Renewable_electricity_by_local_authority_2014_2022.xlsx") {
  year <- str_extract(sheet_name, "\\d{4}")
  
  read_xlsx(
    path = path,
    sheet = sheet_name,
    range = "A7:R500"
  ) %>%
    clean_names() %>%
    mutate(year = year) %>%
    filter(
      !is.na(local_authority_code_note_1),
      !str_detect(local_authority_code_note_1, pattern = "Grand")
    ) %>%
    select(la_code = local_authority_code_note_1, year, total_mwh = total)
  
}

# update path with where the spreadsheet from 
# https://www.gov.uk/government/statistics/regional-renewable-statistics

re_generation_la_tbl <- 
contents %>% 
  get_year_range() %>% 
  get_sheet_names() %>% 
  map( ~ get_sheet(.x, 
                   path = "data/Renewable_electricity_by_local_authority_2014_2022.xlsx")) %>%
  bind_rows()
  
re_generation_la_tbl %>% write_csv(file = "data/re_generation_la_tbl.csv")
