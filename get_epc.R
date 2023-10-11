pacman::p_load(tidyverse, # Data wrangling
               fastverse,
               glue, # Manipulating strings
               janitor, # general data cleaning
               fs, #files
               config, # securely store API parameters
               duckdb
)

# This is all now in epc_polars_group.ipynb which runs very much faster than R #sadface

# we download the big zip file from EPC site because using the API would take too long
# even for the subset of CA LA's

lep_vec <- read_csv('data/ca_la_tbl.csv') %>% 
  clean_names() %>% 
  pull(lad22cd)

# folder for all the subfolders by LA
dirs <- fs::dir_ls("data/all-domestic-certificates/")

# create a vector of paths of certificate csv's for CA LAs
ca_paths <- dirs %>% 
  as_tibble() %>%
  mutate(la_code = str_extract(value, pattern = "[EW][0-9]+")) %>% 
  filter(la_code %in% lep_vec) %>% 
  mutate(cert_file_path = glue("{value}/certificates.csv")) %>% 
  pull(cert_file_path)  

# spec for certificate csvs
col_types_domestic_fnc <- function(){
  col_types <- cols(
    LMK_KEY = col_character(),
    ADDRESS1 = col_character(),
    ADDRESS2 = col_character(),
    ADDRESS3 = col_character(),
    POSTCODE = col_character(),
    BUILDING_REFERENCE_NUMBER = col_character(),
    CURRENT_ENERGY_RATING = col_character(),
    POTENTIAL_ENERGY_RATING = col_character(),
    CURRENT_ENERGY_EFFICIENCY = col_double(),
    POTENTIAL_ENERGY_EFFICIENCY = col_double(),
    PROPERTY_TYPE = col_character(),
    BUILT_FORM = col_character(),
    INSPECTION_DATE = col_date(format = ""),
    LOCAL_AUTHORITY = col_character(),
    CONSTITUENCY = col_character(),
    COUNTY = col_skip(),
    LODGEMENT_DATE = col_date(format = ""),
    TRANSACTION_TYPE = col_character(),
    ENVIRONMENT_IMPACT_CURRENT = col_double(),
    ENVIRONMENT_IMPACT_POTENTIAL = col_double(),
    ENERGY_CONSUMPTION_CURRENT = col_double(),
    ENERGY_CONSUMPTION_POTENTIAL = col_double(),
    CO2_EMISSIONS_CURRENT = col_double(),
    CO2_EMISS_CURR_PER_FLOOR_AREA = col_double(),
    CO2_EMISSIONS_POTENTIAL = col_double(),
    LIGHTING_COST_CURRENT = col_double(),
    LIGHTING_COST_POTENTIAL = col_double(),
    HEATING_COST_CURRENT = col_double(),
    HEATING_COST_POTENTIAL = col_double(),
    HOT_WATER_COST_CURRENT = col_double(),
    HOT_WATER_COST_POTENTIAL = col_double(),
    TOTAL_FLOOR_AREA = col_double(),
    ENERGY_TARIFF = col_character(),
    MAINS_GAS_FLAG = col_character(),
    FLOOR_LEVEL = col_character(),
    FLAT_TOP_STOREY = col_character(),
    FLAT_STOREY_COUNT = col_double(),
    MAIN_HEATING_CONTROLS = col_skip(),
    MULTI_GLAZE_PROPORTION = col_double(),
    GLAZED_TYPE = col_character(),
    GLAZED_AREA = col_character(),
    EXTENSION_COUNT = col_double(),
    NUMBER_HABITABLE_ROOMS = col_double(),
    NUMBER_HEATED_ROOMS = col_double(),
    LOW_ENERGY_LIGHTING = col_double(),
    NUMBER_OPEN_FIREPLACES = col_double(),
    HOTWATER_DESCRIPTION = col_character(),
    HOT_WATER_ENERGY_EFF = col_character(),
    HOT_WATER_ENV_EFF = col_character(),
    FLOOR_DESCRIPTION = col_character(),
    FLOOR_ENERGY_EFF = col_character(),
    FLOOR_ENV_EFF = col_character(),
    WINDOWS_DESCRIPTION = col_character(),
    WINDOWS_ENERGY_EFF = col_character(),
    WINDOWS_ENV_EFF = col_character(),
    WALLS_DESCRIPTION = col_character(),
    WALLS_ENERGY_EFF = col_character(),
    WALLS_ENV_EFF = col_character(),
    SECONDHEAT_DESCRIPTION = col_character(),
    SHEATING_ENERGY_EFF = col_character(),
    SHEATING_ENV_EFF = col_character(),
    ROOF_DESCRIPTION = col_character(),
    ROOF_ENERGY_EFF = col_character(),
    ROOF_ENV_EFF = col_character(),
    MAINHEAT_DESCRIPTION = col_character(),
    MAINHEAT_ENERGY_EFF = col_character(),
    MAINHEAT_ENV_EFF = col_character(),
    MAINHEATCONT_DESCRIPTION = col_character(),
    MAINHEATC_ENERGY_EFF = col_character(),
    MAINHEATC_ENV_EFF = col_character(),
    LIGHTING_DESCRIPTION = col_character(),
    LIGHTING_ENERGY_EFF = col_character(),
    LIGHTING_ENV_EFF = col_character(),
    MAIN_FUEL = col_character(),
    WIND_TURBINE_COUNT = col_double(),
    HEAT_LOSS_CORRIDOR = col_character(),
    UNHEATED_CORRIDOR_LENGTH = col_double(),
    FLOOR_HEIGHT = col_double(),
    PHOTO_SUPPLY = col_double(),
    SOLAR_WATER_HEATING_FLAG = col_character(),
    MECHANICAL_VENTILATION = col_character(),
    ADDRESS = col_character(),
    LOCAL_AUTHORITY_LABEL = col_character(),
    CONSTITUENCY_LABEL = col_character(),
    POSTTOWN = col_character(),
    CONSTRUCTION_AGE_BAND = col_character(),
    LODGEMENT_DATETIME = col_datetime(format = ""),
    TENURE = col_character(),
    FIXED_LIGHTING_OUTLETS_COUNT = col_double(),
    LOW_ENERGY_FIXED_LIGHT_COUNT = col_double(),
    UPRN = col_double(),
    UPRN_SOURCE = col_character()
  )
  return(col_types)
}

# iterate over paths to construct huge epc data table
domestic_cert_ca_tbl <- map(ca_paths, 
                            .progress = TRUE,
                            ~read_csv(.x, col_types = col_types_domestic_fnc()), ) %>% 
  bind_rows()

domestic_cert_ca_tbl %>% write_rds('data/domestic_cert_ca_tbl.rds')

domestic_cert_ca_tbl <- read_rds('data/domestic_cert_ca_tbl.rds')

domestic_cert_ca_tbl %>% write_csv("data/domestic_cert_ca_tbl.csv", na = "")

# data.table to the rescue to clean and reduce to keep only 
# latest EPC cert data for CA areas
# very slow - polars is a lot quicker

domestic_cert_ca_tbl %>% glimpse()

ca_cert_subset_tbl <- setDT(domestic_cert_ca_tbl)[, 
                                           .(LMK_KEY,
                                             POSTCODE,
                                             CURRENT_ENERGY_RATING,
                                             LOCAL_AUTHORITY,
                                             PROPERTY_TYPE,
                                             LODGEMENT_DATETIME,
                                             TRANSACTION_TYPE,
                                             ENVIRONMENT_IMPACT_CURRENT,
                                             TENURE,
                                             UPRN)
][,
  .SD[which.max(LODGEMENT_DATETIME)],
  by=UPRN
]

domestic_cert_ca_tbl %>% 
  glimpse()
# even collapse very slow compared to polars (total 22s)
ca_cert_collapse_tbl <- domestic_cert_ca_tbl %>% 
  fgroup_by(UPRN) %>% 
  roworder(LODGEMENT_DATETIME) %>% 
  flast(na.rm = FALSE)
  
test <- flast(domestic_cert_ca_tbl$LODGEMENT_DATETIME,
              g = domestic_cert_ca_tbl$UPRN, use.g.names = TRUE)

ca_cert_subset_tbl %>% write_rds('data/ca_cert_subset_tbl.rds') 

ca_cert_subset_tbl <- read_rds("data/ca_cert_subset_tbl.rds")

ca_cert_subset_tbl %>% glimpse()

epc_subset_polars <- fread("data/epc_subset_polars_last.csv")


epc_subset_polars %>% glimpse()

setkey(ca_cert_subset_tbl, LMK_KEY)
setkey(epc_subset_polars, LMK_KEY)
haskey(epc_subset_polars)

unmatched <- ca_cert_subset_tbl[!epc_subset_polars]

glimpse(unmatched)

base::setdiff(ca_cert_subset_tbl[, LMK_KEY],
              epc_subset_polars[, LMK_KEY])

ca_cert_subset_tbl[UPRN == "10014487620"]
epc_subset_polars[UPRN == "10014487620"]


# DUCKDB ----
# still very slow to slice by group
# polars rules this game
con <- dbConnect(duckdb(), dbdir = "data/epc_la.duckdb", read_ony = FALSE)

dbWriteTable(con, "epc_domestic_ca", domestic_cert_ca_tbl)


ca_cert_subset_db_tbl <- dbExecute(con, "CREATE VIEW max_date_uprn AS SELECT max(LODGEMENT_DATE) _DATE,
                                             UPRN FROM
                                    epc_domestic_ca,
                                    GROUP BY UPRN")

de_dupe_epc <- dbGetQuery(con, "WITH RankedEPC AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY UPRN ORDER BY LODGEMENT_DATE DESC) AS rn
    FROM epc_domestic_ca
)

SELECT *
FROM RankedEPC
WHERE rn = 1;")

