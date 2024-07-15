# prepare data from EPC and other tables in ca_epc.duckdb for publishing on the open data portal

pacman::p_load(tidyverse, # Data wrangling
               fastverse,
               glue, # Manipulating strings
               janitor, # general data cleaning
               duckdb,
               sf,
               fs,
               DBI,
               sjmisc,
               gt,
               santoku
)


imd_least_dep <- 32844L
imd_deciles = seq(1, imd_least_dep, length.out = 11) %>% round()

con <- DBI::dbConnect(duckdb(), dbdir = "data/ca_epc.duckdb")

make_epochs <- function(year_integer, breaks = c(1799, 1900, 1930, 2024),
                        labels = c("Pre 1900", "1900 - 1930", "1930 - Present")){
  # use santoku::chop to create bands of construction ages
  year_integer %>% 
    map(~chop(.x,
              breaks,
              labels)) %>%
    unlist()
}

get_mid_age <- function(construction_age_band){
  int_vec <- str_extract_all(construction_age_band, "\\b\\d{4}\\b") %>%
    # unlist() %>%
    pluck(1) %>% 
    as.integer()

  out = case_when(
    is.na(int_vec) ~ NA_integer_,
    length(int_vec) == 1 & int_vec == 1900L ~ 1899L,
    length(int_vec) == 1 ~ int_vec,
    length(int_vec) == 2 ~ mean(int_vec) %>% round(0),
    .default = NA_integer_,
    .ptype = integer()
  )
  out[1]
}

test_cab <- sample(lep_epc_domestic_tbl$construction_age_band, size = 100)
# 
# test_cab <- "Before 1900"
map(test_cab, get_mid_age) #%>%
#   map(~mean(.x, na.rm = TRUE))


#map_int(lep_epc_domestic_tbl$construction_age_band[1:20], get_mid_age) #%>% length()

# 
# ren_func <- function(x){
#   if(str_starts(x, "d_")){
#     y = paste(str_remove(x, "_d"), "_percent")
#   } else {
#     y = x
#   }
#   return(y)
# }

# Create a function to remove attributes
remove_attributes <- function(x) {
  attributes(x) <- NULL
  return(x)
}

dbListTables(con)

lep_la_tbl <- tbl(con, "ca_la_tbl") %>% 
  filter(cauthnm == "West of England") %>% 
  collect() 

lep_codes <- lep_la_tbl  %>% 
  pull(ladcd)

lep_postcodes_tbl <- tbl(con, "postcode_centroids_tbl") %>% 
  filter(laua %in% lep_codes) %>%
  select(postcode = pcds, ladcd = laua, lsoa21, lsoa11, msoa11, msoa21, imd, lat, long, x, y) %>%
  collect()

uniqueN(lep_postcodes_tbl$lsoa21) 

lep_epc_domestic_tbl <- tbl(con, "epc_domestic_tbl") %>%  
  #head(1000) %>% # <------------------------------------------
  filter(local_authority %in% lep_codes) %>% 
  collect() %>% 
  left_join(lep_postcodes_tbl, 
            by = join_by(postcode == postcode)) %>%
  left_join(tbl(con, "ca_tenure_lsoa_tbl") %>% collect(),
            by = join_by(lsoa21 == lsoacd)) %>% 
  select(lmk_key, # include as otherwise duplicate values occur and these are removed by ODS
         local_authority,
         property_type,
         transaction_type,
         tenure,
         walls_description,
         roof_description,
         walls_energy_eff,
         roof_energy_eff,
         mainheat_description,
         mainheat_energy_eff,
         mainheat_env_eff,
         main_fuel,
         solar_water_heating_flag,
         construction_age_band,
         current_energy_rating,
         potential_energy_rating,
         co2_emissions_current,
         co2_emissions_potential,
         co2_emiss_curr_per_floor_area,
         number_habitable_rooms,
         number_heated_rooms,
         photo_supply,
         total_floor_area,
         building_reference_number,
         built_form,
         lsoa21,
         msoa21,
         lat,
         long,
         imd,
         total,
         owned,
         social_rented,
         private_rented,
         date,
         year,
         month
         ) %>% 
  mutate(         # imd decile 1= most deprived
    n_imd_decile = if_else(!is.na(imd), cut(imd,
                                            breaks = imd_deciles,
                                            labels = FALSE,
                                            include.lowest = TRUE
    ),
    NA_integer_),
    n_nominal_construction_date = map_int(construction_age_band, get_mid_age),
    # tenure =   case_when(
    #   str_detect(tenure, "wner-occupied") ~ "Owner occupied",
    #   str_detect(tenure, "social") ~ "Social rented",
    #   str_detect(tenure, "private") ~ "Private rented",
    #   .default = "Unknown"
    # ),
    tenure = recode_char(tenure,
                         "owner-occupied" = "Owner occupied",
                         "Owner-occupied" = "Owner occupied",
                         "Rented (social)" = "Social rented",
                         "rental (social)" = "Social rented",
                         "private" = "Private rented",
                         "rental (private)" = "Private rented",
                         "Rented (private)" =  "Private rented",
                         default = "Unknown"),
    construction_epoch = make_epochs(n_nominal_construction_date)
    )

lep_epc_domestic_tbl %>% glimpse()

tabyl(lep_epc_domestic_tbl, construction_epoch, n_nominal_construction_date)

nom_tbl <- lep_epc_domestic_tbl %>% 
  transmute(nom_list = map(construction_age_band, ~str_extract_all(.x, "\\b\\d{4}\\b") %>%
           # unlist() %>%
           pluck(1) %>% 
           as.integer() %>% 
             mean(na.rm = TRUE) ))


nom_tbl %>% glimpse()

table(lep_epc_domestic_tbl$tenure) %>% 
enframe()

# map_chr(tenure, ~make_sensible_tenure(.x)



epc_source_1 <- lep_epc_domestic_tbl %>% 
  mutate(d_roof_good = if_else(str_detect(roof_energy_eff, "Good"), 1, 0),
         d_roof_poor = if_else(str_detect(roof_energy_eff, "Poor"), 1, 0),
         d_walls_good = if_else(str_detect(walls_energy_eff, "Good"), 1, 0),
         d_walls_poor = if_else(str_detect(walls_energy_eff, "Poor"), 1, 0),
         d_mf_electricity = if_else(str_detect(main_fuel, "lectricity"), 1, 0),
         d_mf_gas = if_else(str_detect(main_fuel, "mains gas"), 1, 0),
         d_mf_oil = if_else(str_detect(main_fuel, "oil"), 1, 0),
         d_epc_rent_social = if_else(str_detect(tenure, "social"), 1, 0),
         d_epc_owned = if_else(str_detect(tenure, "wn"), 1, 0),
         d_epc_rent_private = if_else(str_detect(tenure, "private"), 1, 0),
         built_form = na_if(built_form, "NO DATA!"),
         d_terrace = if_else(str_detect(built_form, "Terrace"),
                             1, 0),
         d_detached  = if_else(built_form == "Detached",
                               1, 0),
         d_semi_detached = if_else(built_form == "Semi-Detached",
                                   1, 0)) %>% 
  rename(n_dwell_lsoa = total)

epc_source_2 <- epc_source_1 %>% 
  sjmisc::to_dummy(property_type,
           current_energy_rating,
           suffix = "label") %>% 
  rename_with(~paste0("d_", .x)) %>% 
  bind_cols(epc_source_1) %>% 
  clean_names() %>% 
  map(remove_attributes) %>% # remove attributes
  bind_rows()


epc_pc_tbl <- epc_source_2 %>% 
  group_by(local_authority, lsoa21) %>% 
  summarise(across(starts_with("d_"), ~sum(.x, na.rm = TRUE) * 100/ n()) %>% round(1),
            across(starts_with("n_"), ~mean(.x, na.rm = TRUE) %>% round()),
            .groups = "drop") %>% 
  rename_with(~if_else(str_starts(.x, "d_"),
                       paste0(str_remove(.x, "d_"), "_percent"),
                             .x))
epc_pc_tbl %>% glimpse()


epc_sum_tbl <- epc_source_2 %>% 
  group_by(local_authority, lsoa21) %>% 
  summarise(across(starts_with("d_"), ~sum(.x, na.rm = TRUE)),
            epc_properties_count = n(),
            .groups = "drop") %>% 
  rename_with(~if_else(str_starts(.x, "d_"),
                       paste0(str_remove(.x, "d_"), "_sum"),
                       .x))
epc_sum_tbl %>% glimpse()

epc_final_tbl <- epc_pc_tbl %>% 
  inner_join(epc_sum_tbl, by = join_by(lsoa21 == lsoa21,
                                       local_authority == local_authority)) %>% 
  rename_with(~str_remove(.x, "n_"))

epc_final_tbl %>% write_csv("data/epc_final_tbl.csv", na = "")

epc_final_tbl %>% glimpse()


epc_final_tbl <- read_csv("data/epc_final_tbl.csv")

epc_final_tbl %>% 
  summarise(epc_count = sum(epc_properties_count),
            dwellings = sum(dwell_lsoa, na.rm = TRUE))


pcd_lu_tbl <- read_csv("data/postcode_lookup/PCD_OA21_LSOA21_MSOA21_LAD_AUG23_UK_LU.csv")

lsoa_geogs_tbl <- pcd_lu_tbl %>% 
  filter(ladcd %in% lep_codes) %>% 
  group_by(lsoacd = lsoa21cd) %>% 
  summarise(msoacd = first(msoa21cd),
            ladnm = first(ladnm),
            ladcd = first(ladcd)) 


lsoa_ons <- st_read("data/Lower_layer_Super_Output_Areas_2021_EW_BFC_V8_9148850466833614344.gpkg", geometry_column = "SHAPE") %>% st_transform(crs = 4326)


st_crs(lsoa_ons)


lep_lsoa <- lsoa_ons %>% 
  filter(LSOA21CD %in% epc_final_tbl$lsoa21) %>% 
  inner_join(lsoa_geogs_tbl, by = join_by(LSOA21CD == lsoacd)) %>% 
  st_write("data/geojson/lep_lsoa_geog.geojson", append = FALSE)

lsoa_ons %>% 
  inner_join(epc_final_tbl, by = join_by(LSOA21CD == lsoa21)) %>% 
  st_write("data/geojson/lsoa_ons.geojson")


lsoa_poly <- st_read("data/geojson/ca_lsoa_poly_wgs84.geojson") %>% 
  st_transform(crs = 4326)

lsoa_out <- lsoa_poly %>% 
  inner_join(epc_final_tbl,
             by = join_by(lsoacd == lsoa21)) %>% 
  st_transform(crs = 4326)

lsoa_out %>% 
  st_write("data/geojson/lsoa_epc_out.geojson", append = FALSE)

dbListTables(con)

lsoa_poly_sf <- tbl(con, "lsoa_poly_tbl") %>% 
  collect() 


lsf <- st_read(con, "lsoa_poly_tbl", EWKB = FALSE)

# export cleaned EPC property data as point data to ODS
lep_epc_domestic_point_ods_tbl <- lep_epc_domestic_tbl %>% 
  # mutate(lat_jitter = lat + add_runif(lat, latlon = "lat"),
  #        long_jitter = long + add_runif(long, latlon = "long"),
  #        lat = NULL,
  #        long = NULL) %>% 
  left_join(lep_la_tbl %>% 
              select(ladcd, ladnm),
            by = join_by(local_authority == ladcd)) %>% 
  rownames_to_column()

lep_epc_domestic_point_ods_tbl %>% glimpse()

# a demo table for the report

lep_epc_domestic_point_ods_tbl %>% 
  filter(!is.na(construction_age_band)) %>% 
  group_by(`Construction Age Band` = construction_age_band,
           `EPC Rating` = current_energy_rating
           ) %>% 
  summarise(properties = n(), .groups = "drop") %>% 
  pivot_wider(id_cols = `Construction Age Band`,
              names_from =`EPC Rating`,
              values_from = properties) %>% 
  write_csv("data/age_epc_table.csv", na = "")

# ODS output for the pojnt data

lep_epc_domestic_point_ods_tbl %>% 
  write_csv("data/lep_epc_domestic_point_ods_tbl.csv", na = "")


# records disappearing demo data
  sample_norownames <- lep_epc_domestic_point_ods_tbl %>% 
  # rownames_to_column() %>% 
  head(1000) 

sample_rownames <- lep_epc_domestic_point_ods_tbl %>% 
  rownames_to_column() %>% 
  head(1000) 


sample_norownames %>% 
  write_csv("data/sample_norownames.csv", na = "")

sample_rownames %>% 
  write_csv("data/sample_rownames.csv", na = "")


dbDisconnect(con, shutdown=TRUE)


#-------------------------



