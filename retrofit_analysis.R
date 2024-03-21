pacman::p_load(tidyverse, # Data wrangling
               fastverse,
               glue, # Manipulating strings
               janitor, # general data cleaning
               duckdb,
               sf,
               fs,
               DBI,
               sjmisc
               
)

imd_least_dep <- 32844L
imd_deciles = seq(1, imd_least_dep, length.out = 11) %>% round()

con <- DBI::dbConnect(duckdb(), dbdir = "data/ca_epc.duckdb")

make_integer_age <- function(construction_age_band){

  # clean the construction band field
  # making a nominal date of construction where there is a range
  # 1900 means before 1900
  
mid_age <- function(age){
  
  if(!is.na(age)){
  if(str_detect(age, "-")){
    
  ints <- str_split(age, pattern = "-") %>% 
    pluck(1) %>% 
    map_dbl(as.numeric) %>% 
    round()
  
  (((ints[2] - ints[1]) / 2) + ints[1]) %>% round()
  } else {
    round(as.numeric(age), 0)
  }} else {
    NA_integer_
  }
  
  }

construction_age_band %>% 
  str_remove("England and Wales: ") %>% 
  str_remove("before ") %>% 
  str_remove(" onwards") %>% 
  str_remove("INVALID!") %>% 
  str_remove("NO DATA!") %>% 
  na_if("") %>% 
  map_int(mid_age)

}

ren_func <- function(x){
  if(str_starts(x, "d_")){
    y = paste(str_remove(x, "_d"), "_percent")
  } else {
    y = x
  }
  return(y)
}


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

pc_centroids_tbl <- read_csv("data/postcode_centroids.csv")

lep_postcodes_tbl <- tbl(con, "postcodes_tbl") %>% 
  filter(ladcd %in% lep_codes) %>% 
  collect() %>% 
  inner_join(pc_centroids_tbl, by = join_by(postcode == PCDS)) %>%
  clean_names() %>% 
  select(lsoacd, postcode, ladcd, lat, long, imd, msoacd)

unique(lep_postcodes_tbl$lsoacd) 


lep_epc_domestic_tbl <- tbl(con, "epc_domestic_tbl") %>%  
  filter(local_authority %in% lep_codes) %>% 
  collect() %>%
  left_join(lep_postcodes_tbl, 
            by = join_by(postcode == postcode)) %>%
  left_join(tbl(con, "ca_tenure_lsoa_tbl") %>% collect(),
            by = join_by(lsoacd == lsoacd)) %>% 
  select(local_authority,
         property_type,
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
         built_form,
         lsoacd,
         msoacd,
         lat,
         long,
         imd,
         total,
         owned,
         social_rented,
         private_rented
         ) 

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
         # imd decile 1= most deprived
         n_imd_decile = if_else(!is.na(imd), cut(imd,
                                            breaks = imd_deciles,
                                            labels = FALSE,
                                            include.lowest = TRUE
                                            ),
                           NA_integer_),
         n_nominal_construction_date = make_integer_age(construction_age_band),
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
  group_by(local_authority, lsoacd) %>% 
  summarise(across(starts_with("d_"), ~sum(.x, na.rm = TRUE) * 100/ n()) %>% round(1),
            across(starts_with("n_"), ~mean(.x, na.rm = TRUE) %>% round()),
            .groups = "drop") %>% 
  rename_with(~if_else(str_starts(.x, "d_"),
                       paste0(str_remove(.x, "d_"), "_percent"),
                             .x))
epc_pc_tbl %>% glimpse()

epc_sum_tbl <- epc_source_2 %>% 
  group_by(local_authority, lsoacd) %>% 
  summarise(across(starts_with("d_"), ~sum(.x, na.rm = TRUE)),
            epc_properties_count = n(),
            .groups = "drop") %>% 
  rename_with(~if_else(str_starts(.x, "d_"),
                       paste0(str_remove(.x, "d_"), "_sum"),
                       .x))

epc_final_tbl <- epc_pc_tbl %>% 
  inner_join(epc_sum_tbl, by = join_by(lsoacd == lsoacd,
                                       local_authority == local_authority)) %>% 
  rename_with(~str_remove(.x, "n_"))

epc_final_tbl %>% write_csv("data/epc_final_tbl.csv", na = "")

epc_final_tbl %>% glimpse()


epc_final_tbl <- read_csv("data/epc_final_tbl.csv")


pcd_lu_tbl <- read_csv("data/PCD_OA21_LSOA21_MSOA21_LAD_AUG23_UK_LU.csv")

lsoa_geogs_tbl <- pc_lu_tbl %>% 
  filter(ladcd %in% lep_codes) %>% 
  group_by(lsoacd = lsoa21cd) %>% 
  summarise(msoacd = first(msoa21cd),
            ladnm = first(ladnm),
            ladcd = first(ladcd)) 


lsoa_ons <- st_read("data/Lower_layer_Super_Output_Areas_2021_EW_BFC_V8_9148850466833614344.gpkg", geometry_column = "SHAPE") %>% st_transform(crs = 4326)


st_crs(lsoa_ons)


lep_lsoa <- lsoa_ons %>% 
  filter(LSOA21CD %in% epc_final_tbl$lsoacd) %>% 
  inner_join(lsoa_geogs_tbl, by = join_by(LSOA21CD == lsoacd)) %>% 
  st_write("data/geojson/lep_lsoa_geog.geojson", append = FALSE)

lsoa_ons %>% 
  inner_join(epc_final_tbl, by = join_by(LSOA21CD == lsoacd)) %>% 
  st_write("data/geojson/lsoa_ons.geojson")


lsoa_poly <- st_read("data/geojson/ca_lsoa_poly_wgs84.geojson") %>% 
  st_transform(crs = 4326)

lsoa_out <- lsoa_poly %>% 
  inner_join(epc_final_tbl,
             by = join_by(lsoacd == lsoacd)) %>% 
  st_transform(crs = 4326)

lsoa_out %>% 
  st_write("data/geojson/lsoa_epc_out.geojson")

dbListTables(con)

lsoa_poly_sf <- tbl(con, "lsoa_poly_tbl") %>% 
  collect() 


lsf <- st_read(con, "lsoa_poly_tbl", EWKB = FALSE)


lsoa_poly_sf$geometry <- sf::st_as_sfc(structure(as.list(lsoa_poly_sf$geom), class = "WKB")) 



dbDisconnect(con, shutdown=TRUE)

#-------------------------

vec <- 1:100
breaks = seq(1, 101, by = 10)

breaks

cut(vec, breaks = breaks, labels = FALSE, include.lowest = TRUE)

?cut





