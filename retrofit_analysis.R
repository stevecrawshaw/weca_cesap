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




con <- DBI::dbConnect(duckdb(), dbdir = "data/ca_epc.duckdb")

# the EPC data is now within a view in the duckdb database
# calculations for construction epoch and the joins with other tables 
# are much faster than doing it in R
# The cleaning process are done in Polars and are much more performant than R

lep_epc_domestic_point_ods_tbl <- tbl(con, "epc_lep_domestic_ods_vw") %>% 
  collect()

lep_epc_domestic_point_ods_tbl %>% 
  write_csv("data/lep_epc_domestic_point_ods_tbl.csv", na = "")

# Create a function to remove attributes
remove_attributes <- function(x) {
  attributes(x) <- NULL
  return(x)
}

dbListTables(con)

tabyl(lep_epc_domestic_point_ods_tbl,
      construction_epoch,
      n_nominal_construction_date)

nom_tbl <- lep_epc_domestic_point_ods_tbl %>% 
  transmute(nom_list = map(construction_age_band, ~str_extract_all(.x, "\\b\\d{4}\\b") %>%
           # unlist() %>%
           pluck(1) %>% 
           as.integer() %>% 
             mean(na.rm = TRUE) ))


nom_tbl %>% glimpse()

table(lep_epc_domestic_tbl$tenure) %>% 
enframe()

# map_chr(tenure, ~make_sensible_tenure(.x)



epc_source_1 <- lep_epc_domestic_point_ods_tbl %>% 
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



