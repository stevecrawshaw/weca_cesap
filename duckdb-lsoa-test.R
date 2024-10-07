pacman::p_load(tidyverse,
               fastverse,
               janitor,
               glue,
               duckdb,
               sf
               )

con <- dbConnect(duckdb(), dbdir = "data/test_arcgis_2000.duckdb")

con %>% 
  dbListTables()

con |> 
  dbSendQuery("INSTALL SPATIAL;")

con |> 
  dbSendQuery("LOAD SPATIAL;")

con |> 
  dbGetQuery("SELECT * FROM ST_Read('lsoa_poly_2021_cauth_tbl')")

t <- st_read(con,
             query = "SELECT * FROM lsoa_poly_2021_cauth_tbl",
             geometry_column = "geom")

t$geometry <- st_as_sfc(t$geom, crs = 4326)

st_as_sf(t, wkt = "geometry_raw")

t |> head()

tbl("lsoa_poly_2021_cauth_tbl") |> 
  # select(geom)
st_as_sf(sf_column_name = "geom")


con %>% dbDisconnect()
con

creds <- config::get(file = "../config.yml", config = "weca_gis")[c(-1)]  
tryCatch({
      print("Connecting to GIS Database…")
      con <- dbConnect(RPostgres::Postgres(),
      host = creds$hostname,
      dbname = creds$database,
      user = creds$uid,
      password = creds$pwd)
      print("Database Connected! :D")
},
error=function(cond) {
    print("Unable to connect to GIS Database. Check VPN")
})
  all_schemas <- dbGetQuery(con,
  "SELECT schema_name FROM information_schema.schemata")
  ne_tables <- dbGetQuery(con,
  "SELECT table_name FROM information_schema.tables WHERE table_schema='natural_england'")
  sssi <- st_read(con,
  query = "SELECT * FROM natural_england.sssi")

