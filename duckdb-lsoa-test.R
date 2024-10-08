pacman::p_load(tidyverse,
               fastverse,
               janitor,
               glue,
               duckdb,
               sf
               )

con <- dbConnect(duckdb(),
                 dbdir = "data/test_arcgis_2000.duckdb")

# the spatial extension needs installing and loading explicitly
con |> 
  dbExecute("INSTALL SPATIAL;")

con |> 
  dbExecute("LOAD SPATIAL;")
con %>% 
  dbListTables()

# turn the raw geometry data into well known text with the query
query <- "SELECT *, ST_AsText(geom) AS geom_wkt FROM lsoa_poly_2021_cauth_tbl"

sf_data <- con |> 
  dbGetQuery(query) |> 
  # create an SF column of geometry from the well known text column
  mutate(geometry = st_as_sfc(geom_wkt, crs = 4326)) |> 
  # read the tbl into sf spatial format
  st_as_sf() |> 
  # remove the redundant columns
  mutate(geom = NULL, geom_wkt = NULL)

# this writes valid geojson as output

con %>% dbDisconnect()
