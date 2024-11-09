#%%
import duckdb
import polars as pl

#%%
epc_bris_url = r"https://westofenglandca.opendatasoft.com/api/explore/v2.1/catalog/datasets/lep-epc-domestic-point/exports/csv?lang=en&refine=ladnm%3A%22Bristol%2C%20City%20of%22&facet=facet(name%3D%22ladnm%22%2C%20disjunctive%3Dtrue)&timezone=Europe%2FLondon&use_labels=false&delimiter=%2C"

#%%
con = duckdb.connect(database=':memory:', read_only=False)
con.execute("INSTALL httpfs")
con.execute("LOAD httpfs")
#%%
con.read_csv(epc_bris_url).to_table("epc")
#%%
con.sql("SUMMARIZE TABLE epc;")

#%%
epc_table = con.table("epc")

#%%

epc_table.count("*")


#%%
epc_table.columns

#%%

(epc_table
 .project("current_energy_rating, main_fuel, co2_emissions_current")
 .filter("co2_emissions_current > 10")
 .aggregate("current_energy_rating, CAST(avg(co2_emissions_current) AS int) AS avg_co2")
 .show())

#%%