#%%
import polars as pl
import duckdb

#%% [markdown]
# # Testing how to create a view which can export the dataset for lep-epc-domestic-point
#%%

con = duckdb.connect('data/ca_epc.duckdb')

#%%

con.sql("SHOW ALL TABLES;")
#%%

# %%
con.sql('DESCRIBE epc_domestic_tbl')

#%%
con.sql('FROM epc_domestic_tbl LIMIT 5').pl().glimpse()
#%%
con.sql('DESCRIBE postcode_centroids_tbl')

#%%
con.sql('DESCRIBE ca_la_tbl')

#%%

con.sql('''
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'postcode_centroids_tbl'
        '''
        ).pl()

#%%
con.sql('SELECT DISTINCT(cauthnm) FROM ca_la_tbl')

#%%

con.sql('''
        SELECT ladcd FROM ca_la_tbl
        WHERE cauthnm = \'West of England\' 
        '''
        )
#%%

con.sql('DESCRIBE postcode_centroids_tbl')
# %%
create_view_qry = '''
CREATE OR REPLACE VIEW epc_lep_domestic_ods_vw AS

SELECT   ROW_NUMBER() OVER (ORDER BY lmk_key) AS rowname,
         lmk_key,
         local_authority,
         property_type,
         transaction_type,
         tenure_epc as tenure,
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
         month,
         imd_decile as n_imd_decile,
         n_nominal_construction_date,
         CASE WHEN n_nominal_construction_date < 1900 THEN 'Before 1900'
         WHEN (n_nominal_construction_date >= 1900) AND (n_nominal_construction_date <= 1930) THEN '1900 - 1930'
         WHEN n_nominal_construction_date > 1930 THEN '1930 to present'
         ELSE 'Unknown' END AS construction_epoch,
         ca_la_tbl.ladnm as ladnm,
        FROM epc_domestic_tbl

        LEFT JOIN postcode_centroids_tbl ON epc_domestic_tbl.postcode = postcode_centroids_tbl.pcds

        LEFT JOIN ca_tenure_lsoa_tbl ON postcode_centroids_tbl.lsoa21 = ca_tenure_lsoa_tbl.lsoacd

        LEFT JOIN ca_la_tbl 
        ON ca_la_tbl.ladcd = epc_domestic_tbl.local_authority

        LEFT JOIN imd_tbl
        ON ca_tenure_lsoa_tbl.lsoacd = imd_tbl.lsoacd

        WHERE local_authority IN 
        (SELECT ladcd
        FROM ca_la_tbl
        WHERE cauthnm = \'West of England\')
'''
#%%
# SQL for a non materialised view of EPC data
con.execute(create_view_qry)

#%%
con.sql('FROM epc_lep_domestic_ods_vw LIMIT 10').pl().glimpse()
#%%
con.sql('SELECT COUNT(*) num_rows FROM lsoa_pwc_tbl')
#%%
#check
ods_url = "https://opendata.westofengland-ca.gov.uk/api/explore/v2.1/catalog/datasets/lep-epc-domestic-point/exports/csv?limit=10&timezone=UTC&use_labels=false&epsg=4326"

#%%

pl.read_csv(ods_url, separator=";").glimpse()

# %%
con.close()
# %%
