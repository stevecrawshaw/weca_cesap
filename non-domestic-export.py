# %%
import duckdb
import polars as pl

#%%
con = duckdb.connect('data/ca_epc.duckdb')


# %%
con.sql('show tables')
#%%
list[con.sql('''
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'epc_non_domestic_tbl'
        '''
        )]

# %%
con.sql('SELECT lat, long FROM postcode_centroids_tbl LIMIT 5')


# %%
con.sql('DESCRIBE postcode_centroids_tbl')


# %%
# create a view for the non-domestic EPC data which includes just the local authorities in the West of England and the postcode centroids
con.sql("""
        CREATE OR REPLACE VIEW epc_non_domestic_ods_vw AS
        (SELECT * FROM epc_non_domestic_tbl
        INNER JOIN ca_la_tbl 
        ON epc_non_domestic_tbl.local_authority = ca_la_tbl.ladcd
        INNER JOIN 
        
        (SELECT pcds, lsoa21, lat, long 
        FROM postcode_centroids_tbl) as p

        ON epc_non_domestic_tbl.postcode = p.pcds

        WHERE ca_la_tbl.ladnm 
        IN ('Bristol, City of',
        'Bath and North East Somerset',
        'North Somerset',
        'South Gloucestershire'));
        """)
#%%
revised_non_dom_qry = """
 CREATE OR REPLACE VIEW epc_non_domestic_ods_vw AS
        SELECT
                epc_non_domestic_tbl.* EXCLUDE postcode,
                ca_la_tbl.*,
                p.lsoa21,
                p.lat,
                p.long
        FROM epc_non_domestic_tbl

INNER JOIN
        ca_la_tbl 
        ON epc_non_domestic_tbl.local_authority = ca_la_tbl.ladcd
INNER JOIN 
        
        (SELECT pcds, lsoa21, lat, long 
        FROM postcode_centroids_tbl) as p
        ON epc_non_domestic_tbl.postcode = p.pcds
WHERE ca_la_tbl.ladnm 
        IN ('Bristol, City of',
        'Bath and North East Somerset',
        'North Somerset',
        'South Gloucestershire');
"""

#%%

con.execute(revised_non_dom_qry)

#%%

con.sql('show tables')

#%%

con.sql('SELECT * FROM epc_non_domestic_ods_vw LIMIT 5').pl().glimpse()

# %%
# check numbers missing in the view
dr = con.sql("""
        SELECT * FROM epc_non_domestic_tbl
        INNER JOIN ca_la_tbl ON epc_non_domestic_tbl.local_authority = ca_la_tbl.ladcd
        INNER JOIN 
        (SELECT pcds, lsoa21, lat, long FROM postcode_centroids_tbl) as p 
        ON epc_non_domestic_tbl.postcode = p.pcds
        WHERE ca_la_tbl.ladnm IN ('Bristol, City of', 'Bath and North East Somerset', 'North Somerset', 'South Gloucestershire');
        """).pl()

dr.glimpse()

# %%
con.sql('show tables')

# %%
# elide the postcode, pcds and uprn columns from the view and export the data to a csv file
con.sql(
    """
    COPY (SELECT * EXCLUDE (postcode, pcds) FROM epc_non_domestic_ods_vw) TO 'data/non_domestic_epc.csv' (HEADER, DELIMITER ',');"""
    )

# %%
con.close()

# %%
epc_nd_df = pl.read_csv('data/non_domestic_epc.csv')
epc_nd_df.glimpse()
# %%
