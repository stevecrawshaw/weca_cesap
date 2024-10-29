#%%
import polars as pl
import duckdb
#%%

con = duckdb.connect("data/epc.duckdb", read_only=False)

#%%

con.sql("SHOW TABLES;")

#%%
con.sql("CREATE OR REPLACE VIEW epc AS SELECT * FROM certificates;")

#%%

con.sql("FROM epc;")

#%%
con.sql("DESCRIBE epc;")
#%%
qry_day = """
CREATE OR REPLACE VIEW epc_day AS
(SELECT DISTINCT *,
    date_part('day', LODGEMENT_DATETIME)
        AS LODGEMENT_DAY,
    date_part('month', LODGEMENT_DATETIME)
        AS LODGEMENT_MONTH,
    date_part('year', LODGEMENT_DATETIME)
        AS LODGEMENT_YEAR
    FROM epc);
"""

con.sql(qry_day)

#%%

latest_epc = """
CREATE OR REPLACE VIEW latest_certificates AS
SELECT c.*,
    date_part('day', c.LODGEMENT_DATETIME)
        AS LODGEMENT_DAY,
    date_part('month', c.LODGEMENT_DATETIME)
        AS LODGEMENT_MONTH,
    date_part('year', c.LODGEMENT_DATETIME)
        AS LODGEMENT_YEAR
FROM certificates c
INNER JOIN (
    SELECT UPRN, MAX(LODGEMENT_DATETIME) as max_date
    FROM certificates
    GROUP BY UPRN
) latest ON c.UPRN = latest.UPRN 
    AND c.LODGEMENT_DATETIME = latest.max_date;
"""

con.sql(latest_epc)
#%%
con.sql("SUMMARIZE latest_certificates;")

#%%
con.sql("SUMMARIZE epc_day;")

#%%
qry_con_age = """
CREATE OR REPLACE VIEW construction_age AS
SELECT DISTINCT CONSTRUCTION_AGE_BAND
FROM certificates;
"""

con.sql(qry_con_age)

# %%


#%%

qry_clean_ages = """
SELECT 
    CONSTRUCTION_AGE_BAND,
    REGEXP_REPLACE(CONSTRUCTION_AGE_BAND, '[A-Za-z\\s:!]', '', 'g') AS cleaned_string,
    CAST(
        CASE 
            WHEN REGEXP_REPLACE(CONSTRUCTION_AGE_BAND, '[A-Za-z\\s:!]', '', 'g') = '' THEN NULL
            WHEN LOWER(CONSTRUCTION_AGE_BAND) LIKE '%before%' THEN 1899
            WHEN REGEXP_REPLACE(CONSTRUCTION_AGE_BAND, '[A-Za-z\\s:!]', '', 'g') LIKE '%-%' 
            THEN (
                CAST(SPLIT_PART(REGEXP_REPLACE(CONSTRUCTION_AGE_BAND, '[A-Za-z\\s:!]', '', 'g'), '-', 1) AS INTEGER) +
                CAST(SPLIT_PART(REGEXP_REPLACE(CONSTRUCTION_AGE_BAND, '[A-Za-z\\s:!]', '', 'g'), '-', 2) AS INTEGER)
            ) / 2
            ELSE CAST(REGEXP_REPLACE(CONSTRUCTION_AGE_BAND, '[A-Za-z\\s:!]', '', 'g') AS INTEGER)
        END AS INTEGER
    ) AS derived_year
FROM construction_age
WHERE CONSTRUCTION_AGE_BAND IS NOT NULL
AND REGEXP_REPLACE(CONSTRUCTION_AGE_BAND, '[A-Za-z\\s:!]', '', 'g') != '';
"""
con.sql(qry_clean_ages)
#%%
con_age_pldf = con.sql(qry_clean_ages).pl()
con_age_pldf.write_csv("data/construction_age.csv")
# %%
