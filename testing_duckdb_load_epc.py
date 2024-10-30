# %%
# use .venv - make sure to restart jupyter kernel.
import get_ca_data as get_ca # functions for retrieving CA \ common data

download_epc = False
# %%
ca_la_df = get_ca.get_ca_la_df(2024, inc_ns = True) # include NS

if download_epc:
    la_zipfile_nondom_list = get_ca.make_zipfile_list(ca_la_df, type = "non-domestic")
    get_ca.dl_bulk_epc_zip(la_zipfile_nondom_list, path = "data/epc_bulk_nondom_zips")
    get_ca.extract_and_rename_csv_from_zips("data/epc_bulk_nondom_zips")
    la_zipfile_list = get_ca.make_zipfile_list(ca_la_df, type = "domestic")
    get_ca.dl_bulk_epc_zip(la_zipfile_list, path = "data/epc_bulk_zips")
    get_ca.extract_and_rename_csv_from_zips("data/epc_bulk_zips")

# %%
con = get_ca.connect_duckdb("data/ca_epc.duckdb")

#%%
# create the certificates table according to the schema in the sql file
get_ca.load_csv_duckdb(con = con,
                       csv_path="data/epc_bulk_nondom_zips",
                       schema_file="nondom_certificates_schema.sql",
                       table_name="nondom_certificates",
                       schema_cols = get_ca.cols_schema_nondom)

#%%
# now domestic certs
get_ca.load_csv_duckdb(con = con,
                       csv_path="data/epc_bulk_zips",
                       schema_file="certificates_schema.sql",
                       table_name="domestic_certificates",
                       schema_cols=get_ca.cols_schema_domestic)


#%%
