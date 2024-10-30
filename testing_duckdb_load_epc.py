# %%
# use .venv - make sure to restart jupyter kernel.
import duckdb
import get_ca_data as get_ca # functions for retrieving CA \ common data
import glob
import os
from epc_schema import cols_schema_domestic, cols_schema_nondom # schema for the EPC certificates

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
con = duckdb.connect('data/ca_epc.duckdb')
# %%

def load_csv_duckdb(con,
                    csv_path,
                    schema_file, 
                    table_name: str = 'domestic_certificates',
                    schema_cols: str = cols_schema_domestic):
    """
    Load CSV files into a DuckDB database.
    This function reads a schema from a specified file and uses it to create a table in the DuckDB database.
    It then imports all CSV files from a given directory into the created table.
    Parameters:
    con (duckdb.DuckDBPyConnection): The DuckDB connection object.
    csv_path (str): The path to the directory containing the CSV files to be loaded.
    schema_file (str): The path to the file containing the schema definition for the table.
    Raises:
    Exception: If there is an error during the import of any CSV file, an exception is caught and an error message is printed.
    """
    with open(schema_file, 'r') as f:
        con.execute(f.read())

    csv_files = glob.glob(f'{csv_path}/*.csv')
    if not csv_files:
        print(f"No CSV files found in {csv_path}")
        return None
    
    for file in csv_files:
        try:
            print(f"Importing {os.path.basename(file)}...")
            con.execute(f"""
                    INSERT INTO {table_name} 
                    SELECT * FROM read_csv(?, 
                                         header=true,
                                         auto_detect=false,
                                         columns= ?,
                                         parallel=true,
                                         filename = ?)
                """, [file, schema_cols, csv_path])
        except Exception as e:
            print(f"Error importing {file}: {str(e)}")
#%%
# create the certificates table according to the schema in the sql file
load_csv_duckdb(con = con,
                       csv_path="data/epc_bulk_nondom_zips",
                       schema_file="nondom_certificates_schema.sql",
                       table_name="nondom_certificates",
                       schema_cols=cols_schema_nondom)

#%%
# now domestic certs
load_csv_duckdb(con = con,
                       csv_path="data/epc_bulk_zips",
                       schema_file="certificates_schema.sql",
                       table_name="domestic_certificates",
                       schema_cols=cols_schema_domestic)


#%%
