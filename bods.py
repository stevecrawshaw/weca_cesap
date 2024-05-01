# %%
import importlib
import io
import yaml
import polars as pl
from BODSDataExtractor.extractor import TimetableExtractor
from BODSDataExtractor import otc_db_download

# %%

# Open the YAML file storing credentials
with open('../config.yml', 'r') as file:
    # Load the YAML content
    data = yaml.safe_load(file)
# %%

apikey = data['bods']['apikey']

 # %%

my_bus_data_object = TimetableExtractor(api_key=apikey 
                                 ,limit=100000 # How many datasets to view
                                 ,status = 'published' # Only view published datasets
                                 ,service_line_level=True # True if you require Service line data
                                 , atco_code=['010', '017', '018', '019'] 
                                 #, nocs=['FBRI']
                                 ,stop_level=True # True if you require stop level data
                                 )

# %%

#save the extracted dataset level data to dataset_level variable
dataset_level = my_bus_data_object.metadata
# %%

type(dataset_level)

# %%
#save this data to a csv file in your downloads directory
#note that entries in the 'localities' field may be truncated to comply with excel cell limits
my_bus_data_object.save_metadata_to_csv()
# %%

bdo_sll = TimetableExtractor(api_key=apikey 
                                 ,limit=1000 # How many datasets to view
                                 ,status = 'published' # Only view published datasets
                                 ,service_line_level=True # True if you require Service line data 
                                 ,stop_level=False # True if you require stop level data
                                 )

service_line_level = bdo_sll.service_line_extract

# %%

service_line_level.to_csv('data/service_line_level.csv')

# %%
print(services_df)


# %%

#note that in downloading the service line level data, the dataset level will also be downloaded. Can access this as below:
dataset_level = bdo_sll.metadata
# %%
#save these data to a csv file in your downloads directory
bdo_sll.save_metadata_to_csv()
bdo_sll.save_service_line_extract_to_csv()
# %%

#local_otc = otc_db_download.save_otc_db() # download and save a copy of the otc database, as well as assigning it to the 'otc' variable

otc = otc_db_download.fetch_otc_db() #assign a copy of the otc database to the 'otc' variable
# %%
otc.to_csv('data/otc.csv')

# %%

count_of_operators = bdo_sll.count_operators() #returns count of distinct operators (measured by operator_name) in a chosen dataset
 # %%
count_of_service_codes = bdo_sll.count_service_codes()# returns count of unique service codes chosen dataset

# %%

valid_service_codes = bdo_sll.valid_service_codes()# returns count of unique and valid service codes chosen dataset, a dataframe with all the records with valid service codes and a dataframe with all the invalid service codes.
# %%

services_published_in_TXC_2_4 = bdo_sll.services_published_in_TXC_2_4()#returns percentage of services published in TXC 2.4 schema, and a dataframe of these records, and a dataframe of the records that are not published in this schema
# %%
datasets_published_in_TXC_2_4 = bdo_sll.datasets_published_in_TXC_2_4()# returns percentage of datasets published in TXC 2.4 schema, and a dataframe of these records, and a dataframe of the records that are not published in this schema
# %%
red_dq = bdo_sll.red_dq_scores() #returns the number of operators in a table with red dq scores
# %%
less_than_90 = bdo_sll.dq_less_than_x(90) # takes an integer as input (in this case 90) and returns a list of operators with a data quality score less than that integer
# %%
no_lic_no = bdo_sll.no_licence_no() # returns a report listing which datasets contain files which do not have a licence number
# %%
#import the csv file as a text string from the BODSDataExtractor package
atco_lookup_file = importlib.resources.read_text('BODSDataExtractor','ATCO_code_to_LA_lookup.csv')
 # %%
#wrap lookup_file string into a stringIO object, so it can be read by pandas
atco_lookup_string = io.StringIO(atco_lookup_file)
# %%

#load into a DataFrame
la_lookup = pl.read_csv(atco_lookup_string , dtypes={'ATCO Code':str})

# %%
la_lookup.write_csv('ATCO_code_to_LA_lookup.csv')


# %%

services_by_area = bdo_sll.services_on_bods_or_otc_by_area() #Generates a dataframe of all service codes published on BODS and/or in the OTC database, indicates whether they are published on both or one of BODS and OTC, and provides the admin area the services has stops within
services_by_area.to_csv('services_by_area.csv')
# %%
services_by_area_MI = bdo_sll.services_on_bods_or_otc_by_area_mi() #Generates MI from dataframe that lists all service codes from BODS and/or OTC database, by admin area. Specifically notes the number of services from these sources, and the fraction of all within BODS and OTC respectively.
# %%
percent_published_licences = bdo_sll.percent_published_licences() #percentage of registered licences with at least one published service
# %%
unpublished_services = bdo_sll.registered_not_published_services() #feed in a copy of the BODS timetable data and otc database to return a list of unpublished service codes
# %%
published_not_registered_services = bdo_sll.published_not_registered_services()#returns a dataframe of services found in the published data from the api which are not found in the otc database
# %%
