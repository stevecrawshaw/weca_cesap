# %%
import yaml
import polars as pl

# %%

# Open the YAML file
with open('../config.yml', 'r') as file:
    # Load the YAML content
    data = yaml.safe_load(file)
# %%

apikey = data['bods']['apikey']

# %%

#intiate an object instance called my_bus_data_object with desired parameters
from BODSDataExtractor.extractor import TimetableExtractor
 # %%

my_bus_data_object = TimetableExtractor(api_key=apikey 
                                 ,limit=100000 # How many datasets to view
                                 ,status = 'published' # Only view published datasets
                                 ,service_line_level=False # True if you require Service line data 
                                 ,stop_level=False # True if you require stop level data
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
