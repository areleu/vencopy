# %%
# # VencoPy Tutorial 2
# This tutorial aims to give a more in depth overview into the DataParser class
# and showcases some features that can be customised.

from pathlib import Path

from vencopy.core.dataparsers import parse_data
from vencopy.utils.utils import load_configs, create_output_folders

# %%
base_path = Path.cwd() / "vencopy"
configs = load_configs(base_path)
create_output_folders(configs=configs)

# Adapt relative paths in config for tutorials
configs['dev_config']['global']['relative_path']['parse_output'] = Path.cwd() / "vencopy" / configs['dev_config']['global']['relative_path']['parse_output']
configs['dev_config']['global']['relative_path']['diary_output'] = Path.cwd() / "vencopy" / configs['dev_config']['global']['relative_path']['diary_output']
configs['dev_config']['global']['relative_path']['grid_output'] = Path.cwd() / "vencopy" / configs['dev_config']['global']['relative_path']['grid_output']
configs['dev_config']['global']['relative_path']['flex_output'] = Path.cwd() / "vencopy" / configs['dev_config']['global']['relative_path']['flex_output']
configs['dev_config']['global']['relative_path']['aggregator_output'] = Path.cwd() / "vencopy" / configs['dev_config']['global']['relative_path']['aggregator_output']
configs['dev_config']['global']['relative_path']['processor_output'] = Path.cwd() / "vencopy" / configs['dev_config']['global']['relative_path']['processor_output']

# Set reference dataset
dataset_id = 'MiD17'

# Modify the localPathConfig file to point to the .csv file in the sampling
# folder in the tutorials directory where the dataset for the tutorials lies.
configs['user_config']['global']['absolute_path'][dataset_id] = Path.cwd() / 'tutorials' / 'data_sampling'

# Similarly we modify the dataset_id in the global config file
configs['dev_config']['global']['files'][dataset_id]['trips_data_raw'] = dataset_id + '.csv'

# We also modify the parseConfig by removing some of the columns that are
# normally parsed from the MiD, which are not available in our semplified test
# dataframe
del configs['dev_config']['dataparsers']['data_variables']['household_id']
del configs['dev_config']['dataparsers']['data_variables']['person_id']

# %%
# ## DataParser config file

# The DataParser config file defines which variables are to be parsed (i.e. the
# ones needed to create trip diaries and calculate fleet flexibility) and sets
# some filtering options, such as the conditions for trips to be included of
# excluded from the parsing.

configs['dev_config']

# %%
# ## _DataParser_ class

# Let's first run the class and see the outputs we get.
data = parse_data(configs=configs)
data.process()

# %%
# We can see from the print statements in the class that after reading in the
# initial dataset, which contained 2124 rows, and applying 8 filters, we end up
# with a database containing 1980 suitable entries, which corresponds to about
# 93% of the initial sample. These trip respect the condition that they all need
# to be shorter than 1000km, which is set in the parseConfig under the
# 'filterDict' key. Now we can, for example, change in the filters the maximum
# allowed trip distance from 1000km to 50km and see how this affects the
# resulting available trips (the extreme case of 50km is only used for the
# tutorial purpose).

configs['dev_config']['dataparsers']['filters'][dataset_id]['smaller_than']['trip_distance'] = [50]

# %%
data = parse_data(configs=configs)
data.process()

# %% We can see how with a maximum trip distance of 1000km, all filters combined
# yield a total of 1980 trips, which corresponds to about 93% of the original
# dataset. By changing this values to 50km, additional 82 trips have been
# excluded, resulting in 1898 trips (89% ofthe initial dataset).

# ## Next Steps
# In the next tutorial, you will learn more in detail the internal workings of
# the FlexEstimator class and how to customise some settings.
