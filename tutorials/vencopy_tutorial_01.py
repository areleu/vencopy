# %% [markdown]
# # Tutorial 1 venco.py

# This tutorial showcases the general structure and workflow of venco.py, as well as some basic features of its 6 main classes:
# - DataParser
# - GridModeller
# - FlexEstimator
# - DiaryBuilder
# - Aggregator
# - PostProcessor
# 
# All tutorials run on a very small subset of data from the 2017 German national travel survey (Mobilität in Deutschland (MiD17)), which might result in profiles having uncommon shapes. As such, the calculations and the examples proposed throughout all tutorials have the mere goal to exemplify the modelling steps and guide the use throughout the structure of venco.py and do not aim at providing an accurate quantification of demand-side flexibility from EVs.
# 
# For a more detailed description of venco.py, you can refer to the documentation at https://dlr-ve.gitlab.io/esy/vencopy/vencopy/

# ## Setting up the working space

# This section allows you to import all required Python packages for data input and manipulation. The function os.chdir(path) allows us to point Python towards the top most directory which contains all useful venco.py functions that are going to be used in the tutorials.
# Additionally, we set and read in the input dataframe (here the MiD17) and load the necessary yaml file, which contains some configuration settings.

# %%
import time
from pathlib import Path

# %%
from vencopy.core.dataparsers import parse_data
from vencopy.core.gridmodellers import GridModeller
from vencopy.core.flexestimators import FlexEstimator
from vencopy.core.diarybuilders import DiaryBuilder
from vencopy.core.profileaggregators import ProfileAggregator
from vencopy.core.postprocessors import PostProcessor
from vencopy.utils.utils import load_configs, create_output_folders

start_time = time.time()

# %% [markdown]
# We will have a look more in detail at each config file and what you can specify within it for each class throughtout the tutorials. For the time being, it is enough to know that the config files specify configurations, variable namings and settings for the different classes. There is one config file for each class, a global config and a local configuration config to specify eventual file paths on your machine.
# 

# %%
base_path = Path.cwd() / "vencopy"
configs = load_configs(base_path)

# %% [markdown]
# ## _DataParser_ class

# To be able to estimate EV electric consumption and flexibililty, the first step in the venco.py framework implies accessing a travel survey data set, such as the MiD. This is carried out through a parsing interface to the original database. In the parsing interface to the data set, three main operations are carried out: the read-in of the travel survey trip data, stored in .dta or .csv files, filtering and cleaning of the original raw data set and a set of variable replacement operations to allow the composition of travel diaries in a following step (in the DiaryBuilder class).
# 
# 
# In order to have consistent entry data for all variables and for different data sets, all database entries are harmonised, which includes generating unified data types and consistent variable naming. The naming convention for the variables and their respective input type can be specified in the venco.py config files that have been loaded previously.

# First off, we modify the localConfig and globalConfig files so that it point to the current working directory and to the database subset we will use to explain the different classes.
# 

# %%
# Adapt relative paths in config for tutorials
configs['dev_config']['global']['relative_path']['parse_output'] = Path.cwd() / "vencopy" / configs['dev_config']['global']['relative_path']['parse_output']
configs['dev_config']['global']['relative_path']['diary_output'] = Path.cwd()/ "vencopy" / configs['dev_config']['global']['relative_path']['diary_output']
configs['dev_config']['global']['relative_path']['grid_output'] = Path.cwd()/ "vencopy" / configs['dev_config']['global']['relative_path']['grid_output']
configs['dev_config']['global']['relative_path']['flex_output'] = Path.cwd()/ "vencopy" / configs['dev_config']['global']['relative_path']['flex_output']
configs['dev_config']['global']['relative_path']['aggregator_output'] = Path.cwd()/ "vencopy" / configs['dev_config']['global']['relative_path']['aggregator_output']
configs['dev_config']['global']['relative_path']['processor_output'] = Path.cwd()/ "vencopy" / configs['dev_config']['global']['relative_path']['processor_output']


# Set reference dataset
datasetID = 'MiD17'

# Modify the localPathConfig file to point to the .csv file in the sampling folder in the tutorials directory where the dataset for the tutorials lies.
configs['user_config']['global']['absolute_path'][datasetID] = Path.cwd() /'tutorials' /'data_sampling'

# Similarly we modify the datasetID in the global config file
configs['dev_config']['global']['files'][datasetID]['trips_data_raw'] = datasetID + '.csv'

# We also modify the parseConfig by removing some of the columns that are normally parsed from the MiD, which are not available in our semplified test dataframe
del configs['dev_config']['dataparsers']['data_variables']['household_id']
del configs['dev_config']['dataparsers']['data_variables']['person_id']

# %%
create_output_folders(configs=configs)

# %% [markdown]
# We can now run the first class and parse the dataset with the collection of mobility patterns into a more useful form for our scope.

# %%
data = parse_data(configs=configs)
data.process()

# %% [markdown]
# ## _GridModeller_ class

# The charging infrastructure allocation makes use of a basic charging infrastructure model, which assumes the availability of charging stations when vehicles are parked. Since the analytical focus of the framework lies on a regional level (NUTS1-NUTS0), the infrastructure model is kept simple in the current version.
# 
# 
# Charging availability is allocated based on a binary True–False mapping to a respective trip purpose in the venco.py config. Thus, different scenarios describing different charging availability scenarios, e.g., at home or at home and at work etc. can be distinguished, but neither a regional differentiation nor a charging availability probability or distribution are assumed.
# 
# At the end of the execution of the GridModeller class, a column representing the available charging power is added to the activities dataset.

# %%
grid = GridModeller(configs=configs, activities=data.activities)
grid.assign_grid()

# %% [markdown]
# ## _FlexEstimator_ class

# The flexEstimator class is the final class that is used to estimate the charging flexibility based on driving profiles and charge connection shares.
# There are three integral inputs to the flexibililty estimation:
# - A profile describing driven distances for each vehicle
# - A profiles describing the available charging power if a vehicle is connected to the grid
# - Techno–economic input assumptions

# %%
flex = FlexEstimator(configs=configs, activities=grid.activities)
flex.estimate_technical_flexibility_through_iteration()

# %% [markdown]
# ## _DiaryBuilder_ class

# In the DiaryBuilder, individual trips at the survey day are consolidated into person-specific travel diaries comprising multiple trips.
# 
# 
# The daily travel diary composition consists of three main steps: reformatting the database, allocating trip purposes and merging the obtained dataframe with other relevant variables from the original database.
# 
# 
# In the first step, reformatting, the time dimension is transferred from the raw data (usually in minutes) to the necessary output format (e.g., hours). Each trip is split into shares, which are then assigned to the respective hour in which they took place, generating an hourly dataframe with a timestamp instead of a dataframe containing single trip entries.
# 
# 
# Similarly, miles driven and the trip purpose are allocated to their respective hour and merged into daily travel diaries. Trips are assumed to determine the respective person’s stay in the consecutive hours up to the next trip and therefore are related to the charging availability between two trips. Trip purposes included in surveys may comprise trips carried out for work or education reasons, trips returning to home, trips to shopping facilities and other leisure activities. Currently, trips whose purpose is not specified are allocated to trips returning to their own household.

# %%
diary = DiaryBuilder(configs=configs, activities=flex.activities)
diary.create_diaries()

# %% [markdown]
# ## _ProfileAggregator_ class
# 

# %%

profiles = ProfileAggregator(configs=configs, activities=diary.activities, profiles=diary)
profiles.aggregate_profiles()

# %% [markdown]
# ## _PostProcessor_ class
#
# %%
post = PostProcessor(configs=configs, profiles=profiles)
post.create_annual_profiles()
post.normalise()

# %%
elapsed_time = time.time() - start_time
print(f"Elapsed time: {elapsed_time}.")

# %% [markdown]
# ## Next Steps
# In the next tutorials, you will learn more in detail the internal workings of each class and how to customise some settings.


