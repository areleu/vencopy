# %% [markdown]
# # Tutorial 4 venco.py

# This tutorial aims to give a more in depth overview into the FlexEstimator
# class and showcases some features that can be customised.

from pathlib import Path
import matplotlib.pyplot as plt

from vencopy.core.dataparsers import parse_data
from vencopy.core.flexestimators import FlexEstimator
from vencopy.core.gridmodellers import GridModeller
from vencopy.core.diarybuilders import DiaryBuilder
from vencopy.core.profileaggregators import ProfileAggregator
from vencopy.utils.utils import load_configs, create_output_folders

# %%
base_path = Path.cwd() / 'vencopy'
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
configs["user_config"]["global"]["absolute_path"]["vencopy_root"] = Path.cwd()
configs['user_config']['global']['absolute_path'][dataset_id] = Path.cwd() / 'tutorials' / 'data_sampling'

# Similarly we modify the dataset_id in the global config file
configs['dev_config']['global']['files'][dataset_id]['trips_data_raw'] = dataset_id + '.csv'


# We also modify the parseConfig by removing some of the columns that are
# normally parsed from the MiD, which are not available in our semplified test
# dataframe
del configs['dev_config']['dataparsers']['data_variables']['household_id']
del configs['dev_config']['dataparsers']['data_variables']['person_id']


# %%
# ## FlexEstimator config file

# The FlexEstimator config file contains the technical specifications.

configs['user_config']['flexestimators']

# %%
# ## _FlexEstimator_ class

# To use the FlexEstimator class, we first need to run the DataParses as well as
# the GridModeller as they have an argument to the FlexEstimator class.


base_path = Path.cwd() / 'vencopy'

data = parse_data(configs=configs)
data = data.process()

grid = GridModeller(configs=configs, activities=data)
grid.assign_grid()


# %%
# Now we can display results on the distance all the trips have

# Filter out rows where trip_distance is not NaN
filtered_df = data.dropna(subset=['trip_distance'])

max_index = filtered_df['index'].max()

# Plot trip distances
plt.figure(figsize=(10, 6))
plt.scatter(filtered_df.index, filtered_df['trip_distance'], color='blue', alpha=0.5)
plt.title('Trip Distance Distribution')
plt.xlabel('Index')
plt.ylabel('Trip Distance')
plt.grid(True)
plt.xlim(0, max_index)
plt.show()

# %% Estimate charging flexibility based on driving profiles and charge
# connection
flex = FlexEstimator(configs=configs, activities=grid.activities)
flex.estimate_technical_flexibility_through_iteration()


# %%
diary = DiaryBuilder(configs=configs, activities=flex.activities)
diary.create_diaries()

profiles = ProfileAggregator(configs=configs, activities=diary.activities, profiles=diary)
profiles.aggregate_profiles()

plt.figure(figsize=(10, 6))
plt.plot(profiles.uncontrolled_charging_weekly.index, profiles.uncontrolled_charging_weekly.iloc[:])
plt.xlim(profiles.uncontrolled_charging_weekly.index[0], profiles.uncontrolled_charging_weekly.index[-1])
plt.title('Uncontrolled Charge Volume')
plt.xlabel('Time')
plt.ylabel('Charge Volume')
plt.grid(True)
plt.show()


# %%
# To analyse its influence on the demand-side flexibility from EV, we will
# though charge the assumed size of the battery from 50 kWh to 100 kWh.

configs['user_config']['flexestimators']['battery_capacity'] = 20.0

# %% Estimate charging flexibility based on driving profiles and charge
# connection
flex = FlexEstimator(configs=configs, activities=grid.activities)
flex.estimate_technical_flexibility_through_iteration()

diary = DiaryBuilder(configs=configs, activities=flex.activities)
diary.create_diaries()

profiles = ProfileAggregator(configs=configs, activities=diary.activities, profiles=diary)
profiles.aggregate_profiles()

plt.figure(figsize=(10, 6))
plt.plot(profiles.uncontrolled_charging_weekly.index, profiles.uncontrolled_charging_weekly.iloc[:])
plt.xlim(profiles.uncontrolled_charging_weekly.index[0], profiles.uncontrolled_charging_weekly.index[-1])
plt.title('Uncontrolled Charge Volume')
plt.xlabel('Time')
plt.ylabel('Charge Volume')
plt.grid(True)
plt.show()

# By reducing the battery capacity to 20 kWh we can see from the graphs flatter
# curves, which results from vehicles charging less and reduced range of
# profiles that can be considered.

# ## Next Steps
# Come back, there will be more upcoming tutorials! :)
