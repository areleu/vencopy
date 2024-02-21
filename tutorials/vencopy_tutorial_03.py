# %%
# # Tutorial 3 venco.py
# This tutorial aims to give a more in depth overview into the GridModeler class
# and showcases some features that can be customised.

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

# Modify the config file to point to the .csv file in the sampling folder in the
# tutorials directory where the dataset for the tutorials lies.
configs["user_config"]["global"]["absolute_path"]["vencopy_root"] = Path.cwd()
configs['user_config']['global']['absolute_path'][dataset_id] = Path.cwd() / 'tutorials' / 'data_sampling'

# Similarly we modify the dataset_id in the global config file
configs['dev_config']['global']['files'][dataset_id]['trips_data_raw'] = dataset_id + '.csv'

# We also modify the config file for dataparsers by removing some of the columns
# that are normally parsed from the MiD, which are not available in our
# semplified test dataframe
del configs['dev_config']['dataparsers']['data_variables']['household_id']
del configs['dev_config']['dataparsers']['data_variables']['person_id']

# %%
configs['dev_config']['global']['relative_path']['parse_output']

# %%
# ## GridModeller config file

# Let's print the GridModeller config file.

configs['user_config']['gridmodellers']

# %% As we can see the GridModeler config file contains two keys:
# chargingInfrastructureMappings and chargingInfrastructureDistributions. The
# first one basically sets for which trip purpose the infrastructure
# availability should be considered, the second one specifies the probabilities
# given for each location (trip purpose) and the respective charging power.

# ## _GridModeller_ class

# The charging infrastructure allocation makes use of a basic charging
# infrastructure model, which assumes the availability of charging stations when
# vehicles are parked. Since the analytical focus of the framework lies on a
# regional level (NUTS1-NUTS0), the infrastructure model is kept simple in the
# current version. Charging availability is allocated based on a binary
# True–False mapping to a respective trip purpose in the venco.py config. Thus,
# different scenarios describing different charging availability scenarios,
# e.g., at home or at home and at work etc. can be distinguished, but neither a
# regional differentiation nor a charging availability probability or
# distribution are assumed.

# At the end of the execution of the GridModeler class, the available charging
# power during parking times is added to the activities dataframe.

# %%
# Run the first two classes to generate data

data = parse_data(configs=configs)
data = data.process()

grid = GridModeller(configs=configs, activities=data)
grid.assign_grid()

# %%
# Estimate charging flexibility based on driving profiles and charge
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

# %%
# We can now change the grid availability from home to the
# workplace only and see how this affects the available charging flexibility.

configs['user_config']['gridmodellers']['charging_infrastructure_mappings']['HOME'] = False
configs['user_config']['gridmodellers']['charging_infrastructure_mappings']['WORK'] = True
configs['user_config']['gridmodellers']['charging_infrastructure_mappings']

# %%
grid = GridModeller(configs=configs, activities=data)
grid.assign_grid()

# %%
# Estimate charging flexibility based on driving profiles and charge
# connection
flex = FlexEstimator(configs=configs, activities=grid.activities)
flex.estimate_technical_flexibility_through_iteration()

diary = DiaryBuilder(configs=configs, activities=flex.activities)
diary.create_diaries()

profiles = ProfileAggregator(configs=configs, activities=diary.activities, profiles=diary)
profiles.aggregate_profiles()

plt.figure(figsize=(10, 6))
plt.plot(profiles.charging_power_weekly.index, profiles.charging_power_weekly.iloc[:])
plt.xlim(profiles.charging_power_weekly.index[0], profiles.charging_power_weekly.index[-1])
plt.title('Uncontrolled Charge Volume')
plt.xlabel('Time')
plt.ylabel('Charge Volume')
plt.grid(True)
plt.show()

# %%
# From the plots we can see how different the charging power
# looks, when the option of home charging is not available.

# ## Next Steps

# In the next tutorial, you will learn more in detail the internal workings of
# the FlexEstimator class and how to customise some settings.
