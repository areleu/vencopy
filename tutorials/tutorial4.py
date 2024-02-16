import pandas as pd

from pathlib import Path
from typing import Union

from vencopy.core.dataparsers import parse_data
from vencopy.core.flexestimators import FlexEstimator
from vencopy.core.gridmodellers import GridModeller
from vencopy.utils.utils import create_file_name, write_out,load_configs, create_output_folders


basePath = Path.cwd() /'vencopy'
configs = load_configs(basePath)
create_output_folders(configs=configs)

# Adapt relative paths in config for tutorials
configs['dev_config']['global']['relative_path']['parse_output'] = Path.cwd().parent.parent / configs['dev_config']['global']['relative_path']['parse_output']
configs['dev_config']['global']['relative_path']['diary_output'] = Path.cwd().parent.parent / configs['dev_config']['global']['relative_path']['diary_output']
configs['dev_config']['global']['relative_path']['grid_output'] = Path.cwd().parent.parent / configs['dev_config']['global']['relative_path']['grid_output']
configs['dev_config']['global']['relative_path']['flex_output'] = Path.cwd().parent.parent / configs['dev_config']['global']['relative_path']['flex_output']
configs['dev_config']['global']['relative_path']['aggregator_output'] = Path.cwd().parent.parent / configs['dev_config']['global']['relative_path']['aggregator_output']
configs['dev_config']['global']['relative_path']['processor_output'] = Path.cwd().parent.parent / configs['dev_config']['global']['relative_path']['processor_output']

# Set reference dataset
datasetID = 'MiD17'

# Modify the localPathConfig file to point to the .csv file in the sampling folder in the tutorials directory where the dataset for the tutorials lies.
configs['user_config']['global']['absolute_path'][datasetID] = Path.cwd() /'tutorials'/'data_sampling'

# Similarly we modify the datasetID in the global config file
configs['dev_config']['global']['files'][datasetID]['trips_data_raw'] = datasetID + '.csv'

# We also modify the parseConfig by removing some of the columns that are normally parsed from the MiD, which are not available in our semplified test dataframe
del configs['dev_config']['dataparsers']['data_variables']['household_id']
del configs['dev_config']['dataparsers']['data_variables']['person_id']


data = parse_data(configs=configs)
data= data.process()

grid = GridModeller(configs=configs, activities=data)
grid.assign_grid()


# Estimate charging flexibility based on driving profiles and charge connection
flex = FlexEstimator(configs=configs, activities=grid.activities)
flex.estimate_technical_flexibility_no_boundary_constraints()