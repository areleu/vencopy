from vencopy.core.gridModelers import GridModeler
from vencopy.utils.globalFunctions import load_configs
import pandas as pd
from pathlib import Path

activities = pd.DataFrame()
purposes = ['DRIVING', 'HOME', 'WORK', 'SCHOOL', 'SHOPPING', 'LEISURE', 'OTHER']
activities['purposeStr'] = purposes
activities['hhID'] = range(len(purposes))

basePath = Path(__file__).parent.parent.parent/'vencopy'
datasetID = "MiD17"
configNames = ("globalConfig", "localPathConfig", "parseConfig", "diaryConfig",
               "gridConfig", "flexConfig", "aggregatorConfig", "evaluatorConfig")
configDict = load_configs(configNames, basePath=basePath)
vpGrid = GridModeler(configDict=configDict, datasetID=datasetID, activities=activities, gridModel='simple')
vpGrid.assignGrid()


# INSTANTIATION
print('DatasetID should either be MiD, KiD or VF', vpGrid.datasetID)
print('DatasetID should be string', type(vpGrid.datasetID))
print('Grid model option should be string', type(vpGrid.gridModel))
print('Activities should be a dataframe', type(vpGrid.activities))
print('Grid model should either be "simple" or "probability"', vpGrid.gridModel)
if 'purposeStr' in vpGrid.activities.columns:
    print('Activities contains a column representing trip purposes')
else:
    print('Activities does not contain a column representing trip purposes')

# assignGridViaPurposes
if 'chargingPower' in vpGrid.activities.columns:
    print('Activities contains a column representing charging powers')
else:
    print('Activities does not a column representing charging powers')
simple_power_values_config = {configDict['gridConfig']['ratedPowerSimple'], 0}
power_values_df = set(vpGrid.activities.chargingPower.values)
print(type(power_values_df))
print(type(simple_power_values_config))

if power_values_df.difference(simple_power_values_config):
    print('Not valid values')
else:
    print('Valid values')

# _homeProbabilityDistribution
print('hhID' in vpGrid.activities.columns)
assert 'hhID' in vpGrid.activities.columns, 'hhID is not in the columns of the class'

