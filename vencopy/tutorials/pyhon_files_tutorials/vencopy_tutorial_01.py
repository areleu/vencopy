import sys
import os
from os import path
import pandas as pd
import numpy as np
import yaml
from pathlib import Path
from ruamel.yaml import YAML

sys.path.append(path.dirname(path.dirname(path.dirname(path.dirname(__file__)))))

from vencopy.classes.dataParsers import DataParser
from vencopy.classes.tripDiaryBuilders import TripDiaryBuilder
from vencopy.classes.gridModelers import GridModeler
from vencopy.classes.flexEstimators import FlexEstimator
from vencopy.classes.evaluators import Evaluator

print("Current working directory: {0}".format(os.getcwd()))

pathGlobalConfig = Path(__file__).parent.parent.parent / 'config' / 'globalConfig.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
with open(pathGlobalConfig) as ipf:
    globalConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
pathLocalPathConfig = Path(__file__).parent.parent.parent / 'config' / 'localPathConfig.yaml'
with open(pathLocalPathConfig) as ipf:
    localPathConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
pathParseConfig = Path(__file__).parent.parent.parent / 'config' / 'parseConfig.yaml'
with open(pathParseConfig) as ipf:
    parseConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
pathTripConfig = Path(__file__).parent.parent.parent / 'config' / 'tripConfig.yaml'
with open(pathTripConfig) as ipf:
    tripConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
pathGridConfig = Path(__file__).parent.parent.parent / 'config' / 'gridConfig.yaml'
with open(pathGridConfig) as ipf:
    gridConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
pathEvaluatorConfig = Path(__file__).parent.parent.parent / 'config' / 'evaluatorConfig.yaml'
with open(pathEvaluatorConfig) as ipf:
    evaluatorConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
pathFlexConfig = Path(__file__).parent.parent.parent / 'config' / 'flexConfig.yaml'
with open(pathFlexConfig) as ipf:
    flexConfig = yaml.load(ipf, Loader=yaml.SafeLoader)

# Set reference dataset
datasetID = 'MiD17'

# Modify the localPathConfig file to point to the .csv file in the sampling folder in the tutorials directory where the dataset for the tutorials lies.
localPathConfig['pathAbsolute'][datasetID] = Path.cwd().parent / 'data_sampling'

# Assign to vencoPyRoot the folder in which you cloned your repository
localPathConfig['pathAbsolute']['vencoPyRoot'] = Path.cwd().parent.parent

# Similarly we modify the datasetID in the global config file
globalConfig['files'][datasetID]['tripsDataRaw'] = datasetID + '.csv'

# Adapt relative paths in config for tutorials
globalConfig['pathRelative']['plots'] = Path.cwd().parent.parent / globalConfig['pathRelative']['plots']
globalConfig['pathRelative']['parseOutput'] = Path.cwd().parent.parent / globalConfig['pathRelative']['parseOutput']
globalConfig['pathRelative']['diaryOutput'] = Path.cwd().parent.parent / globalConfig['pathRelative']['diaryOutput']
globalConfig['pathRelative']['gridOutput'] = Path.cwd().parent.parent / globalConfig['pathRelative']['gridOutput']
globalConfig['pathRelative']['flexOutput'] = Path.cwd().parent.parent / globalConfig['pathRelative']['flexOutput']
globalConfig['pathRelative']['evalOutput'] = Path.cwd().parent.parent / globalConfig['pathRelative']['evalOutput']

# We also modify the parseConfig by removing some of the columns that are normally parsed from the MiD, which are not available in our semplified test dataframe
del parseConfig['dataVariables']['hhID']
del parseConfig['dataVariables']['personID']

vpData = DataParser(datasetID=datasetID, parseConfig=parseConfig, globalConfig=globalConfig, localPathConfig=localPathConfig, loadEncrypted=False)

vpTripDiary = TripDiaryBuilder(datasetID=datasetID, tripConfig=tripConfig, globalConfig=globalConfig, ParseData=vpData, debug=True)

vpGrid = GridModeler(gridConfig=gridConfig, globalConfig=globalConfig, datasetID=datasetID)
vpGrid.assignSimpleGridViaPurposes()
vpGrid.writeOutGridAvailability()

vpEval = Evaluator(globalConfig=globalConfig, evaluatorConfig=evaluatorConfig,
                   parseData=pd.Series(data=vpData, index=[datasetID]))
vpEval.hourlyAggregates = vpEval.calcVariableSpecAggregates(by=['tripStartWeekday'])
vpEval.plotAggregates()

# Estimate charging flexibility based on driving profiles and charge connection
vpFlex = FlexEstimator(flexConfig=flexConfig, globalConfig=globalConfig, evaluatorConfig=evaluatorConfig, datasetID=datasetID, ParseData=vpData)
vpFlex.baseProfileCalculation()
vpFlex.filter()
vpFlex.aggregate()
vpFlex.correct()
vpFlex.normalize()
vpFlex.writeOut()

vpEval.plotProfiles(flexEstimator=vpFlex)