import os, sys
import pandas as pd
import numpy as np
import yaml
import pathlib
from ruamel.yaml import YAML

from vencopy.classes.dataParsers import DataParser
from vencopy.classes.tripDiaryBuilders import TripDiaryBuilder
from vencopy.classes.gridModelers import GridModeler
from vencopy.classes.flexEstimators import FlexEstimator
from vencopy.classes.evaluators import Evaluator

print("Current working directory: {0}".format(os.getcwd()))

pathGlobalConfig = pathlib.Path.cwd().parent.parent / 'config' / 'globalConfig.yaml'
with open(pathGlobalConfig) as ipf:
    globalConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
pathLocalPathConfig = pathlib.Path.cwd().parent.parent / 'config' / 'localPathConfig.yaml'
with open(pathLocalPathConfig) as ipf:
    localPathConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
pathParseConfig = pathlib.Path.cwd().parent.parent / 'config' / 'parseConfig.yaml'
with open(pathParseConfig) as ipf:
    parseConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
pathTripConfig = pathlib.Path.cwd().parent.parent / 'config' / 'tripConfig.yaml'
with open(pathTripConfig) as ipf:
    tripConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
pathGridConfig = pathlib.Path.cwd().parent.parent / 'config' / 'gridConfig.yaml'
with open(pathGridConfig) as ipf:
    gridConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
pathEvaluatorConfig = pathlib.Path.cwd().parent.parent / 'config' / 'evaluatorConfig.yaml'
with open(pathEvaluatorConfig) as ipf:
    evaluatorConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
pathFlexConfig = pathlib.Path.cwd().parent.parent / 'config' / 'flexConfig.yaml'
with open(pathFlexConfig) as ipf:
    flexConfig = yaml.load(ipf, Loader=yaml.SafeLoader)

# Adapt relative paths in config for tutorials
globalConfig['pathRelative']['plots'] = pathlib.Path.cwd().parent.parent / globalConfig['pathRelative']['plots']
globalConfig['pathRelative']['parseOutput'] = pathlib.Path.cwd().parent.parent / globalConfig['pathRelative'][
    'parseOutput']
globalConfig['pathRelative']['diaryOutput'] = pathlib.Path.cwd().parent.parent / globalConfig['pathRelative'][
    'diaryOutput']
globalConfig['pathRelative']['gridOutput'] = pathlib.Path.cwd().parent.parent / globalConfig['pathRelative'][
    'gridOutput']
globalConfig['pathRelative']['flexOutput'] = pathlib.Path.cwd().parent.parent / globalConfig['pathRelative'][
    'flexOutput']
globalConfig['pathRelative']['evalOutput'] = pathlib.Path.cwd().parent.parent / globalConfig['pathRelative'][
    'evalOutput']

# Set reference dataset
datasetID = 'MiD17'

# Modify the localPathConfig file to point to the .csv file in the sampling folder in the tutorials directory where the dataset for the tutorials lies.
localPathConfig['pathAbsolute'][datasetID] = pathlib.Path.cwd().parent / 'data_sampling'

# Assign to vencoPyRoot the folder in which you cloned your repository
localPathConfig['pathAbsolute']['vencoPyRoot'] = pathlib.Path.cwd().parent.parent

# Similarly we modify the datasetID in the global config file
globalConfig['files'][datasetID]['tripsDataRaw'] = datasetID + '.csv'

# We also modify the parseConfig by removing some of the columns that are normally parsed from the MiD, which are not available in our semplified test dataframe
del parseConfig['dataVariables']['hhID']
del parseConfig['dataVariables']['personID']

vpData = DataParser(datasetID=datasetID, parseConfig=parseConfig, globalConfig=globalConfig, localPathConfig=localPathConfig, loadEncrypted=False)

