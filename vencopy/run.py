__version__ = '0.1.0'
__maintainer__ = 'Niklas Wulff'
__contributors__ = 'Fabia Miorelli, Parth Butte'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '23.10.2020'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'


#----- imports & packages ------
# NOTE: REMOVE BEFORE PACKAGING 
if __package__ is None or __package__ == '':
    import sys
    from os import path
    sys.path.append(path.dirname(path.dirname(__file__)))

import yaml
from pathlib import Path
import pandas as pd
from vencopy.classes.dataParsers import DataParser
from vencopy.classes.tripDiaryBuilders import TripDiaryBuilder
from vencopy.classes.gridModelers import GridModeler
from vencopy.classes.flexEstimators import FlexEstimator
from vencopy.classes.evaluators import Evaluator

if __name__ == '__main__':
    # Set dataset and config to analyze
    #datasetID = 'KiD'
    datasetID = 'MiD17'
    # review: should the datasetID not be part of the config files?
    pathGlobalConfig = Path(__file__).parent / 'config' / 'globalConfig.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    with open(pathGlobalConfig) as ipf:
        globalConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    pathLocalPathConfig = Path(__file__).parent / 'config' / 'localPathConfig.yaml'
    with open(pathLocalPathConfig) as ipf:
        localPathConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    pathParseConfig = Path(__file__).parent / 'config' / 'parseConfig.yaml'
    with open(pathParseConfig) as ipf:
        parseConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    pathTripConfig = Path(__file__).parent / 'config' / 'tripConfig.yaml'
    with open(pathTripConfig) as ipf:
        tripConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    pathGridConfig = Path(__file__).parent / 'config' / 'gridConfig.yaml'
    with open(pathGridConfig) as ipf:
        gridConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    pathEvaluatorConfig = Path(__file__).parent / 'config' / 'evaluatorConfig.yaml'
    with open(pathEvaluatorConfig) as ipf:
        evaluatorConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    pathFlexConfig = Path(__file__).parent / 'config' / 'flexConfig.yaml'
    with open(pathFlexConfig) as ipf:
        flexConfig = yaml.load(ipf, Loader=yaml.SafeLoader)


vpData = DataParser(datasetID=datasetID, parseConfig=parseConfig, globalConfig=globalConfig, localPathConfig=localPathConfig, loadEncrypted=False)
# Trip distance and purpose diary compositions
vpTripDiary = TripDiaryBuilder(datasetID=datasetID, tripConfig=tripConfig, globalConfig=globalConfig, ParseData=vpData,
                               debug=True)

# Grid model applications
vpGrid = GridModeler(gridConfig=gridConfig, globalConfig=globalConfig, flexConfig=flexConfig,  datasetID=datasetID)
vpGrid.assignSimpleGridViaPurposes()
# fastChargingHHID = vpGrid.fastChargingList()
# vpGrid.assignGridViaProbabilities(model='distribution', fastChargingHHID=fastChargingHHID)
vpGrid.writeOutGridAvailability()
# vpGrid.stackPlot()
# review: Is this still valid code or left overs? I ignored this code for now.
#  Maybe we should schedule a cleanup.
# Evaluate drive and trip purpose profiles
vpEval = Evaluator(globalConfig=globalConfig, evaluatorConfig=evaluatorConfig,
                   parseData=pd.Series(data=vpData, index=[datasetID]))
vpEval.hourlyAggregates = vpEval.calcVariableSpecAggregates(by=['tripStartWeekday'])
vpEval.plotAggregates()

# Grid model applications
vpGrid = GridModeler(gridConfig=gridConfig, globalConfig=globalConfig, flexConfig=flexConfig, datasetID=datasetID)
vpGrid.assignSimpleGridViaPurposes()
vpGrid.writeOutGridAvailability()

# Evaluate drive and trip purpose profiles
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


