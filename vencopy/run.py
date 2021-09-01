__version__ = '0.0.9'
__maintainer__ = 'Niklas Wulff'
__contributors__ = 'Fabia Miorelli, Parth Butte'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '23.10.2020'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'


#----- imports & packages ------
import yaml
import pathlib
import pandas as pd
from classes.dataParsers import DataParser
from classes.tripDiaryBuilders import TripDiaryBuilder
from classes.gridModelers import GridModeler
from classes.flexEstimators import FlexEstimator
from classes.evaluators import Evaluator

if __name__ == '__main__':
    # Set dataset and config to analyze
    #datasetID = 'KiD'
    datasetID = 'MiD17'
    # review: should the datasetID not be part of the config files?
    pathGlobalConfig = pathlib.Path.cwd() / 'config' / 'globalConfig.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    with open(pathGlobalConfig) as ipf:
        globalConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    pathLocalPathConfig = pathlib.Path.cwd() / 'config' / 'localPathConfig.yaml'
    with open(pathLocalPathConfig) as ipf:
        localPathConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    pathParseConfig = pathlib.Path.cwd() / 'config' / 'parseConfig.yaml'
    with open(pathParseConfig) as ipf:
        parseConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    pathTripConfig = pathlib.Path.cwd() / 'config' / 'tripConfig.yaml'
    with open(pathTripConfig) as ipf:
        tripConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    pathGridConfig = pathlib.Path.cwd() / 'config' / 'gridConfig.yaml'
    with open(pathGridConfig) as ipf:
        gridConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    pathEvaluatorConfig = pathlib.Path.cwd() / 'config' / 'evaluatorConfig.yaml'
    with open(pathEvaluatorConfig) as ipf:
        evaluatorConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    pathFlexConfig = pathlib.Path.cwd() / 'config' / 'flexConfig.yaml'
    with open(pathFlexConfig) as ipf:
        flexConfig = yaml.load(ipf, Loader=yaml.SafeLoader)


vpData = DataParser(datasetID=datasetID, parseConfig=parseConfig, globalConfig=globalConfig, localPathConfig=localPathConfig, loadEncrypted=False)
# Trip distance and purpose diary compositions
vpTripDiary = TripDiaryBuilder(datasetID=datasetID, tripConfig=tripConfig, globalConfig=globalConfig, ParseData=vpData,
                               debug=True)

# Grid model applications
vpGrid = GridModeler(gridConfig=gridConfig, flexConfig=flexConfig, globalConfig=globalConfig, datasetID=datasetID)
vpGrid.profileCalulation()

# review: Is this still valid code or left overs? I ignored this code for now.
#  Maybe we should schedule a cleanup.
# Evaluate drive and trip purpose profiles
# vpEval = Evaluator(globalConfig=globalConfig, evaluatorConfig=evaluatorConfig,
#                    parseData=pd.Series(data=vpData, index=[datasetID]))
# vpEval.hourlyAggregates = vpEval.calcVariableSpecAggregates(by=['tripStartWeekday'])
# vpEval.plotAggregates()

# Estimate charging flexibility based on driving profiles and charge connection
vpFlex = FlexEstimator(flexConfig=flexConfig, globalConfig=globalConfig, evaluatorConfig=evaluatorConfig,
                       datasetID=datasetID, ParseData=vpData, transactionHourStart=vpGrid.transactionHourStart)
vpFlex.baseProfileCalculation()
vpFlex.filter()
vpFlex.aggregate()
vpFlex.correct()
vpFlex.normalize()
vpFlex.writeOut()

vpEval.plotProfiles(flexEstimator=vpFlex)


