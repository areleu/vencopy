__version__ = '0.1.0'
__maintainer__ = 'Niklas Wulff'
__contributors__ = 'Fabia Miorelli, Parth Butte'
__email__ = 'niklas.wulff@dlr.de'
__birthdate__ = '23.10.2020'
__status__ = 'prod'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'


# ----- imports & packages ------
import yaml
import pathlib
import pandas as pd
from vencopy.classes.dataParsers import DataParser
from vencopy.classes.tripDiaryBuilders import TripDiaryBuilder
from vencopy.classes.gridModelers import GridModeler
from vencopy.classes.flexEstimators import FlexEstimator
from vencopy.classes.evaluators import Evaluator


if __name__ == '__main__':
    # Set dataset and config to analyze
    datasetID = 'MiD17'
    # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    pathGlobalConfig = pathlib.Path.cwd() / 'config' / 'globalConfig.yaml'
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
    vpTripDiary = TripDiaryBuilder(datasetID=datasetID, tripConfig=tripConfig, globalConfig=globalConfig,
                                   ParseData=vpData, debug=True)

    # Grid model applications
    vpGrid = GridModeler(gridConfig=gridConfig, globalConfig=globalConfig, datasetID=datasetID)
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
