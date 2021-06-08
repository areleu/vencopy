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
from classes.dataParsers import DataParser
from classes.tripDiaryBuilders import TripDiaryBuilder
from classes.gridModelers import GridModeler
from classes.flexEstimators import FlexEstimator

# Set dataset and config to analyze
datasetID = 'MiD17'
# review: should the datasetID not be part of the config files?
pathGlobalConfig = pathlib.Path.cwd() / 'config' / 'globalConfig.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
globalConfig = yaml.load(open(pathGlobalConfig), Loader=yaml.SafeLoader)
pathParseConfig = pathlib.Path.cwd() / 'config' / 'parseConfig.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
parseConfig = yaml.load(open(pathParseConfig), Loader=yaml.SafeLoader)
pathTripConfig = pathlib.Path.cwd() / 'config' / 'tripConfig.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
tripConfig = yaml.load(open(pathTripConfig), Loader=yaml.SafeLoader)
pathGridConfig = pathlib.Path.cwd() / 'config' / 'gridConfig.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
gridConfig = yaml.load(open(pathGridConfig), Loader=yaml.SafeLoader)
pathEvaluatorConfig = pathlib.Path.cwd() / 'config' / 'evaluatorConfig.yaml'
evaluatorConfig = yaml.load(open(pathEvaluatorConfig), Loader=yaml.SafeLoader)
pathFlexConfig = pathlib.Path.cwd() / 'config' / 'flexConfig.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
flexConfig = yaml.load(open(pathFlexConfig), Loader=yaml.SafeLoader)


vpData = DataParser(datasetID=datasetID, config=parseConfig, globalConfig=globalConfig, loadEncrypted=False)

# Trip distance and purpose diary compositions
vpTripDiary = TripDiaryBuilder(datasetID=datasetID, config=tripConfig, globalConfig=globalConfig, ParseData=vpData)

# Grid model applications
vpGrid = GridModeler(config=gridConfig, globalConfig=globalConfig, datasetID=datasetID)
vpGrid.assignSimpleGridViaPurposes()
vpGrid.writeOutGridAvailability()

# review: Is this still valid code or left overs? I ignored this code for now.
#  Maybe we should schedule a cleanup.
# Evaluate drive and trip purpose profiles
# vpEval = Evaluator(config, label='SESPaperTest')
# vpEval.hourlyAggregates = vpEval.calcVariableSpecAggregates(by=['tripStartWeekday'])
# vpEval.plotAggregates()

# Estimate charging flexibility based on driving profiles and charge connection
vpFlex = FlexEstimator(config=flexConfig, globalConfig=globalConfig, evaluatorConfig=evaluatorConfig, datasetID=datasetID, ParseData=vpData)
vpFlex.baseProfileCalculation()
vpFlex.filter()
vpFlex.aggregate()
vpFlex.correct()
vpFlex.normalize()
vpFlex.writeOut()
vpFlex.plotProfiles()


