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
from classes.parseManager import DataParser
from classes.tripDiaryManager import TripDiaryBuilder
from classes.gridModelManager import GridModeler
from classes.flexEstimationManager import FlexEstimator

# Set dataset and config to analyze
datasetID = 'MiD17'
linkGlobalConfig = pathlib.Path.cwd() / 'config' / 'globalConfig.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
globalConfig = yaml.load(open(linkGlobalConfig), Loader=yaml.SafeLoader)
linkParseConfig = pathlib.Path.cwd() / 'config' / 'parseConfig.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
parseConfig = yaml.load(open(linkParseConfig), Loader=yaml.SafeLoader)
linkTripConfig = pathlib.Path.cwd() / 'config' / 'tripConfig.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
tripConfig = yaml.load(open(linkTripConfig), Loader=yaml.SafeLoader)
linkGridConfig = pathlib.Path.cwd() / 'config' / 'gridConfig.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
gridConfig = yaml.load(open(linkGridConfig), Loader=yaml.SafeLoader)
linkEvaluatorConfig = pathlib.Path.cwd() / 'config' / 'evaluatorConfig.yaml'
evaluatorConfig = yaml.load(open(linkEvaluatorConfig), Loader=yaml.SafeLoader)
linkFlexConfig = pathlib.Path.cwd() / 'config' / 'flexConfig.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
flexConfig = yaml.load(open(linkFlexConfig), Loader=yaml.SafeLoader)


vpData = DataParser(datasetID=datasetID, config=parseConfig, globalConfig=globalConfig, loadEncrypted=False)

# Trip distance and purpose diary compositions
vpTripDiary = TripDiaryBuilder(config=tripConfig, globalConfig=globalConfig, ParseData=vpData, datasetID=datasetID)

# Grid model applications
vpGrid = GridModeler(config=gridConfig, globalConfig=globalConfig, datasetID=datasetID)
vpGrid.assignSimpleGridViaPurposes()
vpGrid.writeOutGridAvailability()

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


