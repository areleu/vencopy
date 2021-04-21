__version__ = '0.0.1'
__maintainer__ = 'Niklas Wulff'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '23.10.2020'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'


#----- imports & packages ------
import pathlib
import time
import yaml
from scripts.utilsParsing import *
from scripts.libLogging import logger
from classes.parseManager import DataParser
from classes.tripDiaryManager import TripDiaryBuilder
from classes.gridModelManager import GridModeler
from classes.flexEstimationManager import FlexEstimator
from classes.evaluationManager import Evaluator

# Set dataset and config to analyze
linkConfig = pathlib.Path.cwd() / 'config' / 'config.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
config = yaml.load(open(linkConfig), Loader=yaml.SafeLoader)
datasetID = 'MiD17'

vpData = DataParser(datasetID=datasetID, config=config, loadEncrypted=False)

# Trip distance and purpose diary compositions
vpTripDiary = TripDiaryBuilder(config=config, ParseData=vpData, datasetID=datasetID)

# Grid model applications
vpGrid = GridModeler(config=config, dataset=datasetID)
vpGrid.assignSimpleGridViaPurposes()
vpGrid.writeOutGridAvailability()

# Evaluate drive and trip purpose profiles
# vpEval = Evaluator(config, label='SESPaperTest')
# vpEval.hourlyAggregates = vpEval.calcVariableSpecAggregates(by=['tripStartWeekday'])
# vpEval.plotAggregates()

# Estimate charging flexibility based on driving profiles and charge connection
vpFlex = FlexEstimator(config=config, datasetID=datasetID, ParseData=vpData)
vpFlex.baseProfileCalculation()
vpFlex.filter()
vpFlex.aggregate()
vpFlex.correct()
vpFlex.normalize()
vpFlex.writeOut()
vpFlex.plotProfiles()


