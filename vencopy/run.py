__version__ = '0.1.3'
__maintainer__ = 'Niklas Wulff'
__contributors__ = 'Fabia Miorelli, Parth Butte'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '23.10.2020'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'


#----- imports & packages ------
if __package__ is None or __package__ == '':
    import sys
    from os import path
    sys.path.append(path.dirname(path.dirname(__file__)))

import pandas as pd
from vencopy.classes.dataParsers import DataParser
from vencopy.classes.tripDiaryBuilders import TripDiaryBuilder
from vencopy.classes.gridModelers import GridModeler
from vencopy.classes.flexEstimators import FlexEstimator
from vencopy.classes.evaluators import Evaluator
from vencopy.scripts.globalFunctions import loadConfigDict, createOutputFolders

if __name__ == '__main__':
    # Set dataset and config to analyze, create output folders
    #datasetID = 'KiD'
    datasetID = 'MiD17'
    # review: should the datasetID not be part of the config files?
    configNames = ('globalConfig', 'localPathConfig', 'parseConfig', 'tripConfig', 'gridConfig', 'flexConfig',
                   'evaluatorConfig')
    configDict = loadConfigDict(configNames)
    createOutputFolders(configDict=configDict)

    # Parse datasets
    vpData = DataParser(datasetID=datasetID, configDict=configDict, loadEncrypted=False)
    vpData.process()

    # Trip distance and purpose diary compositions
    vpTripDiary = TripDiaryBuilder(datasetID=datasetID, configDict=configDict, ParseData=vpData, debug=False)

    # Grid model applications
    vpGrid = GridModeler(configDict=configDict, datasetID=datasetID)
    vpGrid.assignSimpleGridViaPurposes()
    # fastChargingHHID = vpGrid.getFastChargingList()
    # vpGrid.assignGridViaProbabilities(model='distribution', fastChargingHHID=fastChargingHHID)
    vpGrid.writeOutGridAvailability()
    # vpGrid.stackPlot()

    # Evaluate drive and trip purpose profile
    vpEval = Evaluator(configDict=configDict, parseData=pd.Series(data=vpData, index=[datasetID]))
    vpEval.hourlyAggregates = vpEval.calcVariableSpecAggregates(by=['tripStartWeekday'])
    vpEval.plotAggregates()

    # Estimate charging flexibility based on driving profiles and charge connection
    vpFlex = FlexEstimator(configDict=configDict, datasetID=datasetID, ParseData=vpData)
    vpFlex.baseProfileCalculation()
    vpFlex.filter()
    vpFlex.aggregate()
    vpFlex.correct()
    vpFlex.normalize()
    vpFlex.writeOut()

    vpEval.plotProfiles(flexEstimator=vpFlex)
