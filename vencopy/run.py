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


import pandas as pd
import numpy as np
from vencopy.classes.dataParsers import DataParser
from vencopy.classes.tripDiaryBuilders import TripDiaryBuilder
from vencopy.classes.gridModelers import GridModeler
from vencopy.classes.flexEstimators import FlexEstimator
from vencopy.classes.evaluators import Evaluator
from vencopy.scripts.globalFunctions import loadConfigDict

if __name__ == '__main__':
    # Set dataset and config to analyze
    #datasetID = 'KiD'
    datasetID = 'MiD17'
    # review: should the datasetID not be part of the config files?

    configNames = ('globalConfig', 'localPathConfig', 'parseConfig', 'tripConfig', 'gridConfig', 'flexConfig', 'evaluatorConfig')
    configDict = loadConfigDict(configNames)
    flexConfig = configDict['flexConfig']

    vpData = DataParser(datasetID=datasetID, configDict=configDict, loadEncrypted=False)
    vpData.process()
    # Trip distance and purpose diary compositions
    vpTripDiary = TripDiaryBuilder(datasetID=datasetID,configDict=configDict, ParseData=vpData, debug=False)

    # Grid model applications
    vpGrid = GridModeler(configDict=configDict, datasetID=datasetID,
                         gridPowerDict=configDict['gridConfig']['gridAvailabilityDistribution'])
    vpGrid.calcGrid()

    # Evaluate drive and trip purpose profile
    vpEval = Evaluator(configDict=configDict, parseData=pd.Series(data=vpData, index=[datasetID]))
    vpEval.hourlyAggregates = vpEval.calcVariableSpecAggregates(by=['tripStartWeekday'])
    vpEval.plotAggregates()

    # cumSumAgg = pd.Series()
    # a = pd.Series()
    # for i in range(10, 210, 10):
    #     flexConfig['inputDataScalars'][datasetID]['Battery_capacity'] += 10
    #     vencoPyBatCap = flexConfig['inputDataScalars'][datasetID]['Battery_capacity']
    #     vpFlex = FlexEstimator(configDict=configDict, datasetID=datasetID, ParseData=vpData,
    #                            transactionStartHour=vpGrid.transactionStartHour)
    #     vpFlex.baseProfileCalculation()
    #     VEP = pd.Series(vpFlex.VEP, index=[vencoPyBatCap])
    #     # batCap.reset_index(drop=True, inplace=True)
    #
    #     # xThreshold = pd.Series(np.where(batCap > (0.2 * vencoPyBatCap), 1, 0))
    #     # cumSum = pd.Series(xThreshold.sum()/len(batCap), index=[vencoPyBatCap])
    #
    #     cumSumAgg = cumSumAgg.append(VEP)
    #     cumSumAgg

    # Estimate charging flexibility based on driving profiles and charge connection
    vpFlex = FlexEstimator(configDict=configDict, datasetID=datasetID, ParseData=vpData,
                           transactionStartHour=vpGrid.transactionStartHour)
    vpFlex.baseProfileCalculation()
    vpFlex.filter()
    vpFlex.aggregate()
    vpFlex.correct()
    vpFlex.normalize()
    vpFlex.writeOut()
    print(f'Total absolute electricity charged in uncontrolled charging: '
          f'{vpFlex.chargeProfilesUncontrolled.sum().sum()} based on MiD17')

    vpEval.plotProfiles(flexEstimator=vpFlex)
