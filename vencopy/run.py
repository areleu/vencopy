__version__ = '0.1.X'
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
from pathlib import Path
from vencopy.classes.dataParsers import DataParser, ParseMiD
from vencopy.classes.tripDiaryBuilders import TripDiaryBuilder
from vencopy.classes.gridModelers import GridModeler
from vencopy.classes.flexEstimators import FlexEstimator
from vencopy.classes.evaluators import Evaluator
from vencopy.scripts.globalFunctions import loadConfigDict, createOutputFolders

if __name__ == '__main__':
    # Set dataset and config to analyze, create output folders
    #datasetID = 'KiD'
    datasetID = 'MiD17'
    configNames = ('globalConfig', 'localPathConfig', 'parseConfig', 'tripConfig', 'gridConfig', 'flexConfig',
                   'evaluatorConfig')
    basePath = Path(__file__).parent
    configDict = loadConfigDict(configNames, basePath)
    createOutputFolders(configDict=configDict)

    # Parse datasets
    # vpData = DataParser(configDict=configDict,
    #                     filepath=Path(configDict['globalConfig']['pathAbsolute']['encryptedZipfile']) /
    #                                   configDict['globalConfig']['files'][datasetID]['encryptedZipFileB2'],
    #                     fpInZip=configDict['globalConfig']['files'][datasetID]['tripDataZipFileRaw'],
    #                     loadEncrypted=False)
    # vpData.process(filterDict=configDict['parseConfig']['filterDicts'][datasetID])


    vpData = ParseMiD(configDict=configDict, datasetID=datasetID, loadEncrypted=False)
    vpData.process()

    # Trip distance and purpose diary compositions
    # vpTripDiary = TripDiaryBuilder(datasetID=datasetID, configDict=configDict, ParseData=vpData, debug=True)

    # Grid model application
    vpGrid = GridModeler(configDict=configDict, datasetID=datasetID)
    vpGrid.calcGrid(grid='probability')

    # Evaluate drive and trip purpose profile
    vpEval = Evaluator(configDict=configDict, parseData=pd.Series(data=vpData, index=[datasetID]))
    vpEval.plotParkingAndPowers(vpGrid=vpGrid)
    vpEval.hourlyAggregates = vpEval.calcVariableSpecAggregates(by=['tripStartWeekday'])
    vpEval.plotAggregates()

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
