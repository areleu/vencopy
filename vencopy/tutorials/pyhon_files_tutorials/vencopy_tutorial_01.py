import sys
import os
from os import path
import pandas as pd
from pathlib import Path

sys.path.append(path.dirname(path.dirname(path.dirname(path.dirname(__file__)))))


from vencopy.classes.dataParsers import DataParser
from vencopy.classes.tripDiaryBuilders import TripDiaryBuilder
from vencopy.classes.gridModelers import GridModeler
from vencopy.classes.flexEstimators import FlexEstimator
from vencopy.classes.evaluators import Evaluator
from vencopy.scripts.globalFunctions import loadConfigDict

print("Current working directory: {0}".format(os.getcwd()))


configNames = ('globalConfig', 'localPathConfig', 'parseConfig', 'tripConfig', 'gridConfig', 'flexConfig', 'evaluatorConfig')
configDict = loadConfigDict(configNames)

# Set reference dataset
datasetID = 'MiD17'

# Modify the localPathConfig file to point to the .csv file in the sampling folder in the tutorials directory where the dataset for the tutorials lies.
configDict['localPathConfig']['pathAbsolute'][datasetID] = Path(__file__).parent.parent / 'data_sampling'

# Assign to vencoPyRoot the folder in which you cloned your repository
#localPathConfig['pathAbsolute']['vencoPyRoot'] = Path.cwd().parent.parent

# Similarly we modify the datasetID in the global config file
configDict['globalConfig']['files'][datasetID]['tripsDataRaw'] = datasetID + '.csv'

# Adapt relative paths in config for tutorials
configDict['globalConfig']['pathRelative']['plots'] = Path(__file__).parent.parent / configDict['globalConfig']['pathRelative']['plots']
configDict['globalConfig']['pathRelative']['parseOutput'] = Path(__file__).parent.parent / configDict['globalConfig']['pathRelative']['parseOutput']
configDict['globalConfig']['pathRelative']['diaryOutput'] = Path(__file__).parent.parent / configDict['globalConfig']['pathRelative']['diaryOutput']
configDict['globalConfig']['pathRelative']['gridOutput'] = Path(__file__).parent.parent / configDict['globalConfig']['pathRelative']['gridOutput']
configDict['globalConfig']['pathRelative']['flexOutput'] = Path(__file__).parent.parent / configDict['globalConfig']['pathRelative']['flexOutput']
configDict['globalConfig']['pathRelative']['evalOutput'] = Path(__file__).parent.parent / configDict['globalConfig']['pathRelative']['evalOutput']

# We also modify the parseConfig by removing some of the columns that are normally parsed from the MiD, which are not available in our semplified test dataframe
del configDict['parseConfig']['dataVariables']['hhID']
del configDict['parseConfig']['dataVariables']['personID']

vpData = DataParser(datasetID=datasetID, configDict=configDict, loadEncrypted=False)

vpTripDiary = TripDiaryBuilder(datasetID=datasetID, configDict=configDict, ParseData=vpData, debug=True)

vpGrid = GridModeler(datasetID=datasetID, configDict=configDict)
vpGrid.assignSimpleGridViaPurposes()
vpGrid.writeOutGridAvailability()

vpEval = Evaluator(configDict=configDict, parseData=pd.Series(data=vpData, index=[datasetID]))
vpEval.hourlyAggregates = vpEval.calcVariableSpecAggregates(by=['tripStartWeekday'])
vpEval.plotAggregates()

# Estimate charging flexibility based on driving profiles and charge connection
vpFlex = FlexEstimator(datasetID=datasetID, configDict=configDict, ParseData=vpData)
vpFlex.baseProfileCalculation()
vpFlex.filter()
vpFlex.aggregate()
vpFlex.correct()
vpFlex.normalize()
vpFlex.writeOut()

vpEval.plotProfiles(flexEstimator=vpFlex)
