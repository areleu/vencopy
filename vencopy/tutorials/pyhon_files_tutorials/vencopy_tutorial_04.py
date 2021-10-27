import sys
import os
from os import path
import pandas as pd
from pathlib import Path
import pathlib
from ruamel.yaml import YAML

sys.path.append(path.dirname(path.dirname(path.dirname(path.dirname(__file__)))))

from vencopy.classes.dataParsers import DataParser
from vencopy.classes.tripDiaryBuilders import TripDiaryBuilder
from vencopy.classes.gridModelers import GridModeler
from vencopy.classes.flexEstimators import FlexEstimator
from vencopy.classes.evaluators import Evaluator
from vencopy.scripts.globalFunctions import loadConfigDict, createOutputFolders

print("Current working directory: {0}".format(os.getcwd()))


configNames = ('globalConfig', 'localPathConfig', 'parseConfig', 'tripConfig', 'gridConfig', 'flexConfig', 'evaluatorConfig')
configDict = loadConfigDict(configNames)

# Adapt relative paths in config for tutorials
configDict['globalConfig']['pathRelative']['plots'] = Path(__file__).parent.parent.parent / configDict['globalConfig']['pathRelative']['plots']
configDict['globalConfig']['pathRelative']['parseOutput'] = Path(__file__).parent.parent.parent / configDict['globalConfig']['pathRelative']['parseOutput']
configDict['globalConfig']['pathRelative']['diaryOutput'] = Path(__file__).parent.parent.parent / configDict['globalConfig']['pathRelative']['diaryOutput']
configDict['globalConfig']['pathRelative']['gridOutput'] = Path(__file__).parent.parent.parent / configDict['globalConfig']['pathRelative']['gridOutput']
configDict['globalConfig']['pathRelative']['flexOutput'] = Path(__file__).parent.parent.parent / configDict['globalConfig']['pathRelative']['flexOutput']
configDict['globalConfig']['pathRelative']['evalOutput'] = Path(__file__).parent.parent.parent / configDict['globalConfig']['pathRelative']['evalOutput']

# Set reference dataset
datasetID = 'MiD17'

# Modify the localPathConfig file to point to the .csv file in the sampling folder in the tutorials directory where the dataset for the tutorials lies.
configDict['localPathConfig']['pathAbsolute'][datasetID] = Path(__file__).parent.parent / 'data_sampling'

# Similarly we modify the datasetID in the global config file
configDict['globalConfig']['files'][datasetID]['tripsDataRaw'] = datasetID + '.csv'

# We also modify the parseConfig by removing some of the columns that are normally parsed from the MiD, which are not available in our semplified test dataframe
del configDict['parseConfig']['dataVariables']['hhID']
del configDict['parseConfig']['dataVariables']['personID']

createOutputFolders(configDict=configDict)


# Run the first two classes to generate data
vpData = DataParser(datasetID=datasetID, configDict=configDict, loadEncrypted=False)
vpData.process()

vpTripDiary = TripDiaryBuilder(datasetID=datasetID, configDict=configDict, ParseData=vpData, debug=True)

vpGrid = GridModeler(datasetID=datasetID, configDict=configDict)
vpGrid.assignSimpleGridViaPurposes()
vpGrid.writeOutGridAvailability()

vpFlex = FlexEstimator(configDict=configDict, datasetID=datasetID, ParseData=vpData)
vpFlex.baseProfileCalculation()
vpFlex.filter()
vpFlex.aggregate()
vpFlex.correct()
vpFlex.normalize()
vpFlex.writeOut()

vpEval = Evaluator(configDict=configDict, parseData=pd.Series(data=vpData, index=[datasetID]))
vpEval.plotProfiles(flexEstimator=vpFlex)

configDict['gridConfig']['chargingInfrastructureMappings']['HOME'] = False
configDict['gridConfig']['chargingInfrastructureMappings']['WORK'] = True

vpGrid = GridModeler(datasetID=datasetID, configDict=configDict)
vpGrid.assignSimpleGridViaPurposes()
vpGrid.writeOutGridAvailability()

vpEval.plotProfiles(flexEstimator=vpFlex)
