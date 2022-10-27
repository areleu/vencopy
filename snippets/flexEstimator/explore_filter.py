from vencopy.utils.globalFunctions import loadConfigDict, createOutputFolders
from vencopy.core.dataParsers import ParseMiD
from vencopy.core.gridModelers import GridModeler
from vencopy.core.diaryBuilders import WeekDiaryBuilder
from vencopy.core.flexEstimators import FlexEstimator, WeekFlexEstimator
from pathlib import Path
import sys
import os
from os import path
sys.path.append(path.dirname(path.dirname(__file__)))


datasetID = "MiD17"
configNames = ("globalConfig", "localPathConfig", "parseConfig", "gridConfig", "flexConfig")
configDict = loadConfigDict(configNames, basePath=Path(os.getcwd()) / 'vencopy')
createOutputFolders(configDict=configDict)

vpData = ParseMiD(configDict=configDict, datasetID=datasetID, debug=True)
vpData.process(splitOvernightTrips=False)

vpGrid = GridModeler(configDict=configDict, datasetID=datasetID, activities=vpData.activities, gridModel='simple')
vpGrid.assignGrid(losses=True)

# Testing daily flexibility estimation
vpFlex = FlexEstimator(configDict=configDict, datasetID=datasetID, activities=vpGrid.activities)
vpFlex.estimateTechnicalFlexibility()

# Testing weekly flexibility estimation
vpWDB = WeekDiaryBuilder(activities=vpGrid.activities, catCols=['bundesland', 'areaType'])
vpWDB.summarizeSamplingBases()
vpWDB.composeWeekActivities(seed=42, nWeeks=500, replace=True)

# Estimate charging flexibility based on driving profiles and charge connection
vpWeFlex = WeekFlexEstimator(configDict=configDict,
                             datasetID=datasetID,
                             activities=vpWDB.weekActivities,
                             threshold=0.8)
vpWeFlex.estimateTechnicalFlexibility()

print('Sum of uncontrolled charging of avtivities')

print('END breakpoint')
