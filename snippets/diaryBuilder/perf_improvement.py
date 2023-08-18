__author__ = "Niklas Wulff"
__email__ = "Niklas.Wulff@dlr.de"
__birthdate__ = "16.11.2022"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


# ----- imports & packages ------
if __package__ is None or __package__ == "":
    import sys
    from os import path

    sys.path.append(path.dirname(path.dirname(__file__)))

import profilehooks
from pathlib import Path
from vencopy.core.dataParsers.dataParsers import ParseMiD
from vencopy.core.diaryBuilders import DiaryBuilder
from vencopy.core.gridModelers import GridModeler
from vencopy.core.flexEstimators import FlexEstimator
from vencopy.utils.globalFunctions import load_configs, createOutputFolders

if __name__ == "__main__":
    # Set dataset and config to analyze, create output folders
    # datasetID options: 'MiD08' - 'MiD17' - 'KiD' - 'VF'
    basePath = Path(__file__).parent.parent.parent / 'vencopy'
    configNames = ("globalConfig", "localPathConfig", "parseConfig", "diaryConfig",
                   "gridConfig", "flexConfig", "aggregatorConfig", "evaluatorConfig")
    configDict = load_configs(configNames, basePath=basePath)
    createOutputFolders(configDict=configDict)

    datasetID = 'MiD17'
    vpData = ParseMiD(configDict=configDict, datasetID=datasetID, debug=False)
    vpData.process()

    vpGrid = GridModeler(configDict=configDict, datasetID=datasetID, activities=vpData.activities, gridModel='simple')
    vpGrid.assignGrid()

    vpFlex = FlexEstimator(configDict=configDict, datasetID=datasetID, activities=vpGrid.activities)
    vpFlex.estimateTechnicalFlexibility()

    vpDiary = DiaryBuilder(configDict=configDict, datasetID=datasetID, activities=vpFlex.activities)
    vpDiary.createDiaries()

    # vpWDB = WeekDiaryBuilder(activities=vpGrid.activities, catCols=['areaType'])
    # vpWDB.composeWeekActivities(nWeeks=nWeeks, seed=seed, replace=replace)

    # vpWeFlex = WeekFlexEstimator(configDict=configDict, datasetID=datasetID, activities=weekActs,
    #                                  threshold=t)
    # vpWeFlex.estimateTechnicalFlexibility()
