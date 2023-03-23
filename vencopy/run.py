__version__ = "1.0.0"
__maintainer__ = "Niklas Wulff"
__contributors__ = "Fabia Miorelli"
__email__ = "Niklas.Wulff@dlr.de"
__birthdate__ = "23.10.2020"
__status__ = "test"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


# ----- imports & packages ------
if __package__ is None or __package__ == "":
    import sys
    from os import path

    sys.path.append(path.dirname(path.dirname(__file__)))

from pathlib import Path
from vencopy.core.dataParsers import parseData
from vencopy.core.diaryBuilders import WeekDiaryBuilder, DiaryBuilder
from vencopy.core.gridModelers import GridModeler
from vencopy.core.flexEstimators import FlexEstimator, WeekFlexEstimator
from vencopy.core.profileAggregators import ProfileAggregator

# from vencopy.core.evaluators import Evaluator
from vencopy.utils.globalFunctions import (
    dumpReferenceData,
    loadConfigDict,
    createOutputFolders,
)

if __name__ == "__main__":
    basePath = Path(__file__).parent
    configNames = (
        "globalConfig",
        "localPathConfig",
        "parseConfig",
        "diaryConfig",
        "gridConfig",
        "flexConfig",
        "aggregatorConfig",
        "evaluatorConfig",
    )
    configDict = loadConfigDict(configNames, basePath=basePath)
    createOutputFolders(configDict=configDict)

    vpData = parseData(configDict)
    vpData.process()

    vpGrid = GridModeler(
        configDict=configDict,
        activities=vpData.activities,
        gridModel="simple",
    )
    vpGrid.assignGrid()

    vpFlex = FlexEstimator(configDict=configDict, activities=vpGrid.activities)
    vpFlex.estimateTechnicalFlexibility()

    vpDiary = DiaryBuilder(configDict=configDict, activities=vpFlex.activities)
    vpDiary.createDiaries()

    vpWDB = WeekDiaryBuilder(activities=vpGrid.activities, catCols=["areaType"])
    vpWDB.composeWeekActivities(nWeeks=500, seed=42, replace=True)

    vpWeFlex = WeekFlexEstimator(
        configDict=configDict,
        activities=vpWDB.activities,
        threshold=0.8,
    )
    vpWeFlex.estimateTechnicalFlexibility()

    vpProfile = ProfileAggregator(
        configDict=configDict,
        activities=vpDiary.activities,
        profiles=vpDiary,
        cluster=True,
    )
    vpProfile.createTimeseries()

    # Evaluate drive and trip purpose profile
    # vpEval = Evaluator(configDict=configDict, parseData=pd.Series(data=vpData, index=[datasetID]))
    # vpEval.plotParkingAndPowers(vpGrid=vpGrid)
    # vpEval.hourlyAggregates = vpEval.calcVariableSpecAggregates(by=["tripStartWeekday"])
    # vpEval.plotAggregates()
    # vpEval.plotProfiles(flexEstimator=vpFlex)
