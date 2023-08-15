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

import time
from pathlib import Path
from vencopy.core.dataParsers import parseData
from vencopy.core.diaryBuilders import DiaryBuilder
from vencopy.core.gridModelers import GridModeler
from vencopy.core.flexEstimators import FlexEstimator
from vencopy.core.profileAggregators import ProfileAggregator
from vencopy.core.outputFormatters import OutputFormatter
from vencopy.core.normalizers import Normalizer

# from vencopy.core.evaluators import Evaluator
from vencopy.utils.globalFunctions import (
    loadConfigDict,
    createOutputFolders,
)

if __name__ == "__main__":
    startTime = time.time()

    basePath = Path(__file__).parent
    configDict = loadConfigDict(basePath=basePath)
    createOutputFolders(configDict=configDict)

    vpData = parseData(configDict=configDict)
    vpData.process()

    vpGrid = GridModeler(configDict=configDict, activities=vpData.activities)
    vpGrid.assignGrid()

    vpFlex = FlexEstimator(configDict=configDict, activities=vpGrid.activities)
    vpFlex.estimateTechnicalFlexibilityIterating()

    vpDiary = DiaryBuilder(configDict=configDict, activities=vpFlex.activities)
    vpDiary.createDiaries()

    vpProfile = ProfileAggregator(configDict=configDict, activities=vpDiary.activities, profiles=vpDiary)
    vpProfile.aggregateProfile()

    vpOutput = OutputFormatter(configDict=configDict, profiles=vpProfile)
    vpOutput.createTimeseries()

    vpNormProf = Normalizer(configDict=configDict, profiles=vpOutput)
    vpNormProf.normalize(
        flow_profiles={"drain": vpOutput.drain, "uncontrolledCharge": vpOutput.uncontrolledCharge},
        state_profiles={
            "maxBatteryLevel": vpOutput.maxBatteryLevelWeekly,
            "minBatteryLevel": vpOutput.minBatteryLevelWeekly,
        },
        capacity_profiles={"chargeCapacity": vpOutput.chargingPower},
    )

    # vpEval = Evaluator(configDict=configDict, parseData=pd.Series(data=vpData, index=[datasetID]))
    # vpEval.plotParkingAndPowers(vpGrid=vpGrid)
    # vpEval.hourlyAggregates = vpEval.calcVariableSpecAggregates(by=["tripStartWeekday"])
    # vpEval.plotAggregates()
    # vpEval.plotProfiles(flexEstimator=vpFlex)

    elapsedTime = time.time() - startTime
    print(f"Elapsed time: {elapsedTime}.")
