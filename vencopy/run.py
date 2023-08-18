__version__ = "1.0.0"
__maintainer__ = "Niklas Wulff"
__contributors__ = "Fabia Miorelli"
__email__ = "Niklas.Wulff@dlr.de"
__birthdate__ = "23.10.2020"
__status__ = "test"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


import time
from pathlib import Path
from vencopy.core.dataParsers import parseData
from vencopy.core.diaryBuilders import DiaryBuilder
from vencopy.core.gridModelers import GridModeler
from vencopy.core.flexEstimators import FlexEstimator
from vencopy.core.profileAggregators import ProfileAggregator
from vencopy.core.postProcessors import PostProcessing
from vencopy.utils.globalFunctions import load_configs, createOutputFolders

if __name__ == "__main__":
    startTime = time.time()

    basePath = Path(__file__).parent
    configDict = load_configs(basePath=basePath)
    createOutputFolders(configDict=configDict)

    vpData = parseData(configDict=configDict)
    vpData.process()

    vpGrid = GridModeler(config_dict=configDict, activities=vpData.activities)
    vpGrid.assign_grid()

    vpFlex = FlexEstimator(configDict=configDict, activities=vpGrid.activities)
    vpFlex.estimateTechnicalFlexibilityIterating()

    vpDiary = DiaryBuilder(configDict=configDict, activities=vpFlex.activities)
    vpDiary.createDiaries()

    vpProfile = ProfileAggregator(configDict=configDict, activities=vpDiary.activities, profiles=vpDiary)
    vpProfile.aggregate_profiles()

    vpPost = PostProcessing(configDict=configDict)
    vpPost.week_to_annual(profiles=vpProfile)
    vpPost.normalize()

    elapsedTime = time.time() - startTime
    print(f"Elapsed time: {elapsedTime}.")
