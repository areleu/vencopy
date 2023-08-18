__version__ = "1.0.0"
__maintainer__ = "Niklas Wulff"
__contributors__ = "Fabia Miorelli"
__email__ = "Niklas.Wulff@dlr.de"
__birthdate__ = "23.10.2020"
__status__ = "test"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


import time
from pathlib import Path
from vencopy.core.dataParsers import parse_data
from vencopy.core.diaryBuilders import DiaryBuilder
from vencopy.core.gridModelers import GridModeler
from vencopy.core.flexEstimators import FlexEstimator
from vencopy.core.profileAggregators import ProfileAggregator
from vencopy.core.postProcessors import PostProcessing
from vencopy.utils.globalFunctions import load_configs, create_output_folders

if __name__ == "__main__":
    startTime = time.time()

    base_path = Path(__file__).parent
    config_dict = load_configs(base_path=base_path)
    create_output_folders(configDict=config_dict)

    vpData = parse_data(config_dict=config_dict)
    vpData.process()

    vpGrid = GridModeler(config_dict=config_dict, activities=vpData.activities)
    vpGrid.assign_grid()

    vpFlex = FlexEstimator(config_dict=config_dict, activities=vpGrid.activities)
    vpFlex.estimateTechnicalFlexibilityIterating()

    vpDiary = DiaryBuilder(config_dict=config_dict, activities=vpFlex.activities)
    vpDiary.create_diaries()

    vpProfile = ProfileAggregator(config_dict=config_dict, activities=vpDiary.activities, profiles=vpDiary)
    vpProfile.aggregate_profiles()

    vpPost = PostProcessing(config_dict=config_dict)
    vpPost.week_to_annual(profiles=vpProfile)
    vpPost.normalize()

    elapsedTime = time.time() - startTime
    print(f"Elapsed time: {elapsedTime}.")
