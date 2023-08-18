__version__ = "1.0.0"
__maintainer__ = "Niklas Wulff, Fabia Miorelli"
__birthdate__ = "23.10.2020"
__status__ = "prod"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


import time

from pathlib import Path

from vencopy.core.dataParsers import parse_data
from vencopy.core.diaryBuilders import DiaryBuilder
from vencopy.core.gridModelers import GridModeler
from vencopy.core.flexEstimators import FlexEstimator
from vencopy.core.profileAggregators import ProfileAggregator
from vencopy.core.postProcessors import PostProcessor
from vencopy.utils.globalFunctions import load_configs, create_output_folders

if __name__ == "__main__":
    start_time = time.time()

    base_path = Path(__file__).parent
    configs = load_configs(base_path=base_path)
    create_output_folders(configs=configs)

    vpData = parse_data(configs=configs)
    vpData.process()

    vpGrid = GridModeler(configs=configs, activities=vpData.activities)
    vpGrid.assign_grid()

    vpFlex = FlexEstimator(configs=configs, activities=vpGrid.activities)
    vpFlex.estimate_technical_flexibility_through_iteration()

    vpDiary = DiaryBuilder(configs=configs, activities=vpFlex.activities)
    vpDiary.create_diaries()

    vpProfile = ProfileAggregator(configs=configs, activities=vpDiary.activities, profiles=vpDiary)
    vpProfile.aggregate_profiles()

    vpPost = PostProcessor(configs=configs)
    vpPost.create_annual_profiles(profiles=vpProfile)
    vpPost.normalise()

    elapsed_time = time.time() - start_time
    print(f"Elapsed time: {elapsed_time}.")
