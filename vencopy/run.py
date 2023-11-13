__version__ = "1.0.0"
__maintainer__ = "Niklas Wulff, Fabia Miorelli"
__birthdate__ = "23.10.2020"
__status__ = "prod"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


import time

from pathlib import Path

from vencopy.core.dataparsers import parse_data
from vencopy.core.gridmodellers import GridModeller
from vencopy.core.flexestimators import FlexEstimator
from vencopy.core.diarybuilders import DiaryBuilder
from vencopy.core.profileaggregators import ProfileAggregator
from vencopy.core.postprocessors import PostProcessor
from vencopy.utils.utils import load_configs, create_output_folders

if __name__ == "__main__":
    start_time = time.time()

    base_path = Path(__file__).parent
    configs = load_configs(base_path=base_path)
    create_output_folders(configs=configs)

    data = parse_data(configs=configs)
    data.process()

    grid = GridModeller(configs=configs, activities=data.activities)
    grid.assign_grid()

    flex = FlexEstimator(configs=configs, activities=grid.activities)
    flex.estimate_technical_flexibility_through_iteration()

    diary = DiaryBuilder(configs=configs, activities=flex.activities)
    diary.create_diaries()

    profile = ProfileAggregator(configs=configs, activities=diary.activities, profiles=diary)
    profile.aggregate_profiles()

    post = PostProcessor(configs=configs, profiles=profile)
    post.create_annual_profiles()
    post.normalise()

    elapsed_time = time.time() - start_time
    print(f"Elapsed time: {elapsed_time}.")
