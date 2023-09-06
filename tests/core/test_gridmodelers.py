__version__ = "1.0.0"
__maintainer__ = "Fabia Miorelli"
__birthdate__ = "04.09.2023"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


import pytest

import pandas as pd
from pathlib import Path

from vencopy.core.gridmodelers import GridModeler


# TESTS GridModeler class instantiation
# def test_gridmodeler_instantiation():
#     configs = {
#         "user_config": {
#             "global": {
#                 "dataset": "sample_dataset",
#             },
#             "gridmodelers": {
#                 "grid_model": "sample_grid_model",
#                 "force_last_trip_home": True,
#                 "charging_infrastructure_mappings": {
#                     "location1": True,
#                     "location2": False,
#                 },
#                 # TODO: check if syntax reflects configs
#                 # "grid_availability_distribution":
#                 #     "location1": [[11, 0.2], [22, 0.2], [0, 0.6]],
#                 #     "location2": [[11, 0.2], [22, 0.2], [0, 0.6]],
#                 },
#             },
#         "dev_config": {},
#     }
#     # TODO: fix activities dataset
#     activities = ["activity1", "activity2"]

#     my_obj = GridModeler(configs, activities)

#     assert isinstance(my_obj, GridModeler)

#     assert my_obj.user_config == configs["user_config"]
#     assert my_obj.dev_config == configs["dev_config"]
#     assert my_obj.dataset == "sample_dataset"
#     assert my_obj.grid_model == "sample_grid_model"
#     assert my_obj.activities == activities
#     assert my_obj.grid_availability_simple == {
#         "location1": True,
#         "location2": False,
#     }
#     assert my_obj.grid_availability_probability == {
#                     "location1": [0.1, 0.9],
#                     "location2": [0.1, 0.9],
#                 }
#     assert my_obj.charging_availability is None


# TESTS _adjust_power_short_parking_time
@pytest.fixture
def sample_activities():
    data = {
        "park_id": [1, 2, 3],
        "time_delta": [300, 600, 1200],  # 5 minutes, 10 minutes, 20 minutes
        "rated_power": [3.6, 11, 22],
    }
    return pd.DataFrame(data)

# def test_adjust_power_short_parking_time(sample_activities: pd.DataFrame):
#     user_config = {
#         "gridmodelers": {
#             "minimum_parking_time": 900,  # 15 minutes in seconds
#         }
#     }

#     charging_instance = GridModeler(user_config, sample_activities.copy())
#     charging_instance.__adjust_power_short_parking_time()

#     expected_rated_power = [0, 0, 30]  # Only the last activity should have rated_power unchanged
#     assert list(charging_instance.activities["rated_power"]) == expected_rated_power
