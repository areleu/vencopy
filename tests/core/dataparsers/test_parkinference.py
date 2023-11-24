__version__ = "1.0.0"
__maintainer__ = "Fabia Miorelli"
__birthdate__ = "04.09.2023"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"

import pytest

import pandas as pd
import numpy as np

from ....vencopy.core.dataparsers.parkinference import ParkInference
from ....vencopy.core.dataparsers.parkinference import OvernightSplitter

# NOT TESTED: add_parking_rows(), __adjust_park_timestamps(), __overnight_split_decider()


@pytest.fixture
def sample_configs():
    configs = {
        'user_config': {
            'global': {
                'dataset': "dataset1",
                'debug': False,
                'absolute_path': {
                    'dataset1': '/path/to/dataset1',
                    'dataset2': '/path/to/dataset2'
                    }
                },
            'diarybuilders': {
                'time_resolution': 15
            }
            },
        'dev_config': {
            'dataparsers': {
                'data_variables': False
                }
            }
        }
    return configs


@pytest.fixture
def sample_activities():
    activities = pd.DataFrame({
        "trip_id": [1, 2, 1, 2],
        "unique_id": [1, 1, 2, 2],
        "activity_duration": [pd.Timedelta(minutes=76), pd.Timedelta(minutes=80), pd.Timedelta(minutes=30), pd.Timedelta(minutes=75)],
        "timestamp_start": pd.DatetimeIndex(["2023-09-12 08:00:00", "2023-09-12 10:30:00", "2023-09-15 13:30:00", "2023-09-15 14:30:00"]),
        "timestamp_end": pd.DatetimeIndex(["2023-09-12 09:16:00", "2023-09-12 11:50:00", "2023-09-15 14:00:00", "2023-09-15 15:45:00"])
        })
    return activities


def test_park_inference_init(sample_configs):
    park_inference = ParkInference(sample_configs)

    assert park_inference.user_config == sample_configs["user_config"]
    assert park_inference.activities_raw is None
    assert isinstance(park_inference.overnight_splitter, OvernightSplitter)


def test_copy_rows(sample_activities):
    result = ParkInference._copy_rows(sample_activities)
    result = result[["trip_id", "park_id"]]

    expected_result = pd.DataFrame({
        "trip_id": [1, np.nan, 2, np.nan, 1, np.nan, 2, np.nan],
        "park_id": [np.nan, 1, np.nan, 2, np.nan, 1, np.nan, 2],
    })

    assert len(result) == 2 * len(sample_activities)
    assert result.equals(expected_result)


def test_add_util_attributes(sample_activities):
    result = ParkInference._add_util_attributes(sample_activities)

    assert "previous_unique_id" in result.columns
    assert "is_first_activity" in result.columns
    assert "next_unique_id" in result.columns
    assert "is_last_activity" in result.columns

    assert result["previous_unique_id"].equals(pd.Series([0, 1, 1, 2]))
    assert result["is_first_activity"].equals(pd.Series([True, False, True, False]))
    assert result["next_unique_id"].equals(pd.Series([1, 2, 2, 0]))
    assert result["is_last_activity"].equals(pd.Series([False, True, False, True]))


def test_add_park_act_before_first_trip(sample_activities):
    input_data = ParkInference._add_util_attributes(sample_activities)
    result = ParkInference._add_park_act_before_first_trip(input_data)

    assert "park_id" in result.columns
    assert "purpose_string" in result.columns

    assert result.loc[result["is_first_activity"], "park_id"].equals(pd.Series([float(0), float(0)], index=[0, 2]))
    assert result.loc[result["is_first_activity"], "purpose_string"].equals(pd.Series(["HOME", "HOME"], index=[0, 2]))
    assert result.loc[result["is_first_activity"] & (result["park_id"] == 0), "trip_id"].equals(pd.Series([np.nan, np.nan], index=[0, 2]))


def test_adjust_park_attrs(sample_activities):
    input_data = ParkInference._add_util_attributes(sample_activities)
    input_data = ParkInference._add_park_act_before_first_trip(input_data)
    result = ParkInference._adjust_park_attrs(input_data)

    assert "trip_distance" in result.columns
    assert "travel_time" in result.columns
    assert "trip_is_intermodal" in result.columns
    assert "column_from_index" in result.columns

    parking_activities = result[result["trip_id"].isna()]
    assert parking_activities["trip_distance"].equals(pd.Series([np.nan, np.nan], index=[0, 2]))
    assert parking_activities["travel_time"].equals(pd.Series([np.nan, np.nan], index=[0, 2]))
    assert parking_activities["trip_is_intermodal"].equals(pd.Series([np.nan, np.nan], index=[0, 2]))

    expected_column_from_index = [0, 0, 1, 2, 2, 3]
    assert result["column_from_index"].equals(pd.Series(expected_column_from_index, index=[0, 0, 1, 2, 2, 3]))


def test_drop_redundant_columns(sample_activities):
    input_data = ParkInference._add_util_attributes(sample_activities)
    input_data = ParkInference._add_park_act_before_first_trip(input_data)
    input_data = ParkInference._adjust_park_attrs(input_data)
    input_data["trip_start_clock"] = pd.NA
    input_data["trip_start_year"] = pd.NA
    input_data["trip_start_month"] = pd.NA
    input_data["trip_start_week"] = pd.NA
    input_data["trip_start_hour"] = pd.NA
    input_data["trip_start_minute"] = pd.NA
    input_data["trip_end_clock"] = pd.NA
    input_data["trip_end_hour"] = pd.NA
    input_data["trip_end_minute"] = pd.NA
    result = ParkInference._drop_redundant_columns(input_data)

    expected_columns = [
        "trip_id",
        "unique_id",
        "activity_duration",
        "timestamp_start",
        "timestamp_end",
        "park_id",
        "is_first_activity",
        "purpose_string",
    ]

    assert all(col in result.columns for col in expected_columns)

    dropped_columns = [
        "trip_start_clock",
        "trip_end_clock",
        "trip_start_year",
        "trip_start_month",
        "trip_start_week",
        "trip_start_hour",
        "trip_start_minute",
        "trip_end_hour",
        "trip_end_minute",
        "previous_unique_id",
        "next_unique_id",
        "column_from_index",
    ]
    assert not any(col in result.columns for col in dropped_columns)