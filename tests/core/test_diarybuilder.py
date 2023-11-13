__version__ = "1.0.0"
__maintainer__ = "Fabia Miorelli"
__birthdate__ = "04.09.2023"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"

import pytest

import pandas as pd

from ...vencopy.core.diarybuilders import DiaryBuilder

# NOT TESTED: create_diaries(), __update_activities()


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


def test_diarybuilder_init(sample_configs):
    sample_activities_data = pd.DataFrame({})
    builder = DiaryBuilder(configs=sample_configs, activities=sample_activities_data)

    assert builder.dev_config == sample_configs["dev_config"]
    assert builder.user_config == sample_configs["user_config"]
    assert builder.dataset == "dataset1"
    assert builder.activities.equals(sample_activities_data)
    assert builder.time_resolution == 15
    assert builder.is_week_diary == False


@pytest.fixture
def sample_activities():
    activities = pd.DataFrame({
        "activity_id": [1, 2, 3, 4],
        "activity_duration": [pd.Timedelta(minutes=76), pd.Timedelta(minutes=80), pd.Timedelta(0), pd.Timedelta(minutes=45)],
        "timestamp_start": pd.DatetimeIndex(["2023-09-12 08:00:00", "2023-09-12 10:30:00", "2023-09-12 10:30:00", "2023-09-12 10:00:00"]),
        "timestamp_end": pd.DatetimeIndex(["2023-09-12 09:16:00", "2023-09-12 11:50:00", "2023-09-12 10:30:00", "2023-09-12 10:45:00"])
        })
    return activities


def test_correct_timestamps(sample_configs, sample_activities):
    builder = DiaryBuilder(configs=sample_configs, activities=sample_activities)
    time_resolution = 15
    result = builder._correct_timestamps(dataset=sample_activities, time_resolution=time_resolution)
    expected_result = pd.DataFrame({
        "timestamp_start_corrected": pd.DatetimeIndex(["2023-09-12 08:00:00", "2023-09-12 10:30:00", "2023-09-12 10:30:00", "2023-09-12 10:00:00"]),
        "timestamp_end_corrected": pd.DatetimeIndex(["2023-09-12 09:15:00", "2023-09-12 11:45:00", "2023-09-12 10:30:00", "2023-09-12 10:45:00"])
    })

    pd.testing.assert_series_equal(result["timestamp_start_corrected"], expected_result["timestamp_start_corrected"])
    pd.testing.assert_series_equal(result["timestamp_end_corrected"], expected_result["timestamp_end_corrected"])


def test_removes_zero_length_activities(sample_activities):
    result = DiaryBuilder._removes_zero_length_activities(dataset=sample_activities)

    assert len(result) == 3