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

# NOT TESTED: add_parking_rows(), 


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
        "activity_id": [1, 2, 3, 4],
        "activity_duration": [pd.Timedelta(minutes=76), pd.Timedelta(minutes=80), pd.Timedelta(0), pd.Timedelta(minutes=45)],
        "timestamp_start": pd.DatetimeIndex(["2023-09-12 08:00:00", "2023-09-12 10:30:00", "2023-09-12 10:30:00", "2023-09-12 10:00:00"]),
        "timestamp_end": pd.DatetimeIndex(["2023-09-12 09:16:00", "2023-09-12 11:50:00", "2023-09-12 10:30:00", "2023-09-12 10:45:00"])
        })
    return activities


def test_park_inference_init(sample_configs):
    park_inference = ParkInference(sample_configs)

    assert park_inference.user_config == sample_configs["user_config"]
    assert park_inference.activities is None
    assert park_inference.activities_raw is None
    assert isinstance(park_inference.overnight_splitter, OvernightSplitter)


def test_copy_rows():
    sample_trips_data = {
        "trip_id": [1, 2, 3],
    }
    sample_trips_df = pd.DataFrame(sample_trips_data)

    result = ParkInference._copy_rows(sample_trips_df)
    expected_result = pd.DataFrame({
        "trip_id": [1, np.nan, 2, np.nan, 3, np.nan],
        "park_id": [np.nan, 1, np.nan, 2, np.nan, 3],
    })

    assert len(result) == 2 * len(sample_trips_df)
    assert result.equals(expected_result)
