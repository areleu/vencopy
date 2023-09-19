__version__ = "1.0.0"
__maintainer__ = "Fabia Miorelli"
__birthdate__ = "04.09.2023"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"

import pytest

import pandas as pd

from ...vencopy.core.diarybuilders import TimeDiscretiser

# NOT TESTED: __write_output(), discretise()


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


def test_timediscretiser_init(sample_configs, sample_activities):

    discretiser = TimeDiscretiser(dataset="dataset1", user_config=sample_configs["user_config"], dev_config=sample_configs["dev_config"], time_resolution=15, activities=sample_activities)

    assert discretiser.activities.equals(sample_activities)
    assert discretiser.dev_config == sample_configs["dev_config"]
    assert discretiser.user_config == sample_configs["user_config"]
    assert discretiser.dataset == "dataset1"
    assert discretiser.activities.equals(sample_activities)
    assert discretiser.time_resolution == 15
    assert discretiser.quantum == pd.Timedelta(value=1, unit="min")
    assert discretiser.is_week == False
    assert discretiser.data_to_discretise is None

