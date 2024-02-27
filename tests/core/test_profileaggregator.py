__version__ = "1.0.0"
__maintainer__ = "Fabia Miorelli"
__birthdate__ = "04.09.2023"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"

import pytest

import pandas as pd

from ...vencopy.core.profileaggregators import ProfileAggregator

# NOT TESTED: aggregate_profiles()


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
            'profileaggregators': {
                'aggregation_timespan': 'weekly',
                'weight_flow_profiles': False,
                'alpha': 10
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


@pytest.fixture
def sample_profiles():
    profiles = pd.DataFrame({
        "drain": {'unique_id': [0, 1], 'bin': [0, 1]},
        "charging_power": {'unique_id': [0, 1], 'bin': [0, 1]},
        "uncontrolled_charging": {'unique_id': [0, 1], 'bin': [0, 1]},
        "max_battery_level": {'unique_id': [0, 1], 'bin': [0, 1]},
        "min_battery_level": {'unique_id': [0, 1], 'bin': [0, 1]}
        })
    return profiles


def test_profile_aggregator_init(sample_configs, sample_activities, sample_profiles):
    profile_aggregator = ProfileAggregator(
        configs=sample_configs,
        activities=sample_activities,
        profiles=sample_profiles
    )

    assert profile_aggregator.user_config == sample_configs["user_config"]
    assert profile_aggregator.dev_config == sample_configs["dev_config"]
    assert profile_aggregator.dataset == "dataset1"
    assert profile_aggregator.weighted == False
    assert isinstance(profile_aggregator.activities, pd.DataFrame)
    assert isinstance(profile_aggregator.profiles, pd.DataFrame)

    #TODO: fix test below, profiles are wrong type in fixtures
    # assert isinstance(profile_aggregator.drain, pd.DataFrame)
    # assert isinstance(profile_aggregator.charging_power, pd.DataFrame)
    # assert isinstance(profile_aggregator.uncontrolled_charging, pd.DataFrame)
    # assert isinstance(profile_aggregator.max_battery_level, pd.DataFrame)
    # assert isinstance(profile_aggregator.min_battery_level, pd.DataFrame)

    assert profile_aggregator.drain_weekly is None
    assert profile_aggregator.charging_power_weekly is None
    assert profile_aggregator.uncontrolled_charging_weekly is None
    assert profile_aggregator.max_battery_level_weekly is None
    assert profile_aggregator.min_battery_level_weekly is None
    # TODO: add check for aggregator class instantiation