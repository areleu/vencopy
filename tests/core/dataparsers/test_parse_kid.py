__version__ = "1.0.0"
__maintainer__ = "Fabia Miorelli"
__birthdate__ = "04.09.2023"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"

import pytest
import pandas as pd
import numpy as np


from ....vencopy.core.dataparsers.parkinference import ParkInference
from ....vencopy.core.dataparsers.parseKiD import ParseKiD

# NOT TESTED: process(), __add_string_columns(), _load_unencrypted_data()

@pytest.fixture
def parse_kid_instance():
    configs = {
        'user_config': {
            'global': {
                'debug': False,
                'absolute_path': {
                    'dataset1': '/path/to/dataset1',
                    'dataset2': '/path/to/dataset2'
                    }
                },
        "dataparsers": {
            "subset_vehicle_segment": True,
            "vehicle_segment": {
                "dataset1": "Car",
                "dataset2": "L"
            }}
            },
        'dev_config': {
            'global': {
                    'files': {
                        "dataset1": {
                            "trips_data_raw": "trips01.csv"
                                    },
                        "dataset2": {
                            "trips_data_raw": "trips02.csv"
                                    }
                            }
                        },
            'dataparsers': {
                'data_variables': {
                    'dataset': ['dataset1', 'dataset2', 'dataset3'],
                    'var1': ['var1dataset1', 'var1dataset2', 'var1dataset3'],
                    'var2': ['var2dataset1', 'var2dataset2', 'var2dataset3'],
                    },
                "filters": {
                        "dataset1": {
                            "filter1": [1, 2, 3],
                            "filter2": ["A", "B", "C"]
                                    },
                        "dataset2": {
                            "filter3": [True, False, True]
                                    }
                                },
                "input_data_types": {
                    "dataset1": {
                        "var1dataset1": int,
                        "var2dataset1": str
                    }},
                "replacements":{
                    "dataset1": {
                        "var1dataset1": {
                            "1": "One",
                            "2": "Two",
                            "3": "Three"
                        }}},
                "id_variables_names": {
                    "dataset1": "id01",
                    "dataset2": "id02"
                            }
                        }
                    }
        }
    dataset = "dataset1"
    return ParseKiD(configs, dataset)


def test_parse_kid_init(parse_kid_instance):
    assert isinstance(parse_kid_instance, ParseKiD)
    assert isinstance(parse_kid_instance.park_inference, ParkInference)


@pytest.mark.skip(reason="no way of currently testing this") 
def test_change_separator():
    trips = pd.DataFrame({'trip_distance': ['1234,56', '789,01'],
                          'trip_weight': ['3.45', '6,789']})
    result_df = ParseKiD._change_separator(trips=trips)

    expected_result = pd.DataFrame({'trip_distance': ['1234.56', '789.01'],
                                    'trip_weight': ['3.45', '6.789']})
    pd.testing.assert_frame_equal(result_df, expected_result)


@pytest.mark.skip(reason="no way of currently testing this") 
def test_extract_timestamps():
    trips = pd.DataFrame({'trip_start_date': ['01.09.2023', '02.09.2023', '03.09.2023'],
                          'trip_start_clock': ['08:00', '09:30', '10:15'],
                          'trip_end_clock': ['08:45', '10:00', '11:30']})

    result = ParseKiD._extract_timestamps(trips=trips)

    expected_result = pd.DataFrame({
        'trip_start_date': pd.to_datetime(['2023-09-01', '2023-09-02', '2023-09-03']),
        'trip_start_clock': ['08:00', '09:30', '10:15'],
        'trip_end_clock': ['08:45', '10:00', '11:30'],
        'trip_start_year': [2023, 2023, 2023],
        'trip_start_month': [9, 9, 9],
        'trip_start_day': [1, 2, 3],
        'trip_start_weekday': [4, 5, 6],
        'trip_start_week': [35, 35, 35],
        'trip_start_hour': [8, 9, 10],
        'trip_start_minute': [0, 30, 15],
        'trip_end_hour': [8, 10, 11],
        'trip_end_minute': [45, 0, 30],
    })
    expected_result['trip_start_week'] = pd.Series(expected_result["trip_start_week"], dtype="int32")
    pd.testing.assert_frame_equal(result, expected_result)


@pytest.mark.skip(reason="no way of currently testing this") 
def test_update_end_timestamp():
    trips = pd.DataFrame({
        "timestamp_start": pd.DatetimeIndex(["2023-09-01 08:00:00", "2023-09-01 09:30:00", "2023-09-02 23:15:00"]),
        "timestamp_end": pd.DatetimeIndex(["2023-09-01 08:45:00", "2023-09-01 10:00:00", "2023-09-02 00:30:00"]),
    })

    result = ParseKiD._update_end_timestamp(trips)

    expected_result = pd.DataFrame({
        "timestamp_start": pd.DatetimeIndex(["2023-09-01 08:00:00", "2023-09-01 09:30:00", "2023-09-02 23:15:00"]),
        "timestamp_end": pd.DatetimeIndex(["2023-09-01 08:45:00", "2023-09-01 10:00:00", "2023-09-02 00:30:00"]),
        "trip_end_next_day": [0, 0, 0]})
    expected_result['trip_end_next_day'] = expected_result['trip_end_next_day'].astype(np.int32)
    pd.testing.assert_frame_equal(result, expected_result)


def test_exclude_hours():
    trips = pd.DataFrame({
        'trip_start_clock': ['08:00', '-1:-1', '10:15', '-1:-1'],
        'trip_end_clock': ['08:45', '09:30', '-1:-1', '-1:-1']})

    result = ParseKiD._exclude_hours(trips=trips)

    expected_result = pd.DataFrame({
        'trip_start_clock': ['08:00'],
        'trip_end_clock': ['08:45'],
    })

    pd.testing.assert_frame_equal(result, expected_result)
