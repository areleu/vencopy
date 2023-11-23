__version__ = "1.0.0"
__maintainer__ = "Fabia Miorelli"
__birthdate__ = "04.09.2023"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"

import pytest
import pandas as pd

from ....vencopy.core.dataparsers.dataparsers import IntermediateParsing

# NOT TESTED: _complex_filters(), _compose_start_and_end_timestamps(), 


@pytest.fixture
def sample_configs():
    configs = {
        'user_config': {
            'global': {
                'debug': False,
                'absolute_path': {
                    'dataset1': '/path/to/dataset1',
                    'dataset2': '/path/to/dataset2'
                    }
                }
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
                                }
                            }
                        }
                    }
    return configs


def test_intermediate_parsing_init(sample_configs):
    dataset = "dataset1"
    parser = IntermediateParsing(sample_configs, dataset)

    assert parser.user_config == sample_configs["user_config"]
    assert parser.dev_config == sample_configs["dev_config"]
    assert parser.debug == False
    assert parser.dataset == dataset
    assert str(parser.raw_data_path) == "\\path\\to\\dataset1\\trips01.csv"
    assert parser.raw_data is None
    assert parser.trips is None
    assert parser.activities is None
    assert parser.filters == {'filter1': [1, 2, 3], 'filter2': ['A', 'B', 'C']}
    assert parser.filters == sample_configs["dev_config"]["dataparsers"]["filters"][dataset]
    assert parser.var_datatype_dict == {}
    assert parser.columns == ['var1dataset1', 'var2dataset1']


def test_compile_variable_list(sample_configs):
    dataset = "dataset1"
    parser = IntermediateParsing(sample_configs, dataset)

    variables = parser._compile_variable_list()
    expected_variables = ['var1dataset1', 'var2dataset1']
    assert variables == expected_variables


def test_remove_na():
    variables = ["var1", "var2", "NA"]
    IntermediateParsing._remove_na(variables)

    assert "NA" not in variables


@pytest.fixture
def intermediate_parser_instance():
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
    return IntermediateParsing(configs, dataset)


def test_select_columns(intermediate_parser_instance):
    raw_data = pd.DataFrame({
        "var1dataset1": [1.0, 2.0, 3.0],
        "var2dataset1": ["A", "B", "C"],
    })

    intermediate_parser_instance.raw_data = raw_data
    intermediate_parser_instance._select_columns()
    expected_columns = ["var1dataset1", "var2dataset1"]
    assert list(intermediate_parser_instance.trips.columns) == expected_columns


def test_convert_types(intermediate_parser_instance):
    trips_data = pd.DataFrame({
        "var1dataset1": [1.0, 2.0, 3.0],
        "var2dataset1": ["A", "B", "C"],
    })

    intermediate_parser_instance.trips = trips_data
    intermediate_parser_instance._convert_types()

    assert intermediate_parser_instance.trips["var1dataset1"].dtype == int
    assert intermediate_parser_instance.trips["var2dataset1"].dtype == object #TODO: should be string here


def test_filter_consistent_hours():
    dataset = pd.DataFrame({
        "timestamp_start": [pd.to_datetime("2023-09-12 08:00:00"), pd.to_datetime("2023-09-12 09:00:00"), pd.to_datetime("2023-09-12 09:50:00")],
        "timestamp_end": [pd.to_datetime("2023-09-12 09:00:00"), pd.to_datetime("2023-09-12 10:00:00"), pd.to_datetime("2023-09-12 09:30:00")]
    })

    result = IntermediateParsing._filter_consistent_hours(dataset)
    
    expected_result = pd.Series([True, True, False], name="trip_start_after_end")
    pd.testing.assert_series_equal(result, expected_result)


def test_filter_zero_length_trips():
    dataset = pd.DataFrame({
        "trip_start_hour": [8, 9, 10],
        "trip_start_minute": [0, 0, 0],
        "trip_end_hour": [8, 9, 10],
        "trip_end_minute": [0, 0, 1],
        "trip_end_next_day": [False, False, False]
    })

    result = IntermediateParsing._filter_zero_length_trips(dataset)
    
    expected_result = pd.Series([False, False, True], name="is_no_zero_length_trip")
    pd.testing.assert_series_equal(result, expected_result)


def test_add_string_column_from_variable(intermediate_parser_instance):
    trips_data = pd.DataFrame({
        "var1dataset1": ["1", "2", "2", "3"],
        "var2dataset1": ["A", "B", "A", "B"],
    })

    intermediate_parser_instance .trips = trips_data
    intermediate_parser_instance._add_string_column_from_variable("new_var", "var1dataset1")

    expected_result = pd.Series(["One", "Two", "Two", "Three"], name="new_var")
    pd.testing.assert_series_equal(intermediate_parser_instance.trips["new_var"], expected_result)


def test_compose_timestamp():
    data = pd.DataFrame({
        "col_year": [2023, 2023, 2023],
        "col_week": [37, 37, 38],
        "col_day": [2, 3, 4],
        "col_hour": [8, 9, 10],
        "col_min": [0, 15, 30],
    })
    col_name = "composed_timestamp"

    result = IntermediateParsing._compose_timestamp(data, "col_year", "col_week", "col_day", "col_hour", "col_min", col_name)
    expected_result = pd.DataFrame({
        "col_year": [2023, 2023, 2023],
        "col_week": [37, 37, 38],
        "col_day": [2, 3, 4],
        "col_hour": [8, 9, 10],
        "col_min": [0, 15, 30],
        "composed_timestamp": pd.DatetimeIndex(["2023-09-19 08:00:00", "2023-09-20 09:15:00", "2023-09-28 10:30:00"])
        })
    assert (result[col_name] == expected_result[col_name]).all()


def test_update_end_timestamp():
    trips = pd.DataFrame({
        "trip_end_next_day": [0, 1, 0],
        "timestamp_end": pd.DatetimeIndex(["2023-09-12 08:00:00", "2023-09-13 01:00:00", "2023-09-13 10:00:00"])
    })

    result = IntermediateParsing._update_end_timestamp(trips=trips)

    expected_result = pd.DataFrame({"timestamp_end": pd.to_datetime(["2023-09-12 08:00:00", "2023-09-14 01:00:00", "2023-09-13 10:00:00"])})
    pd.testing.assert_series_equal(result["timestamp_end"], expected_result["timestamp_end"])


def test_harmonize_variables_unique_id_names(intermediate_parser_instance):
    trips = pd.DataFrame({
        "id01": [1, 2, 3],
    })

    intermediate_parser_instance.trips = trips
    intermediate_parser_instance._harmonize_variables_unique_id_names()

    expected_result = pd.Series([1, 2, 3], name="unique_id", dtype="int32")
    pd.testing.assert_series_equal(intermediate_parser_instance.trips["unique_id"], expected_result)


def test_subset_vehicle_segment(intermediate_parser_instance):
    activities = pd.DataFrame({
        "vehicle_segment_string": ["Car", "SUV", "Car", "SUV"],
        "unique_id": [1, 2, 3, 4],
    })

    intermediate_parser_instance.activities = activities
    intermediate_parser_instance._subset_vehicle_segment()

    expected_result = pd.DataFrame({
        "vehicle_segment_string": ["Car", "Car"],
        "unique_id": [1, 3],
    })
    pd.testing.assert_frame_equal(intermediate_parser_instance.activities, expected_result)

