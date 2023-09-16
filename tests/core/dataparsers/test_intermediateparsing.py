__version__ = "1.0.0"
__maintainer__ = "Fabia Miorelli"
__birthdate__ = "04.09.2023"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"

import pytest
import pandas as pd

from ....vencopy.core.dataparsers.dataparsers import IntermediateParsing

# NOT TESTED: _complex_filters(), 


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
    assert parser.debug == sample_configs["user_config"]["global"]["debug"]
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
                        }}}
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

    results = IntermediateParsing._filter_consistent_hours(dataset)

    expected_results = pd.Series([True, True, False], name="trip_start_after_end")
    pd.testing.assert_series_equal(results, expected_results)


def test_filter_zero_length_trips():
    dataset = pd.DataFrame({
        "trip_start_hour": [8, 9, 10],
        "trip_start_minute": [0, 0, 0],
        "trip_end_hour": [8, 9, 10],
        "trip_end_minute": [0, 0, 1],
        "trip_end_next_day": [False, False, False]
    })

    results = IntermediateParsing._filter_zero_length_trips(dataset)

    expected_results = pd.Series([False, False, True], name="is_no_zero_length_trip")
    pd.testing.assert_series_equal(results, expected_results)


def test_add_string_column_from_variable(intermediate_parser_instance):
    trips_data = pd.DataFrame({
        "var1dataset1": ["1", "2", "2", "3"],
        "var2dataset1": ["A", "B", "A", "B"],
    })

    intermediate_parser_instance .trips = trips_data
    intermediate_parser_instance._add_string_column_from_variable("new_var", "var1dataset1")

    expected_result = pd.Series(["One", "Two", "Two", "Three"], name="new_var")
    pd.testing.assert_series_equal(intermediate_parser_instance.trips["new_var"], expected_result)
