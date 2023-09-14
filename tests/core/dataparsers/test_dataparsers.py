__version__ = "1.0.0"
__maintainer__ = "Fabia Miorelli"
__birthdate__ = "04.09.2023"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"

import pytest
import pandas as pd


from dateutil import parser
from typing import Any, Literal

from ....vencopy.core.dataparsers.dataparsers import DataParser
from ....vencopy.core.dataparsers.dataparsers import IntermediateParsing



""" sample_configs = {
    "user_config": {
        "global": {
            "absolute_path": {
                "MiD08": "/path/to/MiD08_data", "MiD17": "/path/to/MiD17_data"
            }
        }
    }, "dev_config": {
        "global": {
            "files": {
                "MiD08": {"trips_data_raw": "raw_data_MiD08.csv"}, "MiD17":
                {"trips_data_raw": "raw_data_MiD17.csv"}
            }
        }
    }
}

 @pytest.mark.parametrize(
    "configs, dataset, load_encrypted, expected_message", [
        (sample_configs, "MiD08", True, "Starting to retrieve encrypted data
        file from"), (sample_configs, "MiD17", False, "Starting to retrieve
        local data file from"),
    ],
)

def test_dataparsers_initialization(configs: dict[str, Any], dataset:
Literal['MiD08', 'MiD17'], load_encrypted: bool, expected_message:
Literal['Starting to retrieve encrypted data file from', 'Starting to retrieve
local data file from']):
    obj = DataParser(configs, dataset, load_encrypted)

    assert obj.user_config == configs["user_config"] assert obj.dev_config ==
    configs["dev_config"] assert obj.dataset == dataset assert
    obj.raw_data_path == expected_raw_data_path assert obj.raw_data is None
    assert obj.trips is None assert obj.activities is None assert
    obj.trips_end_next_day_raw is None assert obj.debug == debug captured =
    capsys.readouterr() assert expected_message in captured.out """


""" 
class MockDataParser:
    def __init__(self):
        dev_config = {
            "dataparsers": {
                "data_variables": {
                    "dataset": ["dataset1", "dataset2", "dataset3"]
                    }}}
        self.dev_config = dev_config
        self.debug = False


@pytest.fixture
def mock_data_parser():
    return MockDataParser()


def test_check_dataset_id(mock_data_parser):
    dataset = "dataset2"
    result = DataParser()._check_dataset_id(dataset=dataset)
    assert result == dataset

    dataset = "non_existent_dataset"
    with pytest.raises(AssertionError) as excinfo:
        DataParser._check_dataset_id(dataset=dataset)
    assert "Defined dataset non_existent_dataset not specified" in str(excinfo.value)

    dataset = "invalid_dataset" with pytest.raises(AssertionError) as e:
        DataParser._check_dataset_id(dataset=dataset)

    expected_error_message = (f"Defined dataset {dataset} not specified under "
                              "data_variables in dev_config. Specified "
                              "dataset_id are ['dataset1', 'dataset2'].")
    assert str(e.value) == expected_error_message """


def test_create_replacement_dict():
    data_variables = {"dataset": ["dataset1", "dataset2"],
                      "vencopy_var1": ["dataset1_var1", "dataset2_var1"],
                      "vencopy_var2": ["dataset1_var2", "dataset2_var2"],}

    dataset = "dataset1"
    expected_result = {"dataset1": "dataset", "dataset1_var1": "vencopy_var1", "dataset1_var2": "vencopy_var2"}
    result = DataParser._create_replacement_dict(dataset=dataset, data_variables=data_variables)
    assert expected_result == result

    dataset = "dataset3"
    # Use a context manager to check for the raised ValueError
    with pytest.raises(ValueError, match="Dataset dataset3 not specified in dev_config variable dictionary."):
        DataParser._create_replacement_dict(dataset=dataset, data_variables=data_variables)


@pytest.fixture
def sample_data_frame_filters():
    data = {"transport_mode": ["car", "bus", "bike"],
            "age": [25, 30, 40]
            }
    return pd.DataFrame(data)


def test_set_include_filter(sample_data_frame_filters):
    include_filter_dict = {
        "transport_mode": ["car"],
        "age": [30, 35],
    }

    result = DataParser._set_include_filter(dataset=sample_data_frame_filters, include_filter_dict=include_filter_dict)

    assert isinstance(result, pd.DataFrame)
    assert set(result.columns) == set(include_filter_dict.keys())

    expected_result = pd.DataFrame({
        "transport_mode": [True, False, False],
        "age": [False, True, False],
    }, index=sample_data_frame_filters.index)
    assert result.equals(expected_result)


def test_set_exclude_filter(sample_data_frame_filters):
    exclude_filter_dict = {
        "transport_mode": ["car"],
        "age": [30, 35],
    }

    result = DataParser._set_exclude_filter(dataset=sample_data_frame_filters, exclude_filter_dict=exclude_filter_dict)

    assert isinstance(result, pd.DataFrame)
    assert set(result.columns) == set(exclude_filter_dict.keys())

    expected_result = pd.DataFrame({
        "transport_mode": [False, True, True],
        "age": [True, False, True],
    }, index=sample_data_frame_filters.index)
    assert result.equals(expected_result)


def test_set_greater_than_filter(sample_data_frame_filters):
    greater_than_filter_dict = {
        "age": [30],
    }

    result = DataParser._set_greater_than_filter(dataset=sample_data_frame_filters, greater_than_filter_dict=greater_than_filter_dict)

    assert isinstance(result, pd.DataFrame)
    assert set(result.columns) == set(greater_than_filter_dict.keys())

    expected_result = pd.DataFrame({
        "age": [False, True, True],
    }, index=sample_data_frame_filters.index)
    assert result.equals(expected_result)


def test_set_smaller_than_filter(sample_data_frame_filters):
    smaller_than_filter_dict = {
        "age": [25],
    }

    result = DataParser._set_smaller_than_filter(dataset=sample_data_frame_filters, smaller_than_filter_dict=smaller_than_filter_dict)

    assert isinstance(result, pd.DataFrame)
    assert set(result.columns) == set(smaller_than_filter_dict.keys())

    expected_result = pd.DataFrame({
        "age": [True, False, False],
    }, index=sample_data_frame_filters.index)
    assert result.equals(expected_result)


@pytest.fixture
def sample_data_frame_other_filters():
    data = {
        "unique_id": [1, 1, 2, 2, 3],
        "timestamp_start": ["2023-09-01 08:00", "2023-09-01 08:45", "2023-09-01 10:00", "2023-09-01 10:30", "2023-09-01 11:00"],
        "timestamp_end": ["2023-09-01 09:00", "2023-09-01 09:30", "2023-09-01 10:15", "2023-09-01 11:15", "2023-09-01 11:30"],
        "trip_distance": [60.0, 15.0, 10.0, 10.0, 10.0],
        "travel_time": [60, 45, 90, 90, 90],
    }
    data["timestamp_start"] = [parser.parse(x) for x in data["timestamp_start"]]
    data["timestamp_end"] = [parser.parse(x) for x in data["timestamp_end"]]
    return pd.DataFrame(data)


def test_filter_inconsistent_speeds(sample_data_frame_other_filters):
    lower_speed_threshold = 10.0
    higher_speed_threshold = 30.0
    result = DataParser._filter_inconsistent_speeds(sample_data_frame_other_filters, lower_speed_threshold, higher_speed_threshold)

    assert isinstance(result, pd.Series)
    assert len(result) == len(sample_data_frame_other_filters)

    expected_result = pd.Series([False, True, False, False, False])
    assert result.equals(expected_result)


def test_filter_inconsistent_travel_times(sample_data_frame_other_filters):
    result = DataParser._filter_inconsistent_travel_times(sample_data_frame_other_filters)

    assert isinstance(result, pd.Series)
    assert len(result) == len(sample_data_frame_other_filters)

    expected_result = pd.Series([True, True, False, False, False])
    assert result.equals(expected_result)


def test_filter_overlapping_trips(sample_data_frame_other_filters):
    result = DataParser._filter_overlapping_trips(sample_data_frame_other_filters, lookahead_periods=1)

    assert isinstance(result, pd.Series)
    assert len(result) == len(sample_data_frame_other_filters)

    expected_result = pd.Series([True, False, True, True, True])
    assert result.equals(expected_result)


def test_identify_overlapping_trips(sample_data_frame_other_filters):
    result = DataParser._identify_overlapping_trips(sample_data_frame_other_filters, 1)

    assert isinstance(result, pd.Series)
    assert len(result) == len(sample_data_frame_other_filters)

    expected_result = pd.Series([True, False, True, True, True])
    assert result.equals(expected_result)


def test_filter_analysis(capsys):
    filter_data = pd.DataFrame({
        "Filter1": [True, False, True, True],
        "Filter2": [False, True, True, False],
    })

    DataParser._filter_analysis(filter_data)

    captured = capsys.readouterr()
    expected_output = (
        "The following number of observations were taken into account after filtering:\n"
        "{'Filter1': 3, 'Filter2': 2}\n"
        "All filters combined yielded that a total of 1 trips are taken into account.\n"
        "This corresponds to 25.0 percent of the original data.\n"
    )
    assert captured.out == expected_output