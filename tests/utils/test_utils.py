__version__ = "1.0.0"
__maintainer__ = "Fabia Miorelli"
__birthdate__ = "04.09.2023"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"

import pytest
import yaml
import os

from unittest.mock import mock_open, patch
import pandas as pd
from pathlib import Path

from vencopy.utils.utils import load_configs, return_lowest_level_dict_keys, return_lowest_level_dict_values, replace_vec, create_output_folders, create_file_name, merge_variables, write_out


# TESTS load_config
# Define a fixture to provide a temporary directory for testing
@pytest.fixture
def temp_dir(tmp_path):
    return tmp_path / "temp_dir"


""" def test_load_configs_with_valid_files(temp_dir):
    temp_dir.mkdir()
    user_config_path = temp_dir / "config" / "user_config.yaml"
    dev_config_path = temp_dir / "config" / "dev_config.yaml"

    user_config_data = {"user_key": "user_value"}
    dev_config_data = {"dev_key": "dev_value"}

    with open(user_config_path, "w") as user_file:
        yaml.dump(user_config_data, user_file, default_flow_style=False)
    with open(dev_config_path, "w") as dev_file:
        yaml.dump(dev_config_data, dev_file, default_flow_style=False)

    configs = load_configs(temp_dir)

    assert "user_config" in configs
    assert "dev_config" in configs
    assert configs["user_config"] == user_config_data
    assert configs["dev_config"] == dev_config_data 


def test_load_configs_with_missing_files(temp_dir):
    temp_dir.mkdir()
    configs = load_configs(temp_dir)
    assert configs == {}
"""

def test_load_configs():
    base_path = os.getcwd() + "/tests/data"
    expected_result = {
        "user_config": {"user_key": {"user_key_next_level": "user_value"}},
        "dev_config": {"dev_key": {"dev_key_next_level": "dev_value"}},
    }
    # with patch("builtins.open", mock_open()) as mock_file:
    result = load_configs(base_path)
    assert result == expected_result
    # assert mock_file.call_args_list == [
    #     (("test_config/config/user_config.yaml",),),
    #     (("test_config/config/dev_config.yaml",),),
    # ]


# TESTS return_lowest_level_dict_keys
def test_return_lowest_level_dict_keys():
    # Test when the input dictionary has nested dictionaries
    dictionary = {
        "key1": {
            "subkey1": "value1",
            "subkey2": {
                "subsubkey1": "value2"
            }
        },
        "key2": "value3"
    }
    expected_result = ["subsubkey1", "subkey1", "key2"]

    result = return_lowest_level_dict_keys(dictionary)
    assert set(result) == set(expected_result)

    # Test when the input dictionary has no nested dictionaries
    dictionary = {
        "key1": "value1",
        "key2": "value2",
        "key3": None
    }
    expected_result = ["key1", "key2"]

    result = return_lowest_level_dict_keys(dictionary)
    assert result == expected_result

    # Test when the input dictionary is empty
    dictionary = {}
    expected_result = []

    result = return_lowest_level_dict_keys(dictionary)
    assert result == expected_result



# TESTS return_lowest_level_dict_values
def test_return_lowest_level_dict_values():
    # Test when the input dictionary has nested dictionaries
    dictionary = {
        "key1": {
            "subkey1": "value1",
            "subkey2": {
                "subsubkey1": "value2"
            }
        },
        "key2": "value3"
    }
    expected_result = ["value1", "value2", "value3"]

    result = return_lowest_level_dict_values(dictionary)
    assert set(result) == set(expected_result)

    # Test when the input dictionary has no nested dictionaries
    dictionary = {
        "key1": "value1",
        "key2": "value2",
        "key3": None
    }
    expected_result = ["value1", "value2"]

    result = return_lowest_level_dict_values(dictionary)
    assert set(result) == set(expected_result)

    # Test when the input dictionary is empty
    dictionary = {}
    expected_result = []

    result = return_lowest_level_dict_values(dictionary)
    assert result == expected_result


# TESTS replace_vec
def test_replace_vec():
    data = pd.DataFrame({
        "timestamp": [pd.to_datetime("2021-01-01 12:30:45"), pd.to_datetime("2022-02-02 13:45:00")]
    })

    # Test replacing only the year
    result = replace_vec(data["timestamp"], year=2023)
    expected_result = pd.to_datetime(["2023-01-01 12:30:45", "2023-02-02 13:45:00"])
    assert all(result == expected_result)

    # Test replacing only the month
    result = replace_vec(data["timestamp"], month=5)
    expected_result = pd.to_datetime(["2021-05-01 12:30:45", "2022-05-02 13:45:00"])
    assert all(result == expected_result)

    # Test replacing only the day
    result = replace_vec(data["timestamp"], day=15)
    expected_result = pd.to_datetime(["2021-01-15 12:30:45", "2022-02-15 13:45:00"])
    assert all(result == expected_result)

    # Test replacing only the hour
    result = replace_vec(data["timestamp"], hour=7)
    expected_result = pd.to_datetime(["2021-01-01 07:30:45", "2022-02-02 07:45:00"])
    assert all(result == expected_result)

    # Test replacing only the minute
    result = replace_vec(data["timestamp"], minute=15)
    expected_result = pd.to_datetime(["2021-01-01 12:15:45", "2022-02-02 13:15:00"])
    assert all(result == expected_result)

    # Test replacing multiple components
    result = replace_vec(data["timestamp"], year=2023, month=5, day=15, hour=7, minute=15, second=30)
    expected_result = pd.to_datetime(["2023-05-15 07:15:30", "2023-05-15 07:15:30"])
    assert all(result == expected_result)

    # Test not replacing any component
    result = replace_vec(data["timestamp"])
    expected_result = pd.to_datetime(["2021-01-01 12:30:45", "2022-02-02 13:45:00"])
    assert all(result == expected_result)


# TESTS create_output_folders
@pytest.fixture
def sample_configs(tmp_path):
    root_path = tmp_path / "sample_root"
    root_path.mkdir(parents=True)

    configs = {
        "user_config": {
            "global": {
                "absolute_path": {
                    "vencopy_root": str(root_path)
                }
            }
        }
    }

    return configs

"""
def test_create_output_folders(sample_configs):
    with patch("os.path.exists", return_value=False), \
         patch("os.mkdir"):
        create_output_folders(sample_configs)

    assert os.path.exists(Path(sample_configs["user_config"]["global"]["absolute_path"]["vencopy_root"]))
    main_dir = "output"
    assert os.path.exists(Path(sample_configs["user_config"]["global"]["absolute_path"]["vencopy_root"]) / main_dir)

    sub_dirs = (
        "dataparser",
        "diarybuilder",
        "gridmodeler",
        "flexestimator",
        "profileaggregator",
        "postprocessor"
    )

    for sub_dir in sub_dirs:
        assert os.path.exists(Path(sample_configs["user_config"]["global"]["absolute_path"]["vencopy_root"]) / main_dir / sub_dir)
"""


# TESTS create_file_name
def test_create_file_name():
    dev_config = {
        "global": {
            "disk_file_names": {
                "file1": "file1_dev"
            }
        }
    }
    user_config = {
        "global": {
            "run_label": "run123"
        }
    }

    # Test when dataset is None, manual_label is empty, and suffix is 'csv'
    result = create_file_name(dev_config, user_config, "file1", None)
    assert result == "file1_dev_run123.csv"

    # Test when dataset is provided, manual_label is empty, and suffix is 'txt'
    result = create_file_name(dev_config, user_config, "file1", "dataset1", suffix="txt")
    assert result == "file1_dev_run123_dataset1.txt"

    # Test when manual_label is provided, and dataset and suffix are None
    result = create_file_name(dev_config, user_config, "file1", None, manual_label="label123")
    assert result == "file1_dev_run123_label123.csv"

    # Test when all parameters are provided
    result = create_file_name(dev_config, user_config, "file1", "dataset1", manual_label="label123", suffix="txt")
    assert result == "file1_dev_run123_label123_dataset1.txt"

"""
# TESTS merge_variables
def test_merge_variables():
    data = pd.DataFrame({
        "unique_id": [1, 2, 3],
        "var1": [10, 20, 30],
        "var2": [100, 200, 300]
    })
    dataset = pd.DataFrame({
        "unique_id": [1, 2, 3],
        "var1": [11, 22, 33],
        "var3": [111, 222, 333]
    })
    variables = ["var1", "var2", "var3"]

    # Test when 'unique_id' is not in data.index.names
    result = merge_variables(data, dataset, variables)

    expected_result = pd.DataFrame({
        "unique_id": [1, 2, 3],
        "var1": [10, 20, 30],
        "var2": [100, 200, 300],
        "var3": [111, 222, 333]
    })

    assert result == expected_result

    # Test when 'unique_id' is already in data.index.names
    data.set_index("unique_id", inplace=True)
    result = merge_variables(data, dataset, variables)

    expected_result = pd.DataFrame({
        "unique_id": [1, 2, 3],
        "var1": [10, 20, 30],
        "var2": [100, 200, 300],
        "var3": [111, 222, 333]
    })

    pd.testing.assert_frame_equal(result, expected_result)


# TESTS write_out
def test_write_out(temp_dir):
    data = pd.DataFrame({
        "A": [1, 2, 3],
        "B": [4, 5, 6]
    })
    output_path = Path(temp_dir) / "output.csv"

    write_out(data, output_path)

    assert os.path.isfile(output_path)
    assert output_path.exists()

    loaded_data = pd.read_csv(output_path)
    assert data.equals(loaded_data)

    captured = capsys.readouterr()  # capsys is a built-in fixture for capturing printed output
    assert f"Dataset written to {output_path}." in captured.out
"""
