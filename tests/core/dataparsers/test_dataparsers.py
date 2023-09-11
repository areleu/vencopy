__version__ = "1.0.0"
__maintainer__ = "Fabia Miorelli"
__birthdate__ = "04.09.2023"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"

import pytest

from typing import Any, Literal

from ....vencopy.core.dataparsers.dataparsers import DataParser
from ....vencopy.core.dataparsers.parseMiD import ParseMiD
from ....vencopy.core.dataparsers.parseKiD import ParseKiD


"""
sample_configs = {
    "user_config": {
        "global": {
            "absolute_path": {
                "MiD08": "/path/to/MiD08_data",
                "MiD17": "/path/to/MiD17_data"
            }
        }
    },
    "dev_config": {
        "global": {
            "files": {
                "MiD08": {"trips_data_raw": "raw_data_MiD08.csv"},
                "MiD17": {"trips_data_raw": "raw_data_MiD17.csv"}
            }
        }
    }
}

 @pytest.mark.parametrize(
    "configs, dataset, load_encrypted, expected_message",
    [
        (sample_configs, "MiD08", True, "Starting to retrieve encrypted data file from"),
        (sample_configs, "MiD17", False, "Starting to retrieve local data file from"),
    ],
)
def test_dataparsers_initialization(configs: dict[str, Any], dataset: Literal['MiD08', 'MiD17'], load_encrypted: bool, expected_message: Literal['Starting to retrieve encrypted data file from', 'Starting to retrieve local data file from']):
    obj = DataParser(configs, dataset, load_encrypted)

    assert obj.user_config == configs["user_config"]
    assert obj.dev_config == configs["dev_config"]
    assert obj.dataset == dataset
    assert obj.raw_data_path == expected_raw_data_path
    assert obj.raw_data is None  
    assert obj.trips is None  
    assert obj.activities is None  
    assert obj.trips_end_next_day_raw is None  
    assert obj.debug == debug
    captured = capsys.readouterr()
    assert expected_message in captured.out """

@pytest.fixture
def dataparser_class_instance():
    dev_config = {
        "dataparsers": {
            "data_variables": {
                "dataset": ["dataset1", "dataset2"]
            }
        }
    }
    user_config = {
        "global": {
            "dataset": "dataset"
        }
    }
    dev_config = dev_config
    user_config = user_config
    debug = True
    dataset = "dataset"
    return DataParser(user_config, dev_config, dataset, debug)


def test_check_dataset_id_valid(dataparser_class_instance):
    dataset = "dataset1"
    result = dataparser_class_instance.__check_dataset_id(dataset)
    assert result == dataset


def test_check_dataset_id_invalid(dataparser_class_instance):
    dataset = "invalid_dataset"
    with pytest.raises(AssertionError) as e:
        dataparser_class_instance.__check_dataset_id(dataset)

    expected_error_message = (
        f"Defined dataset {dataset} not specified "
        f"under data_variables in dev_config. "
        f"Specified datasetIDs are ['dataset1', 'dataset2']"
    )

    assert str(e.value) == expected_error_message
