__version__ = "1.0.0"
__maintainer__ = "Fabia Miorelli"
__birthdate__ = "04.09.2023"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"

import pytest
import pandas as pd


from ....vencopy.core.dataparsers.parkinference import ParkInference
from ....vencopy.core.dataparsers.parseMiD import ParseMiD

# NOT TESTED: process(), __add_string_columns()

@pytest.fixture
def parse_mid_instance():
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
    return ParseMiD(configs, dataset)


def test_parse_mid_init(parse_mid_instance):
    assert isinstance(parse_mid_instance, ParseMiD)
    assert isinstance(parse_mid_instance.park_inference, ParkInference)


def test_harmonise_variables(parse_mid_instance):
    trips = pd.DataFrame({
        "var1dataset1": [1, 2, 3],
        "var2dataset1": [4, 5, 6],
    })

    parse_mid_instance.trips = trips
    parse_mid_instance._harmonise_variables()

    expected_result = pd.DataFrame({
        "var1": [1, 2, 3],
        "var2": [4, 5, 6],
    })
    pd.testing.assert_frame_equal(parse_mid_instance.trips, expected_result)

<<<<<<< HEAD

=======
>>>>>>> main
