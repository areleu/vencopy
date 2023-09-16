__version__ = "1.0.0"
__maintainer__ = "Fabia Miorelli"
__birthdate__ = "04.09.2023"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"

import pytest
import pandas as pd

from ....vencopy.core.dataparsers.dataparsers import IntermediateParsing

# NOT TESTED: 


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
                    'dataset': ['dataset1', 'dataset2', 'dataset3']
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
