__version__ = "1.0.0"
__maintainer__ = "Fabia Miorelli"
__birthdate__ = "04.09.2023"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"

import pytest

import pandas as pd

from ...vencopy.core.diarybuilders import DiaryBuilder

# NOT TESTED: 


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


def test_diarybuilder_init(sample_configs):
    sample_activities_data = pd.DataFrame({})
    builder = DiaryBuilder(configs=sample_configs, activities=sample_activities_data)

    assert builder.dev_config == sample_configs["dev_config"]
    assert builder.user_config == sample_configs["user_config"]
    assert builder.dataset == "dataset1"
    assert builder.activities.equals(sample_activities_data)
    assert builder.time_resolution == 15
    assert builder.is_week_diary == False

