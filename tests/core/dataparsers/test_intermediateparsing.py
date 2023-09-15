__version__ = "1.0.0"
__maintainer__ = "Fabia Miorelli"
__birthdate__ = "04.09.2023"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"

import pytest
import pandas as pd

from mock import patch
from dateutil import parser
from typing import Any, Literal

from ....vencopy.core.dataparsers.dataparsers import DataParser
from ....vencopy.core.dataparsers.dataparsers import IntermediateParsing

# NOT TESTED: 

class MockIntermediateParsing(DataParser):
    def __init__(self):
        dev_config = {
            "dataparsers": {
                "data_variables": {
                    "dataset": ["dataset1", "dataset2", "dataset3"]
                    }}}
        self.dev_config = dev_config
        # self.debug = False


@pytest.fixture
def mock_data_parser():
    return MockIntermediateParsing()
