__version__ = "1.0.0"
__maintainer__ = "Fabia Miorelli"
__birthdate__ = "04.09.2023"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"

import pytest
import pandas as pd

from typing import Any, Literal

from ....vencopy.core.dataparsers.dataparsers import DataParser
from ....vencopy.core.dataparsers.parseMiD import ParseMiD


# TODO: test init
# TODO: test __harmonize_variables()
# TODO: test __add_string_columns()
# TODO: test_drop_redundant_columns()