__version__ = "1.0.0"
__maintainer__ = "Fabia Miorelli"
__birthdate__ = "04.09.2023"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"

import pytest
import pandas as pd

from typing import Any, Literal

from ....vencopy.core.dataparsers.dataparsers import DataParser
from ....vencopy.core.dataparsers.parseKiD import ParseKiD


# TODO: test init
# TODO: test _load_data()
# TODO: test __harmonise_variables()
# TODO: test __add_string_columns()
# TODO: test __change_separator()()
# TODO: test _extract_timestamps()
# TODO: test __update_end_timestamp()
# TODO: test __exclude_hours()

