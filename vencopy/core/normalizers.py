__version__ = "1.0.X"
__maintainer__ = "Niklas Wulff, Fabia Miorelli"
__birthdate__ = "12.07.2023"
__status__ = "test"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"

if __package__ is None or __package__ == "":
    import sys
    from os import path

    sys.path.append(path.dirname(path.dirname(path.dirname(__file__))))

from pathlib import Path
import pandas as pd
import warnings

from vencopy.core.profileAggregators import ProfileAggregator
from vencopy.utils.globalFunctions import createFileName, writeOut


class Normalizer:
    def __init__(self, configDict: dict, profiles: ProfileAggregator):
        self.globalConfig = configDict["globalConfig"]
        self.localPathConfig = configDict["localPathConfig"]
        self.grid_config = configDict["gridConfig"]
        self.__rated_power_simple = self.grid_config["ratedPowerSimple"]
        self.__bat_cap = configDict["flexConfig"]["Battery_capacity"]
        self.flow_profiles = {}
        self.capacity_profiles = {}
        self.state_profiles = {}
