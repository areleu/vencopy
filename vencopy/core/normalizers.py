__version__ = "0.4.X"
__maintainer__ = "Niklas Wulff, Fabia Miorelli"
__birthdate__ = "12.07.2023"
__status__ = "dev"  # options are: dev, test, prod
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

    def _writeOutput(self):
        root = Path(self.localPathConfig["pathAbsolute"]["vencoPyRoot"])
        folder = self.globalConfig["pathRelative"]["normaliserOutput"]
        fileName = createFileName(
            globalConfig=self.globalConfig,
            manualLabel=("_" + ""),
            fileNameID="outputNormaliser",
            datasetID=self.datasetID,
        )
        writeOut(data=self.profile, path=root / folder / fileName)

    def normalize(self, flow_profiles: dict, state_profiles: dict, capacity_profiles: dict):
        # Normalization basis: Total annual energy
        for key, value in flow_profiles.items():
            self.flow_profiles[key] = self.__normalize_flows(value)

        # Normalization basis: Rated power
        for key, value in capacity_profiles.items():
            self.capacity_profiles[key] = self.__normalize_states(profile=value, base=self.__rated_power_simple)
            if self.grid_config["gridModel"] != "simple":
                warnings.warn(
                    f"You selected a grid model where normalization is not meaningful. For normalization, the"
                    f" rated power of {self.__rated_power_simple}kW was used."
                )

        # Normalization of battery level profiles using battery capacity
        for key, value in state_profiles.items():
            self.state_profiles[key] = self.__normalize_states(profile=value, base=self.__bat_cap)

    def __normalize_flows(self, profile: pd.Series) -> pd.Series:
        return profile / profile.sum()

    def __normalize_states(self, profile: pd.Series, base: int) -> pd.Series:
        return profile / base

    def __normalize_max(self, profile):
        return profile / profile.max()
