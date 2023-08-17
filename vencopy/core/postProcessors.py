__version__ = "0.4.X"
__maintainer__ = "Niklas Wulff, Fabia Miorelli"
__birthdate__ = "12.07.2023"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


from pathlib import Path
import pandas as pd
import warnings

from vencopy.core.profileAggregators import ProfileAggregator
from vencopy.utils.globalFunctions import createFileName, writeOut


class PostProcessing:
    """
    This class contains functions to post process aggregated venco.py profiles. As of now (August 2023), the class
    contains cloning weekly profiles to year and normalizing it with different normalization bases.
    """

    def __init__(self, configDict: dict):
        self.user_config = configDict["user_config"]
        self.dev_config = configDict["dev_config"]
        self.datasetID = self.user_config["global"]["dataset"]
        self.deltaTime = self.user_config["diaryBuilders"]["TimeDelta"]
        self.timeIndex = list(pd.timedelta_range(start="00:00:00", end="24:00:00", freq=f"{self.deltaTime}T"))

        self.drain = None
        self.charge_power = None
        self.uncontrolled_charge = None
        self.max_battery_level = None
        self.min_battery_level = None

        self.input_profiles = {}
        self.annual_profiles = {}
        self.norm_profiles = {}

    def __store_input(self, name: str, profile: pd.Series):
        self.input_profiles[name] = profile

    def __create_annual_profiles(self, profile: pd.Series) -> pd.Series:
        startWeekday = self.user_config["postProcessing"]["startWeekday"]  # (1: Monday, 7: Sunday)
        nTimeSlotsPerDay = len(list(self.timeIndex))

        # Shift input profiles to the right weekday and start with first bin of chosen weekday
        annual = profile.iloc[((startWeekday - 1) * (nTimeSlotsPerDay - 1)) :]
        annual = pd.DataFrame(annual.to_list() * 53)
        return annual.drop(annual.tail(len(annual) - (nTimeSlotsPerDay - 1) * 365).index)

    def week_to_annual(self, profiles: ProfileAggregator):
        profiles = (
            profiles.drainWeekly,
            profiles.uncontrolledChargeWeekly,
            profiles.chargingPowerWeekly,
            profiles.maxBatteryLevelWeekly,
            profiles.minBatteryLevelWeekly,
        )
        pnames = ("drain", "uncontrolled_charge", "charge_power", "max_battery_level", "min_battery_level")
        for pname, p in zip(pnames, profiles):
            self.__store_input(name=pname, profile=p)
            self.annual_profiles[pname] = self.__create_annual_profiles(profile=p)
            if self.user_config["global"]["writeOutputToDisk"]["processorOutput"]["absolute_annual_profiles"]:
                self._write_output(
                    profile_name=pname, profile=self.annual_profiles[pname], filename_id="outputPostProcessorAnnual"
                )
        print("Run finished.")

    # Simpler less generic implementation
    def normalize(self):
        self.drain_norm = self.__normalize_flows(self.input_profiles["drain"])
        self.drain_uc = self.__normalize_flows(self.input_profiles["uncontrolled_charge"])
        self.charge_power = self.__normalize_states(
            profile=self.input_profiles["charge_power"], base=self.user_config["gridModelers"]["ratedPowerSimple"]
        )
        self.soc_max = self.__normalize_states(
            profile=self.input_profiles["max_battery_level"],
            base=self.user_config["flexEstimators"]["Battery_capacity"],
        )
        self.soc_min = self.__normalize_states(
            profile=self.input_profiles["min_battery_level"],
            base=self.user_config["flexEstimators"]["Battery_capacity"],
        )

    def __normalize_flows(self, profile: pd.Series) -> pd.Series:
        return profile / profile.sum()

    def __normalize_states(self, profile: pd.Series, base: int) -> pd.Series:
        return profile / base

    def _write_output(self, profile_name: str, profile: pd.Series, filename_id: str):
        root = Path(self.user_config["global"]["pathAbsolute"]["vencopyRoot"])
        folder = self.dev_config["global"]["pathRelative"]["post_processing_output"]
        fileName = createFileName(
            dev_config=self.dev_config,
            user_config=self.user_config,
            manualLabel=profile_name,
            fileNameID=filename_id,
            datasetID=self.datasetID,
        )
        writeOut(data=profile, path=root / folder / fileName)
