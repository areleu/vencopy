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
    """This class contains functions to post process aggregated venco.py profiles. As of now (August 2023), the class
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
            if self.user_config["global"]["writeOutputToDisk"]["absolute_annual_output"]:
                self._write_output(
                    profile_name=pname, profile=self.annual_profiles[pname], filename_id="outputPostProcessorAnnual"
                )
        print("Run finished.")

    def __categorize_profiles(self, profiles: dict) -> dict:
        p_dict = {}
        p_dict["flow_profiles"] = {}
        p_dict["flow_profiles"] = {
            "drain": profiles["drain"],
            "uncontrolled_charge": profiles["uncontrolled_charge"],
        }
        p_dict["state_profiles"] = {
            "charge_power": profiles["charge_power"],
            "max_battery_level": profiles["max_battery_level"],
            "min_battery_level": profiles["min_battery_level"],
        }
        return p_dict

    def normalize(self, profiles: ProfileAggregator = None):
        if profiles:
            p_dict = self.__categorize_profiles(profiles.profile_dict())
        else:
            p_dict = self.__categorize_profiles(self.input_profiles)

        if self.user_config["gridModelers"]["gridModel"] != "simple":
            warnings.warn(
                f"You selected a grid model where normalization is not meaningful. For normalization, the"
                f" rated power of {self.__rated_power_simple}kW was used."
            )

        write = self.user_config["global"]["writeOutputToDisk"]["normalized_annual_output"]

        # Normalization basis: Total annual energy
        for key, value in p_dict["flow_profiles"].items():
            p = self.__normalize_flows(value)
            self.norm_profiles[key] = p
            if write:
                self._write_output(profile_name=key, profile=p, filename_id="outputPostProcessorNorm")

        # Normalization of battery level profiles using battery capacity
        for key, value in p_dict["state_profiles"].items():
            if "battery_level" in key:
                p = self.__normalize_states(profile=value, base=self.user_config["flexEstimators"]["Battery_capacity"])
            else:
                p = self.__normalize_states(profile=value, base=self.user_config["gridModelers"]["ratedPowerSimple"])
            self.norm_profiles[key] = p
            if write:
                self._write_output(profile_name=key, profile=p, filename_id="outputPostProcessorNorm")

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
