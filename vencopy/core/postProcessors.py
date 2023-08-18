__version__ = "0.4.X"
__maintainer__ = "Niklas Wulff, Fabia Miorelli"
__birthdate__ = "12.07.2023"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


from pathlib import Path
import pandas as pd
import warnings

from vencopy.core.profileAggregators import ProfileAggregator
from vencopy.utils.globalFunctions import create_file_name, write_out


class PostProcessing:
    """
    This class contains functions to post process aggregated venco.py profiles. As of now (August 2023), the class
    contains cloning weekly profiles to year and normalizing it with different normalization bases.
    """

    def __init__(self, config_dict: dict):
        self.user_config = config_dict["user_config"]
        self.dev_config = config_dict["dev_config"]
        self.dataset_ID = self.user_config["global"]["dataset"]
        self.delta_time = self.user_config["diaryBuilders"]["TimeDelta"]
        self.time_idx = list(pd.timedelta_range(start="00:00:00", end="24:00:00", freq=f"{self.delta_time}T"))

        self.drain = None
        self.charge_power = None
        self.uncontrolled_charge = None
        self.max_battery_level = None
        self.min_battery_level = None

        self.input_profiles = {}
        self.annual_profiles = {}

        self.drain_norm = None
        self.charge_power_norm = None
        self.uncontrolled_charge_norm = None
        self.max_battery_level_norm = None
        self.min_battery_level_norm = None

    def __store_input(self, name: str, profile: pd.Series):
        self.input_profiles[name] = profile

    def __create_annual_profiles(self, profile: pd.Series) -> pd.Series:
        start_weekday = self.user_config["postProcessing"]["start_weekday"]  # (1: Monday, 7: Sunday)
        n_timeslots_per_day = len(list(self.time_idx))

        # Shift input profiles to the right weekday and start with first bin of chosen weekday
        annual = profile.iloc[((start_weekday - 1) * (n_timeslots_per_day - 1)) :]
        annual = pd.DataFrame(annual.to_list() * 53)
        return annual.drop(annual.tail(len(annual) - (n_timeslots_per_day - 1) * 365).index)

    def week_to_annual(self, profiles: ProfileAggregator):
        profiles = (
            profiles.drain_weekly,
            profiles.uncontrolled_charge_weekly,
            profiles.charge_power_weekly,
            profiles.max_battery_level_weekly,
            profiles.min_battery_level_weekly,
        )
        pnames = ("drain", "uncontrolled_charge", "charge_power", "max_battery_level", "min_battery_level")
        for pname, p in zip(pnames, profiles):
            self.__store_input(name=pname, profile=p)
            self.annual_profiles[pname] = self.__create_annual_profiles(profile=p)
            if self.user_config["global"]["writeOutputToDisk"]["processorOutput"]["absolute_annual_profiles"]:
                self.__write_output(
                    profile_name=pname, profile=self.annual_profiles[pname], filename_id="outputPostProcessorAnnual"
                )
        print("Run finished.")

    def normalize(self):
        self.drain_norm = self.__normalize_flows(self.input_profiles["drain"])
        self.uncontrolled_charge_norm = self.__normalize_flows(self.input_profiles["uncontrolled_charge"])
        self.charge_power_norm = self.__normalize_states(
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

        if self.user_config["gridModelers"]["gridModel"] != "simple":
            warnings.warn(
                f"You selected a grid model where normalization is not meaningful. For normalization, the"
                f" rated power of {self.user_config['gridModelers']['chargePowerSimple']}kW was used."
            )

        if self.user_config["global"]["writeOutputToDisk"]["processorOutput"]["normalised_annual_profiles"]:
            self.__write_out_profiles(filename_id="outputPostProcessorNorm")

    def __normalize_flows(self, profile: pd.Series) -> pd.Series:
        return profile / profile.sum()

    def __normalize_states(self, profile: pd.Series, base: int) -> pd.Series:
        return profile / base

    def __write_out_profiles(self, filename_id: str):
        self.__write_output(profile_name="drain", profile=self.drain_norm, filename_id=filename_id)
        self.__write_output(
            profile_name="uncontrolled_charge", profile=self.uncontrolled_charge_norm, filename_id=filename_id
        )
        self.__write_output(profile_name="charge_power", profile=self.charge_power_norm, filename_id=filename_id)
        self.__write_output(profile_name="max_battery_level", profile=self.soc_max, filename_id=filename_id)
        self.__write_output(profile_name="min_battery_level", profile=self.soc_min, filename_id=filename_id)

    def __write_output(self, profile_name: str, profile: pd.Series, filename_id: str):
        root = Path(self.user_config["global"]["pathAbsolute"]["vencopyRoot"])
        folder = self.dev_config["global"]["pathRelative"]["post_processing_output"]
        fileName = create_file_name(
            dev_config=self.dev_config,
            user_config=self.user_config,
            manualLabel=profile_name,
            fileNameID=filename_id,
            datasetID=self.dataset_ID,
        )
        write_out(data=profile, path=root / folder / fileName)
