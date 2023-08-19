__version__ = "1.0.0"
__maintainer__ = "Niklas Wulff, Fabia Miorelli"
__birthdate__ = "01.08.2023"
__status__ = "test"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


from pathlib import Path
import pandas as pd
import warnings

from vencopy.core.profileaggregators import ProfileAggregator
from vencopy.utils.utils import create_file_name, write_out


class PostProcessor:
    """
    This class contains functions to post process aggregated venco.py profiles. As of now (August 2023), the class
    contains cloning weekly profiles to year and normalizing it with different normalization bases.
    """

    def __init__(self, configs: dict):
        self.user_config = configs["user_config"]
        self.dev_config = configs["dev_config"]
        self.dataset = self.user_config["global"]["dataset"]
        self.time_resolution = self.user_config["diarybuilders"]["time_resolution"]
        self.time_delta = pd.timedelta_range(start="00:00:00", end="24:00:00", freq=f"{self.time_resolution}T")
        self.time_index = list(self.time_delta)

        self.drain = None
        self.charging_power = None
        self.uncontrolled_charging = None
        self.max_battery_level = None
        self.min_battery_level = None

        self.input_profiles = {}
        self.annual_profiles = {}

        self.drain_normalised = None
        self.charging_power_normalised = None
        self.uncontrolled_charging_normalised = None
        self.max_battery_level_normalised = None
        self.min_battery_level_normalised = None

    def __store_input(self, name: str, profile: pd.Series):
        self.input_profiles[name] = profile

    def __week_to_annual_profile(self, profile: pd.Series) -> pd.Series:
        start_weekday = self.user_config["postprocessor"]["start_weekday"]  # (1: Monday, 7: Sunday)
        n_timeslots_per_day = len(list(self.time_index))
        # Shift input profiles to the right weekday and start with first bin of chosen weekday
        annual = profile.iloc[((start_weekday - 1) * (n_timeslots_per_day - 1)) :]
        annual = pd.DataFrame(annual.to_list() * 53)
        return annual.drop(annual.tail(len(annual) - (n_timeslots_per_day - 1) * 365).index)

    def create_annual_profiles(self, profiles: ProfileAggregator):
        profiles = (
            profiles.drain_weekly,
            profiles.uncontrolled_charging_weekly,
            profiles.charging_power_weekly,
            profiles.max_battery_level_weekly,
            profiles.min_battery_level_weekly,
        )
        profile_names = ("drain", "uncontrolled_charging", "charging_power", "max_battery_level", "min_battery_level")
        for profile_name, profile in zip(profile_names, profiles):
            self.__store_input(name=profile_name, profile=profile)
            self.annual_profiles[profile_name] = self.__week_to_annual_profile(profile=profile)
            if self.user_config["global"]["write_output_to_disk"]["processor_output"]["absolute_annual_profiles"]:
                self.__write_output(
                    profile_name=profile_name, profile=self.annual_profiles[profile_name], filename_id="output_postprocessor_annual"
                )
        print("Run finished.")

    def normalise(self):
        self.drain_normalised = self.__normalize_flows(self.input_profiles["drain"])
        self.uncontrolled_charging_normalised = self.__normalize_flows(self.input_profiles["uncontrolled_charging"])
        self.charging_power_normalised = self.__normalize_states(
            profile=self.input_profiles["charging_power"], base=self.user_config["gridmodelers"]["rated_power_simple"]
        )
        self.max_battery_level_normalised = self.__normalize_states(
            profile=self.input_profiles["max_battery_level"],
            base=self.user_config["flexestimators"]["battery_capacity"],
        )
        self.min_battery_level_normalised = self.__normalize_states(
            profile=self.input_profiles["min_battery_level"],
            base=self.user_config["flexestimators"]["battery_capacity"],
        )

        if self.user_config["gridmodelers"]["grid_model"] != "simple":
            raise(TypeError(
                f"You selected a grid model where normalization is not meaningful. For normalization, the"
                f" rated power of {self.user_config['gridmodelers']['rated_power_simple']}kW was used."
            ))

        if self.user_config["global"]["write_output_to_disk"]["processor_output"]["normalised_annual_profiles"]:
            self.__write_out_profiles(filename_id="output_postProcessor_normalised")

    def __normalize_flows(self, profile: pd.Series) -> pd.Series:
        return profile / profile.sum()

    def __normalize_states(self, profile: pd.Series, base: int) -> pd.Series:
        return profile / base

    def __write_out_profiles(self, filename_id: str):
        self.__write_output(profile_name="drain", profile=self.drain_normalised, filename_id=filename_id)
        self.__write_output(
            profile_name="uncontrolled_charge", profile=self.uncontrolled_charging_normalised, filename_id=filename_id
        )
        self.__write_output(profile_name="charge_power", profile=self.charging_power_normalised, filename_id=filename_id)
        self.__write_output(profile_name="max_battery_level", profile=self.max_battery_level_normalised, filename_id=filename_id)
        self.__write_output(profile_name="min_battery_level", profile=self.min_battery_level_normalised, filename_id=filename_id)

    def __write_output(self, profile_name: str, profile: pd.Series, filename_id: str):
        root = Path(self.user_config["global"]["absolute_path"]["vencopy_root"])
        folder = self.dev_config["global"]["relative_path"]["processor_output"]
        file_name = create_file_name(
            dev_config=self.dev_config,
            user_config=self.user_config,
            manual_label=profile_name,
            file_name_id=filename_id,
            dataset=self.dataset,
        )
        write_out(data=profile, path=root / folder / file_name)
