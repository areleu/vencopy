__version__ = "1.0.0"
__maintainer__ = "Niklas Wulff, Fabia Miorelli"
__birthdate__ = "01.08.2023"
__status__ = "test"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


from pathlib import Path
import pandas as pd

from ..core.profileaggregators import ProfileAggregator
from ..utils.utils import create_file_name, write_out


class PostProcessor:
    """
    This class contains functions to post process aggregated venco.py profiles. As of now (August 2023), the class
    contains cloning weekly profiles to year and normalizing it with different normalization bases.
    """
    def __init__(self, configs: dict, profiles: ProfileAggregator):
        self.user_config = configs["user_config"]
        self.dev_config = configs["dev_config"]
        self.dataset = self.user_config["global"]["dataset"]
        self.time_resolution = self.user_config["diarybuilders"]["time_resolution"]
        self.time_delta = pd.timedelta_range(start="00:00:00", end="24:00:00", freq=f"{self.time_resolution}T")
        self.time_index = list(self.time_delta)
        self.vehicle_numbers = [len(_) for _ in profiles.activities.groupby(by="trip_start_weekday").unique_id.unique().to_list()]

        self.drain = profiles.drain_weekly
        self.charging_power = profiles.charging_power_weekly
        self.uncontrolled_charging = profiles.uncontrolled_charging_weekly
        self.max_battery_level = profiles.max_battery_level_weekly
        self.min_battery_level = profiles.min_battery_level_weekly

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

    def __normalize_flows(self, profile: pd.Series) -> pd.Series:
        return profile / profile.sum()

    def __normalize_states(self, profile: pd.Series, base: int) -> pd.Series:
        return profile / base

    def __normalize_charging_power(self, profile: pd.Series, base: int) -> pd.Series:
        profile_normalised = []
        for day in range(0, 7):
            start = day * len(self.time_delta)
            end = start + len(self.time_delta) - 1
            profile_day = profile[start:end] / base[day]
            if profile_normalised == []:
                profile_normalised = profile_day.to_list()
            else:
                profile_normalised.extend(profile_day.to_list())
        profile = pd.Series(profile_normalised)
        return profile

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

    def create_annual_profiles(self):
        if self.user_config["profileaggregators"]['aggregation_timespan'] == "daily":
            print('The annual profiles cannot be generated as the aggregation was performed over a single day.')
        else:
            profiles = (self.drain, self.uncontrolled_charging, self.charging_power, self.max_battery_level, self.min_battery_level)
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
        if self.user_config["profileaggregators"]['aggregation_timespan'] == "daily":
            print('The annual profiles cannot be normalised as the aggregation was performed over a single day.')
            print('Run finished.')
        else:
            self.drain_normalised = self.__normalize_flows(self.input_profiles["drain"])
            self.uncontrolled_charging_normalised = self.__normalize_flows(self.input_profiles["uncontrolled_charging"])
            self.charging_power_normalised = self.__normalize_charging_power(
                profile=self.input_profiles["charging_power"], base=self.vehicle_numbers
                )
            self.max_battery_level_normalised = self.__normalize_states(
                profile=self.input_profiles["max_battery_level"],
                base=self.user_config["flexestimators"]["battery_capacity"],
            )
            self.min_battery_level_normalised = self.__normalize_states(
                profile=self.input_profiles["min_battery_level"],
                base=self.user_config["flexestimators"]["battery_capacity"],
            )
            if self.user_config["global"]["write_output_to_disk"]["processor_output"]["normalised_annual_profiles"]:
                self.__write_out_profiles(filename_id="output_postprocessor_normalised")
            print("Run finished.")
