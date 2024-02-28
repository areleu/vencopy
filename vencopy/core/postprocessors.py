__maintainer__ = "Niklas Wulff, Fabia Miorelli"
__license__ = "BSD-3-Clause"


import pandas as pd

from pathlib import Path

from ..core.profileaggregators import ProfileAggregator
from ..utils.utils import create_file_name, write_out


class PostProcessor:
    def __init__(self, configs: dict, profiles: ProfileAggregator):
        """
        This class contains functions to post-process aggregated venco.py profiles for cloning weekly profiles
        to year and normalizing it with different normalization bases.

        In the PostProcessor, two steps happen. First, the aggregated weekly timeseries for the fleet are translated
        into annual timeseries by cloning e.g. a week by around 52 times to span a full year. The second purpose is to
        normalize the aggregated fleet profiles in order to provide profiles independent of the specific input and to be
        able to scale the venco.py output in a consecutive stept with feet scenarios or annual energy demands. For the
        flow profiles, two normalization bases are applied: The drain and uncontrolled charging profiles are normalized
        to the annual energy volume of the profiles to be scaled with an annual energy demand. The charging power
        profile is normalized by the number of vehicles to retrieve a rated charging power per representative vehicle.
        The two state profiles - minimum and maximum battery level profiles - are normalized using the battery capacity
        given in the flexestimaros part of the user_config.


        Args:
            configs (dict): Dictionary holding a user_config and a dev_config dictionary
            profiles (ProfileAggregator): An instance of class ProfileAggregator
        """
        self.user_config = configs["user_config"]
        self.dev_config = configs["dev_config"]
        self.dataset = self.user_config["global"]["dataset"]
        self.time_resolution = self.user_config["diarybuilders"]["time_resolution"]
        self.time_delta = pd.timedelta_range(
            start="00:00:00", end="24:00:00", freq=f"{self.time_resolution}T"
        )
        self.time_index = list(self.time_delta)
        self.vehicle_numbers = [
            len(_)
            for _ in profiles.activities.groupby(by="trip_start_weekday")
            .unique_id.unique()
            .to_list()
        ]

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
        """
        Utility function to safe a given profile under a specific name (key) in the class attribute
        self.input_profiles.

        Args:
            name (str): A string specifying the profile name
            profile (pd.Series): A profile that can be of arbitrary length
        """
        self.input_profiles[name] = profile

    def __week_to_annual_profile(self, profile: pd.Series) -> pd.Series:
        """
        This function clones a profile with a given temporal resolution from a weekly profile to the full year.
        Overreaching time intervals are being truncated at the year border (December, 31st). Currently, gap years are
        not considered.

        Args:
            profile (pd.Series): Any profile spanning one week in variable resolution

        Returns:
            pd.Series: Annual profile in the same temporal resolution as the input profile
        """
        start_weekday = self.user_config["postprocessor"][
            "start_weekday"
        ]  # (1: Monday, 7: Sunday)
        n_timeslots_per_day = len(list(self.time_index))
        # Shift input profiles to the right weekday and start with first bin of chosen weekday
        annual = profile.iloc[((start_weekday - 1) * (n_timeslots_per_day - 1)) :]
        annual = pd.DataFrame(annual.to_list() * 53)
        return annual.drop(
            annual.tail(len(annual) - (n_timeslots_per_day - 1) * 365).index
        )

    @staticmethod
    def __normalize_flows(profile: pd.Series) -> pd.Series:
        """
        Function to normalise a timeseries according to its annual sum. Used in venco.py for normalisation of
        uncontrolled charging and drain profiles.

        Args:
            profile (pd.Series): Profile to be normalised

        Returns:
            pd.Series: Normalized timeseries
        """
        return profile / profile.sum()

    @staticmethod
    def __normalize_states(profile: pd.Series, base: int) -> pd.Series:
        """
        Function to normalise a state profile according to a baseline value. Used in venco.py for normalisation of the
        minimum and maximum battery level profiles based on the vehicle battery capacity assumed in the flexestimator
        config.

        Args:
            profile (pd.Series): State profile to be normalised
            base (int): normalisation basis, e.g. battery capacity in kWh

        Returns:
            pd.Series: Normalized battery level between 0 and 1
        """
        return profile / base

    @staticmethod
    def __normalize_charging_power(
        profile: pd.Series, base: int, time_delta: pd.timedelta_range
    ) -> pd.Series:
        """
        Function to normalise a timeseries according to a baseline value for each weekday. Used in venco.py for
        normalisation of the charging power profiles based on the number of vehicles for each weekday.

        Args:
            profile (pd.Series): Profile to be normalised
            base (int): normalisation basis, e.g. number of vehicles
            time_delta (pd.timedelta_range): Temporal resolution of the run

        Returns:
            pd.Series: Charging power profile of the fleet normalized to the number of vehicles of the fleet
        """
        profile_normalised = []
        for day in range(0, 7):
            start = day * len(time_delta)
            end = start + len(time_delta) - 1
            profile_day = profile[start:end] / base[day]
            if profile_normalised == []:
                profile_normalised = profile_day.to_list()
            else:
                profile_normalised.extend(profile_day.to_list())
        profile = pd.Series(profile_normalised)
        return profile

    def __write_out_profiles(self, filename_id: str):
        """
        Quality-of-life wrapper-function to write-out the main five output profiles of venco.py, namely drain,
        uncontrolled charging, charging power, max battery level and min battery level

        Args:
            filename_id (str): Label that is added to the filename on write-out
        """
        self.__write_output(
            profile_name="drain", profile=self.drain_normalised, filename_id=filename_id
        )
        self.__write_output(
            profile_name="uncontrolled_charging",
            profile=self.uncontrolled_charging_normalised,
            filename_id=filename_id,
        )
        self.__write_output(
            profile_name="charging_power",
            profile=self.charging_power_normalised,
            filename_id=filename_id,
        )
        self.__write_output(
            profile_name="max_battery_level",
            profile=self.max_battery_level_normalised,
            filename_id=filename_id,
        )
        self.__write_output(
            profile_name="min_battery_level",
            profile=self.min_battery_level_normalised,
            filename_id=filename_id,
        )

    def __write_output(self, profile_name: str, profile: pd.Series, filename_id: str):
        """
        Write out the profile given in profile adding the profile_name and a filename_id (overarching file label) to the
        written file.

        Args:
            profile_name (str): A string describing the name of a profile to write
            profile (pd.Series): A profile to write to disk
            filename_id (str): An additional overarching label provided to the file written to disk
        """
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
        """
        Overarching procedural wrapper function to clone the five main venco.py profiles from weekly profiles to annual
        profiles and write them to disk. This function is meant to be called in an overarching venco.py workflow.
        """
        if self.user_config["profileaggregators"]["aggregation_timespan"] == "daily":
            print(
                "The annual profiles cannot be generated as the aggregation was performed over a single day."
            )
        else:
            profiles = (
                self.drain,
                self.uncontrolled_charging,
                self.charging_power,
                self.max_battery_level,
                self.min_battery_level,
            )
            profile_names = (
                "drain",
                "uncontrolled_charging",
                "charging_power",
                "max_battery_level",
                "min_battery_level",
            )
            for profile_name, profile in zip(profile_names, profiles):
                self.__store_input(name=profile_name, profile=profile)
                self.annual_profiles[profile_name] = self.__week_to_annual_profile(
                    profile=profile
                )
                if self.user_config["global"]["write_output_to_disk"][
                    "processor_output"
                ]["absolute_annual_profiles"]:
                    self.__write_output(
                        profile_name=profile_name,
                        profile=self.annual_profiles[profile_name],
                        filename_id="output_postprocessor_annual",
                    )
                    print("Run finished.")

    def normalise(self):
        """
        Procedural wrapper-function to normalise the five venco.py profiles to default normalization bases. These are
        the year-sum of the profiles for drain and uncontrolled charging, the vehicle fleet for charging power and the
        battery capacity for min and max battery level profiles.
        This profile is supposed to be called in the venco.py workflow after PostProcessor instantiation.
        """
        if self.user_config["profileaggregators"]["aggregation_timespan"] == "daily":
            print(
                "The annual profiles cannot be normalised as the aggregation was performed over a single day."
            )
            print("Run finished.")
        else:
            self.drain_normalised = self.__normalize_flows(self.input_profiles["drain"])
            self.uncontrolled_charging_normalised = self.__normalize_flows(
                self.input_profiles["uncontrolled_charging"]
            )
            self.charging_power_normalised = self.__normalize_charging_power(
                profile=self.input_profiles["charging_power"],
                base=self.vehicle_numbers,
                time_delta=self.time_delta,
            )
            self.max_battery_level_normalised = self.__normalize_states(
                profile=self.input_profiles["max_battery_level"],
                base=self.user_config["flexestimators"]["battery_capacity"],
            )
            self.min_battery_level_normalised = self.__normalize_states(
                profile=self.input_profiles["min_battery_level"],
                base=self.user_config["flexestimators"]["battery_capacity"],
            )
            if self.user_config["global"]["write_output_to_disk"]["processor_output"][
                "normalised_annual_profiles"
            ]:
                self.__write_out_profiles(filename_id="output_postprocessor_normalised")
            print("Run finished.")
