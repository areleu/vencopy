__version__ = "1.0.0"
__maintainer__ = "Fabia Miorelli"
__birthdate__ = "01.08.2023"
__status__ = "test"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


import pandas as pd

from pathlib import Path

from ..core.profileaggregators import ProfileAggregator
from ..utils.utils import create_file_name, write_out
from ..utils.metadata import read_metadata_config, write_out_metadata


class PostProcessor:
    def __init__(self, configs: dict, profiles: ProfileAggregator):
        """
        This class contains functions to post process aggregated venco.py profiles. The class 
        contains cloning weekly profiles to year and normalizing it with different normalization bases.

        Args:
            configs (dict): _description_
            profiles (ProfileAggregator): _description_
        """
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
        """
        _summary_

        Args:
            name (str): _description_
            profile (pd.Series): _description_
        """
        self.input_profiles[name] = profile

    def __week_to_annual_profile(self, profile: pd.Series) -> pd.Series:
        """
        _summary_

        Args:
            profile (pd.Series): _description_

        Returns:
            pd.Series: _description_
        """
        start_weekday = self.user_config["postprocessor"]["start_weekday"]  # (1: Monday, 7: Sunday)
        n_timeslots_per_day = len(list(self.time_index))
        # Shift input profiles to the right weekday and start with first bin of chosen weekday
        annual = profile.iloc[((start_weekday - 1) * (n_timeslots_per_day - 1)) :]
        annual = pd.DataFrame(annual.to_list() * 53)
        return annual.drop(annual.tail(len(annual) - (n_timeslots_per_day - 1) * 365).index)

    @staticmethod
    def __normalize_flows(profile: pd.Series) -> pd.Series:
        """
        Function to normalise a timeseries according to its annual sum. Used in venco.py for normalisation of uncontrolled charging and drain profiles.

        Args:
            profile (pd.Series): timeseries to be normalised

        Returns:
            pd.Series: _description_
        """
        return profile / profile.sum()

    @staticmethod
    def __normalize_states(profile: pd.Series, base: int) -> pd.Series:
        """
        Function to normalise a timeseries according to a baseline value. Used in venco.py for normalisation of the minimum and maximum battery level profiles based on the assumed vehicle battery capacity.

        Args:
            profile (pd.Series): timeseries to be normalised
            base (int): normalisation basis

        Returns:
            pd.Series: _description_
        """
        return profile / base

    @staticmethod
    def __normalize_charging_power(profile: pd.Series, base: int, time_delta) -> pd.Series:
        """
        Function to normalise a timeseries according to a baseline value for each weekday. Used in venco.py for normalisation of the charging power profiles based on the number of vehicle for each weekday.

        Args:
            profile (pd.Series): timeseries to be normalised
            base (int): normalisation basis
            time_delta (_type_): temporal resolution of the run

        Returns:
            pd.Series: _description_
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
        _summary_

        Args:
            filename_id (str): _description_
        """
        self.__write_output(profile_name="drain", profile=self.drain_normalised, filename_id=filename_id)
        self.__write_output(
            profile_name="uncontrolled_charging", profile=self.uncontrolled_charging_normalised, filename_id=filename_id
        )
        self.__write_output(profile_name="charging_power", profile=self.charging_power_normalised, filename_id=filename_id)
        self.__write_output(profile_name="max_battery_level", profile=self.max_battery_level_normalised, filename_id=filename_id)
        self.__write_output(profile_name="min_battery_level", profile=self.min_battery_level_normalised, filename_id=filename_id)
        self._write_metadata(file_name=(Path(self.user_config["global"]["absolute_path"]["vencopy_root"] / self.dev_config["global"]["relative_path"]["processor_output"] / create_file_name(
            dev_config=self.dev_config,
            user_config=self.user_config,
            file_name_id=filename_id,
            dataset=self.dataset,
        ))))


    def __write_output(self, profile_name: str, profile: pd.Series, filename_id: str):
        """
        _summary_

        Args:
            profile_name (str): _description_
            profile (pd.Series): _description_
            filename_id (str): _description_
        """
        root = Path(self.user_config["global"]["absolute_path"]["vencopy_root"])
        folder = self.dev_config["global"]["relative_path"]["processor_output"]
        self.user_config["global"]["run_label"] = "_" + profile_name + "_"
        file_name = create_file_name(
            dev_config=self.dev_config,
            user_config=self.user_config,
            file_name_id=filename_id,
            dataset=self.dataset,
        )
        write_out(data=profile, path=root / folder / file_name)

    def generate_metadata(self, metadata_config, file_name):
        metadata_config["name"] = file_name
        metadata_config["title"] = "National Travel Survey activities dataframe"
        metadata_config["description"] = "Time discrete profile at single vehicle level."
        metadata_config["sources"] = [f for f in metadata_config["sources"] if f["title"] in self.dataset]
        reference_resource = metadata_config["resources"][0]
        this_resource = reference_resource.copy()
        this_resource["name"] = file_name.rstrip(".csv")
        this_resource["path"] = file_name
        these_fields = [f for f in reference_resource["schema"][self.dataset]["fields"]["postprocessors"] if f["name"] in self.activities.columns]
        this_resource["schema"] = {"fields": these_fields}
        metadata_config["resources"].pop()
        metadata_config["resources"].append(this_resource)
        return metadata_config

    def _write_metadata(self, file_name):
        metadata_config = read_metadata_config()
        class_metadata = self.generate_metadata(metadata_config=metadata_config, file_name=("postprocessors" + file_name))
        write_out_metadata(metadata_yaml=class_metadata, file_name=(file_name + "metadata.yaml"))


    def create_annual_profiles(self):
        """
        _summary_
        """
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
            self._write_metadata(file_name=("vencopy_output_postprocessor_annual_" + str(self.dataset)))
            print("Run finished.")

    def normalise(self):
        """
        _summary_
        """
        if self.user_config["profileaggregators"]['aggregation_timespan'] == "daily":
            print('The annual profiles cannot be normalised as the aggregation was performed over a single day.')
            print('Run finished.')
        else:
            self.drain_normalised = self.__normalize_flows(self.input_profiles["drain"])
            self.uncontrolled_charging_normalised = self.__normalize_flows(self.input_profiles["uncontrolled_charging"])
            self.charging_power_normalised = self.__normalize_charging_power(
                profile=self.input_profiles["charging_power"], base=self.vehicle_numbers, time_delta=self.time_delta
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
            self._write_metadata(file_name=("vencopy_output_postprocessor_normalised_" + str(self.dataset)))
            #TODO: in metadata generation add normalisation basis for the timeseries
            print("Run finished.")
