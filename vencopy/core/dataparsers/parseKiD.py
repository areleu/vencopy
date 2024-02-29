__maintainer__ = "Fabia Miorelli"
__license__ = "BSD-3-Clause"


import pandas as pd
import numpy as np
from pathlib import Path

from ...core.dataparsers.dataparsers import IntermediateParsing
from ...core.dataparsers.parkinference import ParkInference


class ParseKiD(IntermediateParsing):
    def __init__(self, configs: dict, dataset: str):
        """
        Inherited data class to differentiate between abstract interfaces such
        as vencopy internal variable namings and data set specific functions
        such as filters. Specific class for the German KiD dataset.

        Args:
            configs (dict): A dictionary containing a user_config dictionary and a dev_config dictionary.
            dataset (str): Abbreviation of the National Travel Survey to be parsed.
        """
        super().__init__(configs=configs, dataset=dataset)
        self.park_inference = ParkInference(configs=configs)

    def _load_unencrypted_data(self):
        """
        Loads the dataset specified in the user_config.
        """
        raw_data_path_trips = (
            Path(self.user_config["global"]["absolute_path"][self.dataset])
            / self.dev_config["global"]["files"][self.dataset]["trips_data_raw"]
        )
        raw_data_path_vehicles = (
            Path(self.user_config["global"]["absolute_path"][self.dataset])
            / self.dev_config["global"]["files"][self.dataset]["vehicles_data_raw"]
        )
        raw_data_trips = pd.read_stata(
            raw_data_path_trips,
            convert_categoricals=False,
            convert_dates=False,
            preserve_dtypes=False,
        )
        raw_data_vehicles = pd.read_stata(
            raw_data_path_vehicles,
            convert_categoricals=False,
            convert_dates=False,
            preserve_dtypes=False,
        )
        raw_data_vehicles.set_index("k00", inplace=True)
        raw_data = raw_data_trips.join(raw_data_vehicles, on="k00")
        self.raw_data = raw_data
        print(f"Finished loading {len(self.raw_data)} " f"rows of raw data of type .dta.")

    @staticmethod
    def _change_separator(trips):
        """
        Replaces commas with dots in the dataset (German datasets).

        Args:
            trips (pd.DataFrame): A dataframe containing all trips.

        Returns:
            pd.DataFrame: A dataframe containing all trips.
        """
        for i, x in enumerate(list(trips.trip_distance)):
            trips.at[i, "trip_distance"] = x.replace(",", ".")
        for i, x in enumerate(list(trips.trip_weight)):
            trips.at[i, "trip_weight"] = x.replace(",", ".")
        return trips

    def __add_string_columns(self, weekday=True, purpose=True, vehicle_segment=True):
        """
        Adds string columns for either weekday or purpose.

        Args:
            weekday (bool, optional): Boolean identifier if weekday should be added in a separate column as string. Defaults to True.
            purpose (bool, optional): Boolean identifier if purpose should be added in a separate column as string. Defaults to True.
            vehicle_segment (bool, optional): Boolean identifier if the vehicle segment should be added in a separate column as string. Defaults to True.
        """
        if weekday:
            self._add_string_column_from_variable(col_name="weekday_string", var_name="trip_start_weekday")
        if purpose:
            self._add_string_column_from_variable(col_name="purpose_string", var_name="trip_purpose")
        if vehicle_segment:
            self._add_string_column_from_variable(col_name="vehicle_segment_string", var_name="vehicle_segment")

    @staticmethod
    def _extract_timestamps(trips):
        """
        Extracts timestamps from the trip start date.

        Args:
            trips (pd.DataFrame): A dataframe containing all trips.

        Returns:
            pd.DataFrame: A dataframe containing all trips.
        """
        trips["trip_start_date"] = pd.to_datetime(trips["trip_start_date"], format="%d.%m.%Y")
        trips["trip_start_year"] = trips["trip_start_date"].dt.year
        trips["trip_start_month"] = trips["trip_start_date"].dt.month
        trips["trip_start_day"] = trips["trip_start_date"].dt.day
        trips["trip_start_weekday"] = trips["trip_start_date"].dt.weekday
        trips["trip_start_week"] = trips["trip_start_date"].dt.isocalendar().week
        trips["trip_start_week"] = trips["trip_start_week"].astype(int)
        trips["trip_start_hour"] = pd.to_datetime(trips["trip_start_clock"], format="%H:%M").dt.hour
        trips["trip_start_minute"] = pd.to_datetime(trips["trip_start_clock"], format="%H:%M").dt.minute
        trips["trip_end_hour"] = pd.to_datetime(trips["trip_end_clock"], format="%H:%M").dt.hour
        trips["trip_end_minute"] = pd.to_datetime(trips["trip_end_clock"], format="%H:%M").dt.minute
        return trips

    @staticmethod
    def _update_end_timestamp(trips):
        """
        Separate implementation for the KID dataset. Overwrites parent method.

        Args:
            trips (pd.DataFrame): A dataframe containing all trips.

        Returns:
            pd.DataFrame: A dataframe containing all trips.
        """
        trips["trip_end_next_day"] = False
        trips["trip_end_next_day"] = trips["trip_end_next_day"].where(~(trips["timestamp_start"] > trips["timestamp_end"]), True)
        ends_following_day = trips["trip_end_next_day"] == True 
        trips.loc[ends_following_day, "timestamp_end"] = trips.loc[
            ends_following_day, "timestamp_end"
        ] + pd.offsets.Day(1)
        return trips

    @staticmethod
    def _exclude_hours(trips):
        """
        Removes trips where either start and end trip time are missing. KID-specific function.

        Args:
            trips (pd.DataFrame): A dataframe containing all trips.

        Returns:
            pd.DataFrame: A dataframe containing all trips.
        """
        trips.drop(trips.loc[trips["trip_start_clock"].str.contains("-1")].index, inplace=True)
        trips.drop(trips.loc[trips["trip_end_clock"].str.contains("-1")].index, inplace=True)
        return trips

    @staticmethod
    def _cleanup_dataset(activities):
        activities.drop(
            columns=['vehicle_id',
                     'vehicle_segment',
                     'vehicle_segment_string',
                     'trip_start_date',
                     'trip_start_day',
                     'trip_scale_factor',
                     'trip_end_next_day',
                     'trip_is_intermodal',
                     'trip_purpose',
                     'weekday_string',
                     'is_first_trip'], inplace=True)
        return activities

    def process(self) -> pd.DataFrame:
        """
        Wrapper function for harmonising and filtering the trips dataset as well
        as adding parking rows.
        """
        self._load_data()
        self._select_columns()
        self._harmonise_variables()
        self._harmonize_variables_unique_id_names()
        self._change_separator(trips=self.trips)
        self._convert_types()
        self.trips = self._exclude_hours(trips=self.trips)
        self._extract_timestamps(trips=self.trips)
        self.__add_string_columns()
        self._compose_start_and_end_timestamps()
        self._update_end_timestamp(trips=self.trips)
        self._check_filter_dict(dictionary=self.filters)
        self._filter(filters=self.filters)
        self._filter_consistent_hours(dataset=self.trips)
        self.activities = self.park_inference.add_parking_rows(trips=self.trips)
        self._subset_vehicle_segment()
        self.activities = self._cleanup_dataset(activities=self.activities)
        self.write_output()
        print("Parsing KiD dataset completed.")
        return self.activities

