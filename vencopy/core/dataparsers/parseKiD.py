__version__ = "1.0.0"
__maintainer__ = "Fabia Miorelli"
__birthdate__ = "17.08.2023"
__status__ = "test"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


import pandas as pd
import numpy as np
from pathlib import Path

from vencopy.core.dataparsers.dataparsers import IntermediateParsing
from vencopy.core.dataparsers.parkinference import ParkInference


class ParseKiD(IntermediateParsing):
    def __init__(self, configs: dict, dataset: str, debug, load_encrypted=False):
        """
        Inherited data class to differentiate between abstract interfaces such
        as vencopy internal variable namings and data set specific functions
        such as filters etc.
        """
        super().__init__(configs=configs, dataset=dataset, load_encrypted=load_encrypted, debug=debug)
        self.park_inference = ParkInference(configs=configs)

    def _load_data(self):
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

    def __change_separator(self):
        """
        Replaces commas with dots in the dataset (German datasets).

        :return: None
        """
        for i, x in enumerate(list(self.trips.trip_distance)):
            self.trips.at[i, "trip_distance"] = x.replace(",", ".")
        for i, x in enumerate(list(self.trips.trip_weight)):
            self.trips.at[i, "trip_weight"] = x.replace(",", ".")

    def __add_string_columns(self, weekday=True, purpose=True, vehicle_segment=True):
        """
        Adds string columns for either weekday or purpose.

        :param weekday: Boolean identifier if weekday string info should be
                        added in a separate column
        :param purpose: Boolean identifier if purpose string info should be
                        added in a separate column
        :return: None
        """
        self.trips["trip_start_date"] = pd.to_datetime(self.trips["trip_start_date"], format="%d.%m.%Y")
        self.trips["trip_start_year"] = self.trips["trip_start_date"].dt.year
        self.trips["trip_start_month"] = self.trips["trip_start_date"].dt.month
        self.trips["trip_start_day"] = self.trips["trip_start_date"].dt.day
        self.trips["trip_start_weekday"] = self.trips["trip_start_date"].dt.weekday
        self.trips["trip_start_week"] = self.trips["trip_start_date"].dt.isocalendar().week
        self.trips["trip_start_hour"] = pd.to_datetime(self.trips["trip_start_clock"], format="%H:%M").dt.hour
        self.trips["trip_start_minute"] = pd.to_datetime(self.trips["trip_start_clock"], format="%H:%M").dt.minute
        self.trips["trip_end_hour"] = pd.to_datetime(self.trips["trip_end_clock"], format="%H:%M").dt.hour
        self.trips["trip_end_minute"] = pd.to_datetime(self.trips["trip_end_clock"], format="%H:%M").dt.minute
        if weekday:
            self._add_string_column_from_variable(col_name="weekday_string", var_name="trip_start_weekday")
        if purpose:
            self._add_string_column_from_variable(col_name="purpose_string", var_name="trip_purpose")
        if vehicle_segment:
            self._add_string_column_from_variable(col_name="vehicle_segment_string", var_name="vehicle_segment")

    def __update_end_timestamp(self):
        """
        Separate implementation for the KID dataset. Overwrites parent method.

        :return: None
        """
        self.trips["trip_end_next_day"] = np.where(
            self.trips["timestamp_end"].dt.day > self.trips["timestamp_start"].dt.day, 1, 0
        )
        ends_following_day = self.trips["trip_end_next_day"] == 1
        self.trips.loc[ends_following_day, "timestamp_end"] = self.trips.loc[
            ends_following_day, "timestamp_end"
        ] + pd.offsets.Day(1)

    def __exclude_hours(self):
        """
        Removes trips where both start and end trip time are missing. KID-specific function.
        """
        self.trips = self.trips.loc[
            (self.trips["trip_start_clock"] != "-1:-1") & (self.trips["trip_end_clock"] != "-1:-1"),
            :,
        ]

    def process(self) -> pd.DataFrame:
        """
        Wrapper function for harmonising and filtering the dataset.
        """
        self._select_columns()
        self._harmonize_variables()
        self._harmonize_variables_unique_id_names()
        self.__change_separator()
        self._convert_types()
        self.__exclude_hours()
        self.__add_string_columns()
        self._compose_start_and_end_timestamps()
        self.__update_end_timestamp()
        self._check_filter_dict()
        self._filter(self.filters)
        self._filter_consistent_hours()
        self.activities = self.park_inference.add_parking_rows(trips=self.trips)
        self._subset_vehicle_segment()
        self._cleanup_dataset()
        self.write_output()
        print("Parsing KiD dataset completed.")
        return self.activities
