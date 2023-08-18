__version__ = "1.0.X"
__maintainer__ = "Niklas Wulff, Fabia Miorelli"
__email__ = "Niklas.Wulff@dlr.de"
__birthdate__ = "17.08.2023"
__status__ = "test"  # options are: dev, test, prod

import pandas as pd
from pathlib import Path

from vencopy.core.dataParsers.dataParsers import IntermediateParsing
from vencopy.core.dataParsers.parkInference import ParkInference

class ParseKiD(IntermediateParsing):
    def __init__(self, configDict: dict, datasetID: str, debug, load_encrypted=False):
        """
        Inherited data class to differentiate between abstract interfaces such
        as vencopy internal variable namings and data set specific functions
        such as filters etc.
        """
        super().__init__(configDict=configDict, datasetID=datasetID, load_encrypted=load_encrypted, debug=debug)
        self.park_inference = ParkInference(configDict=configDict)

    def _load_data(self):
        rawDataPathTrips = (
            Path(self.user_config["global"]["pathAbsolute"][self.datasetID])
            / self.dev_config["global"]["files"][self.datasetID]["tripsDataRaw"]
        )
        rawDataPathVehicles = (
            Path(self.user_config["global"]["pathAbsolute"][self.datasetID])
            / self.dev_config["global"]["files"][self.datasetID]["vehiclesDataRaw"]
        )
        rawDataTrips = pd.read_stata(
            rawDataPathTrips,
            convert_categoricals=False,
            convert_dates=False,
            preserve_dtypes=False,
        )
        rawDataVehicles = pd.read_stata(
            rawDataPathVehicles,
            convert_categoricals=False,
            convert_dates=False,
            preserve_dtypes=False,
        )
        rawDataVehicles.set_index("k00", inplace=True)
        raw_data = rawDataTrips.join(rawDataVehicles, on="k00")
        self.raw_data = raw_data
        print(f"Finished loading {len(self.raw_data)} " f"rows of raw data of type .dta.")

    def __change_separator(self):
        """
        Replaces commas with dots in the dataset (German datasets).

        :return: None
        """
        for i, x in enumerate(list(self.trips.tripDistance)):
            self.trips.at[i, "tripDistance"] = x.replace(",", ".")
        for i, x in enumerate(list(self.trips.tripWeight)):
            self.trips.at[i, "tripWeight"] = x.replace(",", ".")

    def __add_string_columns(self, weekday=True, purpose=True, vehicleSegment=True):
        """
        Adds string columns for either weekday or purpose.

        :param weekday: Boolean identifier if weekday string info should be
                        added in a separate column
        :param purpose: Boolean identifier if purpose string info should be
                        added in a separate column
        :return: None
        """
        self.trips["tripStartDate"] = pd.to_datetime(self.trips["tripStartDate"], format="%d.%m.%Y")
        self.trips["tripStartYear"] = self.trips["tripStartDate"].dt.year
        self.trips["tripStartMonth"] = self.trips["tripStartDate"].dt.month
        self.trips["tripStartDay"] = self.trips["tripStartDate"].dt.day
        self.trips["tripStartWeekday"] = self.trips["tripStartDate"].dt.weekday
        self.trips["tripStartWeek"] = self.trips["tripStartDate"].dt.isocalendar().week
        self.trips["tripStartHour"] = pd.to_datetime(self.trips["tripStartClock"], format="%H:%M").dt.hour
        self.trips["tripStartMinute"] = pd.to_datetime(self.trips["tripStartClock"], format="%H:%M").dt.minute
        self.trips["tripEndHour"] = pd.to_datetime(self.trips["tripEndClock"], format="%H:%M").dt.hour
        self.trips["tripEndMinute"] = pd.to_datetime(self.trips["tripEndClock"], format="%H:%M").dt.minute
        if weekday:
            self._add_string_column_from_variable(colName="weekdayStr", varName="tripStartWeekday")
        if purpose:
            self._add_string_column_from_variable(colName="purposeStr", varName="tripPurpose")
        if vehicleSegment:
            self._add_string_column_from_variable(colName="vehicleSegmentStr", varName="vehicleSegment")

    def __update_end_timestamp(self):
        """
        Separate implementation for the KID dataset. Overwrites parent method.

        :return: None
        """
        self.trips["tripEndNextDay"] = np.where(
            self.trips["timestampEnd"].dt.day > self.trips["timestampStart"].dt.day, 1, 0
        )
        endsFollowingDay = self.trips["tripEndNextDay"] == 1
        self.trips.loc[endsFollowingDay, "timestampEnd"] = self.trips.loc[
            endsFollowingDay, "timestampEnd"
        ] + pd.offsets.Day(1)

    def __exclude_hours(self):
        """
        Removes trips where both start and end trip time are missing. KID-specific function.
        """
        self.trips = self.trips.loc[
            (self.trips["tripStartClock"] != "-1:-1") & (self.trips["tripEndClock"] != "-1:-1"),
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

