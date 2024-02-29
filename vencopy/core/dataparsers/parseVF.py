__maintainer__ = "Niklas Wulff, Fabia Miorelli"
__license__ = "BSD-3-Clause"


import pandas as pd
from pathlib import Path

from ...core.dataparsers.dataparsers import IntermediateParsing
from ...core.dataparsers.parkinference import ParkInference


class ParseVF(IntermediateParsing):
    def __init__(self, configs: dict, dataset: str):
        """
        Class for parsing MiD data sets. The venco.py configs globalConfig,
        parseConfig and localPathConfig have to be given on instantiation as
        well as the data set ID, e.g. 'MiD2017' that is used as key in the
        config lookups. Also, an option can be specified to load the file from
        an encrypted ZIP-file. For this, a password has to be given in the
        parseConfig.

        Args:
            configs (dict): _description_
            dataset (str): _description_
        """
        super().__init__(configs=configs, dataset=dataset)
        self.park_inference = ParkInference(configs=configs)

    def _load_unencrypted_data(self):
        """
        raw_data_path_trips, unlike for other MiD classes is taken from the MiD B1 dataset.
        raw_data_path_vehicles is an internal dataset from VF.
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
        raw_data_vehicles = pd.read_csv(raw_data_path_vehicles, encoding="ISO-8859-1")
        raw_data_vehicles = raw_data_vehicles.drop(columns=["Unnamed: 0"])
        raw_data_vehicles = raw_data_vehicles.drop_duplicates(subset=["HP_ID"], keep="first")
        raw_data_vehicles.set_index("HP_ID", inplace=True)
        raw_data = raw_data_trips.join(raw_data_vehicles, on="HP_ID", rsuffix="VF")
        self.raw_data = raw_data
        print(f"Finished loading {len(self.raw_data)} rows of raw data of type .dta.")

    def _harmonise_variables(self):
        """
        Harmonizes the input data variables to match internal venco.py names given as specified in the mapping in
        self.dev_config["dataparsers"]['data_variables']. Mappings for MiD08 and MiD17 are given. Since the MiD08 does not provide a
        combined household and person unique identifier, it is synthesized of the both IDs.
        """
        replacement_dict = self._create_replacement_dict(self.dataset, self.dev_config["dataparsers"]["data_variables"])
        data_renamed = self.trips.rename(columns=replacement_dict)
        if self.dataset == "MiD08":
            data_renamed["household_person_id"] = (
                data_renamed["household_id"].astype("string") + data_renamed["person_id"].astype("string")
            ).astype("int")
        self.trips = data_renamed
        print("Finished harmonization of variables")

    def __pad_missing_car_segments(self):
        """
        _summary_
        """
        # remove vehicle_segment nicht zuzuordnen
        self.trips = self.trips[self.trips.vehicle_segment != "nicht zuzuordnen"]
        # pad missing car segments
        # self.trips.vehicle_segment = self.trips.groupby('household_id').vehicle_segment.transform('first')
        # self.trips.drivetrain = self.trips.groupby('household_id').drivetrain.transform('first')
        # self.trips.vehicle_id = self.trips.groupby('household_id').vehicle_id.transform('first')
        # remove remaining NaN
        self.trips = self.trips.dropna(subset=["vehicle_segment"])
        # self.trips = self.trips.dropna(subset=['vehicle_segment', 'drivetrain', 'vehicle_id'])

    def __exclude_hours(self):
        """
        Removes trips where both start and end trip time are missing. KID-specific function.
        """
        self.trips = self.trips.dropna(subset=["trip_start_clock", "trip_end_clock"])

    def __add_string_columns(self, weekday=True, purpose=True, vehicle_segment=True):
        """
        Adds string columns for either weekday or purpose.

        Args:
            weekday (bool, optional): Boolean identifier if weekday string info should be added in a separate column. Defaults to True.
            purpose (bool, optional): Boolean identifier if purpose string info should be added in a separate column. Defaults to True.
        """
        if weekday:
            self._add_string_column_from_variable(col_name="weekday_string", var_name="trip_start_weekday")
        if purpose:
            self._add_string_column_from_variable(col_name="purpose_string", var_name="trip_purpose")
        if vehicle_segment:
            self.trips = self.trips.replace("groß", "gross")
            self._add_string_column_from_variable(col_name="vehicle_segment_string", var_name="vehicle_segment")

    def _drop_redundant_columns(self):
        """
        Removes temporary redundant columns.
        """
        self.trips.drop(
            columns=[
                "trip_start_clock",
                "trip_end_clock",
                "trip_start_year",
                "trip_start_month",
                "trip_start_week",
                "trip_start_hour",
                "trip_start_minute",
                "trip_end_hour",
                "trip_end_minute",
                "previous_unique_id",
                "next_unique_id",
                "column_from_index",
            ],
            inplace=True,
        )

    @staticmethod
    def _cleanup_dataset(activities):
        activities.drop(
            columns=['household_id',
                     'person_id',
                     'vehicle_id',
                     'household_person_id',
                     'trip_scale_factor',
                     'trip_end_next_day',
                     'trip_is_intermodal',
                     'trip_purpose',
                     'weekday_string',
                     'is_first_trip',
                     'vehicle_segment',
                     'vehicle_segment_string',
                     'drivetrain'], inplace=True)
        return activities

    def process(self) -> pd.DataFrame:
        """
        Wrapper function for harmonising and filtering the dataset.
        """
        self._load_data()
        self._select_columns()
        self._harmonise_variables()
        self._harmonize_variables_unique_id_names()
        self.__pad_missing_car_segments()
        self.__exclude_hours()
        self._convert_types()
        self.__add_string_columns()
        self._compose_start_and_end_timestamps()
        self._update_end_timestamp(trips=self.trips)
        self._check_filter_dict(dictionary=self.filters)
        self._filter(filters=self.filters)
        # self._filter_consistent_hours(dataset=self.trips)
        self.activities = self.park_inference.add_parking_rows(trips=self.trips)
        self._subset_vehicle_segment()
        self.activities = self._cleanup_dataset(activities=self.activities)
        self.write_output()
        print("Parsing VF dataset completed.")
        return self.activities
