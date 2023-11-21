__version__ = "1.0.0"
__maintainer__ = "Niklas Wulff, Fabia Miorelli"
__birthdate__ = "17.08.2023"
__status__ = "test"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


import warnings
from pathlib import Path
from zipfile import ZipFile

import pandas as pd


from ...utils.utils import (
    create_file_name,
    write_out,
    return_lowest_level_dict_keys,
    return_lowest_level_dict_values,
)


class DataParser:
    def __init__(self, configs: dict, dataset: str):
        """
        Basic class for parsing a mobility survey trip data set. Currently both
        German travel surveys MiD 2008 and MiD 2017 are pre-configured and one
        of the two can be given (default: MiD 2017).
        The data set can be provided from an encrypted file on a server in
        which case the link to the ZIP-file as well as a link to the file
        within the ZIP-file have to be supplied in the globalConfig and a
        password has to be supplied in the parseConfig.
        Columns relevant for the EV simulation are selected from the entirety
        of the data and renamed to venco.py internal variable names given in
        the dictionary parseConfig['data_variables'] for the respective survey
        data set. Manually configured exclude, include, greater_than and
        smaller_than filters are applied as they are specified in parseConfig.
        For some columns, raw data is transferred to human readable strings
        and respective columns are added. Pandas timestamp columns are
        synthesized from the given trip start and trip end time information.

        :param configs: A dictionary containing multiple yaml config files
        :param dataset: Currently, MiD08 and MiD17 are implemented as travel
                          survey data sets
        :param load_encrypted: If True, load an encrypted ZIP file as specified
                              in user_config
        """
        self.user_config = configs["user_config"]
        self.dev_config = configs["dev_config"]
        self.debug = configs["user_config"]["global"]["debug"]
        self.dataset = self._check_dataset_id(dataset=dataset)
        self.raw_data_path = (
            Path(self.user_config["global"]["absolute_path"][self.dataset])
            / self.dev_config["global"]["files"][self.dataset]["trips_data_raw"]
        )
        self.raw_data = None
        self.trips = None
        self.activities = None
        self.filters = {}
        print("Generic file parsing properties set up.")

    def _load_data(self):
        """ """
        number_lines_debug = self.user_config["global"]["number_lines_debug"]
        load_encrypted = False
        if load_encrypted:
            print(f"Starting to retrieve encrypted data file from {self.raw_data_path}.")
            self._load_encrypted_data(zip_path=self.raw_data_path, path_zip_data=self.raw_data_path)
        else:
            print(f"Starting to retrieve local data file from {self.raw_data_path}.")
            self._load_unencrypted_data()
        self.raw_data = self.raw_data.loc[0 : number_lines_debug - 1, :] if self.debug else self.raw_data.copy()
        if self.debug:
            print("Running in debug mode.")

    def _load_unencrypted_data(self) -> pd.DataFrame:
        """
        Loads data specified in self.raw_data_path and stores it in self.raw_data.
        Raises an exception if a invalid suffix is specified in
        self.raw_data_path.

        :return: None
        """
        if self.raw_data_path.suffix == ".dta":
            self.raw_data = pd.read_stata(
                self.raw_data_path,
                convert_categoricals=False,
                convert_dates=False,
                preserve_dtypes=False,
            )
        elif self.raw_data_path.suffix == ".csv":
            self.raw_data = pd.read_csv(self.raw_data_path)
        else:
            Exception(
                f"Data type {self.raw_data_path.suffix} not yet specified. Available types so far are .dta and .csv"
            )
        print(f"Finished loading {len(self.raw_data)} rows of raw data of type {self.raw_data_path.suffix}.")
        return self.raw_data

    def _load_encrypted_data(self, zip_path, path_zip_data):
        """
        Since the MiD data sets are only accessible by an extensive data
        security contract, venco.py provides the possibility to access
        encrypted zip files. An encryption password has to be given in
        user_config.yaml in order to access the encrypted file. Loaded data
        is stored in self.raw_data

        :param zip_path: path from current working directory to the zip file
                          or absolute path to zipfile
        :param path_zip_data: Path to trip data file within the encrypted zipfile
        :return: None
        """
        with ZipFile(zip_path) as myzip:
            if ".dta" in path_zip_data:
                self.raw_data = pd.read_stata(
                    myzip.open(
                        path_zip_data,
                        pwd=bytes(self.user_config["dataparsers"]["encryption_password"], encoding="utf-8"),
                    ),
                    convert_categoricals=False,
                    convert_dates=False,
                    preserve_dtypes=False,
                )
            else:  # if '.csv' in path_zip_data:
                self.raw_data = pd.read_csv(
                    myzip.open(
                        path_zip_data,
                        pwd=bytes(self.user_config["dataparsers"]["encryption_password"], encoding="utf-8"),
                    ),
                    sep=";",
                    decimal=",",
                )

        print(f"Finished loading {len(self.raw_data)} rows of raw data of type {self.raw_data_path.suffix}.")

    def _check_dataset_id(self, dataset: str) -> str:
        """
        General check if data set ID is defined in dev_config.yaml

        :param dataset: list of strings declaring the datasetIDs
                          to be read in
        :return: Returns a string with a dataset name
        """
        available_dataset_ids = self.dev_config["dataparsers"]["data_variables"]["dataset"]
        assert dataset in available_dataset_ids, (
            f"Defined dataset {dataset} not specified "
            f"under data_variables in dev_config. "
            f"Specified dataset_ids are {available_dataset_ids}."
        )
        return dataset

    def _harmonise_variables(self):
        """
        Harmonizes the input data variables to match internal venco.py names
        given as specified in the mapping in dev_config['data_variables'].
        Since the MiD08 does not provide a combined household and person
        unique identifier, it is synthesized of the both IDs.

        :return: None
        """
        replacement_dict = self._create_replacement_dict(self.dataset, self.dev_config["dataparsers"]["data_variables"])
        data_renamed = self.trips.rename(columns=replacement_dict)
        self.trips = data_renamed
        print("Finished harmonization of variables.")

    @staticmethod
    def _create_replacement_dict(dataset: str, data_variables: dict) -> dict:
        """
        Creates the mapping dictionary from raw data variable names to venco.py
        internal variable names as specified in dev_config.yaml
        for the specified data set.

        :param dataset: list of strings declaring the dataset_id to be read
        :param dict_raw: Contains dictionary of the raw data
        :return: Dictionary with internal names as keys and raw data column
                 names as values.
        """
        if dataset not in data_variables["dataset"]:
            raise ValueError(f"Dataset {dataset} not specified in dev_config variable dictionary.")
        list_index = data_variables["dataset"].index(dataset)
        return {val[list_index]: key for (key, val) in data_variables.items()}

    @staticmethod
    def _check_filter_dict(dictionary):
        """
        Checking if all values of filter dictionaries are of type list.
        Currently only checking if list of list str not typechecked
        all(map(self.__checkStr, val). Conditionally triggers an assert.

        :return: None
        """
        assert all(
            isinstance(val, list) for val in return_lowest_level_dict_values(dictionary)
        ), "Not all values in filter dictionaries are lists."

    def _filter(self, filters: dict = None):
        """
        Wrapper function to carry out filtering for the four filter logics of
        including, excluding, greater_than and smaller_than.
        If a filters is defined with a different key, a warning is thrown.
        Filters are defined inclusively, thus boolean vectors will select
        elements (TRUE) that stay in the data set.

        :return: None. The function operates on self.trips class-internally.
        """
        print(f"Starting filtering, applying {len(return_lowest_level_dict_keys(filters))} filters.")

        # Application of simple value-based filters
        simple_filters = self._simple_filters()
        self.data_simple = self.trips[simple_filters.all(axis="columns")]

        # Application of sophisticated filters
        complex_filters = self._complex_filters(data=self.data_simple)
        self.trips = self.data_simple.loc[complex_filters.all(axis="columns"), :]

        # Print user feedback on filtering
        self._filter_analysis(simple_filters.join(complex_filters))

    def _simple_filters(self) -> pd.DataFrame:
        """
        Apply single-column scalar value filtering as defined in the config.

        Returns:
            pd.DataFrame: DataFrame with boolean columns for include, exclude, greater_than and smaller_than filters. True
            means keep the row.
        """
        simple_filter = pd.DataFrame(index=self.trips.index)

        # Simple filters checking single columns for specified values
        for i_key, i_value in self.filters.items():
            if i_key == "include" and i_value:
                simple_filter = simple_filter.join(
                    self._set_include_filter(dataset=self.trips, include_filter_dict=i_value)
                )
            elif i_key == "exclude" and i_value:
                simple_filter = simple_filter.join(
                    self._set_exclude_filter(dataset=self.trips, exclude_filter_dict=i_value)
                )
            elif i_key == "greater_than" and i_value:
                simple_filter = simple_filter.join(
                    self._set_greater_than_filter(dataset=self.trips, greater_than_filter_dict=i_value)
                )
            elif i_key == "smaller_than" and i_value:
                simple_filter = simple_filter.join(
                    self._set_smaller_than_filter(dataset=self.trips, smaller_than_filter_dict=i_value)
                )
            elif i_key not in ["include", "exclude", "greater_than", "smaller_than"]:
                warnings.warn(
                    f"A filter dictionary was defined in the dev_config with an unknown filtering key."
                    f"Current filtering keys comprise include, exclude, smaller_than and greater_than."
                    f"Continuing with ignoring the dictionary {i_key}"
                )
        return simple_filter

    @staticmethod
    def _set_include_filter(dataset: pd.DataFrame, include_filter_dict: dict) -> pd.DataFrame:
        """
        Read-in function for include filter dict from dev_config.yaml

        :param include_filter_dict: Dictionary of include filters defined
                                in dev_config.yaml
        :return: Returns a data frame including the variables specified
        """
        inc_filter_cols = pd.DataFrame(index=dataset.index, columns=include_filter_dict.keys())
        for inc_col, inc_elements in include_filter_dict.items():
            inc_filter_cols[inc_col] = dataset[inc_col].isin(inc_elements)
        return inc_filter_cols

    @staticmethod
    def _set_exclude_filter(dataset: pd.DataFrame, exclude_filter_dict: dict) -> pd.DataFrame:
        """
        Read-in function for exclude filter dict from dev_config.yaml

        :param exclude_filter_dict: Dictionary of exclude filters defined
                                  in dev_config.yaml
        :return: Returns a filtered data frame with exclude filters
        """
        excl_filter_cols = pd.DataFrame(index=dataset.index, columns=exclude_filter_dict.keys())
        for exc_col, exc_elements in exclude_filter_dict.items():
            excl_filter_cols[exc_col] = ~dataset[exc_col].isin(exc_elements)
        return excl_filter_cols

    @staticmethod
    def _set_greater_than_filter(dataset: pd.DataFrame, greater_than_filter_dict: dict):
        """
        Read-in function for greater_than filter dict from dev_config.yaml

        :param greater_than_filter_dict: Dictionary of greater than filters
                                      defined in dev_config.yaml
        :return:
        """
        greater_than_filter_cols = pd.DataFrame(index=dataset.index, columns=greater_than_filter_dict.keys())
        for greater_col, greater_elements in greater_than_filter_dict.items():
            greater_than_filter_cols[greater_col] = dataset[greater_col] >= greater_elements.pop()
            if len(greater_elements) > 0:
                warnings.warn(
                    f"You specified more than one value as lower limit for filtering column {greater_col}."
                    f"Only considering the last element given in the dev_config."
                )
        return greater_than_filter_cols

    @staticmethod
    def _set_smaller_than_filter(dataset: pd.DataFrame, smaller_than_filter_dict: dict) -> pd.DataFrame:
        """
        Read-in function for smaller_than filter dict from dev_config.yaml

        :param smaller_than_filter_dict: Dictionary of smaller than filters
               defined in dev_config.yaml
        :return: Returns a data frame of
        """
        smaller_than_filter_cols = pd.DataFrame(index=dataset.index, columns=smaller_than_filter_dict.keys())
        for smaller_col, smaller_elements in smaller_than_filter_dict.items():
            smaller_than_filter_cols[smaller_col] = dataset[smaller_col] <= smaller_elements.pop()
            if len(smaller_elements) > 0:
                warnings.warn(
                    f"You specified more than one value as upper limit for filtering column {smaller_col}."
                    f"Only considering the last element given in the dev_config."
                )
        return smaller_than_filter_cols

    def _complex_filters(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Collects filters that compare multiple columns or derived variables or calculation results thereof. True
        in this filter means "keep row". The function needs self.trips to determine the length and the index of the
        return argument.

        Returns:
            pd.DataFrame: DataFrame with a boolean column per complex filter. True means keep the row in the trips
            data set.
        """
        complex_filters = pd.DataFrame(index=data.index)
        lower_speed_threshold = self.dev_config["dataparsers"]["filters"]["lower_speed_threshold"]
        higher_speed_threshold = self.dev_config["dataparsers"]["filters"]["higher_speed_threshold"]
        complex_filters = complex_filters.join(
            self._filter_inconsistent_speeds(
                dataset=data,
                lower_speed_threshold=lower_speed_threshold,
                higher_speed_threshold=higher_speed_threshold,
            )
        )
        complex_filters = complex_filters.join(self._filter_inconsistent_travel_times(dataset=data))
        complex_filters = complex_filters.join(~self._filter_overlapping_trips(dataset=data))
        return complex_filters

    @staticmethod
    def _filter_inconsistent_speeds(dataset: pd.DataFrame, lower_speed_threshold, higher_speed_threshold) -> pd.Series:
        """
        Filter out trips with inconsistent average speed. These trips are mainly trips where survey participant
        responses suggest that participants were travelling for the entire time they took for the whole purpose
        (driving and parking) and not just for the real travel.

        :return: Boolean vector with observations marked True that should be
        kept in the data set
        """
        dataset["average_speed"] = dataset["trip_distance"] / (dataset["travel_time"] / 60)
        dataset = (dataset["average_speed"] > lower_speed_threshold) & (
            dataset["average_speed"] <= higher_speed_threshold
        )
        return dataset

    @staticmethod
    def _filter_inconsistent_travel_times(dataset_in: pd.DataFrame) -> pd.Series:
        """
        Calculates a travel time from the given timestamps and compares it
        to the travel time given by the interviewees. Selects observations where
        timestamps are consistent with the travel time given.

        :return: Boolean vector with observations marked True that should be
        kept in the data set
        """
        dataset = dataset_in.copy()
        dataset["travel_time_ts"] = (
            (dataset["timestamp_end"] - dataset["timestamp_start"]).dt.total_seconds().div(60).astype(int)
        )
        filt = dataset["travel_time_ts"] == dataset["travel_time"]
        filt.name = "travel_time"
        return filt

    @staticmethod
    def _filter_overlapping_trips(dataset, lookahead_periods: int = 1) -> pd.DataFrame:
        """
        Filter out trips carried out by the same car as next (second next, third next up to period next etc) trip but
        overlap with at least one of the period next trips.

        Args:
            data (pd.DataFrame): Trip data set including the two variables timestamp_start and timestamp_end
            characterizing a trip

        Returns:
            Pandas DataFrame containing periods columns comparing each trip to their following trips. If True, the
            trip does not overlap with the trip following after period trips (e.g. period==1 signifies no overlap with
            next trip, period==2 no overlap with second next trip etc.).
        """
        lst = []
        for profile in range(1, lookahead_periods + 1):
            ser = DataParser._identify_overlapping_trips(dataset, period=profile)
            ser.name = f"profile={profile}"
            lst.append(ser)
        ret = pd.concat(lst, axis=1).all(axis=1)
        ret.name = "no_overlap_next_trips"
        return ret

    @staticmethod
    def _identify_overlapping_trips(dataset_in: pd.DataFrame, period: int) -> pd.Series:
        """
        Calculates a boolean vector of same length as dat that is True if the current trip does not overlap with
        the next trip. "Next" can relate to the consecutive trip (if period==1) or to a later trip defined by the
        period (e.g. for period==2 the trip after next). For determining if a overlap occurs the end timestamp of the
        current trip is compared to the start timestamp of the "next" trip.

        Args:
            dataset (pd.DataFrame): A trip data set containing consecutive trips containing at least the columns id_col,
                timestamp_start, timestamp_end.
            period (int): Forward looking period to compare trip overlap. Should be the maximum number of trip that one
                vehicle carries out in a time interval (e.g. day) in the data set.

        Returns:
            pd.Series: A boolean vector that is True if the trip does not overlap with the period-next trip but belongs
                to the same vehicle.
        """
        dataset = dataset_in.copy()
        dataset["is_same_id_as_previous"] = dataset["unique_id"] == dataset["unique_id"].shift(period)
        dataset["trip_starts_after_previous_trip"] = dataset["timestamp_start"] >= dataset["timestamp_end"].shift(
            period
        )
        return dataset["is_same_id_as_previous"] & ~dataset["trip_starts_after_previous_trip"]

    @staticmethod
    def _filter_analysis(filter_data: pd.DataFrame):
        """
        Function supplies some aggregate info of the data after filtering to the user.

        :param filter_data:
        :return: None
        """
        len_data = sum(filter_data.all(axis="columns"))
        # bool_dict = {i_column: sum(filter_data[i_column]) for i_column in filter_data}
        # print("The following number of observations were taken into account after filtering:")
        # pprint.pprint(bool_dict)
        # print(f'{filter_data["averageSpeed"].sum()} trips have plausible average speeds')
        # print(f'{(~filter_data["tripDoesNotOverlap"]).sum()} trips overlap and were thus filtered out')
        print(f"All filters combined yielded that a total of {len_data} trips are taken into account.")
        print(f"This corresponds to {len_data / len(filter_data)* 100} percent of the original data.")

    def process(self):
        """
        Wrapper function for harmonising and filtering the dataset.
        """
        raise NotImplementedError("A process method for DataParser is not implemented.")

    def write_output(self):
        if self.user_config["global"]["write_output_to_disk"]["parse_output"]:
            root = Path(self.user_config["global"]["absolute_path"]["vencopy_root"])
            folder = self.dev_config["global"]["relative_path"]["parse_output"]
            file_name = create_file_name(
                dev_config=self.dev_config,
                user_config=self.user_config,
                file_name_id="output_dataparser",
                dataset=self.dataset,
                manual_label="",
            )
            write_out(data=self.activities, path=root / folder / file_name)


class IntermediateParsing(DataParser):
    def __init__(self, configs: dict, dataset: str):
        """
        Intermediate parsing class.

        :param configs: venco.py config dictionary consisting at least of
                           the config dictionaries.
        :param dataset: A string identifying the MiD data set.
        :param load_encrypted: Boolean. If True, data is read from encrypted
                              file. For this, a possword has to be
                              specified in user_config['PW'].
        """
        super().__init__(configs, dataset=dataset)
        self.filters = self.dev_config["dataparsers"]["filters"][self.dataset]
        self.var_datatype_dict = {}
        self.columns = self._compile_variable_list()

    def _compile_variable_list(self) -> list:
        """
        Clean up the replacement dictionary of raw data file variable (column)
        names. This has to be done because some variables that may be relevant
        for the analysis later on are only contained in one raw data set while
        not contained in another one. E.g. if a trip is an intermodal trip was
        only assessed in the MiD 2017 while it was not in the MiD 2008.
        This has to be mirrored by the filter dict for the respective dataset.

        :return: List of variables
        """
        list_index = self.dev_config["dataparsers"]["data_variables"]["dataset"].index(self.dataset)
        variables = [
            val[list_index] if val[list_index] != "NA" else "NA"
            for _, val in self.dev_config["dataparsers"]["data_variables"].items()
        ]

        variables.remove(self.dataset)
        self._remove_na(variables)
        return variables

    @staticmethod
    def _remove_na(variables: list):
        """
        Removes all strings that can be capitalized to 'NA' from the list
        of variables

        :param variables: List of variables of the mobility dataset
        :return: Returns a list with non NA values
        """
        ivars = [i_variable.upper() for i_variable in variables]
        counter = 0
        for indeces, i_variable in enumerate(ivars):
            if i_variable == "NA":
                del variables[indeces - counter]
                counter += 1

    def _select_columns(self):
        """
        Function to filter the raw_data for only relevant columns as specified
        by parseConfig and cleaned in self.compileVariablesList().
        Stores the subset of data in self.trips

        :return: None
        """
        self.trips = self.raw_data.loc[:, self.columns]

    def _convert_types(self):
        """
        Convert raw column types to predefined python types as specified in
        parseConfig['input_data_types'][dataset]. This is mainly done for
        performance reasons. But also in order to avoid index values that are
        of type int to be cast to float. The function operates only on
        self.trips and writes back changes to self.trips

        :return: None
        """
        conversion_dict = self.dev_config["dataparsers"]["input_data_types"][self.dataset]
        keys = {i_column for i_column in conversion_dict.keys() if i_column in self.trips.columns}
        self.var_datatype_dict = {key: conversion_dict[key] for key in conversion_dict.keys() & keys}
        self.trips = self.trips.astype(self.var_datatype_dict)

    def _complex_filters(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Collects filters that compare multiple columns or derived variables or calculation results thereof. True
        in this filter means "keep row". The function needs self.trips to determine the length and the index of the
        return argument.

        Returns:
            pd.DataFrame: DataFrame with a boolean column per complex filter. True means keep the row in the activities
            data set.
        """
        complex_filters = pd.DataFrame(index=data.index)
        lower_speed_threshold = self.dev_config["dataparsers"]["filters"]["lower_speed_threshold"]
        higher_speed_threshold = self.dev_config["dataparsers"]["filters"]["higher_speed_threshold"]
        complex_filters = complex_filters.join(
            self._filter_inconsistent_speeds(
                dataset=self.trips,
                lower_speed_threshold=lower_speed_threshold,
                higher_speed_threshold=higher_speed_threshold,
            )
        )
        complex_filters = complex_filters.join(self._filter_inconsistent_travel_times(dataset_in=data))
        complex_filters = complex_filters.join(~self._filter_overlapping_trips(dataset=data))
        complex_filters = complex_filters.join(self._filter_consistent_hours(dataset=data))
        complex_filters = complex_filters.join(self._filter_zero_length_trips(dataset=data))
        return complex_filters

    @staticmethod
    def _filter_consistent_hours(dataset) -> pd.Series:
        """
        Filtering out records where starting timestamp is before end timestamp. These observations are data errors.

        :return: Returns a boolean Series indicating erroneous rows (trips) with False.
        """
        ser = dataset["timestamp_start"] <= dataset["timestamp_end"]
        ser.name = "trip_start_after_end"
        return ser

    @staticmethod
    def _filter_zero_length_trips(dataset) -> pd.Series:
        """
        Filter out trips that start and end at same hour and minute but are not ending on next day (no 24-hour
        trips).
        """
        ser = ~(
            (dataset.loc[:, "trip_start_hour"] == dataset.loc[:, "trip_end_hour"])
            & (dataset.loc[:, "trip_start_minute"] == dataset.loc[:, "trip_end_minute"])
            & (~dataset.loc[:, "trip_end_next_day"])
        )
        ser.name = "is_no_zero_length_trip"
        return ser

    def _add_string_column_from_variable(self, col_name: str, var_name: str):
        """
        Replaces each occurence of a MiD/KiD variable e.g. 1,2,...,7 for
        weekdays with an explicitly mapped string e.g. 'MON', 'TUE',...,'SUN'.

        :param col_name: Name of the column in self.trips where the explicit
                        string info is stored
        :param var_name: Name of the venco.py internal variable given in
                        dev_config/dataparsers['data_variables']
        :return: None
        """
        self.trips.loc[:, col_name] = self.trips.loc[:, var_name].replace(
            self.dev_config["dataparsers"]["replacements"][self.dataset][var_name]
        )

    @staticmethod
    def _compose_timestamp(
        data: pd.DataFrame = None,
        col_year: str = None,
        col_week: str = None,
        col_day: str = None,
        col_hour: str = None,
        col_min: str = None,
        col_name: str = None,
    ) -> pd.DatetimeIndex:
        """
        :param data: a data frame
        :param col_year: year of start of a particular trip
        :param col_week: week of start of a particular trip
        :param col_day: weekday of start of a particular trip
        :param col_hour: hour of start of a particular trip
        :param col_min: minute of start of a particular trip
        :param col_name:
        :return: Returns a detailed time stamp
        """
        data[col_name] = (
            pd.to_datetime(data.loc[:, col_year], format="%Y")
            + pd.to_timedelta(data.loc[:, col_week] * 7, unit="days")
            + pd.to_timedelta(data.loc[:, col_day], unit="days")
            + pd.to_timedelta(data.loc[:, col_hour], unit="hour")
            + pd.to_timedelta(data.loc[:, col_min], unit="minute")
        )
        return data

    def _compose_start_and_end_timestamps(self):
        """
        :return: Returns start and end time of a trip
        """
        self._compose_timestamp(
            data=self.trips,
            col_year="trip_start_year",
            col_week="trip_start_week",
            col_day="trip_start_weekday",
            col_hour="trip_start_hour",
            col_min="trip_start_minute",
            col_name="timestamp_start",
        )
        self._compose_timestamp(
            data=self.trips,
            col_year="trip_start_year",
            col_week="trip_start_week",
            col_day="trip_start_weekday",
            col_hour="trip_end_hour",
            col_min="trip_end_minute",
            col_name="timestamp_end",
        )

    @staticmethod
    def _update_end_timestamp(trips):
        """
        Updates the end timestamp for overnight trips adding 1 day

        :return: datsets trips
        """
        ends_following_day = trips["trip_end_next_day"] == 1
        trips.loc[ends_following_day, "timestamp_end"] = trips.loc[
            ends_following_day, "timestamp_end"
        ] + pd.offsets.Day(1)
        return trips

    def _harmonize_variables_unique_id_names(self):
        """
        Harmonises ID variables for all datasets.
        """
        self.trips["unique_id"] = (
            self.trips[str(self.dev_config["dataparsers"]["id_variables_names"][self.dataset])]
        ).astype(int)
        print("Finished harmonization of ID variables.")

    def _subset_vehicle_segment(self):
        if self.user_config["dataparsers"]["subset_vehicle_segment"]:
            self.activities = self.activities[
                self.activities["vehicle_segment_string"]
                == self.user_config["dataparsers"]["vehicle_segment"][self.dataset]
            ].reset_index(drop=True)
            print(
                f'The subset contains only vehicles of the class {(self.user_config["dataparsers"]["vehicle_segment"][self.dataset])} for a total of {len(self.activities.unique_id.unique())} individual vehicles.'
            )
