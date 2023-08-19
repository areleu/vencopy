__version__ = "1.0.0"
__maintainer__ = "Niklas Wulff, Fabia Miorelli"
__birthdate__ = "17.08.2023"
__status__ = "test"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


import pprint
import warnings
from pathlib import Path
from zipfile import ZipFile

import numpy as np
import pandas as pd


from vencopy.utils.globalFunctions import create_file_name, write_out
from vencopy.utils.globalFunctions import return_lowest_level_dict_keys, return_lowest_level_dict_values


class DataParser:
    def __init__(self, configs: dict, dataset: str, debug, zip_filepath=None, load_encrypted=False):
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
        self.dataset = self.__check_dataset_id(dataset)
        filepath = (
            Path(self.user_config["global"]["absolute_path"][self.dataset])
            / self.dev_config["global"]["files"][self.dataset]["trips_data_raw"]
        )
        self.raw_data_path = filepath
        self.raw_data = None
        self.trips = None
        self.activities = None
        self.filters = {}
        print("Generic file parsing properties set up.")
        if load_encrypted:
            print(f"Starting to retrieve encrypted data file from {self.raw_data_path}.")
            self._load_encrypted_data(zip_path=filepath, path_zip_data=zip_filepath)
        else:
            print(f"Starting to retrieve local data file from {self.raw_data_path}.")
            self._load_data()
        number_lines_debug = self.user_config["global"]["number_lines_debug"]
        self.raw_data = self.raw_data.loc[0 : number_lines_debug - 1, :] if debug else self.raw_data.copy()
        if debug:
            print("Running in debug mode.")
        # Storage for original data variable that is being overwritten throughout adding of park rows
        self.trips_end_next_day_raw = None

    def _load_data(self) -> pd.DataFrame:
        """
        Loads data specified in self.raw_data_path and stores it in self.raw_data.
        Raises an exception if a invalid suffix is specified in
        self.raw_data_path.

        :return: None
        """
        # Future releases: Are potential error messages (.dta not being a stata
        # file even as the ending matches) readable for the user?
        # Should we have a manual error treatment here?
        if self.raw_data_path.suffix == ".dta":
            self.raw_data = pd.read_stata(
                self.raw_data_path,
                convert_categoricals=False,
                convert_dates=False,
                preserve_dtypes=False,
            )
        # This has not been tested before the beta release
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
                        pwd=bytes(self.user_config["dataParsers"]["encryption_password"], encoding="utf-8"),
                    ),
                    convert_categoricals=False,
                    convert_dates=False,
                    preserve_dtypes=False,
                )
            else:  # if '.csv' in path_zip_data:
                self.raw_data = pd.read_csv(
                    myzip.open(
                        path_zip_data,
                        pwd=bytes(self.user_config["dataParsers"]["encryption_password"], encoding="utf-8"),
                    ),
                    sep=";",
                    decimal=",",
                )

        print(f"Finished loading {len(self.raw_data)} rows of raw data of type {self.raw_data_path.suffix}.")

    def __check_dataset_id(self, dataset: str) -> str:
        """
        General check if data set ID is defined in dev_config.yaml

        :param dataset: list of strings declaring the datasetIDs
                          to be read in
        :param user_config: A yaml config file holding a dictionary with the
                            keys 'relative_path' and 'absolute_path'
        :return: Returns a string value of a mobility data
        """
        available_dataset_IDs = self.dev_config["dataParsers"]["data_variables"]["dataset"]
        assert dataset in available_dataset_IDs, (
            f"Defined dataset {dataset} not specified "
            f"under data_variables in dev_config. "
            f"Specified datasetIDs are {available_dataset_IDs}"
        )
        return dataset

    def _harmonize_variables(self):
        """
        Harmonizes the input data variables to match internal venco.py names
        given as specified in the mapping in dev_config['data_variables'].
        Since the MiD08 does not provide a combined household and person
        unique identifier, it is synthesized of the both IDs.

        :return: None
        """
        replacement_dict = self._create_replacement_dict(
            self.dataset, self.dev_config["dataParsers"]["data_variables"]
        )
        data_renamed = self.trips.rename(columns=replacement_dict)
        self.trips = data_renamed
        print("Finished harmonization of variables.")

    def _create_replacement_dict(self, dataset: str, dict_raw: dict) -> dict:
        """
        Creates the mapping dictionary from raw data variable names to venco.py
        internal variable names as specified in dev_config.yaml
        for the specified data set.

        :param dataset: list of strings declaring the datasetIDs to be read
        :param dict_raw: Contains dictionary of the raw data
        :return: Dictionary with internal names as keys and raw data column
                 names as values.
        """
        if dataset not in dict_raw["dataset"]:
            raise ValueError(f"Data set {dataset} not specified in" f"dev_config variable dictionary.")
        list_index = dict_raw["dataset"].index(dataset)
        return {val[list_index]: key for (key, val) in dict_raw.items()}

    def _check_filter_dict(self):
        """
        Checking if all values of filter dictionaries are of type list.
        Currently only checking if list of list str not typechecked
        all(map(self.__checkStr, val). Conditionally triggers an assert.

        :return: None
        """
        assert all(
            isinstance(val, list) for val in return_lowest_level_dict_values(dictionary=self.filters)
        ), "All values in filter dictionaries have to be lists, but are not"

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
        # Future releases: as discussed before we could indeed work here with a plug and pray approach.
        #  we would need to introduce a filter manager and a folder structure where to look for filters.
        #  this is very similar code than the one from ioproc. If we want to go down this route we should
        #  take inspiration from the code there. It was not easy to get it right in the first place. This
        #  might be easy to code but hard to implement correctly. See issue #445

        # Application of simple value-based filters
        simple_filters = self.__simple_filters()
        self.data_simple = self.trips[simple_filters.all(axis="columns")]

        # Application of sophisticated filters
        complex_filters = self._complex_filters()
        self.trips = self.data_simple.loc[complex_filters.all(axis="columns"), :]

        # Print user feedback on filtering
        self._filter_analysis(simple_filters.join(complex_filters))

    def __simple_filters(self) -> pd.DataFrame:
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
                simple_filter = simple_filter.join(self.__set_include_filter(i_value, self.trips.index))
            elif i_key == "exclude" and i_value:
                simple_filter = simple_filter.join(self.__set_exclude_filter(i_value, self.trips.index))
            elif i_key == "greater_than" and i_value:
                simple_filter = simple_filter.join(self.__set_greater_than_filter(i_value, self.trips.index))
            elif i_key == "smaller_than" and i_value:
                simple_filter = simple_filter.join(self.__set_smaller_than_filter(i_value, self.trips.index))
            elif i_key not in ["include", "exclude", "greater_than", "smaller_than"]:
                warnings.warn(
                    f"A filter dictionary was defined in the dev_config with an unknown filtering key."
                    f"Current filtering keys comprise include, exclude, smaller_than and greater_than."
                    f"Continuing with ignoring the dictionary {i_key}"
                )
        return simple_filter

    def __set_include_filter(self, include_filter_dict: dict, data_idx: pd.Index) -> pd.DataFrame:
        """
        Read-in function for include filter dict from dev_config.yaml

        :param include_filter_dict: Dictionary of include filters defined
                                in dev_config.yaml
        :param data_idx: Index for the data frame
        :return: Returns a data frame with individuals using car
                as a mode of transport
        """
        inc_filter_cols = pd.DataFrame(index=data_idx, columns=include_filter_dict.keys())
        for inc_col, inc_elements in include_filter_dict.items():
            inc_filter_cols[inc_col] = self.trips[inc_col].isin(inc_elements)
        return inc_filter_cols

    def __set_exclude_filter(self, exclude_filter_dict: dict, data_idx: pd.Index) -> pd.DataFrame:
        """
        Read-in function for exclude filter dict from dev_config.yaml

        :param exclude_filter_dict: Dictionary of exclude filters defined
                                  in dev_config.yaml
        :param data_idx: Index for the data frame
        :return: Returns a filtered data frame with exclude filters
        """
        excl_filter_cols = pd.DataFrame(index=data_idx, columns=exclude_filter_dict.keys())
        for exc_col, exc_elements in exclude_filter_dict.items():
            excl_filter_cols[exc_col] = ~self.trips[exc_col].isin(exc_elements)
        return excl_filter_cols

    def __set_greater_than_filter(self, greater_than_filter_dict: dict, data_idx: pd.Index):
        """
        Read-in function for greater_than filter dict from dev_config.yaml

        :param greater_than_filter_dict: Dictionary of greater than filters
                                      defined in dev_config.yaml
        :param data_idx: Index for the data frame
        :return:
        """
        greater_than_filter_cols = pd.DataFrame(index=data_idx, columns=greater_than_filter_dict.keys())
        for greater_col, greater_elements in greater_than_filter_dict.items():
            greater_than_filter_cols[greater_col] = self.trips[greater_col] >= greater_elements.pop()
            if len(greater_elements) > 0:
                warnings.warn(
                    f"You specified more than one value as lower limit for filtering column {greater_col}."
                    f"Only considering the last element given in the dev_config."
                )
        return greater_than_filter_cols

    def __set_smaller_than_filter(self, smaller_than_filter_dict: dict, data_idx: pd.Index) -> pd.DataFrame:
        """
        Read-in function for smaller_than filter dict from dev_config.yaml

        :param smaller_than_filter_dict: Dictionary of smaller than filters
               defined in dev_config.yaml
        :param data_idx: Index for the data frame
        :return: Returns a data frame of trips covering
                 a distance of less than 1000 km
        """
        smaller_than_filter_cols = pd.DataFrame(index=data_idx, columns=smaller_than_filter_dict.keys())
        for smaller_col, smaller_elements in smaller_than_filter_dict.items():
            smaller_than_filter_cols[smaller_col] = self.trips[smaller_col] <= smaller_elements.pop()
            if len(smaller_elements) > 0:
                warnings.warn(
                    f"You specified more than one value as upper limit for filtering column {smaller_col}."
                    f"Only considering the last element given in the dev_config."
                )
        return smaller_than_filter_cols

    def _complex_filters(self) -> pd.DataFrame:
        """
        Collects filters that compare multiple columns or derived variables or calculation results thereof. True
        in this filter means "keep row". The function needs self.trips to determine the length and the index of the
        return argument.

        Returns:
            pd.DataFrame: DataFrame with a boolean column per complex filter. True means keep the row in the trips
            data set.
        """
        complex_filters = pd.DataFrame(index=self.trips.index)
        complex_filters = complex_filters.join(self._filter_inconsistent_speeds())
        complex_filters = complex_filters.join(self._filter_inconsistent_travel_times())
        complex_filters = complex_filters.join(self._filter_overlapping_trips())
        return complex_filters

    def _filter_inconsistent_speeds(self) -> pd.Series:
        """
        Filter out trips with inconsistent average speed. These trips are mainly trips where survey participant
        responses suggest that participants were travelling for the entire time they took for the whole purpose
        (driving and parking) and not just for the real travel.

        :return: Boolean vector with observations marked True that should be
        kept in the data set
        """
        self.trips["averageSpeed"] = self.trips["trip_distance"] / (self.trips["travel_time"] / 60)

        return (self.trips["averageSpeed"] > self.dev_config["dataParsers"]["filters"]["lower_speed_threshold"]) & (
            self.trips["averageSpeed"] <= self.dev_config["dataParsers"]["filters"]["higher_speed_threshold"]
        )

    def _filter_inconsistent_travel_times(self) -> pd.Series:
        """
        Calculates a travel time from the given timestamps and compares it
        to the travel time given by the interviewees. Selects observations where
        timestamps are consistent with the travel time given.

        :return: Boolean vector with observations marked True that should be
        kept in the data set
        """
        self.trips["travelTime_ts"] = (
            (self.trips["timestamp_end"] - self.trips["timestamp_start"]).dt.total_seconds().div(60).astype(int)
        )
        filt = self.trips["travelTime_ts"] == self.trips["travel_time"]
        filt.name = "travel_time"  # Required for column-join in _filter()
        return filt

    def _filter_overlapping_trips(self, lookahead_periods: int = 1) -> pd.DataFrame:
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
            ser = self.__identify_overlapping_trips(dat=self.trips, period=profile)
            ser.name = f"profile={profile}"
            lst.append(ser)
        ret = pd.concat(lst, axis=1).all(axis=1)
        ret.name = "no_overlap_next_trips"
        return ret

    def __identify_overlapping_trips(self, dat: pd.DataFrame, period: int) -> pd.Series:
        """
        Calculates a boolean vector of same length as dat that is True if the current trip does not overlap with
        the next trip. "Next" can relate to the consecutive trip (if period==1) or to a later trip defined by the
        period (e.g. for period==2 the trip after next). For determining if a overlap occurs the end timestamp of the
        current trip is compared to the start timestamp of the "next" trip.

        Args:
            dat (pd.DataFrame): A trip data set containing consecutive trips containing at least the columns id_col,
                timestamp_start, timestamp_end.
            id_col (str): Column that differentiates units of trips e.g. daily trips carried out by the same vehicle.
            period (int): Forward looking period to compare trip overlap. Should be the maximum number of trip that one
                vehicle carries out in a time interval (e.g. day) in the data set.

        Returns:
            pd.Series: A boolean vector that is True if the trip does not overlap with the period-next trip but belongs
                to the same vehicle.
        """
        dat["is_same_id_as_previous"] = dat["unique_id"] == dat["unique_id"].shift(period)
        dat["trip_starts_after_previous_trip"] = dat["timestamp_start"] > dat["timestamp_end"].shift(period)
        return ~(dat["is_same_id_as_previous"] & ~dat["trip_starts_after_previous_trip"])

    def _filter_analysis(self, filter_data: pd.DataFrame):
        """
        Function supplies some aggregate info of the data after filtering to the user Function does not change any
        class attributes

        :param filter_data:
        :return: None
        """
        len_data = sum(filter_data.all(axis="columns"))
        bool_dict = {i_column: sum(filter_data[i_column]) for i_column in filter_data}
        print("The following number of observations were taken into account after filtering:")
        pprint.pprint(bool_dict)
        # print(f'{filter_data["averageSpeed"].sum()} trips have plausible average speeds')
        # print(f'{(~filter_data["tripDoesNotOverlap"]).sum()} trips overlap and were thus filtered out')
        print(f"All filters combined yielded that a total of {len_data} trips are taken into account.")
        print(f"This corresponds to {len_data / len(filter_data)* 100} percent of the original data.")

    def process(self):
        """
        Wrapper function for harmonising and filtering the dataset.
        """
        raise NotImplementedError("Implement process method for DataParser.")

    def write_output(self):
        if self.user_config["global"]["write_output_to_disk"]["parse_output"]:
            root = Path(self.user_config["global"]["absolute_path"]["vencopy_root"])
            folder = self.dev_config["global"]["relative_path"]["parse_output"]
            file_name = create_file_name(
                dev_config=self.dev_config,
                user_config=self.user_config,
                file_name_id="output_dataParser",
                dataset=self.dataset,
                manual_label="",
            )
            write_out(data=self.activities, path=root / folder / file_name)


class IntermediateParsing(DataParser):
    def __init__(self, configs: dict, dataset: str, debug, load_encrypted=False):
        """
        Intermediate parsing class.

        :param configs: venco.py config dictionary consisting at least of
                           the config dictionaries.
        :param dataset: A string identifying the MiD data set.
        :param load_encrypted: Boolean. If True, data is read from encrypted
                              file. For this, a possword has to be
                              specified in user_config['PW'].
        """
        super().__init__(configs, dataset=dataset, load_encrypted=load_encrypted, debug=debug)
        self.filters = self.dev_config["dataParsers"]["filters"][self.dataset]
        self.var_datatype_dict = {}
        self.columns = self.__compile_variable_list()

    def __compile_variable_list(self) -> list:
        """
        Clean up the replacement dictionary of raw data file variable (column)
        names. This has to be done because some variables that may be relevant
        for the analysis later on are only contained in one raw data set while
        not contained in another one. E.g. if a trip is an intermodal trip was
        only assessed in the MiD 2017 while it was not in the MiD 2008.
        This has to be mirrored by the filter dict for the respective dataset.

        :return: List of variables
        """
        list_index = self.dev_config["dataParsers"]["data_variables"]["dataset"].index(self.dataset)
        variables = [
            val[list_index] if val[list_index] != "NA" else "NA"
            for _, val in self.dev_config["dataParsers"]["data_variables"].items()
        ]

        variables.remove(self.dataset)
        self.__remove_na(variables)
        return variables

    def __remove_na(self, variables: list):
        """
        Removes all strings that can be capitalized to 'NA' from the list
        of variables

        :param variables: List of variables of the mobility dataset
        :return: Returns a list with non NA values
        """
        ivars = [iVar.upper() for iVar in variables]
        counter = 0
        for idx, iVar in enumerate(ivars):
            if iVar == "NA":
                del variables[idx - counter]
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
        # Filter for dataset specific columns
        conversion_dict = self.dev_config["dataParsers"]["input_data_types"][self.dataset]
        keys = {i_column for i_column in conversion_dict.keys() if i_column in self.trips.columns}
        self.var_datatype_dict = {key: conversion_dict[key] for key in conversion_dict.keys() & keys}
        self.trips = self.trips.astype(self.var_datatype_dict)

    def _complex_filters(self) -> pd.DataFrame:
        """
        Collects filters that compare multiple columns or derived variables or calculation results thereof. True
        in this filter means "keep row". The function needs self.trips to determine the length and the index of the
        return argument.

        Returns:
            pd.DataFrame: DataFrame with a boolean column per complex filter. True means keep the row in the activities
            data set.
        """
        complex_filters = pd.DataFrame(index=self.trips.index)
        complex_filters = complex_filters.join(self._filter_inconsistent_speeds())
        complex_filters = complex_filters.join(self._filter_inconsistent_travel_times())
        complex_filters = complex_filters.join(self._filter_overlapping_trips())
        complex_filters = complex_filters.join(self._filter_consistent_hours())
        complex_filters = complex_filters.join(self._filter_zero_length_trips())
        return complex_filters

    def _filter_consistent_hours(self) -> pd.Series:
        """
        Filtering out records where starting timestamp is before end timestamp. These observations are data errors.

        :return: Returns a boolean Series indicating erroneous rows (trips) with False.
        """
        ser = self.trips["timestamp_start"] <= self.trips["timestamp_end"]
        ser.name = "trip_start_after_end"
        return ser

    def _filter_zero_length_trips(self) -> pd.Series:
        """
        Filter out trips that start and end at same hour and minute but are not ending on next day (no 24-hour
        trips).

        Returns:
            _type_: _description_
        """

        ser = ~(
            (self.trips.loc[:, "trip_start_hour"] == self.trips.loc[:, "trip_end_hour"])
            & (self.trips.loc[:, "trip_start_minute"] == self.trips.loc[:, "trip_end_minute"])
            & (~self.trips.loc[:, "trip_end_next_day"])
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
                        dev_config/dataParsers['data_variables']
        :return: None
        """
        self.trips.loc[:, col_name] = self.trips.loc[:, var_name].replace(
            self.dev_config["dataParsers"]["replacements"][self.dataset][var_name]
        )

    def __compose_timestamp(
        self,
        data: pd.DataFrame = None,
        col_year: str = "trip_start_year",
        col_week: str = "trip_start_week",
        col_day: str = "trip_start_weekday",
        col_hour: str = "trip_start_hour",
        col_min: str = "trip_start_minute",
        col_name: str = "timestamp_start",
    ) -> np.datetime64:
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
        # return data

    def _compose_start_and_end_timestamps(self):
        """
        :return: Returns start and end time of a trip
        """
        self.__compose_timestamp(data=self.trips)  # Starting timestamp
        self.__compose_timestamp(
            data=self.trips,  # Ending timestamps
            col_hour="trip_end_hour",
            col_min="trip_end_minute",
            col_name="timestamp_end",
        )

    def _update_end_timestamp(self):
        """
        Updates the end timestamp for overnight trips adding 1 day

        :return: None, only activities on the class variable
        """
        ends_following_day = self.trips["trip_end_next_day"] == 1
        self.trips.loc[ends_following_day, "timestamp_end"] = self.trips.loc[
            ends_following_day, "timestamp_end"
        ] + pd.offsets.Day(1)

    def _harmonize_variables_unique_id_names(self):
        """
        Harmonises ID variables for all datasets.
        """
        self.trips["unique_id"] = (
            self.trips[str(self.dev_config["dataParsers"]["id_variables_names"][self.dataset])]
        ).astype(int)
        print("Finished harmonization of ID variables.")

    def _subset_vehicle_segment(self):
        if self.user_config["dataParsers"]["subset_vehicle_segment"]:
            self.activities = self.activities[
                self.activities["vehicle_segment_string"]
                == self.user_config["dataParsers"]["vehicle_segment"][self.dataset]
            ]
            print(
                f'The subset contains only vehicles of the class {(self.user_config["dataParsers"]["vehicle_segment"][self.dataset])} for a total of {len(self.activities.unique_id.unique())} individual vehicles.'
            )

    def _cleanup_dataset(self):
        self.activities.drop(
            columns=[
                "level_0",
                "trip_is_intermodal",
                "timedelta_total",
                "timedelta_morning",
                "time_share_morning",
                "time_share_evening",
                "total_trip_distance",
            ],
            inplace=True,
        )
