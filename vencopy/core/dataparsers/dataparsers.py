__maintainer__ = "Niklas Wulff, Fabia Miorelli"
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

        Args:
            configs (dict): A dictionary containing multiple yaml config files
            dataset (str): Abbreviation representation the National Travel Survey to be parsed
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
        """
        Checks the load_encrypted value and then loads the data specified in self.raw_data_path 
        and stores it in self.raw_data.
        Checks the number_of_debug value only takes on those rows in the self.raw_data.

        """
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
        Raises an exception if a invalid suffix is specified in self.raw_data_path.

        Returns:
            pd.DataFrame: raw_data
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
        Function to provide the possibility to access encrypted zip files. 
        An encryption password has to be given in user_config.yaml in order to access the encrypted file. 
        Loaded data is stored in self.raw_data.

        Args:
            zip_path : Path from current working directory to the zip file or absolute path to zipfile
            path_zip_data : Path to trip data file within the encrypted zipfile
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

        Args:
            dataset (str): String declaring all possible dataset 

        Returns:
            str: String with the dataset name
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
        unique identifier in the child class, it is synthesized of the both IDs.
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
        for the specified dataset.

        Args:
            dataset (str): A list of strings declaring the dataset_id to be read
            data_variables (dict): data_variables contain the column names from the dataset

        Raises:
            ValueError: Raises a value error if the dataset is not specified in the dev_config

        Returns:
            dict: A dictionary with internal names as keys and raw data column names as values
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

        Args:
            dictionary : Dictionary containing filters in the dev_config dictionary
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
        elements (TRUE) that stay in the data set. The function operates on self.trips class-internally.

        Args:
            filters (dict, optional): Defaults to None
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
            pd.DataFrame: DataFrame with boolean columns for include, exclude, greater_than and smaller_than filters, True
            means keep the row
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

        Args:
            dataset (pd.DataFrame): dataframe on which the include filter can be added
            include_filter_dict (dict): Dictionary of include filters defined in dev_config.yaml

        Returns:
            pd.DataFrame: Dataframe including the variables specified
        """
        inc_filter_cols = pd.DataFrame(index=dataset.index, columns=include_filter_dict.keys())
        for inc_col, inc_elements in include_filter_dict.items():
            inc_filter_cols[inc_col] = dataset[inc_col].isin(inc_elements)
        return inc_filter_cols

    @staticmethod
    def _set_exclude_filter(dataset: pd.DataFrame, exclude_filter_dict: dict) -> pd.DataFrame:
        """
        Read-in function for exclude filter dict from dev_config.yaml

        Args:
            dataset (pd.DataFrame): dataframe on which the exclude filter can be added
            exclude_filter_dict (dict): Dictionary of exclude filters defined in dev_config.yaml

        Returns:
            pd.DataFrame: Filtered dataframe
        """
        excl_filter_cols = pd.DataFrame(index=dataset.index, columns=exclude_filter_dict.keys())
        for exc_col, exc_elements in exclude_filter_dict.items():
            excl_filter_cols[exc_col] = ~dataset[exc_col].isin(exc_elements)
        return excl_filter_cols

    @staticmethod
    def _set_greater_than_filter(dataset: pd.DataFrame, greater_than_filter_dict: dict):
        """
        Read-in function for greater_than filter dict from dev_config.yaml

        Args:
            dataset (pd.DataFrame): dataframe on which the greater than filter can be added
            greater_than_filter_dict (dict): Dictionary of greater than filters defined in dev_config.yaml

        Returns:
            _type_: Filtered dataframe
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

        Args:
            dataset (pd.DataFrame): dataframe on which the smaller_than filter can be added
            smaller_than_filter_dict (dict): Dictionary of smaller than filters defined in dev_config.yaml

        Returns:
            pd.DataFrame: Filtered dataframe
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
            pd.DataFrame: DataFrame with a boolean column per complex filter, 'True' means keep the row in the trips
            data set
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
        complex_filters = complex_filters.join(self._filter_overlapping_trips(dataset=data))
        return complex_filters

    @staticmethod
    def _filter_inconsistent_speeds(dataset: pd.DataFrame, lower_speed_threshold, higher_speed_threshold) -> pd.Series:
        """
        Filter out trips with inconsistent average speed. These trips are mainly trips where survey participant
        responses suggest that participants were travelling for the entire time they took for the whole purpose
        (driving and parking) and not just for the real travel.

        Args:
            dataset (pd.DataFrame): dataframe on which the inconsistent speed filter can be added
            lower_speed_threshold : Dictionary of lower speed threshold filters defined in dev_config.yaml
            higher_speed_threshold : Dictionary of higher speed threshold filters defined in dev_config.yaml

        Returns:
            pd.Series: Boolean vector with observations marked True that should be kept in the dataset
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

        Args:
            dataset_in (pd.DataFrame): dataframe on which the inconsistent travel time filter can be added

        Returns:
            pd.Series: Boolean vector with observations marked True that should be kept in the dataset
        """
        dataset = dataset_in.copy()
        dataset["travel_time_ts"] = (
            (dataset["timestamp_end"] - dataset["timestamp_start"]).dt.total_seconds().div(60).astype(int)
        )
        filt = dataset["travel_time_ts"] == dataset["travel_time"]
        filt.name = "travel_time"
        return filt

    @staticmethod
    def _filter_overlapping_trips(dataset, lookahead_periods: int = 7) -> pd.DataFrame:
        """
        Filter out trips carried out by the same car as next (second next, third next up to period next etc) trip but
        overlap with at least one of the period next trips.

        Args:
            data (pd.DataFrame): Trip data set including the two variables timestamp_start and timestamp_end
            characterizing a trip

        Returns:
            Pandas DataFrame containing periods columns comparing each trip to their following trips. If True, the
            trip does not overlap with the trip following after period trips (e.g. period==1 signifies no overlap with
            next trip, period==2 no overlap with second next trip etc.)
        """
        lst = []
        for profile in range(1, lookahead_periods + 1):
            ser = DataParser._identify_overlapping_trips(dataset, period=profile)
            ser.name = f"profile={profile}"
            lst.append(ser)
        ret = pd.concat(lst, axis=1).all(axis=1)
        ret.name = "trip_ends_after_next_start"

        d = dataset.copy()
        d['overlap'] = ret
        filter_ids = d[['unique_id', 'overlap']].groupby(by='unique_id').all()
        d['index'] = d.index
        d = d.set_index('unique_id', drop=False)
        d['overlap'] = filter_ids
        d = d.set_index('index')
        return d['overlap']

    @staticmethod
    def _identify_overlapping_trips(dataset_in: pd.DataFrame, period: int) -> pd.Series:
        """
        Calculates a boolean vector of same length as dat that is True if the current trip does not overlap with
        the next trip. "Next" can relate to the consecutive trip (if period==1) or to a later trip defined by the
        period (e.g. for period==2 the trip after next). For determining if an overlap occurs, the end timestamp of the
        current trip is compared to the start timestamp of the "next" trip.

        Args:
            dataset (pd.DataFrame): A trip data set containing consecutive trips containing at least the columns id_col,
                timestamp_start, timestamp_end
            period (int): Forward looking period to compare trip overlap. Should be the maximum number of trip that one
                vehicle carries out in a time interval (e.g. day) in the data set

        Returns:
            pd.Series: A boolean vector that is True if the trip does not overlap with the period-next trip but belongs
                to the same vehicle
        """
        dataset = dataset_in.copy()
        dataset["is_same_id_as_previous"] = dataset["unique_id"] == dataset["unique_id"].shift(period)
        dataset["trip_starts_after_previous_trip"] = dataset["timestamp_start"] >= dataset["timestamp_end"].shift(
            period
        )
        return ~(dataset["is_same_id_as_previous"] & ~dataset["trip_starts_after_previous_trip"])

    @staticmethod
    def _filter_analysis(filter_data: pd.DataFrame):
        """
        Function returns the total number of remaining rows after applying the filters.

        Args:
            filter_data (pd.DataFrame): dataframe after all filters have been applied
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
        """
        This saves the output from the dataparser class to a csv file.
        """
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

        Args:
            configs (dict): venco.py config dictionary consisting at least of the config dictionaries
            dataset (str): dataset to be processed
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

        Returns:
            list: List of variables
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
        of variables.

        Args:
            variables (list): List of variables of the mobility dataset
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
        """
        self.trips = self.raw_data.loc[:, self.columns]

    def _convert_types(self):
        """
        Convert raw column types to predefined python types as specified in
        parseConfig['input_data_types'][dataset]. This is mainly done for
        performance reasons. But also in order to avoid index values that are
        of type int to be cast to float. The function operates only on
        self.trips and writes back changes to self.trips
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
            data set
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
        complex_filters = complex_filters.join(self._filter_overlapping_trips(dataset=data))
        complex_filters = complex_filters.join(self._filter_consistent_hours(dataset=data))
        complex_filters = complex_filters.join(self._filter_zero_length_trips(dataset=data))
        return complex_filters

    @staticmethod
    def _filter_consistent_hours(dataset) -> pd.Series:
        """
        Filtering out records where starting timestamp is before end timestamp. These observations are data errors.

        Args:
            dataset : dataframe on which we would perform operations to find out the erroneous data

        Returns:
            pd.Series: Boolean Series indicating erroneous rows (trips having start time after end time) with False
        """
        ser = dataset["timestamp_start"] <= dataset["timestamp_end"]
        ser.name = "trip_start_after_end"
        return ser

    @staticmethod
    def _filter_zero_length_trips(dataset) -> pd.Series:
        """
        Filter out trips that start and end at same hour and minute but are not ending on next day (no 24-hour
        trips).

        Args:
            dataset : dataframe on which we would perform operations to find out the erroneous data

        Returns:
            pd.Series: Boolean Series indicating erroneous rows (trips having exact same start time and end time) with False
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

        Args:
            col_name (str): Name of the column in self.trips where the explicit string info is stored
            var_name (str): Name of the venco.py internal variable given in dev_config/dataparsers['data_variables']
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
        Generating pandas timestamp and storing in a new column

        Args:
            data (pd.DataFrame, optional): Defaults to None
            col_year (str, optional): Defaults to None
            col_week (str, optional): Defaults to None
            col_day (str, optional): Defaults to None
            col_hour (str, optional): Defaults to None
            col_min (str, optional): Defaults to None
            col_name (str, optional): Defaults to None

        Returns:
            pd.DatetimeIndex: modified dataframe with the new column containing the pandas timestamp values
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
        _summary_
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
        Updates the end timestamp for overnight trips adding one day.


        Args:
            trips : Dataframe containing details about the trip

        Returns:
            _type_: Returns the modified dataframe after adding an extra day because of the overnight journey
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
        """
        Dividing the vehicles into vechicle segments (eg: S, M, L in case of VF and car, van truck etc. in case of KiD).
        """
        if self.user_config["dataparsers"]["subset_vehicle_segment"]:
            self.activities = self.activities[
                self.activities["vehicle_segment_string"]
                == self.user_config["dataparsers"]["vehicle_segment"][self.dataset]
            ].reset_index(drop=True)
            segment = self.user_config["dataparsers"]["vehicle_segment"][self.dataset]
            n_vehicles = len(self.activities.unique_id.unique())
            print(
                f"The subset contains only vehicles of the class {segment} for a total of {n_vehicles} individual "
                f"vehicles."
            )


class VehicleParsing(DataParser):
    def __init__(self, configs: dict, dataset: str):
        """

        Args:
            configs (dict): 
            dataset (str): 
        """
        super().__init__(configs, dataset)

    def process(self):
        self._load_data()
        self._select_columns()
        self._harmonise_variables()