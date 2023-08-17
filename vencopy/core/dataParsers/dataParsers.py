__version__ = "1.0.X"
__maintainer__ = "Niklas Wulff"
__contributors__ = "Fabia Miorelli"
__email__ = "Niklas.Wulff@dlr.de"
__birthdate__ = "21.04.2022"
__status__ = "test"  # options are: dev, test, prod


import pprint
import warnings
from typing import Union
from pathlib import Path
from zipfile import ZipFile

import numpy as np
import pandas as pd


from vencopy.utils.globalFunctions import createFileName, writeOut
from vencopy.utils.globalFunctions import returnDictBottomKeys, returnDictBottomValues


class DataParser:
    def __init__(self, configDict: dict, datasetID: str, debug, fpInZip=None, loadEncrypted=False):
        """
        Basic class for parsing a mobility survey trip data set. Currently both
        German travel surveys MiD 2008 and MiD 2017 are pre-configured and one
        of the two can be given (default: MiD 2017).
        The data set can be provided from an encrypted file on a server in
        which case the link to the ZIP-file as well as a link to the file
        within the ZIP-file have to be supplied in the globalConfig and a
        password has to be supplied in the parseConfig.
        Columns relevant for the EV simulation are selected from the entirety
        of the data and renamed to VencoPy internal variable names given in
        the dictionary parseConfig['dataVariables'] for the respective survey
        data set. Manually configured exclude, include, greaterThan and
        smallerThan filters are applied as they are specified in parseConfig.
        For some columns, raw data is transferred to human readable strings
        and respective columns are added. Pandas timestamp columns are
        synthesized from the given trip start and trip end time information.

        :param configDict: A dictionary containing multiple yaml config files
        :param datasetID: Currently, MiD08 and MiD17 are implemented as travel
                          survey data sets
        :param loadEncrypted: If True, load an encrypted ZIP file as specified
                              in user_config
        """
        self.user_config = configDict["user_config"]
        self.dev_config = configDict["dev_config"]
        self.datasetID = self.__checkDatasetID(datasetID)
        filepath = (
            Path(self.user_config["global"]["pathAbsolute"][self.datasetID])
            / self.dev_config["global"]["files"][self.datasetID]["tripsDataRaw"]
        )
        self.rawDataPath = filepath
        self.rawData = None
        self.trips = None
        self.activities = None
        self.filters = {}
        print("Generic file parsing properties set up.")
        if loadEncrypted:
            print(f"Starting to retrieve encrypted data file from {self.rawDataPath}.")
            self._loadEncryptedData(pathToZip=filepath, pathInZip=fpInZip)
        else:
            print(f"Starting to retrieve local data file from {self.rawDataPath}.")
            self._loadData()
        nDebugLines = self.user_config["global"]["nDebugLines"]
        self.rawData = self.rawData.loc[0 : nDebugLines - 1, :] if debug else self.rawData.copy()
        if debug:
            print("Running in debug mode.")
        # Storage for original data variable that is being overwritten throughout adding of park rows
        self.tripEndNextDayRaw = None

    def _loadData(self) -> pd.DataFrame:
        """
        Loads data specified in self.rawDataPath and stores it in self.rawData.
        Raises an exception if a invalid suffix is specified in
        self.rawDataPath.

        :return: None
        """
        # Future releases: Are potential error messages (.dta not being a stata
        # file even as the ending matches) readable for the user?
        # Should we have a manual error treatment here?
        if self.rawDataPath.suffix == ".dta":
            self.rawData = pd.read_stata(
                self.rawDataPath,
                convert_categoricals=False,
                convert_dates=False,
                preserve_dtypes=False,
            )
        # This has not been tested before the beta release
        elif self.rawDataPath.suffix == ".csv":
            self.rawData = pd.read_csv(self.rawDataPath)
        else:
            Exception(
                f"Data type {self.rawDataPath.suffix} not yet specified. Available types so far are .dta and .csv"
            )
        print(f"Finished loading {len(self.rawData)} rows of raw data of type {self.rawDataPath.suffix}.")
        return self.rawData

    def _loadEncryptedData(self, pathToZip, pathInZip):
        """
        Since the MiD data sets are only accessible by an extensive data
        security contract, VencoPy provides the possibility to access
        encrypted zip files. An encryption password has to be given in
        user_config.yaml in order to access the encrypted file. Loaded data
        is stored in self.rawData

        :param pathToZip: path from current working directory to the zip file
                          or absolute path to zipfile
        :param pathInZip: Path to trip data file within the encrypted zipfile
        :return: None
        """
        with ZipFile(pathToZip) as myzip:
            if ".dta" in pathInZip:
                self.rawData = pd.read_stata(
                    myzip.open(
                        pathInZip,
                        pwd=bytes(self.user_config["dataParsers"]["encryptionPW"], encoding="utf-8"),
                    ),
                    convert_categoricals=False,
                    convert_dates=False,
                    preserve_dtypes=False,
                )
            else:  # if '.csv' in pathInZip:
                self.rawData = pd.read_csv(
                    myzip.open(
                        pathInZip,
                        pwd=bytes(self.parseConfig["encryptionPW"], encoding="utf-8"),
                    ),
                    sep=";",
                    decimal=",",
                )

        print(f"Finished loading {len(self.rawData)} rows of raw data of type {self.rawDataPath.suffix}.")

    def __checkDatasetID(self, datasetID: str) -> str:
        """
        General check if data set ID is defined in dev_config.yaml

        :param datasetID: list of strings declaring the datasetIDs
                          to be read in
        :param user_config: A yaml config file holding a dictionary with the
                            keys 'pathRelative' and 'pathAbsolute'
        :return: Returns a string value of a mobility data
        """
        availableDatasetIDs = self.dev_config["dataParsers"]["dataVariables"]["datasetID"]
        assert datasetID in availableDatasetIDs, (
            f"Defined datasetID {datasetID} not specified "
            f"under dataVariables in dev_config. "
            f"Specified datasetIDs are {availableDatasetIDs}"
        )
        return datasetID

    def _harmonize_variables(self):
        """
        Harmonizes the input data variables to match internal VencoPy names
        given as specified in the mapping in dev_config['dataVariables'].
        Since the MiD08 does not provide a combined household and person
        unique identifier, it is synthesized of the both IDs.

        :return: None
        """
        replacementDict = self._create_replacement_dict(self.datasetID, self.dev_config["dataParsers"]["dataVariables"])
        dataRenamed = self.trips.rename(columns=replacementDict)
        self.trips = dataRenamed
        print("Finished harmonization of variables.")

    def _create_replacement_dict(self, datasetID: str, dictRaw: dict) -> dict:
        """
        Creates the mapping dictionary from raw data variable names to VencoPy
        internal variable names as specified in dev_config.yaml
        for the specified data set.

        :param datasetID: list of strings declaring the datasetIDs to be read
        :param dictRaw: Contains dictionary of the raw data
        :return: Dictionary with internal names as keys and raw data column
                 names as values.
        """
        if datasetID not in dictRaw["datasetID"]:
            raise ValueError(f"Data set {datasetID} not specified in" f"dev_config variable dictionary.")
        listIndex = dictRaw["datasetID"].index(datasetID)
        return {val[listIndex]: key for (key, val) in dictRaw.items()}

    def _check_filter_dict(self):
        """
        Checking if all values of filter dictionaries are of type list.
        Currently only checking if list of list str not typechecked
        all(map(self.__checkStr, val). Conditionally triggers an assert.

        :return: None
        """
        assert all(
            isinstance(val, list) for val in returnDictBottomValues(baseDict=self.filters)
        ), "All values in filter dictionaries have to be lists, but are not"

    def _filter(self, filters: dict = None):
        """
        Wrapper function to carry out filtering for the four filter logics of
        including, excluding, greaterThan and smallerThan.
        If a filters is defined with a different key, a warning is thrown.
        Filters are defined inclusively, thus boolean vectors will select
        elements (TRUE) that stay in the data set.

        :return: None. The function operates on self.trips class-internally.
        """
        print(f"Starting filtering, applying {len(returnDictBottomKeys(filters))} filters.")
        # Future releases: as discussed before we could indeed work here with a plug and pray approach.
        #  we would need to introduce a filter manager and a folder structure where to look for filters.
        #  this is very similar code than the one from ioproc. If we want to go down this route we should
        #  take inspiration from the code there. It was not easy to get it right in the first place. This
        #  might be easy to code but hard to implement correctly. See issue #445

        # Application of simple value-based filters
        simpleFilters = self.__simpleFilters()
        self.dataSimple = self.trips[simpleFilters.all(axis="columns")]

        # Application of sophisticated filters
        complexFilters = self._complexFilters()
        self.trips = self.dataSimple.loc[complexFilters.all(axis="columns"), :]

        # Print user feedback on filtering
        self._filterAnalysis(simpleFilters.join(complexFilters))

    def __simpleFilters(self) -> pd.DataFrame:
        """Apply single-column scalar value filtering as defined in the config.

        Returns:
            pd.DataFrame: DataFrame with boolean columns for include, exclude, greaterThan and smallerThan filters. True
            means keep the row.
        """
        simpleFilter = pd.DataFrame(index=self.trips.index)

        # Simple filters checking single columns for specified values
        for iKey, iVal in self.filters.items():
            if iKey == "include" and iVal:
                simpleFilter = simpleFilter.join(self.__setIncludeFilter(iVal, self.trips.index))
            elif iKey == "exclude" and iVal:
                simpleFilter = simpleFilter.join(self.__setExcludeFilter(iVal, self.trips.index))
            elif iKey == "greaterThan" and iVal:
                simpleFilter = simpleFilter.join(self.__setGreaterThanFilter(iVal, self.trips.index))
            elif iKey == "smallerThan" and iVal:
                simpleFilter = simpleFilter.join(self.__setSmallerThanFilter(iVal, self.trips.index))
            elif iKey not in ["include", "exclude", "greaterThan", "smallerThan"]:
                warnings.warn(
                    f"A filter dictionary was defined in the dev_config with an unknown filtering key."
                    f"Current filtering keys comprise include, exclude, smallerThan and greaterThan."
                    f"Continuing with ignoring the dictionary {iKey}"
                )
        return simpleFilter

    def __setIncludeFilter(self, includeFilterDict: dict, dataIndex: pd.Index) -> pd.DataFrame:
        """
        Read-in function for include filter dict from dev_config.yaml

        :param includeFilterDict: Dictionary of include filters defined
                                in dev_config.yaml
        :param dataIndex: Index for the data frame
        :return: Returns a data frame with individuals using car
                as a mode of transport
        """
        incFilterCols = pd.DataFrame(index=dataIndex, columns=includeFilterDict.keys())
        for incCol, incElements in includeFilterDict.items():
            incFilterCols[incCol] = self.trips[incCol].isin(incElements)
        return incFilterCols

    def __setExcludeFilter(self, excludeFilterDict: dict, dataIndex: pd.Index) -> pd.DataFrame:
        """
        Read-in function for exclude filter dict from dev_config.yaml

        :param excludeFilterDict: Dictionary of exclude filters defined
                                  in dev_config.yaml
        :param dataIndex: Index for the data frame
        :return: Returns a filtered data frame with exclude filters
        """
        exclFilterCols = pd.DataFrame(index=dataIndex, columns=excludeFilterDict.keys())
        for excCol, excElements in excludeFilterDict.items():
            exclFilterCols[excCol] = ~self.trips[excCol].isin(excElements)
        return exclFilterCols

    def __setGreaterThanFilter(self, greaterThanFilterDict: dict, dataIndex: pd.Index):
        """
        Read-in function for greaterThan filter dict from dev_config.yaml

        :param greaterThanFilterDict: Dictionary of greater than filters
                                      defined in dev_config.yaml
        :param dataIndex: Index for the data frame
        :return:
        """
        greaterThanFilterCols = pd.DataFrame(index=dataIndex, columns=greaterThanFilterDict.keys())
        for greaterCol, greaterElements in greaterThanFilterDict.items():
            greaterThanFilterCols[greaterCol] = self.trips[greaterCol] >= greaterElements.pop()
            if len(greaterElements) > 0:
                warnings.warn(
                    f"You specified more than one value as lower limit for filtering column {greaterCol}."
                    f"Only considering the last element given in the dev_config."
                )
        return greaterThanFilterCols

    def __setSmallerThanFilter(self, smallerThanFilterDict: dict, dataIndex: pd.Index) -> pd.DataFrame:
        """
        Read-in function for smallerThan filter dict from dev_config.yaml

        :param smallerThanFilterDict: Dictionary of smaller than filters
               defined in dev_config.yaml
        :param dataIndex: Index for the data frame
        :return: Returns a data frame of trips covering
                 a distance of less than 1000 km
        """
        smallerThanFilterCols = pd.DataFrame(index=dataIndex, columns=smallerThanFilterDict.keys())
        for smallerCol, smallerElements in smallerThanFilterDict.items():
            smallerThanFilterCols[smallerCol] = self.trips[smallerCol] <= smallerElements.pop()
            if len(smallerElements) > 0:
                warnings.warn(
                    f"You specified more than one value as upper limit for filtering column {smallerCol}."
                    f"Only considering the last element given in the dev_config."
                )
        return smallerThanFilterCols

    def _complexFilters(self) -> pd.DataFrame:
        """Collects filters that compare multiple columns or derived variables or calculation results thereof. True
        in this filter means "keep row". The function needs self.trips to determine the length and the index of the
        return argument.

        Returns:
            pd.DataFrame: DataFrame with a boolean column per complex filter. True means keep the row in the trips
            data set.
        """
        complexFilters = pd.DataFrame(index=self.trips.index)
        complexFilters = complexFilters.join(self._filterInconsistentSpeedTrips())
        complexFilters = complexFilters.join(self._filterInconsistentTravelTimes())
        complexFilters = complexFilters.join(self._filterOverlappingTrips())
        return complexFilters

    def _filterInconsistentSpeedTrips(self) -> pd.Series:
        """
        Filter out trips with inconsistent average speed. These trips are mainly trips where survey participant
        responses suggest that participants were travelling for the entire time they took for the whole purpose
        (driving and parking) and not just for the real travel.

        :return: Boolean vector with observations marked True that should be
        kept in the data set
        """
        self.trips["averageSpeed"] = self.trips["tripDistance"] / (self.trips["travelTime"] / 60)

        return (self.trips["averageSpeed"] > self.dev_config["dataParsers"]["filterDicts"]["lowerSpeedThreshold"]) & (
            self.trips["averageSpeed"] <= self.dev_config["dataParsers"]["filterDicts"]["higherSpeedThreshold"]
        )

    def _filterInconsistentTravelTimes(self) -> pd.Series:
        """Calculates a travel time from the given timestamps and compares it
        to the travel time given by the interviewees. Selects observations where
        timestamps are consistent with the travel time given.

        :return: Boolean vector with observations marked True that should be
        kept in the data set
        """
        self.trips["travelTime_ts"] = (
            (self.trips["timestampEnd"] - self.trips["timestampStart"]).dt.total_seconds().div(60).astype(int)
        )
        filt = self.trips["travelTime_ts"] == self.trips["travelTime"]
        filt.name = "travelTime"  # Required for column-join in _filter()
        return filt

    def _filterOverlappingTrips(self, lookahead_periods: int = 1) -> pd.DataFrame:
        """
        Filter out trips carried out by the same car as next (second next, third next up to period next etc) trip but
        overlap with at least one of the period next trips.

        Args:
            data (pd.DataFrame): Trip data set including the two variables timestampStart and timestampEnd
            characterizing a trip

        Returns:
            Pandas DataFrame containing periods columns comparing each trip to their following trips. If True, the
            trip does not overlap with the trip following after period trips (e.g. period==1 signifies no overlap with
            next trip, period==2 no overlap with second next trip etc.).
        """
        lst = []
        for p in range(1, lookahead_periods + 1):
            ser = self.__identifyOverlappingTrips(dat=self.trips, period=p)
            ser.name = f"p={p}"
            lst.append(ser)
        ret = pd.concat(lst, axis=1).all(axis=1)
        ret.name = "noOverlapNextTrips"
        return ret

    def __identifyOverlappingTrips(self, dat: pd.DataFrame, period: int) -> pd.Series:
        """Calculates a boolean vector of same length as dat that is True if the current trip does not overlap with
        the next trip. "Next" can relate to the consecutive trip (if period==1) or to a later trip defined by the
        period (e.g. for period==2 the trip after next). For determining if a overlap occurs the end timestamp of the
        current trip is compared to the start timestamp of the "next" trip.

        Args:
            dat (pd.DataFrame): A trip data set containing consecutive trips containing at least the columns id_col,
                timestampStart, timestampEnd.
            id_col (str): Column that differentiates units of trips e.g. daily trips carried out by the same vehicle.
            period (int): Forward looking period to compare trip overlap. Should be the maximum number of trip that one
                vehicle carries out in a time interval (e.g. day) in the data set.

        Returns:
            pd.Series: A boolean vector that is True if the trip does not overlap with the period-next trip but belongs
                to the same vehicle.
        """
        dat["isSameIDAsPrev"] = dat["uniqueID"] == dat["uniqueID"].shift(period)
        dat["tripStartsAfterPrevTrip"] = dat["timestampStart"] > dat["timestampEnd"].shift(period)
        return ~(dat["isSameIDAsPrev"] & ~dat["tripStartsAfterPrevTrip"])

    def _filterAnalysis(self, filterData: pd.DataFrame):
        """
        Function supplies some aggregate info of the data after filtering to the user Function does not change any
        class attributes

        :param filterData:
        :return: None
        """
        lenData = sum(filterData.all(axis="columns"))
        boolDict = {iCol: sum(filterData[iCol]) for iCol in filterData}
        print("The following number of observations were taken into account after filtering:")
        pprint.pprint(boolDict)
        # print(f'{filterData["averageSpeed"].sum()} trips have plausible average speeds')
        # print(f'{(~filterData["tripDoesNotOverlap"]).sum()} trips overlap and were thus filtered out')
        print(f"All filters combined yielded that a total of {lenData} trips are taken into account.")
        print(f"This corresponds to {lenData / len(filterData)* 100} percent of the original data.")

    def process(self):
        """
        Wrapper function for harmonising and filtering the dataset.
        """
        raise NotImplementedError("Implement process method for DataParser.")

    def write_output(self):
        if self.user_config["global"]["writeOutputToDisk"]["parseOutput"]:
            root = Path(self.user_config["global"]["pathAbsolute"]["vencopyRoot"])
            folder = self.dev_config["global"]["pathRelative"]["parseOutput"]
            fileName = createFileName(
                dev_config=self.dev_config,
                user_config=self.user_config,
                fileNameID="outputDataParser",
                datasetID=self.datasetID,
                manualLabel="",
            )
            writeOut(data=self.activities, path=root / folder / fileName)


class IntermediateParsing(DataParser):
    def __init__(self, configDict: dict, datasetID: str, debug, loadEncrypted=False):
        """
        Intermediate parsing class.

        :param configDict: VencoPy config dictionary consisting at least of
                           the config dictionaries.
        :param datasetID: A string identifying the MiD data set.
        :param loadEncrypted: Boolean. If True, data is read from encrypted
                              file. For this, a possword has to be
                              specified in user_config['PW'].
        """
        super().__init__(configDict, datasetID=datasetID, loadEncrypted=loadEncrypted, debug=debug)
        self.filters = self.dev_config["dataParsers"]["filterDicts"][self.datasetID]
        self.varDataTypeDict = {}
        self.columns = self.__compileVariableList()

    def __compileVariableList(self) -> list:
        """
        Clean up the replacement dictionary of raw data file variable (column)
        names. This has to be done because some variables that may be relevant
        for the analysis later on are only contained in one raw data set while
        not contained in another one. E.g. if a trip is an intermodal trip was
        only assessed in the MiD 2017 while it was not in the MiD 2008.
        This has to be mirrored by the filter dict for the respective dataset.

        :return: List of variables
        """
        listIndex = self.dev_config["dataParsers"]["dataVariables"]["datasetID"].index(self.datasetID)
        variables = [
            val[listIndex] if val[listIndex] != "NA" else "NA"
            for key, val in self.dev_config["dataParsers"]["dataVariables"].items()
        ]

        variables.remove(self.datasetID)
        self.__removeNA(variables)
        return variables

    def __removeNA(self, variables: list):
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
        Function to filter the rawData for only relevant columns as specified
        by parseConfig and cleaned in self.compileVariablesList().
        Stores the subset of data in self.trips

        :return: None
        """
        self.trips = self.rawData.loc[:, self.columns]

    def _convert_types(self):
        """
        Convert raw column types to predefined python types as specified in
        parseConfig['inputDTypes'][datasetID]. This is mainly done for
        performance reasons. But also in order to avoid index values that are
        of type int to be cast to float. The function operates only on
        self.trips and writes back changes to self.trips

        :return: None
        """
        # Filter for dataset specific columns
        conversionDict = self.dev_config["dataParsers"]["inputDTypes"][self.datasetID]
        keys = {iCol for iCol in conversionDict.keys() if iCol in self.trips.columns}
        self.varDataTypeDict = {key: conversionDict[key] for key in conversionDict.keys() & keys}
        self.trips = self.trips.astype(self.varDataTypeDict)

    def _complexFilters(self) -> pd.DataFrame:
        """Collects filters that compare multiple columns or derived variables or calculation results thereof. True
        in this filter means "keep row". The function needs self.trips to determine the length and the index of the
        return argument.

        Returns:
            pd.DataFrame: DataFrame with a boolean column per complex filter. True means keep the row in the activities
            data set.
        """
        complexFilters = pd.DataFrame(index=self.trips.index)
        complexFilters = complexFilters.join(self._filterInconsistentSpeedTrips())
        complexFilters = complexFilters.join(self._filterInconsistentTravelTimes())
        complexFilters = complexFilters.join(self._filterOverlappingTrips())
        complexFilters = complexFilters.join(self._filter_consistent_hours())
        complexFilters = complexFilters.join(self._filterNoZeroLengthTrips())
        return complexFilters

    def _filter_consistent_hours(self) -> pd.Series:
        """
        Filtering out records where starting timestamp is before end timestamp. These observations are data errors.

        :return: Returns a boolean Series indicating erroneous rows (trips) with False.
        """
        ser = self.trips["timestampStart"] <= self.trips["timestampEnd"]
        ser.name = "tripStartAfterEnd"
        return ser

    def _filterNoZeroLengthTrips(self) -> pd.Series:
        """Filter out trips that start and end at same hour and minute but are not ending on next day (no 24-hour
        trips).

        Returns:
            _type_: _description_
        """

        ser = ~(
            (self.trips.loc[:, "tripStartHour"] == self.trips.loc[:, "tripEndHour"])
            & (self.trips.loc[:, "tripStartMinute"] == self.trips.loc[:, "tripEndMinute"])
            & (~self.trips.loc[:, "tripEndNextDay"])
        )
        ser.name = "isNoZeroLengthTrip"
        return ser

    def _addStrColumnFromVariable(self, colName: str, varName: str):
        """
        Replaces each occurence of a MiD/KiD variable e.g. 1,2,...,7 for
        weekdays with an explicitly mapped string e.g. 'MON', 'TUE',...,'SUN'.

        :param colName: Name of the column in self.trips where the explicit
                        string info is stored
        :param varName: Name of the VencoPy internal variable given in
                        dev_config/dataParsers['dataVariables']
        :return: None
        """
        self.trips.loc[:, colName] = self.trips.loc[:, varName].replace(
            self.dev_config["dataParsers"]["Replacements"][self.datasetID][varName]
        )

    def __composeTimestamp(
        self,
        data: pd.DataFrame = None,
        colYear: str = "tripStartYear",
        colWeek: str = "tripStartWeek",
        colDay: str = "tripStartWeekday",
        colHour: str = "tripStartHour",
        colMin: str = "tripStartMinute",
        colName: str = "timestampStart",
    ) -> np.datetime64:
        """
        :param data: a data frame
        :param colYear: year of start of a particular trip
        :param colWeek: week of start of a particular trip
        :param colDay: weekday of start of a particular trip
        :param colHour: hour of start of a particular trip
        :param colMin: minute of start of a particular trip
        :param colName:
        :return: Returns a detailed time stamp
        """
        data[colName] = (
            pd.to_datetime(data.loc[:, colYear], format="%Y")
            + pd.to_timedelta(data.loc[:, colWeek] * 7, unit="days")
            + pd.to_timedelta(data.loc[:, colDay], unit="days")
            + pd.to_timedelta(data.loc[:, colHour], unit="hour")
            + pd.to_timedelta(data.loc[:, colMin], unit="minute")
        )
        # return data

    def _compose_start_and_end_timestamps(self):
        """
        :return: Returns start and end time of a trip
        """
        self.__composeTimestamp(data=self.trips)  # Starting timestamp
        self.__composeTimestamp(
            data=self.trips,  # Ending timestamps
            colHour="tripEndHour",
            colMin="tripEndMinute",
            colName="timestampEnd",
        )

    def _update_end_timestamp(self):
        """
        Updates the end timestamp for overnight trips adding 1 day

        :return: None, only acts on the class variable
        """
        endsFollowingDay = self.trips["tripEndNextDay"] == 1
        self.trips.loc[endsFollowingDay, "timestampEnd"] = self.trips.loc[
            endsFollowingDay, "timestampEnd"
        ] + pd.offsets.Day(1)

    def _harmonize_variables_unique_id_names(self):
        """
        Harmonises ID variables for all datasets.
        """
        self.trips["uniqueID"] = (
            self.trips[str(self.dev_config["dataParsers"]["IDVariablesNames"][self.datasetID])]
        ).astype(int)
        print("Finished harmonization of ID variables.")

    def _subset_vehicle_segment(self):
        if self.user_config["dataParsers"]["subsetVehicleSegment"]:
            self.activities = self.activities[
                self.activities["vehicleSegmentStr"]
                == self.user_config["dataParsers"]["vehicleSegment"][self.datasetID]
            ]
            print(
                f'The subset contains only vehicles of the class {(self.user_config["dataParsers"]["vehicleSegment"][self.datasetID])} for a total of {len(self.activities.uniqueID.unique())} individual vehicles.'
            )

    def _cleanup_dataset(self):
        self.activities.drop(
            columns=[
                "level_0",
                "tripIsIntermodal",
                "timedelta_total",
                "timedelta_morning",
                "timeShare_morning",
                "timeShare_evening",
                "totalTripDistance",
            ],
            inplace=True,
        )


from vencopy.core.dataParsers.parseMiD import ParseMiD
from vencopy.core.dataParsers.parseKiD import ParseKiD
from vencopy.core.dataParsers.parseVF import ParseVF
def parseData(configDict: dict) -> Union[ParseMiD, ParseKiD, ParseVF]:
    datasetID = configDict["user_config"]["global"]["dataset"]
    debug = configDict["user_config"]["global"]["debug"]
    delegate = {"MiD17": ParseMiD, "KiD": ParseKiD, "VF": ParseVF}
    return delegate[datasetID](configDict=configDict, datasetID=datasetID, debug=debug)
