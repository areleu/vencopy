__version__ = "0.4.X"
__maintainer__ = "Niklas Wulff"
__contributors__ = "Fabia Miorelli"
__email__ = "Niklas.Wulff@dlr.de"
__birthdate__ = "21.04.2022"
__status__ = "dev"  # options are: dev, test, prod


if __package__ is None or __package__ == "":
    import sys
    from os import path

    sys.path.append(path.dirname(path.dirname(path.dirname(__file__))))

import pprint
import warnings
from typing import Union
from pathlib import Path
from zipfile import ZipFile

import numpy as np
import pandas as pd

from vencopy.utils.globalFunctions import (
    createFileName,
    replace_vec,
    writeOut,
)
from vencopy.utils.globalFunctions import returnDictBottomKeys, returnDictBottomValues


class DataParser:
    def __init__(
        self,
        configDict: dict,
        datasetID: str,
        debug,
        fpInZip=None,
        loadEncrypted=False
    ):
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
                              in parseConfig
        """
        self.parseConfig = configDict["parseConfig"]
        self.localPathConfig = configDict["localPathConfig"]
        self.globalConfig = configDict["globalConfig"]
        self.datasetID = self.__checkDatasetID(datasetID, self.parseConfig)
        filepath = (Path(self.localPathConfig["pathAbsolute"][self.datasetID]
                         ) / self.globalConfig["files"][self.datasetID]["tripsDataRaw"])
        self.rawDataPath = filepath
        self.rawData = None
        self.activities = None
        self.filterDict = {}
        print("Generic file parsing properties set up.")
        if loadEncrypted:
            print(
                f"Starting to retrieve encrypted data file from {self.rawDataPath}.")
            self._loadEncryptedData(pathToZip=filepath, pathInZip=fpInZip)
        else:
            print(
                f"Starting to retrieve local data file from {self.rawDataPath}.")
            self._loadData()
        nDebugLines = configDict["globalConfig"]["nDebugLines"]
        self.rawData = self.rawData.loc[0: nDebugLines - 1, :] if debug else self.rawData.copy()
        if debug:
            print("Running in debug mode.")
        # Storage for original data variable that is being overwritten throughout adding of park rows
        self.tripEndNextDayRaw = None

    def _loadData(self):
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
        print(
            f"Finished loading {len(self.rawData)} rows of raw data of type {self.rawDataPath.suffix}."
        )
        return self.rawData

    def _loadEncryptedData(self, pathToZip, pathInZip):
        """
        Since the MiD data sets are only accessible by an extensive data
        security contract, VencoPy provides the possibility to access
        encrypted zip files. An encryption password has to be given in
        parseConfig.yaml in order to access the encrypted file. Loaded data
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
                        pwd=bytes(
                            self.parseConfig["encryptionPW"], encoding="utf-8"),
                    ),
                    convert_categoricals=False,
                    convert_dates=False,
                    preserve_dtypes=False,
                )
            else:  # if '.csv' in pathInZip:
                self.rawData = pd.read_csv(
                    myzip.open(
                        pathInZip,
                        pwd=bytes(
                            self.parseConfig["encryptionPW"], encoding="utf-8"),
                    ),
                    sep=";",
                    decimal=",",
                )

        print(
            f"Finished loading {len(self.rawData)} rows of raw data of type {self.rawDataPath.suffix}."
        )

    def __checkDatasetID(self, datasetID: str, parseConfig: dict) -> str:
        """
        General check if data set ID is defined in parseConfig.yaml

        :param datasetID: list of strings declaring the datasetIDs
                          to be read in
        :param parseConfig: A yaml config file holding a dictionary with the
                            keys 'pathRelative' and 'pathAbsolute'
        :return: Returns a string value of a mobility data
        """
        availableDatasetIDs = parseConfig["dataVariables"]["datasetID"]
        assert datasetID in availableDatasetIDs, (
            f"Defined datasetID {datasetID} not specified "
            f"under dataVariables in parseConfig. "
            f"Specified datasetIDs are {availableDatasetIDs}"
        )
        return datasetID

    def _harmonizeVariables(self):
        """
        Harmonizes the input data variables to match internal VencoPy names
        given as specified in the mapping in parseConfig['dataVariables'].
        Since the MiD08 does not provide a combined household and person
        unique identifier, it is synthesized of the both IDs.

        :return: None
        """
        replacementDict = self._createReplacementDict(
            self.datasetID, self.parseConfig["dataVariables"]
        )
        dataRenamed = self.activities.rename(columns=replacementDict)
        self.activities = dataRenamed
        print("Finished harmonization of variables.")

    def _createReplacementDict(self, datasetID: str, dictRaw: dict) -> dict:
        """
        Creates the mapping dictionary from raw data variable names to VencoPy
        internal variable names as specified in parseConfig.yaml
        for the specified data set.

        :param datasetID: list of strings declaring the datasetIDs to be read
        :param dictRaw: Contains dictionary of the raw data
        :return: Dictionary with internal names as keys and raw data column
                 names as values.
        """
        if datasetID not in dictRaw["datasetID"]:
            raise ValueError(
                f"Data set {datasetID} not specified in"
                f"parseConfig variable dictionary."
            )
        listIndex = dictRaw["datasetID"].index(datasetID)
        return {val[listIndex]: key for (key, val) in dictRaw.items()}

    def _checkFilterDict(self):
        """
        Checking if all values of filter dictionaries are of type list.
        Currently only checking if list of list str not typechecked
        all(map(self.__checkStr, val). Conditionally triggers an assert.

        :return: None
        """
        assert all(
            isinstance(val, list)
            for val in returnDictBottomValues(baseDict=self.filterDict)
        ), "All values in filter dictionaries have to be lists, but are not"

    def _filter(self, filterDict: dict = None):
        """
        Wrapper function to carry out filtering for the four filter logics of
        including, excluding, greaterThan and smallerThan.
        If a filterDict is defined with a different key, a warning is thrown.
        Filters are defined inclusively, thus boolean vectors will select
        elements (TRUE) that stay in the data set.

        :return: None. The function operates on self.activities class-internally.
        """
        print(
            f"Starting filtering, applying {len(returnDictBottomKeys(filterDict))} filters."
        )
        # Future releases: as discussed before we could indeed work here with a plug and pray approach.
        #  we would need to introduce a filter manager and a folder structure where to look for filters.
        #  this is very similar code than the one from ioproc. If we want to go down this route we should
        #  take inspiration from the code there. It was not easy to get it right in the first place. This
        #  might be easy to code but hard to implement correctly. See issue #445

        # Application of simple value-based filters
        simpleFilters = self.__simpleFilters()
        self.dataSimple = self.activities[simpleFilters.all(axis="columns")]

        # Application of sophisticated filters
        complexFilters = self._complexFilters()
        self.activities = self.dataSimple.loc[complexFilters.all(axis="columns"), :]

        # Print user feedback on filtering
        self._filterAnalysis(simpleFilters.join(complexFilters))

    def __simpleFilters(self) -> pd.DataFrame:
        """Apply single-column scalar value filtering as defined in the config.

        Returns:
            pd.DataFrame: DataFrame with boolean columns for include, exclude, greaterThan and smallerThan filters. True
            means keep the row.
        """
        simpleFilter = pd.DataFrame(index=self.activities.index)

        # Simple filters checking single columns for specified values
        for iKey, iVal in self.filterDict.items():
            if iKey == "include" and iVal:
                simpleFilter = simpleFilter.join(
                    self.__setIncludeFilter(iVal, self.activities.index)
                )
            elif iKey == "exclude" and iVal:
                simpleFilter = simpleFilter.join(
                    self.__setExcludeFilter(iVal, self.activities.index)
                )
            elif iKey == "greaterThan" and iVal:
                simpleFilter = simpleFilter.join(
                    self.__setGreaterThanFilter(iVal, self.activities.index)
                )
            elif iKey == "smallerThan" and iVal:
                simpleFilter = simpleFilter.join(
                    self.__setSmallerThanFilter(iVal, self.activities.index)
                )
            elif iKey not in ["include", "exclude", "greaterThan", "smallerThan"]:
                warnings.warn(
                    f"A filter dictionary was defined in the parseConfig with an unknown filtering key."
                    f"Current filtering keys comprise include, exclude, smallerThan and greaterThan."
                    f"Continuing with ignoring the dictionary {iKey}"
                )
        return simpleFilter

    def __setIncludeFilter(self, includeFilterDict: dict, dataIndex) -> pd.DataFrame:
        """
        Read-in function for include filter dict from parseConfig.yaml

        :param includeFilterDict: Dictionary of include filters defined
                                in parseConfig.yaml
        :param dataIndex: Index for the data frame
        :return: Returns a data frame with individuals using car
                as a mode of transport
        """
        incFilterCols = pd.DataFrame(
            index=dataIndex, columns=includeFilterDict.keys())
        for incCol, incElements in includeFilterDict.items():
            incFilterCols[incCol] = self.activities[incCol].isin(incElements)
        return incFilterCols

    def __setExcludeFilter(self, excludeFilterDict: dict, dataIndex) -> pd.DataFrame:
        """
        Read-in function for exclude filter dict from parseConfig.yaml

        :param excludeFilterDict: Dictionary of exclude filters defined
                                  in parseConfig.yaml
        :param dataIndex: Index for the data frame
        :return: Returns a filtered data frame with exclude filters
        """
        exclFilterCols = pd.DataFrame(
            index=dataIndex, columns=excludeFilterDict.keys())
        for excCol, excElements in excludeFilterDict.items():
            exclFilterCols[excCol] = ~self.activities[excCol].isin(excElements)
        return exclFilterCols

    def __setGreaterThanFilter(self, greaterThanFilterDict: dict, dataIndex):
        """
        Read-in function for greaterThan filter dict from parseConfig.yaml

        :param greaterThanFilterDict: Dictionary of greater than filters
                                      defined in parseConfig.yaml
        :param dataIndex: Index for the data frame
        :return:
        """
        greaterThanFilterCols = pd.DataFrame(
            index=dataIndex, columns=greaterThanFilterDict.keys()
        )
        for greaterCol, greaterElements in greaterThanFilterDict.items():
            greaterThanFilterCols[greaterCol] = (
                self.activities[greaterCol] >= greaterElements.pop()
            )
            if len(greaterElements) > 0:
                warnings.warn(
                    f"You specified more than one value as lower limit for filtering column {greaterCol}."
                    f"Only considering the last element given in the parseConfig."
                )
        return greaterThanFilterCols

    def __setSmallerThanFilter(
        self, smallerThanFilterDict: dict, dataIndex
    ) -> pd.DataFrame:
        """
        Read-in function for smallerThan filter dict from parseConfig.yaml

        :param smallerThanFilterDict: Dictionary of smaller than filters
               defined in parseConfig.yaml
        :param dataIndex: Index for the data frame
        :return: Returns a data frame of trips covering
                 a distance of less than 1000 km
        """
        smallerThanFilterCols = pd.DataFrame(
            index=dataIndex, columns=smallerThanFilterDict.keys()
        )
        for smallerCol, smallerElements in smallerThanFilterDict.items():
            smallerThanFilterCols[smallerCol] = (
                self.activities[smallerCol] <= smallerElements.pop()
            )
            if len(smallerElements) > 0:
                warnings.warn(
                    f"You specified more than one value as upper limit for filtering column {smallerCol}."
                    f"Only considering the last element given in the parseConfig."
                )
        return smallerThanFilterCols

    def _complexFilters(self) -> pd.DataFrame:
        """Collects filters that compare multiple columns or derived variables or calculation results thereof. True
        in this filter means "keep row". The function needs self.activities to determine the length and the index of the
        return argument.

        Returns:
            pd.DataFrame: DataFrame with a boolean column per complex filter. True means keep the row in the activities
            data set.
        """
        complexFilters = pd.DataFrame(index=self.activities.index)
        complexFilters = complexFilters.join(self.__filterInconsistentSpeedTrips())
        complexFilters = complexFilters.join(self.__filterInconsistentTravelTimes())
        complexFilters = complexFilters.join(self.__filterOverlappingTrips())
        return complexFilters

    def _filterInconsistentSpeedTrips(self):
        """
        Filter out trips with inconsistent average speed. These trips are mainly trips where survey participant
        responses suggest that participants were travelling for the entire time they took for the whole purpose
        (driving and parking) and not just for the real travel.

        :return: Boolean vector with observations marked True that should be
        kept in the data set
        """
        self.activities["averageSpeed"] = self.activities["tripDistance"] / (
            self.activities["travelTime"] / 60
        )

        return (
            self.activities["averageSpeed"] > self.parseConfig["filterDicts"][
                "lowerSpeedThreshold"]) & (
            self.activities["averageSpeed"] <= self.parseConfig["filterDicts"][
                "higherSpeedThreshold"])

    def _filterInconsistentTravelTimes(self):
        """ Calculates a travel time from the given timestamps and compares it
        to the travel time given by the interviewees. Selects observations where
        timestamps are consistent with the travel time given.

        :return: Boolean vector with observations marked True that should be
        kept in the data set
        """
        self.activities["travelTime_ts"] = (
            self.activities['timestampEnd'] - self.activities[
                'timestampStart']).dt.total_seconds().div(60).astype(int)
        filt = self.activities['travelTime_ts'] == self.activities['travelTime']
        filt.name = 'travelTime'  # Required for column-join in _filter()
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
            ser = self.__identifyOverlappingTrips(dat=self.activities, period=p)
            ser.name = f"p={p}"
            lst.append(ser)
        ret = pd.concat(lst, axis=1).all(axis=1)
        ret.name = "noOverlapNextTrips"
        return ret

    # METHOD MOST LIKELY DEPRECATED
    # def __overlapPeriods(self, data, period) -> pd.Series:
    #     """
    #     New implementation of identifying trips that overlap with previous trips.

    #     Args:
    #         data (pd.DataFrame): Trip data set including the two variables timestampStart and timestampEnd
    #         characterizing a trip
    #         period (int): Trip identifier within trip diary in survey day to compare every trip to

    #     Returns:
    #         pd.Series: A boolean Series being True for non-overlapping trips and False for overlapping trips. Has the
    #         same index as the MID data.
    #     """
    #     dat = data.copy()

    #     if self.datasetID == "KiD":
    #         tripDoesNotOverlap = self.__identifyOverlappingTrips(dat, "vehicleID", period)
    #     else:
    #         tripDoesNotOverlap = self.__identifyOverlappingTrips(
    #         dat, "hhPersonID", period
    #     )
    #     return tripDoesNotOverlap

    def __identifyOverlappingTrips(
            self, dat: pd.DataFrame, period: int) -> pd.Series:
        """ Calculates a boolean vector of same length as dat that is True if the current trip does not overlap with
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
        dat['isSameIDAsPrev'] = dat['uniqueID'] == dat['uniqueID'].shift(period)
        dat["tripStartsAfterPrevTrip"] = dat["timestampStart"] > dat[
            "timestampEnd"].shift(period)
        return ~(dat['isSameIDAsPrev'] & ~dat["tripStartsAfterPrevTrip"])

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
        print(
            f"All filters combined yielded that a total of {lenData} trips are taken into account."
        )
        print(
            f"This corresponds to {lenData / len(filterData)* 100} percent of the original data."
        )

    def process(self):
        """
        Wrapper function for harmonising and filtering the dataset.
        """
        raise NotImplementedError("Implement process method for DataParser.")

    def writeOutput(self):
        root = Path(self.localPathConfig["pathAbsolute"]["vencoPyRoot"])
        folder = self.globalConfig["pathRelative"]["parseOutput"]
        fileName = createFileName(
            globalConfig=self.globalConfig,
            fileNameID="outputDataParser",
            datasetID=self.datasetID,
        )
        writeOut(data=self.activities, path=root / folder / fileName)


class ParkInference:
    def __init__(self, config_dict: dict) -> None:
        self.parse_config = config_dict['parseConfig']
        self.activities = None
        self.overnightSplitter = OvernightSplitter()

    def addParkingRows(self, activities: pd.DataFrame):
        """
        Wrapper function generating park activity rows between the trip data from the original MID dataset. Some
        utility attributes are being added such as isFirstActivity, isLastActivity or the hhPersonID of the next and
        previous activity. Redundant time observations are dropped after timestamp creation for start and end time of
        each activity. Overnight trips (e.g. extending from 23:00 at survey day to 1:30 on the consecutive day) are
        split up into two trips. The first one extends to the end of the day (00:00) and the other one is appended
        to the activity list before the first parking activity (0:00-1:30). The trip distance is split between the two
        based on the time.

        :param splitOvernightTrips: Should trips that end on the consecutive day (not the survey day) be split in two
        trips in such a way that the estimated trip distance the next day is appended in the morning hours of the survey
        day?
        """
        self.activities = activities
        
        splitOvernightTrips = self.parse_config['splitOvernightTrips']
        self.__copyRows()
        self.__addUtilAttributes()
        self.__addParkActAfterLastTrip()
        self.__adjustParkAttrs()
        self._dropRedundantCols()
        self.__removeParkActsAfterOvernightTrips()
        self.__adjustParkTimestamps()
        self.__setTripAttrsNAForParkActs()
        self.__addNextAndPrevIDs()
        self.__ONSplitDecider(split=splitOvernightTrips)  # ON = overnight
        self.__addTimeDeltaCol()
        self.__uniqueIndex()
        print(f'Finished activity composition with {self.activities["tripID"].fillna(0).astype(bool).sum()} trips '
            f'and {self.activities["parkID"].fillna(0).astype(bool).sum()} parking activites.')
        return self.activities

    def __copyRows(self):
        # Adding skeleton duplicate rows for parking activities
        self.activities = pd.concat(
            [self.activities] * 2).sort_index(ignore_index=True)
        self.activities["parkID"] = self.activities["tripID"]
        self.activities.loc[range(0, len(self.activities), 2), "tripID"] = pd.NA
        self.activities.loc[range(1, len(self.activities), 2), "parkID"] = pd.NA

    def __addUtilAttributes(self):
        # Adding additional attribute columns for convenience
        self.activities["uniqueID_prev"] = self.activities["uniqueID"].shift(
            fill_value=0
        )
        self.activities["isFirstActivity"] = (
            self.activities["uniqueID_prev"] != self.activities["uniqueID"]
        )
        self.activities["uniqueID_next"] = self.activities["uniqueID"].shift(
            -1, fill_value=0
        )
        self.activities["isLastActivity"] = (
            self.activities["uniqueID_next"] != self.activities["uniqueID"]
        )

    def __addParkActAfterLastTrip(self):
        # Adding park activities after last trips
        newIndex = self.activities.index[self.activities.isLastActivity]
        dfAdd = self.activities.loc[newIndex, :]
        dfAdd["tripID"] = pd.NA
        self.activities.loc[newIndex, "isLastActivity"] = False
        dfAdd["parkID"] = self.activities.loc[newIndex, "tripID"] + 1
        self.activities = pd.concat([self.activities, dfAdd]).sort_index()

    def __adjustParkAttrs(self):
        # Setting trip attribute values to zero where tripID == NaN (i.e. for all parking activities)
        self.activities.loc[
            self.activities["tripID"].isna(),
            ["tripDistance", "travelTime", "tripIsIntermodal"],
        ] = pd.NA
        self.activities["colFromIndex"] = self.activities.index
        self.activities = self.activities.sort_values(
        by=["colFromIndex", "tripID"])

    def _dropRedundantCols(self):
        # Clean-up of temporary redundant columns
        self.activities.drop(columns=[
            "tripStartClock",
            "tripEndClock",
            "tripStartYear",
            "tripStartMonth",
            "tripStartWeek",
            "tripStartHour",
            "tripStartMinute",
            "tripEndHour",
            "tripEndMinute",
            "uniqueID_prev",
            "uniqueID_next",
            "colFromIndex",
        ],
        inplace=True,
        )

    def __removeParkActsAfterOvernightTrips(self):
        # Checking for trips across day-limit and removing respective parking activities
        indexOvernight = (
            self.activities["isLastActivity"] & self.activities["tripEndNextDay"]
        )
        indexOvernight = indexOvernight.loc[indexOvernight]
        self.activities.loc[indexOvernight.index, "isLastActivity"] = True
        self.activities = self.activities.reset_index()

        # Get rid of park activities after overnight trips
        indexMultiDayActivity = (
            self.activities["isLastActivity"]
            & self.activities["tripEndNextDay"]
            & self.activities["parkID"]
        )
        self.activities = self.activities.loc[~indexMultiDayActivity, :]

    def __adjustParkTimestamps(self):
        """Adjust the start and end timestamps of the newly added rows. This is done via range index, that is reset at
        the beginning. First and last activities have to be treated separately since their dates have to match with
        their daily activity chain.
        """

        self.activities = self.activities.reset_index()
        parkingActwoFirst, parkingActwoLast = self.__getParkingActsWOFirstAndLast()

        self.__updateParkActStart(parkingActwoFirst=parkingActwoFirst)
        self.__updateParkActEnd(parkingActwoLast=parkingActwoLast)

        self.__updateTimestampFirstParkAct()
        self.__updateTimestampLastParkAct()

        print("Completed park timestamp adjustments.")

    def __getParkingActsWOFirstAndLast(self):
        """
        Returns all parking activities except for the last one (return argument 1) and the first one (return argument
        2)

        Return:
            pd.Series: Parking activity indices without the last one
            pd.Series: Parking activity indices without the first one
        """
        parkingAct = ~self.activities["parkID"].isna()
        parkingAct = parkingAct.loc[parkingAct]
        return parkingAct.iloc[1:], parkingAct.iloc[:-1]

    def __updateParkActStart(self, parkingActwoFirst: pd.Series):
        """Updating park start timestamps for newly added rows"""
        set_ts = self.activities.loc[parkingActwoFirst.index -
                                        1, "timestampEnd"]
        set_ts.index = self.activities.loc[
            parkingActwoFirst.index, "timestampStart"
        ].index
        self.activities.loc[parkingActwoFirst.index, "timestampStart"] = set_ts

    def __updateParkActEnd(self, parkingActwoLast: pd.Series):
        """Updating park end timestamps for newly added rows"""
        set_ts = self.activities.loc[parkingActwoLast.index +
                                        1, "timestampStart"]
        set_ts.index = self.activities.loc[parkingActwoLast.index,
                                            "timestampEnd"].index
        self.activities.loc[parkingActwoLast.index, "timestampEnd"] = set_ts

    def __updateTimestampFirstParkAct(self):
        """Updating park end timestamps for last activity in new park rows"""
        idxActs = ~(self.activities["parkID"].isna()) & (
            self.activities["isFirstActivity"]
        )
        self.activities.loc[idxActs, "timestampStart"] = replace_vec(
        self.activities.loc[idxActs, "timestampEnd"], hour=0, minute=0
        )

    def __updateTimestampLastParkAct(self):
        """Updating park end timestamps for last activity in new park rows"""
        idxActs = ~(self.activities["parkID"].isna()) & (
            self.activities["isLastActivity"]
        )
        self.activities.loc[idxActs, "timestampEnd"] = replace_vec(
            self.activities.loc[idxActs, "timestampStart"], hour=0, minute=0
        ) + pd.Timedelta(1, "d")

    def __setTripAttrsNAForParkActs(self):
        # Set tripEndNextDay to False for all park activities
        self.activities.loc[self.activities["tripID"].isna(),
                            "tripEndNextDay"] = pd.NA

    def __addNextAndPrevIDs(self):
        self.activities.loc[~self.activities["tripID"].isna(), "actID"] = self.activities["tripID"]
        self.activities.loc[~self.activities["parkID"].isna(), "actID"] = self.activities["parkID"]
        self.activities.loc[~self.activities["isLastActivity"], "nextActID"] = self.activities.loc[:, "actID"].shift(-1)
        self.activities.loc[~self.activities["isFirstActivity"], "prevActID"] = self.activities.loc[:, "actID"].shift(1)

    def __ONSplitDecider(self, split: bool):
        """Boolean function that differentiates if overnight trips should be split (split==True) or not (split==False).
        In the latter case, overnight trips identified by the variable 'tripEndNextDay' are excluded from the data set.

        Args:
            split (bool): Should trips that end on the consecutive day (not the survey day) be split in two trips in
            such a way that the estimated trip distance the next day is appended in the morning hours of the survey day?
        """
        if split:
            self.activities = self.overnightSplitter.splitOvernightTrips(activities=self.activities)
        else:
            self.__setONVarFalseForLastActTrip()
            self.__neglectONTrips()

    def __setONVarFalseForLastActTrip(self):
        """This function treats the edge case of trips being the last activity in the daily activity chain, i.e. trips
        ending exactly at 00:00. They are falsely labelled as overnight trips which is corrected here.

        """
        idxLastActTrips = (self.activities["isLastActivity"]) & ~(
            self.activities["tripID"].isna()
        )
        idxLastTripEndMidnight = (
            idxLastActTrips
            & (self.activities.loc[idxLastActTrips, "timestampEnd"].dt.hour == 0)
            & (self.activities.loc[idxLastActTrips, "timestampEnd"].dt.minute == 0)
        )
        self.tripEndNextDayRaw = self.activities["tripEndNextDay"]
        self.activities.loc[idxLastTripEndMidnight, "tripEndNextDay"] = False

    def __neglectONTrips(self):
        """
        Removes all overnight trips from the activities data set based on the column 'tripEndNextDay'. Updates
        timestamp end (to 00:00) and isLastActivity for the new last parking activities. Overwrites self.activities.
        """
        # Column for lastActivity setting later
        self.activities["tripEndNextDay_next"] = self.activities[
            "tripEndNextDay"
        ].shift(-1, fill_value=False)

        # Get rid of overnight trips
        idxNoONTrip = ~(self.activities["tripEndNextDay"].fillna(False))
        self.activities = self.activities.loc[idxNoONTrip, :]

        # Update isLastActivity and timestampEnd variables and clean-up column
        idxNewLastAct = self.activities["tripEndNextDay_next"]
        idxNewLastAct = idxNewLastAct.fillna(False).astype(bool)
        self.activities.loc[idxNewLastAct, "isLastActivity"] = True
        self.activities.loc[idxNewLastAct, "timestampEnd"] = replace_vec(
            self.activities.loc[idxNewLastAct, "timestampStart"], hour=0, minute=0
        ) + pd.Timedelta(1, "d")
        self.activities = self.activities.drop(columns=["tripEndNextDay_next"])

    def __addTimeDeltaCol(self):
        # Add timedelta column
        self.activities["timedelta"] = (
            self.activities["timestampEnd"] - self.activities["timestampStart"]
        )

    def __uniqueIndex(self):
        self.activities.drop(columns=["level_0"], inplace=True)
        self.activities.reset_index(
            inplace=True
        )  # Due to copying and appending rows, the index has to be reset


class OvernightSplitter:
    def __init__(self):
        self.activities = None

    def splitOvernightTrips(self, activities: pd.DataFrame) -> pd.DataFrame:
        """Wrapper function for treating edge case trips ending not in the 24 hours of the survey day but stretch
        to the next day. Those overnight (ON) are split up into an evening trip at the regular survey day and a
        morning trip at the next day. Trip distances are split according to the time the person spent on that trip.
        E.g. if a trip lasts from 23:00 to 2:00 the next day and 100 km, the split-up evening trip will last from
        23:00 to 00:00 of the survey day and 33 km and the morning trip from 00:00 to 2:00 and 66 km. In a next step,
        the morning trip is appended to the survey day in the first hours.

        Here, different edge cases occur.
        Edge case 1 (N=5 in MiD17): For trips that overlap with night (early morning) trips at the survey day, e.g. from
        0:30 to 1:00 for the above mentioned example, the morning part of the split overnight trip is completely
        disregarded.
        Edge case 2 (N=3 in MiD17): When overnight mornging split-trips end exactly at the time where the first trip of
        the survey day starts (2:00 in the example), both trips are consolidated to one trip with all attributes of the
        survey trip.
        These edge cases are documented and quantified in issue #358 'Sum of all distances of dataParser at end equals
        sum of all distances after filtering'.
        """
        self.activities = activities
        
        # Split overnight trips and add next day distance in the morning (tripID=0)
        isONTrip, overnightTripsAdd = self.__getOvernightActs()
        overnightTripsAddTS = self.__adjustONTimestamps(trips=overnightTripsAdd)
        self.__setAllLastActEndTSToZero()
        morningTrips = self.__setONTripIDZero(trips=overnightTripsAddTS)
        morningTrips = self.__adjustMorningTripDistance(overnightTrips=overnightTripsAdd, morningTrips=morningTrips)
        self.__adjustEveningTripDistance(morningTrips=morningTrips, isONTrip=isONTrip)
        self.__setFirstLastActs(morningTrips=morningTrips)
        isPrevFirstActs = self.__getPrevFirstAct(morningTrips=morningTrips)  # Activities that were previously firstActs
        morningTrips_noOverlap, isPrevFirstActs = self.__neglectOverlapMorningTrips(
            morningTrips=morningTrips, isPrevFirstActs=isPrevFirstActs
        )
        morningTrips_add = self.__setNextParkingTSStart(
            morningTrips=morningTrips_noOverlap, isONTrip=isONTrip, isPrevFirstActs=isPrevFirstActs
        )
        self.__addMorningTrips(morningTrips=morningTrips_add)
        self.__removeFirstParkingAct()
        self.__mergeAdjacentTrips()
        # Implement DELTA mileage check of overnight morning split trip distances
        self.__checkAndAssert()
        self.__dropONCol()
        self.__sortActivities()
        return self.activities

    def __getOvernightActs(self):
        indexOvernightActs = (
            self.activities["isLastActivity"]
            & self.activities["tripEndNextDay"]
            & ~(
                (self.activities["timestampEnd"].dt.hour == 0)
                & (  # assure that the overnight trip does
                    self.activities["timestampEnd"].dt.minute == 0
                )
            )
        )  # not exactly end at 00:00
        return indexOvernightActs, self.activities.loc[indexOvernightActs, :]

    def __adjustONTimestamps(self, trips: pd.DataFrame):
        tripsRes = trips.copy()
        tripsRes["timestampEnd"] = tripsRes.loc[:, "timestampEnd"] - pd.Timedelta(
            1, "d"
        )
        tripsRes["timestampStart"] = replace_vec(
            tripsRes.loc[:, "timestampEnd"], hour=0, minute=0
        )
        return tripsRes

    def __setAllLastActEndTSToZero(self):
        # Set timestamp end of evening part of overnight trip split to 00:00
        self.activities.loc[
            self.activities["isLastActivity"], "timestampEnd"
        ] = replace_vec(
            self.activities.loc[self.activities["isLastActivity"],
                                "timestampEnd"],
            hour=0,
            minute=0,
        )

    def __setONTripIDZero(self, trips):
        trips["tripID"] = 0
        trips["actID"] = 0
        trips["prevActID"] = pd.NA

        # Update next activity ID
        uniqueID = trips["uniqueID"]
        actIdx = (
            self.activities["uniqueID"].isin(uniqueID)
            & self.activities["isFirstActivity"]
        )
        trips["nextActID"] = self.activities.loc[actIdx, "actID"]

        # Update previous activity ID of previously first activity
        self.activities.loc[actIdx, "prevActID"] = 0
        return trips

    def __adjustMorningTripDistance(
            self, overnightTrips: pd.DataFrame, morningTrips: pd.DataFrame
        ):
        # Splitting the total distance to morning and evening trip time-share dependent
        morningTrips["timedelta_total"] = (
            overnightTrips["timestampEnd"] - overnightTrips["timestampStart"]
        )
        morningTrips["timedelta_morning"] = (
            morningTrips["timestampEnd"] - morningTrips["timestampStart"]
        )
        morningTrips["timeShare_morning"] = (
            morningTrips["timedelta_morning"] / morningTrips["timedelta_total"]
        )
        morningTrips["timeShare_evening"] = (
            morningTrips["timedelta_total"] - morningTrips["timedelta_morning"]
        ) / morningTrips["timedelta_total"]
        morningTrips["totalTripDistance"] = morningTrips["tripDistance"]
        morningTrips["tripDistance"] = (
            morningTrips["timeShare_morning"] *
            morningTrips["totalTripDistance"]
        )
        return morningTrips

    def __adjustEveningTripDistance(
            self, morningTrips: pd.DataFrame, isONTrip: pd.Series
        ):
        self.activities.loc[isONTrip, "tripDistance"] = (
            morningTrips["timeShare_evening"] *
            morningTrips["totalTripDistance"]
        )

    def __setFirstLastActs(self, morningTrips: pd.DataFrame):
        # Setting first and last activities
        morningTrips["isFirstActivity"] = True
        morningTrips["isLastActivity"] = False

    def __getPrevFirstAct(self, morningTrips: pd.DataFrame):
        return (
            self.activities["uniqueID"].isin(morningTrips["uniqueID"])
            & self.activities["isFirstActivity"]
        )

    def __neglectOverlapMorningTrips(
            self, morningTrips: pd.DataFrame, isPrevFirstActs: pd.DataFrame
        ):
        # Option 1 of treating overlaps: After concatenation in the end
        firstTripsEnd = self.activities.loc[isPrevFirstActs, "timestampEnd"].copy()
        firstTripsEnd.index = morningTrips.index  # Adjust index for comparison

        # Filter out morning parts of overnight trip split for persons that already have morning trips in that period
        neglectOvernight = firstTripsEnd < morningTrips["timestampEnd"]
        morningTrips_noOverlap = morningTrips.loc[~neglectOvernight, :]

        # Filter out neglected activities from prevFirstActs accordingly
        neglectOvernightIdx = neglectOvernight
        neglectOvernightIdx.index = isPrevFirstActs[isPrevFirstActs].index  # Align index for filtering
        neglectOvernightIdx = neglectOvernightIdx[neglectOvernightIdx]
        isPrevFirstActs[neglectOvernightIdx.index] = False

        return morningTrips_noOverlap, isPrevFirstActs

    def __setNextParkingTSStart(
            self,
            morningTrips: pd.DataFrame,
            isONTrip: pd.Series,
            isPrevFirstActs: pd.DataFrame,
        ):
        # Setting start timestamp of previously first activity (parking) to end timestamp of morning split of ON trip
        ts_new = morningTrips.loc[isONTrip, "timestampEnd"]
        ts_new.index = self.activities.loc[isPrevFirstActs,
                                            "timestampStart"].index
        self.activities.loc[isPrevFirstActs, "timestampStart"] = ts_new
        self.activities.loc[isPrevFirstActs, "isFirstActivity"] = False

        # Set nextActID column of ON trips to consecutive activity
        return self.__updateNextActID(
            prevFirstActs=self.activities.loc[isPrevFirstActs, :],
            morningTrips=morningTrips,
        )

    def __updateNextActID(
            self, prevFirstActs: pd.DataFrame, morningTrips: pd.DataFrame
        ):
        nextActs = prevFirstActs.loc[prevFirstActs["prevActID"] == 0, "actID"]
        nextActs.index = morningTrips.index
        ret = morningTrips.copy()
        ret.loc[:, "nextActID"] = nextActs
        return ret

    def __addMorningTrips(self, morningTrips: pd.DataFrame):
        # Appending overnight morning trips
        self.activities = pd.concat([self.activities, morningTrips])

    def __removeFirstParkingAct(self):
        # Removing first parking activities for persons where first activity is a trip (starting at 00:00)
        firstParkActs = self.activities.loc[self.activities["parkID"] == 1, :]
        firstTripActs = self.activities.loc[self.activities["tripID"] == 1, :]
        firstTripActs.index = firstParkActs.index  # Aligning trip indices
        idxParkTS = firstParkActs["timestampStart"] == firstTripActs["timestampStart"]
        self.activities = self.activities.drop(idxParkTS[idxParkTS].index)

        # After removing first parking, set first trip to first activity
        self.activities.loc[
            (self.activities["uniqueID"].isin(
                firstParkActs.loc[idxParkTS, "uniqueID"]))
            & (self.activities["tripID"] == 1),
            "isFirstActivity",
        ] = True

    def __mergeAdjacentTrips(self):
        """Consolidate overnight morning trips and first trips for the edge case where morning trips of next day
        end exactly at the beginning of the first trip of the survey day. In this case, the morning split of the
        overnight trip is neglected and the beginning of the first trip is set to 00:00. In the MiD17 data set, there
        were 3 occurences of this case all with end times of the overnight trip between 00:00 and 01:00.

        """
        uniqueID = self.__getUniqueIDsToNeglect()
        self.__neglectZeroTripIDFromActivities(id_neglect=uniqueID)
        self.__updateConsolidatedAct(id_neglect=uniqueID)

    def __checkAndAssert(self):
        # Calculates the neglected trip distances from overnight split trips with regular morning trips
        distance = (
            self.activities["tripDistance"].sum()
            - self.activities.loc[
                ~self.activities["tripID"].isna(), "tripDistance"
            ].sum()
        )
        allTripDistance = self.activities.loc[
            ~self.activities["tripID"].isna(), "tripDistance"
        ].sum()
        ratio = distance / allTripDistance
        print(
            f"From {allTripDistance} km total mileage in the dataset after filtering, {ratio * 100}% were cropped "
            f"because they corresponded to split-trips from overnight trips."
        )
        assert ratio < 0.01

    def __getUniqueIDsToNeglect(self):
        """
        Identifies the household person IDs that should be neglected.
        """
        uniqueIDsOvernight = self.activities.loc[
            self.activities["tripID"] == 0, "uniqueID"
        ]
        acts = self.activities.loc[
            self.activities["uniqueID"].isin(uniqueIDsOvernight), :
        ]
        actsOvernight = acts.loc[acts["tripID"] == 0, :]
                # Next trip after morning part of overnight split
        actsNextTrip = acts.loc[acts["prevActID"] == 0, :]
        return actsOvernight.loc[
            ~actsOvernight["uniqueID"].isin(
                actsNextTrip["uniqueID"]), "uniqueID"
        ]

    def __neglectZeroTripIDFromActivities(self, id_neglect: pd.Series):
        """
        This method filters out the activities with the given hhpid and tripID 0.
        """
        boolNeglect = (self.activities["uniqueID"].isin(id_neglect)) & (
            self.activities["tripID"] == 0
        )
        self.activities = self.activities.loc[~boolNeglect, :]

    def __updateConsolidatedAct(self, id_neglect: pd.Series):
        """
        This method sets the start timestamp of the firstActivity of all hhpids given as argument to 00:00. Additionally
        the prevActID is set to pd.NA
        """
        idxConsolidatedTrips = (self.activities["uniqueID"].isin(id_neglect)) & (
            self.activities["isFirstActivity"]
        )
        self.activities.loc[idxConsolidatedTrips, "timestampStart"] = replace_vec(
            self.activities.loc[idxConsolidatedTrips, "timestampStart"],
            hour=0,
            minute=0,
        )
        self.activities.loc[idxConsolidatedTrips, "prevActID"] = pd.NA

    def __dropONCol(self):
        self.activities = self.activities.drop(columns=["tripEndNextDay"])

    def __sortActivities(self):
        self.activities = self.activities.sort_values(
            by=["uniqueID", "timestampStart"])


class IntermediateParsing(DataParser):
    def __init__(
        self, configDict: dict, datasetID: str, debug, loadEncrypted=False
    ):
        """
        Intermediate parsing class.

        :param configDict: VencoPy config dictionary consisting at least of
                           the config dictionaries globalConfig,
                           parseConfig and localPathConfig.
        :param datasetID: A string identifying the MiD data set.
        :param loadEncrypted: Boolean. If True, data is read from encrypted
                              file. For this, a possword has to be
                              specified in parseConfig['PW'].
        """
        super().__init__(
            configDict, datasetID=datasetID, loadEncrypted=loadEncrypted, debug=debug
        )
        self.filterDict = self.parseConfig["filterDicts"][self.datasetID]
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
        listIndex = self.parseConfig["dataVariables"]["datasetID"].index(
            self.datasetID)
        variables = [
            val[listIndex] if val[listIndex] != "NA" else "NA"
            for key, val in self.parseConfig["dataVariables"].items()
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

    def _selectColumns(self):
        """
        Function to filter the rawData for only relevant columns as specified
        by parseConfig and cleaned in self.compileVariablesList().
        Stores the subset of data in self.activities

        :return: None
        """
        self.activities = self.rawData.loc[:, self.columns]

    def _convertTypes(self):
        """
        Convert raw column types to predefined python types as specified in
        parseConfig['inputDTypes'][datasetID]. This is mainly done for
        performance reasons. But also in order to avoid index values that are
        of type int to be cast to float. The function operates only on
        self.activities and writes back changes to self.activities

        :return: None
        """
        # Filter for dataset specific columns
        conversionDict = self.parseConfig["inputDTypes"][self.datasetID]
        keys = {iCol for iCol in conversionDict.keys()
                if iCol in self.activities.columns}
        self.varDataTypeDict = {
            key: conversionDict[key] for key in conversionDict.keys() & keys
        }
        self.activities = self.activities.astype(self.varDataTypeDict)

    def _complexFilters(self) -> pd.DataFrame:
        """Collects filters that compare multiple columns or derived variables or calculation results thereof. True
        in this filter means "keep row". The function needs self.activities to determine the length and the index of the
        return argument.

        Returns:
            pd.DataFrame: DataFrame with a boolean column per complex filter. True means keep the row in the activities
            data set.
        """
        complexFilters = pd.DataFrame(index=self.activities.index)
        complexFilters = complexFilters.join(self._filterInconsistentSpeedTrips())
        complexFilters = complexFilters.join(self._filterInconsistentTravelTimes())
        complexFilters = complexFilters.join(self._filterOverlappingTrips())
        complexFilters = complexFilters.join(self._filterConsistentHours())
        complexFilters = complexFilters.join(self._filterNoZeroLengthTrips())
        return complexFilters

    def _filterConsistentHours(self) -> pd.Series:
        """
        Filtering out records where starting timestamp is before end timestamp. These observations are data errors.

        :return: Returns a boolean Series indicating erroneous rows (trips) with False.
        """
        ser = (self.activities["timestampStart"] <= self.activities["timestampEnd"])
        ser.name = 'tripStartAfterEnd'
        return ser

    def _filterNoZeroLengthTrips(self) -> pd.Series:
        """Filter out trips that start and end at same hour and minute but are not ending on next day (no 24-hour
        trips).

        Returns:
            _type_: _description_
        """

        ser = ~(
            (self.activities.loc[:, "tripStartHour"]
                == self.activities.loc[:, "tripEndHour"])
            & (
                self.activities.loc[:, "tripStartMinute"]
                == self.activities.loc[:, "tripEndMinute"]
            )
            & (~self.activities.loc[:, "tripEndNextDay"])
            )
        ser.name = 'isNoZeroLengthTrip'
        return ser

    def _addStrColumnFromVariable(self, colName: str, varName: str):
        """
        Replaces each occurence of a MiD/KiD variable e.g. 1,2,...,7 for
        weekdays with an explicitly mapped string e.g. 'MON', 'TUE',...,'SUN'.

        :param colName: Name of the column in self.activities where the explicit
                        string info is stored
        :param varName: Name of the VencoPy internal variable given in
                        config/parseConfig['dataVariables']
        :return: None
        """
        self.activities.loc[:, colName] = self.activities.loc[:, varName].replace(
            self.parseConfig["Replacements"][self.datasetID][varName]
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

    def _composeStartAndEndTimestamps(self):
        """
        :return: Returns start and end time of a trip
        """
        self.__composeTimestamp(data=self.activities)  # Starting timestamp
        self.__composeTimestamp(
            data=self.activities,  # Ending timestamps
            colHour="tripEndHour",
            colMin="tripEndMinute",
            colName="timestampEnd",
        )

    def _updateEndTimestamp(self):
        """
        Updates the end timestamp for overnight trips adding 1 day

        :return: None, only acts on the class variable
        """
        endsFollowingDay = self.activities["tripEndNextDay"] == 1
        self.activities.loc[endsFollowingDay, "timestampEnd"] = self.activities.loc[
            endsFollowingDay, "timestampEnd"
        ] + pd.offsets.Day(1)

    def _harmonizeVariablesUniqueIDNames(self):
        """
        Harmonises ID variables for all datasets.
        """
        self.activities["uniqueID"] = (
            self.activities[str(self.parseConfig["IDVariablesNames"]
                                [self.datasetID])]
        ).astype(int)
        print("Finished harmonization of ID variables.")

    def _subsetVehicleSegment(self):
        if self.parseConfig['subsetVehicleSegment']:
            self.activities = self.activities[(
                self.activities['vehicleSegmentStr'] == self.parseConfig['vehicleSegment'][self.datasetID])]
            print(f"The subset contains only vehicles of the class {self.parseConfig['vehicleSegment'][self.datasetID]} for a total of {len(self.activities.uniqueID.unique())} individual vehicles.")

    def _cleanupDataset(self):
        self.activities.drop(
            columns=['level_0',
                     'tripIsIntermodal',
                     'timedelta_total',
                     'timedelta_morning',
                     'timeShare_morning',
                     'timeShare_evening',
                     'totalTripDistance'], inplace=True)


class ParseMiD(IntermediateParsing):
    def __init__(
        self, configDict: dict, datasetID: str, loadEncrypted=False, debug=False
    ):
        """
        Class for parsing MiD data sets. The VencoPy configs globalConfig,
        parseConfig and localPathConfig have to be given on instantiation as
        well as the data set ID, e.g. 'MiD2017' that is used as key in the
        config lookups. Also, an option can be specified to load the file from
        an encrypted ZIP-file. For this, a password has to be given in the
        parseConfig.

        :param configDict: VencoPy config dictionary consisting at least of the
                           config dictionaries globalConfig, parseConfig and
                           localPathConfig.
        :param datasetID: A string identifying the MiD data set.
        :param loadEncrypted: Boolean. If True, data is read from encrypted
                              file. For this, a possword has to be
                              specified in parseConfig['PW'].
        """
        super().__init__(
            configDict=configDict,
            datasetID=datasetID,
            loadEncrypted=loadEncrypted,
            debug=debug,
        )
        self.parkInference = ParkInference(config_dict=configDict)

    def __harmonizeVariables(self):
        """
        Harmonizes the input data variables to match internal VencoPy names
        given as specified in the mapping in parseConfig['dataVariables'].
        So far mappings for MiD08 and MiD17 are given. Since the MiD08 does
        not provide a combined household and person unique identifier, it is
        synthesized of the both IDs.

        :return: None
        """
        replacementDict = self._createReplacementDict(
            self.datasetID, self.parseConfig["dataVariables"]
        )
        activitiesRenamed = self.activities.rename(columns=replacementDict)
        if self.datasetID == "MiD08":
            activitiesRenamed["hhPersonID"] = (
                activitiesRenamed["hhID"].astype("string")
                + activitiesRenamed["personID"].astype("string")
            ).astype("int")
        self.activities = activitiesRenamed
        print("Finished harmonization of variables.")

    def __addStrColumns(self, weekday=True, purpose=True):
        """
        Adds string columns for either weekday or purpose.

        :param weekday: Boolean identifier if weekday string info should be
                        added in a separate column
        :param purpose: Boolean identifier if purpose string info should be
                        added in a separate column
        :return: None
        """
        if weekday:
            self._addStrColumnFromVariable(
                colName="weekdayStr", varName="tripStartWeekday"
            )
        if purpose:
            self._addStrColumnFromVariable(
                colName="purposeStr", varName="tripPurpose")

    def _dropRedundantCols(self):
        # Clean-up of temporary redundant columns
        self.activities.drop(
            columns=[
                "isMIVDriver",
                "tripStartClock",
                "tripEndClock",
                "tripStartYear",
                "tripStartMonth",
                "tripStartWeek",
                "tripStartHour",
                "tripStartMinute",
                "tripEndHour",
                "tripEndMinute",
                "uniqueID_prev",
                "uniqueID_next",
                "colFromIndex",
            ],
            inplace=True,
        )

    def process(self):
        """
        Wrapper function for harmonising and filtering the activities dataset as well as adding parking rows.

        :param splitOvernightTrips: Should trips that end on the consecutive day (not the survey day) be split in such
        a way that the estimated trip distance the next day is appended in the morning hours of the survey day?
        """
        self._selectColumns()
        self.__harmonizeVariables()
        self._harmonizeVariablesUniqueIDNames()
        self._convertTypes()
        self.__addStrColumns()
        self._composeStartAndEndTimestamps()
        self._updateEndTimestamp()
        self._checkFilterDict()
        self._filter(self.filterDict)
        self._filterConsistentHours()
        self.activities = self.parkInference.addParkingRows(activities=self.activities)
        self._cleanupDataset()
        print("Parsing MiD dataset completed.")
        return self.activities


class ParseVF(IntermediateParsing):
    def __init__(
        self, configDict: dict, datasetID: str, debug, loadEncrypted=False
    ):
        """
        Class for parsing MiD data sets. The VencoPy configs globalConfig,
        parseConfig and localPathConfig have to be given on instantiation as
        well as the data set ID, e.g. 'MiD2017' that is used as key in the
        config lookups. Also, an option can be specified to load the file from
        an encrypted ZIP-file. For this, a password has to be given in the
        parseConfig.

        :param configDict: VencoPy config dictionary consisting at least of the
                           config dictionaries globalConfig, parseConfig and
                           localPathConfig.
        :param datasetID: A string identifying the MiD data set.
        :param loadEncrypted: Boolean. If True, data is read from encrypted
                              file. For this, a possword has to be
                              specified in parseConfig['PW'].
        """
        super().__init__(
            configDict=configDict, datasetID=datasetID, debug=debug, loadEncrypted=loadEncrypted
        )

    def _loadData(self):
        """
        rawDataPathTrip, unlike for other MiD classes is taken from the MiD B1 dataset
        rawDataPathVehicles is an internal dataset from VF
        """
        rawDataPathTrips = (
            Path(self.localPathConfig["pathAbsolute"][self.datasetID])
            / self.globalConfig["files"][self.datasetID]["tripsDataRaw"]
        )
        rawDataPathVehicles = (
            Path(self.localPathConfig["pathAbsolute"][self.datasetID])
            / self.globalConfig["files"][self.datasetID]["vehiclesDataRaw"]
        )
        rawDataTrips = pd.read_stata(
            rawDataPathTrips,
            convert_categoricals=False,
            convert_dates=False,
            preserve_dtypes=False,
        )
        rawDataVehicles = pd.read_csv(
            rawDataPathVehicles, encoding="ISO-8859-1")
        rawDataVehicles = rawDataVehicles.drop(columns=["Unnamed: 0"])
        rawDataVehicles = rawDataVehicles.drop_duplicates(
            subset=["HP_ID"], keep="first"
        )
        rawDataVehicles.set_index("HP_ID", inplace=True)
        rawData = rawDataTrips.join(rawDataVehicles, on="HP_ID", rsuffix="VF")
        self.rawData = rawData
        print(
            f"Finished loading {len(self.rawData)} rows of raw data of type .dta.")

    def __harmonizeVariables(self):
        """
        Harmonizes the input data variables to match internal VencoPy names given as specified in the mapping in
        parseConfig['dataVariables']. Mappings for MiD08 and MiD17 are given. Since the MiD08 does not provide a
        combined household and person unique identifier, it is synthesized of the both IDs.

        :return: None
        """
        replacementDict = self._createReplacementDict(
            self.datasetID, self.parseConfig["dataVariables"]
        )
        dataRenamed = self.activities.rename(columns=replacementDict)
        if self.datasetID == "MiD08":
            dataRenamed["hhPersonID"] = (
                dataRenamed["hhID"].astype("string")
                + dataRenamed["personID"].astype("string")
            ).astype("int")
        self.activities = dataRenamed
        print("Finished harmonization of variables")

    def __padMissingCarSegments(self):
        # remove vehicleSegment nicht zuzuordnen
        self.activities = self.activities[self.activities.vehicleSegment != 'nicht zuzuordnen']
        # pad missing car segments
        # self.activities.vehicleSegment = self.activities.groupby('hhID').vehicleSegment.transform('first')
        # self.activities.drivetrain = self.activities.groupby('hhID').drivetrain.transform('first')
        # self.activities.vehicleID = self.activities.groupby('hhID').vehicleID.transform('first')
        # remove remaining NaN
        self.activities = self.activities.dropna(subset=['vehicleSegment'])
        # self.activities = self.activities.dropna(subset=['vehicleSegment', 'drivetrain', 'vehicleID'])

    def __excludeHours(self):
        """
        Removes trips where both start and end trip time are missing. KID-specific function.
        """
        self.activities = self.activities.dropna(subset=['tripStartClock', 'tripEndClock'])

    def __addStrColumns(self, weekday=True, purpose=True, vehicleSegment=True):
        """
        Adds string columns for either weekday or purpose.

        :param weekday: Boolean identifier if weekday string info should be
                        added in a separate column
        :param purpose: Boolean identifier if purpose string info should be
                        added in a separate column
        :return: None
        """
        if weekday:
            self._addStrColumnFromVariable(
                colName="weekdayStr", varName="tripStartWeekday"
            )
        if purpose:
            self._addStrColumnFromVariable(
                colName="purposeStr", varName="tripPurpose")
        if vehicleSegment:
            self.activities = self.activities.replace('groß', 'gross')
            self._addStrColumnFromVariable(
                colName="vehicleSegmentStr", varName="vehicleSegment")

    def _dropRedundantCols(self):
        # Clean-up of temporary redundant columns
        self.activities.drop(
            columns=[
                "tripStartClock",
                "tripEndClock",
                "tripStartYear",
                "tripStartMonth",
                "tripStartWeek",
                "tripStartHour",
                "tripStartMinute",
                "tripEndHour",
                "tripEndMinute",
                "uniqueID_prev",
                "uniqueID_next",
                "colFromIndex",
            ],
            inplace=True,
        )

    def process(self):
        """
        Wrapper function for harmonising and filtering the dataset.
        """
        self._selectColumns()
        self.__harmonizeVariables()
        self._harmonizeVariablesUniqueIDNames()
        self.__padMissingCarSegments()
        self.__excludeHours()
        self._convertTypes()
        self.__addStrColumns()
        self._composeStartAndEndTimestamps()
        self._updateEndTimestamp()
        self._checkFilterDict()
        self._filter(self.filterDict)
        self._filterConsistentHours()
        self._addParkingRows()
        self._subsetVehicleSegment()
        self._cleanupDataset()
        print("Parsing VF dataset completed.")
        return self.activities


class ParseKiD(IntermediateParsing):
    def __init__(
        self, configDict: dict, datasetID: str, debug, loadEncrypted=False
    ):
        """
        Inherited data class to differentiate between abstract interfaces such
        as vencopy internal variable namings and data set specific functions
        such as filters etc.
        """
        super().__init__(
            configDict=configDict,
            datasetID=datasetID,
            loadEncrypted=loadEncrypted,
            debug=debug
        )

    def _loadData(self):
        rawDataPathTrips = (
            Path(self.localPathConfig["pathAbsolute"][self.datasetID])
            / self.globalConfig["files"][self.datasetID]["tripsDataRaw"]
        )
        rawDataPathVehicles = (
            Path(self.localPathConfig["pathAbsolute"][self.datasetID])
            / self.globalConfig["files"][self.datasetID]["vehiclesDataRaw"]
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
        rawData = rawDataTrips.join(rawDataVehicles, on="k00")
        self.rawData = rawData
        print(
            f"Finished loading {len(self.rawData)} " f"rows of raw data of type .dta.")

    def __changeSeparator(self):
        """
        Replaces commas with dots in the dataset (German datasets).

        :return: None
        """
        for i, x in enumerate(list(self.activities.tripDistance)):
            self.activities.at[i, "tripDistance"] = x.replace(",", ".")
        for i, x in enumerate(list(self.activities.tripWeight)):
            self.activities.at[i, "tripWeight"] = x.replace(",", ".")

    def __addStrColumns(self, weekday=True, purpose=True, vehicleSegment=True):
        """
        Adds string columns for either weekday or purpose.

        :param weekday: Boolean identifier if weekday string info should be
                        added in a separate column
        :param purpose: Boolean identifier if purpose string info should be
                        added in a separate column
        :return: None
        """
        self.activities["tripStartDate"] = pd.to_datetime(self.activities["tripStartDate"], format="%d.%m.%Y")
        self.activities["tripStartYear"] = self.activities["tripStartDate"].dt.year
        self.activities["tripStartMonth"] = self.activities["tripStartDate"].dt.month
        self.activities["tripStartDay"] = self.activities["tripStartDate"].dt.day
        self.activities["tripStartWeekday"] = self.activities["tripStartDate"].dt.weekday
        self.activities["tripStartWeek"] = self.activities["tripStartDate"].dt.isocalendar().week
        self.activities["tripStartHour"] = pd.to_datetime(
            self.activities["tripStartClock"], format="%H:%M"
        ).dt.hour
        self.activities["tripStartMinute"] = pd.to_datetime(
            self.activities["tripStartClock"], format="%H:%M"
        ).dt.minute
        self.activities["tripEndHour"] = pd.to_datetime(
            self.activities["tripEndClock"], format="%H:%M"
        ).dt.hour
        self.activities["tripEndMinute"] = pd.to_datetime(
            self.activities["tripEndClock"], format="%H:%M"
        ).dt.minute
        if weekday:
            self._addStrColumnFromVariable(
                colName="weekdayStr", varName="tripStartWeekday"
            )
        if purpose:
            self._addStrColumnFromVariable(
                colName="purposeStr", varName="tripPurpose")
        if vehicleSegment:
            self._addStrColumnFromVariable(
                colName="vehicleSegmentStr", varName="vehicleSegment")

    def __updateEndTimestamp(self):
        """
        Separate implementation for the KID dataset. Overwrites parent method.

        :return: None
        """
        self.activities["tripEndNextDay"] = np.where(
            self.activities["timestampEnd"].dt.day > self.activities["timestampStart"].dt.day, 1, 0
        )
        endsFollowingDay = self.activities["tripEndNextDay"] == 1
        self.activities.loc[endsFollowingDay, "timestampEnd"] = self.activities.loc[
            endsFollowingDay, "timestampEnd"
        ] + pd.offsets.Day(1)

    def __excludeHours(self):
        """
        Removes trips where both start and end trip time are missing. KID-specific function.
        """
        self.activities = self.activities.loc[
            (self.activities["tripStartClock"] != "-1:-1") & (self.activities["tripEndClock"] != "-1:-1"),
            :,
        ]

    def process(self):
        """
        Wrapper function for harmonising and filtering the dataset.
        """
        self._selectColumns()
        self._harmonizeVariables()
        self._harmonizeVariablesUniqueIDNames()
        self.__changeSeparator()
        self._convertTypes()
        self.__excludeHours()
        self.__addStrColumns()
        self._composeStartAndEndTimestamps()
        self.__updateEndTimestamp()
        self._checkFilterDict()
        self._filter(self.filterDict)
        self._filterConsistentHours()
        self._addParkingRows()
        self._subsetVehicleSegment()
        self._cleanupDataset()
        print("Parsing KiD dataset completed.")
        return self.activities


def parseData(configDict: dict) -> Union[ParseMiD, ParseKiD, ParseVF]:
    datasetID = configDict["globalConfig"]["dataset"]
    debug = configDict["globalConfig"]["debug"]
    delegate = {"MiD17": ParseMiD, "KiD": ParseKiD, "VF": ParseVF}
    return delegate[datasetID](
        configDict=configDict, datasetID=datasetID, debug=debug
    )
