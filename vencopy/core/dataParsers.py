__version__ = "0.4.X"
__maintainer__ = "Niklas Wulff"
__contributors__ = "Fabia Miorelli"
__email__ = "Niklas.Wulff@dlr.de"
__birthdate__ = "21.04.2022"
__status__ = "dev"  # options are: dev, test, prod


# ----- imports & packages ------
if __package__ is None or __package__ == '':
    import sys
    from os import path

    sys.path.append(path.dirname(path.dirname(path.dirname(__file__))))

import pprint
import warnings
from pathlib import Path
from zipfile import ZipFile

import numpy as np
import pandas as pd
from profilehooks import profile

from vencopy.utils.globalFunctions import loadConfigDict, replace_vec, writeOut
from vencopy.utils.globalFunctions import returnDictBottomKeys, returnDictBottomValues


class DataParser:
    def __init__(self, configDict: dict, datasetID: str, fpInZip=None, loadEncrypted=False, debug=False):
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
        filepath = (Path(self.localPathConfig["pathAbsolute"][self.datasetID])
                    / self.globalConfig["files"][self.datasetID]["tripsDataRaw"])
        self.rawDataPath = filepath
        self.rawData = None
        self.data = None
        self.activities = None
        self.filterDict = {}
        print("Generic file parsing properties set up")
        if loadEncrypted:
            print(f"Starting to retrieve encrypted data file from {self.rawDataPath}")
            self._loadEncryptedData(pathToZip=filepath, pathInZip=fpInZip)
        else:
            print(f"Starting to retrieve local data file from {self.rawDataPath}")
            self._loadData()
        self.rawData = self.rawData.loc[0:2000, :] if debug else self.rawData.copy()

    def _loadData(self):
        """
        Loads data specified in self.rawDataPath and stores it in self.rawData.
        Raises an exception if a invalid suffix is specified in
        self.rawDataPath.
        READ IN OF CSV HAS NOT BEEN EXTENSIVELY TESTED BEFORE BETA RELEASE.

        :return: None
        """
        # Future releases: Are potential error messages (.dta not being a stata
        # file even as the ending matches) readable for the user?
        # Should we have a manual error treatment here?
        if self.rawDataPath.suffix == ".dta":
            self.rawData = pd.read_stata(self.rawDataPath, convert_categoricals=False, convert_dates=False,
                                         preserve_dtypes=False)
        # This has not been tested before the beta release
        elif self.rawDataPath.suffix == ".csv":
            self.rawData = pd.read_csv(self.rawDataPath)
        else:
            Exception(
                f"Data type {self.rawDataPath.suffix} not yet specified. Available types so far are .dta and .csv")
        print(f"Finished loading {len(self.rawData)} rows of raw data of type {self.rawDataPath.suffix}")
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
                self.rawData = pd.read_stata(myzip.open(
                    pathInZip, pwd=bytes(self.parseConfig["encryptionPW"], encoding="utf-8"),),
                    convert_categoricals=False, convert_dates=False, preserve_dtypes=False,)
            else:  # if '.csv' in pathInZip:
                self.rawData = pd.read_csv(
                    myzip.open(
                        pathInZip,
                        pwd=bytes(
                            self.parseConfig["encryptionPW"], encoding="utf-8"
                        ),
                    ),
                    sep=";",
                    decimal=",",
                )

        print(f"Finished loading {len(self.rawData)} rows of raw data of type {self.rawDataPath.suffix}")

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
        So far mappings for MiD08 and MiD17 are given. Since the MiD08 does
        not provide a combined household and person unique identifier, it is
        synthesized of the both IDs.

        :return: None
        """
        replacementDict = self.createReplacementDict(
            self.datasetID, self.parseConfig["dataVariables"]
        )
        dataRenamed = self.data.rename(columns=replacementDict)
        self.data = dataRenamed
        print("Finished harmonization of variables")

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

    def _checkFilterDict(self, filterDict):
        """
        Checking if all values of filter dictionaries are of type list.
        Currently only checking if list of list str not typechecked
        all(map(self.__checkStr, val). Conditionally triggers an assert.

        :return: None
        """
        assert all(
            isinstance(val, list) for val in returnDictBottomValues(baseDict=self.filterDict)
        ), ("All values in filter dictionaries have to be lists, but are not")

    def _filter(self, filterDict: dict = None):
        """
        Wrapper function to carry out filtering for the four filter logics of
        including, excluding, greaterThan and smallerThan.
        If a filterDict is defined with a different key, a warning is thrown.
        The function operates on self.data class-internally.

        :return: None
        """
        print(f'Starting filtering, applying {len(returnDictBottomKeys(filterDict))} filters.')
        simpleFilter = pd.DataFrame(index=self.data.index)
        sophFilter = pd.DataFrame(index=self.data.index)
        # Future releases: as discussed before we could indeed work here with a plug and pray approach.
        #  we would need to introduce a filter manager and a folder structure where to look for filters.
        #  this is very similar code than the one from ioproc. If we want to go down this route we should
        #  take inspiration from the code there. It was not easy to get it right in the first place. This
        #  might be easy to code but hard to implement correctly.

        # Simple filters checking single columns for specified values
        for iKey, iVal in filterDict.items():
            if iKey == 'include' and iVal:
                simpleFilter = simpleFilter.join(self.__setIncludeFilter(iVal, self.data.index))
            elif iKey == 'exclude' and iVal:
                simpleFilter = simpleFilter.join(self.__setExcludeFilter(iVal, self.data.index))
            elif iKey == 'greaterThan' and iVal:
                simpleFilter = simpleFilter.join(self.__setGreaterThanFilter(iVal, self.data.index))
            elif iKey == 'smallerThan' and iVal:
                simpleFilter = simpleFilter.join(self.__setSmallerThanFilter(iVal, self.data.index))
            elif iKey not in ['include', 'exclude', 'greaterThan', 'smallerThan']:
                warnings.warn(f'A filter dictionary was defined in the parseConfig with an unknown filtering key.'
                              f'Current filtering keys comprise include, exclude, smallerThan and greaterThan.'
                              f'Continuing with ignoring the dictionary {iKey}')

        # Application of simple value-based filters
        self.dataSimple = self.data[simpleFilter.all(axis='columns')]

        # More sophisticated filtering functions
        sophFilter = sophFilter.join(self.__filterInconsistentSpeedTrips())
        sophFilter = sophFilter.join(self.__filterOverlappingTrips())

        # Application of sophisticated filters
        self.data = self.dataSimple.loc[sophFilter.all(axis='columns'), :]
        self.__filterAnalysis(simpleFilter.join(sophFilter))

    def __setIncludeFilter(self, includeFilterDict: dict, dataIndex) -> pd.DataFrame:
        """
        Read-in function for include filter dict from parseConfig.yaml

        :param includeFilterDict: Dictionary of include filters defined
                                in parseConfig.yaml
        :param dataIndex: Index for the data frame
        :return: Returns a data frame with individuals using car
                as a mode of transport
        """
        incFilterCols = pd.DataFrame(index=dataIndex, columns=includeFilterDict.keys())
        for incCol, incElements in includeFilterDict.items():
            incFilterCols[incCol] = self.data[incCol].isin(incElements)
        return incFilterCols

    def __setExcludeFilter(self, excludeFilterDict: dict, dataIndex) -> pd.DataFrame:
        """
        Read-in function for exclude filter dict from parseConfig.yaml

        :param excludeFilterDict: Dictionary of exclude filters defined
                                  in parseConfig.yaml
        :param dataIndex: Index for the data frame
        :return: Returns a filtered data frame with exclude filters
        """
        exclFilterCols = pd.DataFrame(index=dataIndex, columns=excludeFilterDict.keys())
        for excCol, excElements in excludeFilterDict.items():
            exclFilterCols[excCol] = ~self.data[excCol].isin(excElements)
        return exclFilterCols

    def __setGreaterThanFilter(self, greaterThanFilterDict: dict, dataIndex):
        """
        Read-in function for greaterThan filter dict from parseConfig.yaml

        :param greaterThanFilterDict: Dictionary of greater than filters
                                      defined in parseConfig.yaml
        :param dataIndex: Index for the data frame
        :return:
        """
        greaterThanFilterCols = pd.DataFrame(index=dataIndex, columns=greaterThanFilterDict.keys())
        for greaterCol, greaterElements in greaterThanFilterDict.items():
            greaterThanFilterCols[greaterCol] = (self.data[greaterCol] >= greaterElements.pop())
            if len(greaterElements) > 0:
                warnings.warn(f"You specified more than one value as lower limit for filtering column {greaterCol}."
                              f"Only considering the last element given in the parseConfig.")
        return greaterThanFilterCols

    def __setSmallerThanFilter(self, smallerThanFilterDict: dict, dataIndex) -> pd.DataFrame:
        """
        Read-in function for smallerThan filter dict from parseConfig.yaml

        :param smallerThanFilterDict: Dictionary of smaller than filters
               defined in parseConfig.yaml
        :param dataIndex: Index for the data frame
        :return: Returns a data frame of trips covering
                 a distance of less than 1000 km
        """
        smallerThanFilterCols = pd.DataFrame(index=dataIndex, columns=smallerThanFilterDict.keys())
        for smallerCol, smallerElements in smallerThanFilterDict.items():
            smallerThanFilterCols[smallerCol] = (self.data[smallerCol] <= smallerElements.pop())
            if len(smallerElements) > 0:
                warnings.warn(f"You specified more than one value as upper limit for filtering column {smallerCol}."
                              f"Only considering the last element given in the parseConfig.")
        return smallerThanFilterCols

    def __filterInconsistentSpeedTrips(self):
        """
        Filter out trips with inconsistent average speed. These trips are mainly trips where survey participant
        responses suggest that participants were travelling for the entire time they took for the whole purpose
        (driving and parking) and not just for the real travel.

        :return: None
        """
        # FIXME: Add timestamp comparison instead of or additionally to variable "travel time",
        #  look at hhPersonID 80628472
        self.data['averageSpeed'] = self.data['tripDistance'] / (self.data['travelTime'] / 60)
        return self.data['averageSpeed'] > self.parseConfig['filterDicts']['speedThreshold']

    def __filterOverlappingTrips(self):
        """
        Filter out trips that have same hhPersonID as previous trip but overlap with previous trip.

        :return:
        """
        periods = 7
        lst = []
        for p in range(1, periods + 1):
            ser = self.__overlapPeriods(self.data, period=p)
            ser.name = ser.name + f' p={p}'
            lst.append(ser)
        ret = pd.concat(lst, axis=1).all(axis=1)
        ret.name = 'noOverlapPrevTrips'
        return ret

    # DEPRECATED WILL BE REMOVED ON NEXT RELEASE
    def __overlapPeriodsOld(self, data, period):
        dat = data.copy()
        dat['isSameHHAsPrevTrip'] = dat['hhPersonID'] == dat['hhPersonID'].shift(period)
        dat.loc[dat['tripEndNextDay'], 'tripEndHour'] = dat.loc[dat['tripEndNextDay'], 'tripEndHour'] + 24
        dat['tripStartsAfterPrevTrip'] = (
            (dat['tripStartHour'] > dat['tripEndHour'].shift(period)) | (
                (dat['tripStartHour'] == dat['tripEndHour'].shift(period)) & (
                    dat['tripStartMinute'] >= dat['tripEndMinute'].shift(period)))
        )
        dat['tripDoesNotOverlap'] = ~(dat['isSameHHAsPrevTrip'] & ~dat['tripStartsAfterPrevTrip'])
        return dat['tripDoesNotOverlap']

    def __overlapPeriods(self, data, period):
        """ New implementation of identifying trips that overlap with previous trips. This implementation is cleaner

        Args:
            data (pd.DataFrame): Trip data set including the two variables timestampStart and timestampEnd
            characterizing a trip
            period (int): Trip identifier within trip diary in survey day to compare every trip to

        Returns:
            pd.Series: A boolean Series being True for non-overlapping trips and False for overlapping trips. Has the
            same index as the MID data.
        """
        dat = data.copy()
        dat['isSameHHAsPrevTrip'] = dat['hhPersonID'] == dat['hhPersonID'].shift(period)
        dat['tripStartsAfterPrevTrip'] = dat['timestampStart'] > dat['timestampEnd'].shift(period)
        dat['tripDoesNotOverlap'] = ~(dat['isSameHHAsPrevTrip'] & ~dat['tripStartsAfterPrevTrip'])
        return dat['tripDoesNotOverlap']

    def __filterAnalysis(self, filterData: pd.DataFrame):
        """
        Function supplies some aggregate info of the data after filtering to the user Function does not change any
        class attributes

        :param filterData:
        :return: None
        """
        lenData = sum(filterData.all(axis='columns'))
        boolDict = {iCol: sum(filterData[iCol]) for iCol in filterData}
        print('The following values were taken into account after simple threshold filtering:')
        pprint.pprint(boolDict)
        # print(f'{filterData["averageSpeed"].sum()} trips have plausible average speeds')
        # print(f'{(~filterData["tripDoesNotOverlap"]).sum()} trips overlap and were thus filtered out')
        print(f"All filters combined yielded that a total of {lenData} trips are taken into account")
        print(f'This corresponds to {lenData / len(filterData)* 100} percent of the original data')

    def _addParkingRows(self):
        """Wrapper function generating park activity rows between the trip data from the original MID dataset. Some
        utility attributes are being added such as isFirstActivity, isLastActivity or the hhPersonID of the next and
        previous activity. Redundant time observations are dropped after timestamp creation for start and end time of
        each activity. Overnight trips (e.g. extending from 23:00 at survey day to 1:30 on the consecutive day) are
        split up into two trips. The first one extends to the end of the day (00:00) and the other one is appended
        to the activity list before the first parking activity (0:00-1:30). The trip distance is split between the two
        based on the time.
        """

        self.__copyRows()
        self.__addUtilAttributes()
        self.__addParkActAfterLastTrip()
        self.__adjustParkAttrs()
        self.__dropRedundantCols()
        self.__removeParkActsAfterOvernightTrips()
        self.__adjustParkTimeStamps()
        self.__setTripAttrsNAForParkActs()
        self.__addNextAndPrevIDs()
        self.__splitOvernightTrips()
        self.__addTimeDeltaCol()
        self.__uniqueIndex()

        print(f'Finished activity composition with {self.activities["tripID"].fillna(0).astype(bool).sum()} trips '
              f'and {self.activities["parkID"].fillna(0).astype(bool).sum()} parking activites')

    def __copyRows(self):
        # Adding skeleton duplicate rows for parking activities
        self.activities = pd.concat([self.data]*2).sort_index(ignore_index=True)
        self.activities['parkID'] = self.activities['tripID']
        self.activities.loc[range(0, len(self.activities), 2), 'tripID'] = pd.NA
        self.activities.loc[range(1, len(self.activities), 2), 'parkID'] = pd.NA

    def __addUtilAttributes(self):
        # Adding additional attribute columns for convenience
        self.activities['hhpid_prev'] = self.activities['hhPersonID'].shift(fill_value=0)
        self.activities['isFirstActivity'] = self.activities['hhpid_prev'] != self.activities['hhPersonID']

        self.activities['hhpid_next'] = self.activities['hhPersonID'].shift(-1, fill_value=0)
        self.activities['isLastActivity'] = self.activities['hhpid_next'] != self.activities['hhPersonID']

    def __addParkActAfterLastTrip(self):
        # Adding park activities after last trips
        newIndex = self.activities.index[self.activities.isLastActivity]
        dfAdd = self.activities.loc[newIndex, :]
        dfAdd['tripID'] = pd.NA
        self.activities.loc[newIndex, 'isLastActivity'] = False
        dfAdd['parkID'] = self.activities.loc[newIndex, 'tripID'] + 1
        self.activities = pd.concat([self.activities, dfAdd]).sort_index()

    def __adjustParkAttrs(self):
        # Setting trip attribute values to zero where tripID == NaN (i.e. for all parking activities)
        self.activities.loc[self.activities['tripID'].isna(),
                            ['tripDistance', 'travelTime', 'tripIsIntermodal']] = pd.NA
        self.activities['colFromIndex'] = self.activities.index
        self.activities = self.activities.sort_values(by=['colFromIndex', 'tripID'])

    def __dropRedundantCols(self):
        # Clean-up of temporary redundant columns
        self.activities.drop(columns=[
            'isMIVDriver', 'tripStartClock', 'tripEndClock', 'tripStartYear', 'tripStartMonth',
            'tripStartWeek', 'tripStartHour', 'tripStartMinute', 'tripEndHour', 'tripEndMinute', 'hhpid_prev',
            'hhpid_next', 'colFromIndex'], inplace=True)

    def __removeParkActsAfterOvernightTrips(self):
        # Checking for trips across day-limit and removing respective parking activities
        indexOvernight = (self.activities['isLastActivity'] & self.activities['tripEndNextDay'])
        indexOvernight = indexOvernight.loc[indexOvernight]
        self.activities.loc[indexOvernight.index, 'isLastActivity'] = True
        self.activities = self.activities.reset_index()

        # Get rid of park activities after overnight trips
        indexMultiDayActivity = (self.activities['isLastActivity'] &
                                 self.activities['tripEndNextDay'] &
                                 self.activities['parkID'])
        self.activities = self.activities.loc[~indexMultiDayActivity, :]

    # DEPRECATED WILL BE REMOVED IN NEXT RELEASE
    def __adjustParkTimeStamps_old(self):
        # Setting timestamps
        self.activities = self.activities.reset_index()
        parkingAct = self.activities['parkID'].fillna(0).astype(bool)
        parkingAct = parkingAct.loc[parkingAct]
        parkingActwoLast = parkingAct.iloc[:-1]
        parkingActwoFirst = parkingAct.iloc[1:]

        # Updating park end timestamps
        set_ts = self.activities.loc[parkingActwoLast.index + 1, 'timestampStart']
        set_ts.index = self.activities.loc[parkingActwoLast.index, 'timestampEnd'].index
        self.activities.loc[parkingActwoLast.index, 'timestampEnd'] = set_ts

        # Updating park start timestamps
        set_ts = self.activities.loc[parkingActwoFirst.index - 1, 'timestampEnd']
        set_ts.index = self.activities.loc[parkingActwoFirst.index, 'timestampStart'].index
        self.activities.loc[parkingActwoFirst.index, 'timestampStart'] = set_ts

        # Updating park start timestamps for first activity
        idxActs = self.activities['parkID'].fillna(0).astype(bool) & self.activities['isFirstActivity']
        self.activities.loc[idxActs, 'timestampStart'] = self.activities.loc[
            idxActs, 'timestampEnd'].apply(lambda x: x.replace(hour=0, minute=0))

        # Updating park end timestamps for last activity
        idxActs = self.activities['parkID'].fillna(0).astype(bool) & self.activities['isLastActivity']
        self.activities.loc[idxActs, 'timestampEnd'] = self.activities.loc[
            idxActs, 'timestampStart'].apply(lambda x: x.replace(hour=0, minute=0) + pd.Timedelta(1, 'd'))

    def __adjustParkTimeStamps(self):
        # Setting timestamps
        self.activities = self.activities.reset_index()
        parkingAct = self.activities['parkID'].fillna(0).astype(bool)
        parkingAct = parkingAct.loc[parkingAct]
        parkingActwoLast = parkingAct.iloc[:-1]
        parkingActwoFirst = parkingAct.iloc[1:]

        # Updating park end timestamps
        set_ts = self.activities.loc[parkingActwoLast.index + 1, 'timestampStart']
        set_ts.index = self.activities.loc[parkingActwoLast.index, 'timestampEnd'].index
        self.activities.loc[parkingActwoLast.index, 'timestampEnd'] = set_ts

        # Updating park start timestamps
        set_ts = self.activities.loc[parkingActwoFirst.index - 1, 'timestampEnd']
        set_ts.index = self.activities.loc[parkingActwoFirst.index, 'timestampStart'].index
        self.activities.loc[parkingActwoFirst.index, 'timestampStart'] = set_ts

        # Updating park start timestamps for first activity
        idxActs = self.activities['parkID'].fillna(0).astype(bool) & self.activities['isFirstActivity']
        self.activities.loc[idxActs, 'timestampStart'] = replace_vec(self.activities.loc[idxActs, 'timestampEnd'],
                                                                     hour=0, minute=0)

        # Updating park end timestamps for last activity
        idxActs = self.activities['parkID'].fillna(0).astype(bool) & self.activities['isLastActivity']
        self.activities.loc[idxActs, 'timestampEnd'] = replace_vec(self.activities.loc[idxActs, 'timestampEnd'],
                                                                   hour=0, minute=0)
        self.activities.loc[idxActs, 'timestampEnd'] = self.activities.loc[
            idxActs, 'timestampEnd'] + pd.Timedelta(1, 'd')

    def __setTripAttrsNAForParkActs(self):
        # Set tripEndNextDay to False for all park activities
        self.activities.loc[self.activities['tripID'].isna(), 'tripEndNextDay'] = pd.NA

    def __addNextAndPrevIDs(self):
        self.activities.loc[~self.activities['tripID'].isna(), 'actID'] = self.activities['tripID']
        self.activities.loc[~self.activities['parkID'].isna(), 'actID'] = self.activities['parkID']
        self.activities.loc[~self.activities['isLastActivity'], 'nextActID'] = self.activities.loc[
            :, 'actID'].shift(-1)
        self.activities.loc[~self.activities['isFirstActivity'], 'prevActID'] = self.activities.loc[
            :, 'actID'].shift(1)

    def __splitOvernightTrips(self):
        """ Wrapper function for treating edge case trips ending not in the 24 hours of the survey day but stretch
        to the next day. Those overnight (ON) are split up into an evening trip at the regular survey day and a
        morning trip at the next day. Trip distances are split according to the time the person spent on that trip.
        E.g. if a trip lasts from 23:00 to 2:00 the next day and 100 km, the split-up evening trip will last from
        23:00 to 00:00 of the survey day and 33 km and the morning trip from 00:00 to 2:00 and 66 km. In a next step,
        the morning trip is appended to the survey day in the first hours. Here, different edge cases occur.
        Edge case 1 (N=5 in MiD17): For trips that overlap with night (early morning) trips at the survey day, e.g. from
        0:30 to 1:00 for the above mentioned example, the morning part of the split overnight trip is completely
        disregarded.
        Edge case 2 (N=3 in MiD17): When overnight mornging split-trips end exactly at the time where the first trip of
        the survey day starts (2:00 in the example), both trips are consolidated to one trip with all attributes of the
        survey trip.

        """
        # Split overnight trips and add next day distance in the morning (tripID=0)
        isONTrip, overnightTripsAdd = self.__getOvernightActs()
        overnightTripsAddTS = self.__adjustONTimestamps(trips=overnightTripsAdd)
        self.__setAllLastActEndTSToZero()
        morningTrips = self.__setONTripIDZero(trips=overnightTripsAddTS)
        morningTrips = self.__adjustMorningTripDistance(overnightTrips=overnightTripsAdd, morningTrips=morningTrips)
        self.__adjustEveningTripDistance(morningTrips=morningTrips, isONTrip=isONTrip)
        self.__setFirstLastActs(morningTrips=morningTrips)
        isPrevFirstActs = self.__getPrevFirstAct(morningTrips=morningTrips)  # Activities that were previously firstActs
        morningTrips_noOverlap, isPrevFirstActs = self.__neglectOverlapMorningTrips(morningTrips=morningTrips,
                                                                                    isPrevFirstActs=isPrevFirstActs)
        morningTrips_add = self.__setNextParkingTSStart(morningTrips=morningTrips_noOverlap, isONTrip=isONTrip,
                                                        isPrevFirstActs=isPrevFirstActs)
        self.__addMorningTrips(morningTrips=morningTrips_add)
        self.__removeFirstParkingAct()
        self.__mergeAdjacentTrips()
        self.__dropONCol()
        self.__sortActivities()

    def __getOvernightActs(self):
        indexOvernightActs = (self.activities['isLastActivity'] & self.activities['tripEndNextDay'] &
                              ~((self.activities['timestampEnd'].dt.hour == 0) &  # assure that the overnight trip does
                                (self.activities['timestampEnd'].dt.minute == 0)))  # not exactly end at 00:00
        return indexOvernightActs, self.activities.loc[indexOvernightActs, :]

    def __adjustONTimestamps(self, trips: pd.DataFrame):
        tripsRes = trips.copy()
        tripsRes['timestampEnd'] = tripsRes.loc[:, 'timestampEnd'] - pd.Timedelta(1, 'd')
        tripsRes['timestampStart'] = replace_vec(tripsRes.loc[:, 'timestampEnd'], hour=0, minute=0)
        return tripsRes

    def __setAllLastActEndTSToZero(self):
        # Set timestamp end of evening part of overnight trip split to 00:00
        self.activities.loc[self.activities['isLastActivity'], 'timestampEnd'] = replace_vec(self.activities.loc[
            self.activities['isLastActivity'], 'timestampEnd'], hour=0, minute=0)

    def __setONTripIDZero(self, trips):
        trips['tripID'] = 0
        trips['actID'] = 0
        trips['prevActID'] = pd.NA

        # Update next activity ID
        hhPersonID = trips['hhPersonID']
        actIdx = (self.activities['hhPersonID'].isin(hhPersonID) & self.activities['isFirstActivity'])
        trips['nextActID'] = self.activities.loc[actIdx, 'actID']

        # Update previous activity ID of previously first activity
        self.activities.loc[actIdx, 'prevActID'] = 0
        return trips

    def __adjustMorningTripDistance(self, overnightTrips: pd.DataFrame, morningTrips: pd.DataFrame):
        # Splitting the total distance to morning and evening trip time-share dependent
        morningTrips['timedelta_total'] = overnightTrips[
            'timestampEnd'] - overnightTrips['timestampStart']
        morningTrips['timedelta_morning'] = morningTrips[
            'timestampEnd'] - morningTrips['timestampStart']
        morningTrips['timeShare_morning'] = morningTrips[
            'timedelta_morning'] / morningTrips['timedelta_total']
        morningTrips['timeShare_evening'] = (morningTrips['timedelta_total']
                                             - morningTrips['timedelta_morning']
                                             ) / morningTrips['timedelta_total']
        morningTrips['totalTripDistance'] = morningTrips['tripDistance']
        morningTrips['tripDistance'] = morningTrips[
            'timeShare_morning'] * morningTrips['totalTripDistance']
        return morningTrips

    def __adjustEveningTripDistance(self, morningTrips: pd.DataFrame, isONTrip: pd.Series):
        self.activities.loc[isONTrip, 'tripDistance'] = morningTrips[
            'timeShare_evening'] * morningTrips['totalTripDistance']

    def __setFirstLastActs(self, morningTrips: pd.DataFrame):
        # Setting first and last activities
        morningTrips['isFirstActivity'] = True
        morningTrips['isLastActivity'] = False

    def __getPrevFirstAct(self, morningTrips: pd.DataFrame):
        return (self.activities['hhPersonID'].isin(morningTrips['hhPersonID']) &
                self.activities['isFirstActivity'])

    def __neglectOverlapMorningTrips(self, morningTrips: pd.DataFrame, isPrevFirstActs: pd.DataFrame):
        # Option 1 of treating overlaps: After concatenation in the end
        firstTripsEnd = self.activities.loc[isPrevFirstActs, 'timestampEnd'].copy()
        firstTripsEnd.index = morningTrips.index  # Adjust index for comparison

        # Filter out morning parts of overnight trip split for persons that already have morning trips in that period
        neglectOvernight = firstTripsEnd < morningTrips['timestampEnd']
        morningTrips_noOverlap = morningTrips.loc[~neglectOvernight, :]

        # Filter out neglected activities from prevFirstActs accordingly
        neglectOvernightIdx = neglectOvernight
        neglectOvernightIdx.index = isPrevFirstActs[isPrevFirstActs].index  # Align index for filtering
        neglectOvernightIdx = neglectOvernightIdx[neglectOvernightIdx]
        isPrevFirstActs[neglectOvernightIdx.index] = False

        return morningTrips_noOverlap, isPrevFirstActs

    def __setNextParkingTSStart(self, morningTrips: pd.DataFrame, isONTrip: pd.Series, isPrevFirstActs: pd.DataFrame):
        # Setting start timestamp of previously first activity (parking) to end timestamp of morning split of ON trip
        ts_new = morningTrips.loc[isONTrip, 'timestampEnd']
        ts_new.index = self.activities.loc[isPrevFirstActs, 'timestampStart'].index
        self.activities.loc[isPrevFirstActs, 'timestampStart'] = ts_new
        self.activities.loc[isPrevFirstActs, 'isFirstActivity'] = False

        # Set nextActID column of ON trips to consecutive activity
        return self.__updateNextActID(prevFirstActs=self.activities.loc[isPrevFirstActs, :], morningTrips=morningTrips)

    def __updateNextActID(self, prevFirstActs: pd.DataFrame, morningTrips: pd.DataFrame):
        nextActs = prevFirstActs.loc[prevFirstActs['prevActID'] == 0, 'actID']
        nextActs.index = morningTrips.index

        # FIXME: @Ben: Why does this throw a settingWithCopyWarning and what would be the nicest way to circumvent?
        ret = morningTrips.copy()
        ret.loc[:, 'nextActID'] = nextActs
        return ret

    def __addMorningTrips(self, morningTrips: pd.DataFrame):
        # Appending overnight morning trips
        self.activities = pd.concat([self.activities, morningTrips], )

    def __removeFirstParkingAct(self):
        # Removing first parking activities for persons where first activity is a trip (starting at 00:00)
        firstParkActs = self.activities.loc[self.activities['parkID'] == 1, :]
        firstTripActs = self.activities.loc[self.activities['tripID'] == 1, :]
        firstTripActs.index = firstParkActs.index  # Aligning trip indices
        idxParkTS = firstParkActs['timestampStart'] == firstTripActs['timestampStart']
        self.activities = self.activities.drop(idxParkTS[idxParkTS].index)

        # After removing first parking, set first trip to first activity
        self.activities.loc[(self.activities['hhPersonID'].isin(firstParkActs.loc[idxParkTS, 'hhPersonID'])) &
                            (self.activities['tripID'] == 1),
                            'isFirstActivity'] = True

    def __mergeAdjacentTrips(self):
        """ Consolidate overnight morning trips and first trips for the edge case where morning trips of next day
        end exactly at the beginning of the first trip of the survey day. In this case, the morning split of the
        overnight trip is neglected and the beginning of the first trip is set to 00:00. In the MiD17 data set, there
        were 3 occurences of this case all with end times of the overnight trip between 00:00 and 01:00.

        """

        hhpid = self.__getHHPIDsToNeglect()
        self.__neglectZeroTripIDFromActivities(hhpid_neglect=hhpid)
        self.__updateConsolidatedAct(hhpid_neglect=hhpid)

    def __getHHPIDsToNeglect(self):
        """
        Identifies the household person IDs that should be neglected.
        """
        hhPersonIDsOvernight = self.activities.loc[self.activities['tripID'] == 0, 'hhPersonID']
        acts = self.activities.loc[self.activities['hhPersonID'].isin(hhPersonIDsOvernight), :]
        actsOvernight = acts.loc[acts['tripID'] == 0, :]

        # Next trip after morning part of overnight split
        actsNextTrip = acts.loc[acts['prevActID'] == 0, :]
        return actsOvernight.loc[~actsOvernight['hhPersonID'].isin(actsNextTrip['hhPersonID']), 'hhPersonID']

    def __neglectZeroTripIDFromActivities(self, hhpid_neglect: pd.Series):
        """
        This method filters out the activities with the given hhpid and tripID 0.
        """
        boolNeglect = (self.activities['hhPersonID'].isin(hhpid_neglect)) & (self.activities['tripID'] == 0)
        self.activities = self.activities.loc[~boolNeglect, :]

    def __updateConsolidatedAct(self, hhpid_neglect: pd.Series):
        """
        This method sets the start timestamp of the firstActivity of all hhpids given as argument to 00:00. Additionally
        the prevActID is set to pd.NA
        """
        idxConsolidatedTrips = (self.activities['hhPersonID'].isin(hhpid_neglect)) & (self.activities[
            'isFirstActivity'])
        self.activities.loc[idxConsolidatedTrips, 'timestampStart'] = replace_vec(self.activities.loc[
            idxConsolidatedTrips, 'timestampStart'], hour=0, minute=0)
        self.activities.loc[idxConsolidatedTrips, 'prevActID'] = pd.NA

    def __dropONCol(self):
        self.activities = self.activities.drop(columns=['tripEndNextDay'])

    def __sortActivities(self):
        self.activities = self.activities.sort_values(by=['hhPersonID', 'timestampStart'])

    def __addTimeDeltaCol(self):
        # Add timedelta column
        self.activities['timedelta'] = self.activities['timestampEnd'] - self.activities['timestampStart']

    def __uniqueIndex(self):
        self.activities.drop(columns=['level_0'], inplace=True)
        self.activities.reset_index(inplace=True)  # Due to copying and appending rows, the index has to be reset

    def process(self):
        """
        Wrapper function for harmonising and filtering the dataset.
        """
        raise NotImplementedError('Implement process method for DataParser.')

    def writeOutput(self):
        writeOut(dataset=self.activities, outputFolder='diaryOutput', fileKey='outputDataParser', manualLabel='',
                 datasetID=self.datasetID, localPathConfig=self.localPathConfig, globalConfig=self.globalConfig)


class IntermediateParsing(DataParser):
    def __init__(self, configDict: dict, datasetID: str, loadEncrypted=False, debug=False):
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
        super().__init__(configDict, datasetID=datasetID, loadEncrypted=loadEncrypted, debug=debug)
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
            self.datasetID
        )
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
        Stores the subset of data in self.data

        :return: None
        """
        self.data = self.rawData.loc[:, self.columns]

    def _filterConsistentHours(self):
        """
        Filtering out records where starting hour is after end hour but trip
        takes place on the same day.
        These observations are data errors.

        :return: No returns, operates only on the class instance
        """
        if self.datasetID in ["MiD17", "MiD08"]:
            dat = self.data
            self.data = dat.loc[(dat["tripStartClock"] <= dat["tripEndClock"]) | (dat["tripEndNextDay"] == 1), :]
            filters = (
                (self.data.loc[:, "tripStartHour"] == self.data.loc[:, "tripEndHour"])
                & (self.data.loc[:, "tripStartMinute"] == self.data.loc[:, "tripEndMinute"])
                & (self.data.loc[:, "tripEndNextDay"])
            )
            self.data = self.data.loc[~filters, :]

    def _addStrColumnFromVariable(self, colName: str, varName: str):
        """
        Replaces each occurence of a MiD/KiD variable e.g. 1,2,...,7 for
        weekdays with an explicitly mapped string e.g. 'MON', 'TUE',...,'SUN'.

        :param colName: Name of the column in self.data where the explicit
                        string info is stored
        :param varName: Name of the VencoPy internal variable given in
                        config/parseConfig['dataVariables']
        :return: None
        """
        self.data.loc[:, colName] = self.data.loc[:, varName].replace(
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
        self.__composeTimestamp(data=self.data)  # Starting timestamp
        self.__composeTimestamp(
            data=self.data,  # Ending timestamps
            colHour="tripEndHour",
            colMin="tripEndMinute",
            colName="timestampEnd",
        )

    def _updateEndTimestamp(self):
        """
        Updates the end timestamp for overnight trips adding 1 day

        :return: None, only acts on the class variable
        """
        endsFollowingDay = self.data["tripEndNextDay"] == 1
        self.data.loc[endsFollowingDay, "timestampEnd"] = self.data.loc[
            endsFollowingDay, "timestampEnd"
        ] + pd.offsets.Day(1)

    def _harmonizeVariablesGenericIdNames(self):
        """

        """
        self.data["genericID"] = self.data[
            str(self.parseConfig["IDVariablesNames"][self.datasetID])
        ]
        print("Finished harmonization of ID variables")


class ParseMiD(IntermediateParsing):
    def __init__(self, configDict: dict, datasetID: str, loadEncrypted=False, debug=False):
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
        super().__init__(configDict=configDict, datasetID=datasetID, loadEncrypted=loadEncrypted, debug=debug)

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
        dataRenamed = self.data.rename(columns=replacementDict)
        if self.datasetID == "MiD08":
            dataRenamed["hhPersonID"] = (
                dataRenamed["hhID"].astype("string")
                + dataRenamed["personID"].astype("string")
            ).astype("int")
        self.data = dataRenamed
        print("Finished harmonization of variables")

    def __convertTypes(self):
        """
        Convert raw column types to predefined python types as specified in
        parseConfig['inputDTypes'][datasetID]. This is mainly done for
        performance reasons. But also in order to avoid index values that are
        of type int to be cast to float. The function operates only on
        self.data and writes back changes to self.data

        :return: None
        """
        # Filter for dataset specific columns
        conversionDict = self.parseConfig["inputDTypes"][self.datasetID]
        keys = {iCol for iCol in conversionDict.keys() if iCol in self.data.columns}
        self.varDataTypeDict = {key: conversionDict[key] for key in conversionDict.keys() & keys}
        self.data = self.data.astype(self.varDataTypeDict)

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
                colName="purposeStr", varName="tripPurpose"
            )

    @profile(immediate=True)
    def process(self):
        """
        Wrapper function for harmonising and filtering the dataset.
        """
        self._selectColumns()
        self.__harmonizeVariables()
        self.__convertTypes()
        self.__addStrColumns()
        self._composeStartAndEndTimestamps()
        self._updateEndTimestamp()
        self._checkFilterDict(self.filterDict)
        self._filter(self.filterDict)
        self._filterConsistentHours()
        self._harmonizeVariablesGenericIdNames()
        self._addParkingRows()
        print("Parsing MiD dataset completed")


class ParseVF(IntermediateParsing):
    def __init__(self, configDict: dict, datasetID: str, loadEncrypted=False, debug=False):
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
        super().__init__(configDict=configDict, datasetID=datasetID, loadEncrypted=loadEncrypted)

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
        rawDataTrips = pd.read_stata(rawDataPathTrips, convert_categoricals=False, convert_dates=False,
                                     preserve_dtypes=False)
        rawDataVehicles = pd.read_csv(rawDataPathVehicles, encoding="ISO-8859-1")
        rawDataVehicles = rawDataVehicles.drop(columns=['Unnamed: 0'])
        rawDataVehicles = rawDataVehicles.drop_duplicates(subset=['HP_ID'], keep='first')
        rawDataVehicles.set_index("HP_ID", inplace=True)
        rawData = rawDataTrips.join(rawDataVehicles, on="HP_ID", rsuffix="VF")
        self.rawData = rawData
        print(f"Finished loading {len(self.rawData)} rows of raw data of type .dta")

    def __harmonizeVariables(self):
        """
        Harmonizes the input data variables to match internal VencoPy names given as specified in the mapping in
        parseConfig['dataVariables']. Mappings for MiD08 and MiD17 are given. Since the MiD08 does not provide a
        combined household and person unique identifier, it is synthesized of the both IDs.

        :return: None
        """
        replacementDict = self.createReplacementDict(self.datasetID, self.parseConfig["dataVariables"])
        dataRenamed = self.data.rename(columns=replacementDict)
        if self.datasetID == "MiD08":
            dataRenamed["hhPersonID"] = (
                dataRenamed["hhID"].astype("string") + dataRenamed["personID"].astype("string")
            ).astype("int")
        self.data = dataRenamed
        print("Finished harmonization of variables")

    def __convertTypes(self):
        """
        Convert raw column types to predefined python types as specified in
        parseConfig['inputDTypes'][datasetID]. This is mainly done for
        performance reasons. But also in order to avoid index values that are
        of type int to be cast to float. The function operates only on
        self.data and writes back changes to self.data

        :return: None
        """
        # Filter for dataset specific columns
        conversionDict = self.parseConfig["inputDTypes"][self.datasetID]
        keys = {iCol for iCol in conversionDict.keys() if iCol in self.data.columns}
        self.varDataTypeDict = {key: conversionDict[key] for key in conversionDict.keys() & keys}
        self.data = self.data.astype(self.varDataTypeDict)

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
                colName="purposeStr", varName="tripPurpose"
            )

    # DEPRECATED, WILL BE REMOVED IN NEXT RELEASE
    def copyOverTripNextDay(self):
        pass

    def process(self):
        """
        Wrapper function for harmonising and filtering the dataset.
        """
        self._selectColumns()
        self.__harmonizeVariables()
        self.__convertTypes()
        self.__addStrColumns()
        self._composeStartAndEndTimestamps()
        self._updateEndTimestamp()
        self._checkFilterDict(self.filterDict)
        self._filter(self.filterDict)
        self._filterConsistentHours()
        self._harmonizeVariablesGenericIdNames()
        self._addParkingRows()
        print("Parsing VF dataset completed")


class ParseKiD(IntermediateParsing):
    def __init__(self, configDict: dict, datasetID: str, loadEncrypted=False, debug=False):
        """
        Inherited data class to differentiate between abstract interfaces such
        as vencopy internal variable namings and data set specific functions
        such as filters etc.
        """
        super().__init__(
            configDict=configDict,
            datasetID=datasetID,
            loadEncrypted=loadEncrypted,
        )

    def _loadData(self):
        rawDataPathTrips = (
            Path(self.localPathConfig["pathAbsolute"][self.datasetID])
            / self.globalConfig["files"][self.datasetID][
                "tripsDataRaw"
            ]
        )
        rawDataPathVehicles = (
            Path(self.localPathConfig["pathAbsolute"][self.datasetID])
            / self.globalConfig["files"][self.datasetID][
                "vehiclesDataRaw"
            ]
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
            f"Finished loading {len(self.rawData)} "
            f"rows of raw data of type .dta"
        )

    def __convertTypes(self):
        """
        Convert raw column types to predefined python types as specified
        in parseConfig['inputDTypes'][datasetID].
        This is mainly done for performance reasons. But also in order
        to avoid index values that are of type int
        to be cast to float. The function operates only on self.data
        and writes back changes to self.data

        :return: None
        """
        # TODO: move convertTypes to INtermediate class and create a new
        # class for KiD to change commas to dots
        # Filter for dataset specific columns
        conversionDict = self.parseConfig["inputDTypes"][self.datasetID]
        keys = {
            iCol for iCol in conversionDict.keys() if iCol in self.data.columns
        }
        self.varDataTypeDict = {
            key: conversionDict[key] for key in conversionDict.keys() & keys
        }
        # German df has commas instead of dots in floats
        for i, x in enumerate(list(self.data.tripDistance)):
            self.data.at[i, "tripDistance"] = x.replace(",", ".")
        for i, x in enumerate(list(self.data.tripWeight)):
            self.data.at[i, "tripWeight"] = x.replace(",", ".")
        self.data = self.data.astype(self.varDataTypeDict)

    def __addStrColumns(self, weekday=True, purpose=True):
        """
        Adds string columns for either weekday or purpose.

        :param weekday: Boolean identifier if weekday string info should be
                        added in a separate column
        :param purpose: Boolean identifier if purpose string info should be
                        added in a separate column
        :return: None
        """
        # from tripStartDate retrieve tripStartWeekday, tripStartWeek,
        # tripStartYear, tripStartMonth, tripStartDay
        # from tripStartClock retrieve tripStartHour, tripStartMinute
        # from tripEndClock retrieve tripEndHour, tripEndMinute
        self.data["tripStartDate"] = pd.to_datetime(self.data["tripStartDate"], format="%d.%m.%Y")
        self.data["tripStartYear"] = self.data["tripStartDate"].dt.year
        self.data["tripStartMonth"] = self.data["tripStartDate"].dt.month
        self.data["tripStartDay"] = self.data["tripStartDate"].dt.day
        self.data["tripStartWeekday"] = self.data["tripStartDate"].dt.weekday
        self.data["tripStartWeek"] = (self.data["tripStartDate"].dt.isocalendar().week)
        self.data["tripStartHour"] = pd.to_datetime(self.data["tripStartClock"], format="%H:%M").dt.hour
        self.data["tripStartMinute"] = pd.to_datetime(self.data["tripStartClock"], format="%H:%M").dt.minute
        self.data["tripEndHour"] = pd.to_datetime(self.data["tripEndClock"], format="%H:%M").dt.hour
        self.data["tripEndMinute"] = pd.to_datetime(self.data["tripEndClock"], format="%H:%M").dt.minute
        if weekday:
            self._addStrColumnFromVariable(colName="weekdayStr", varName="tripStartWeekday")
        if purpose:
            self._addStrColumnFromVariable(colName="purposeStr", varName="tripPurpose")

    def __updateEndTimestamp(self):
        """ Separate implementation for the KID data set than for the other two data sets. Overwrites parent method.

        :return: None
        """
        self.data["tripEndNextDay"] = np.where(self.data["timestampEnd"].dt.day > self.data["timestampStart"].dt.day,
                                               1, 0)
        endsFollowingDay = self.data["tripEndNextDay"] == 1
        self.data.loc[endsFollowingDay, "timestampEnd"] = self.data.loc[endsFollowingDay,
                                                                        "timestampEnd"] + pd.offsets.Day(1)

    def __excludeHours(self):
        """
        Removes trips where both start and end trip time are missing. KID-specific function.
        """
        self.data = self.data.loc[(self.data["tripStartClock"] != "-1:-1") & (self.data["tripEndClock"] != "-1:-1"), :]

    def process(self):
        """
        Wrapper function for harmonising and filtering the dataset.
        """
        self._selectColumns()
        self._harmonizeVariables()
        self._convertTypes()
        self.__addStrColumns()
        self._composeStartAndEndTimestamps()
        self.__updateEndTimestamp()
        self._checkFilterDict(self.filterDict)
        self._filter(self.filterDict)
        self.__excludeHours()
        self._filterConsistentHours()
        self._harmonizeVariablesGenericIdNames()
        self._addParkingRows()
        print("Parsing KiD dataset completed")


if __name__ == '__main__':
    basePath = Path(__file__).parent.parent
    configNames = ("globalConfig", "localPathConfig", "parseConfig", "diaryConfig",
                   "gridConfig", "flexConfig", "aggregatorConfig", "evaluatorConfig")
    configDict = loadConfigDict(configNames, basePath)

    datasetID = "MiD17"  # options are MiD08, MiD17, KiD
    if datasetID == "MiD17":
        vpData = ParseMiD(configDict=configDict, datasetID=datasetID, debug=False)
    elif datasetID == "KiD":
        vpData = ParseKiD(configDict=configDict, datasetID=datasetID, debug=False)
    elif datasetID == "VF":
        vpData = ParseVF(configDict=configDict, datasetID=datasetID, debug=False)
    vpData.process()
    # vpData.writeOutput()
