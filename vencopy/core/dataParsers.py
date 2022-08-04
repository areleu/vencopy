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
from vencopy.utils.globalFunctions import loadConfigDict, writeOut


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
        self.datasetID = self.checkDatasetID(datasetID, self.parseConfig)
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
            self.loadEncryptedData(pathToZip=filepath, pathInZip=fpInZip)
        else:
            print(f"Starting to retrieve local data file from {self.rawDataPath}")
            self.loadData()
        self.rawData = self.rawData.loc[0:20, :] if debug else self.rawData.copy()

    def loadData(self):
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

    def loadEncryptedData(self, pathToZip, pathInZip):
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

    def checkDatasetID(self, datasetID: str, parseConfig: dict) -> str:
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

    def harmonizeVariables(self):
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

    def createReplacementDict(self, datasetID: str, dictRaw: dict) -> dict:
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

    def returnDictBottomValues(self, baseDict: dict, lst: list = None) -> list:
        """
        Returns a list of all dictionary values of the last dictionary level
        (the bottom) of baseDict. The parameter
        lst is used as an interface between recursion levels.

        :param baseDict: Dictionary of variables
        :param lst: empty list, is used as interface to next recursion
        :return: Returns a list with all the bottom dictionary values
        """
        if lst is None:
            lst = []
        # iKey not used in returndictBottomValues but used in checkFilterDict to have lists
        for iKey, iVal in baseDict.items():
            if isinstance(iVal, dict):
                lst = self.returnDictBottomValues(iVal, lst)
            elif iVal is not None:
                lst.append(iVal)
        return lst

    def checkFilterDict(self, filterDict):
        """
        Checking if all values of filter dictionaries are of type list.
        Currently only checking if list of list str not typechecked
        all(map(self.__checkStr, val). Conditionally triggers an assert.

        :return: None
        """
        assert all(
            isinstance(val, list) for val in self.returnDictBottomValues(self.filterDict)
        ), ("All values in filter dictionaries have to be lists, but are not")

    def returnDictBottomKeys(self, baseDict: dict, lst: list = None) -> list:
        """
        Returns the lowest level keys of baseDict and returns all of them
        as a list. The parameter lst is used as
        interface between recursion levels.

        :param baseDict: Dictionary of variables
        :param lst: empty list, used as interface between recursion levels
        :return: Returns a list with all the bottom level dictionary keys
        """
        if lst is None:
            lst = []
        for iKey, iVal in baseDict.items():
            if isinstance(iVal, dict):
                lst = self.returnDictBottomKeys(iVal, lst)
            elif iVal is not None:
                lst.append(iKey)
        return lst

    def filter(self, filterDict: dict = None):
        """
        Wrapper function to carry out filtering for the four filter logics of
        including, excluding, greaterThan and smallerThan.
        If a filterDict is defined with a different key, a warning is thrown.
        The function operates on self.data class-internally.

        :return: None
        """
        print(f'Starting filtering, applying {len(self.returnDictBottomKeys(filterDict))} filters.')
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
                simpleFilter = simpleFilter.join(self.setIncludeFilter(iVal, self.data.index))
            elif iKey == 'exclude' and iVal:
                simpleFilter = simpleFilter.join(self.setExcludeFilter(iVal, self.data.index))
            elif iKey == 'greaterThan' and iVal:
                simpleFilter = simpleFilter.join(self.setGreaterThanFilter(iVal, self.data.index))
            elif iKey == 'smallerThan' and iVal:
                simpleFilter = simpleFilter.join(self.setSmallerThanFilter(iVal, self.data.index))
            elif iKey not in ['include', 'exclude', 'greaterThan', 'smallerThan']:
                warnings.warn(f'A filter dictionary was defined in the parseConfig with an unknown filtering key.'
                              f'Current filtering keys comprise include, exclude, smallerThan and greaterThan.'
                              f'Continuing with ignoring the dictionary {iKey}')

        # Application of simple value-based filters
        self.dataSimple = self.data[simpleFilter.all(axis='columns')]

        # More sophisticated filtering functions
        sophFilter = sophFilter.join(self._filterInconsistentSpeedTrips())
        sophFilter = sophFilter.join(self.__filterOverlappingTrips())

        # Application of sophisticated filters
        self.data = self.dataSimple.loc[sophFilter.all(axis='columns'), :]
        self.filterAnalysis(simpleFilter.join(sophFilter))

    def setIncludeFilter(self, includeFilterDict: dict, dataIndex) -> pd.DataFrame:
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

    def setExcludeFilter(self, excludeFilterDict: dict, dataIndex) -> pd.DataFrame:
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

    def setGreaterThanFilter(self, greaterThanFilterDict: dict, dataIndex):
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

    def setSmallerThanFilter(self, smallerThanFilterDict: dict, dataIndex) -> pd.DataFrame:
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

    def _filterInconsistentSpeedTrips(self):
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

    def __overlapPeriods(self, data, period):
        dat = data.copy()
        dat['isSameHHAsPrevTrip'] = dat['hhPersonID'] == dat['hhPersonID'].shift(period)
        dat.loc[dat['tripEndNextDay'], 'tripEndHour'] = dat.loc[dat['tripEndNextDay'], 'tripEndHour'] + 24
        dat['tripStartsAfterPrevTrip'] = ((dat['tripStartHour'] > dat['tripEndHour'].shift(period)) | (
            (dat['tripStartHour'] == dat['tripEndHour'].shift(period)) & (dat['tripStartMinute'] >= dat[
                'tripEndMinute'].shift(period))))
        dat['tripDoesNotOverlap'] = ~(dat['isSameHHAsPrevTrip'] & ~dat['tripStartsAfterPrevTrip'])
        return dat['tripDoesNotOverlap']

    def filterAnalysis(self, filterData: pd.DataFrame):
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

    def addParkingRows(self):
        self.activities = pd.concat([self.data]*2).sort_index(ignore_index=True)
        self.activities['parkID'] = self.activities['tripID']
        self.activities.loc[range(0, len(self.activities), 2), 'tripID'] = pd.NA
        self.activities.loc[range(1, len(self.activities), 2), 'parkID'] = pd.NA

        self.activities['hhpid_prev'] = self.activities['hhPersonID'].shift(fill_value=0)
        self.activities['isFirstActivity'] = self.activities['hhpid_prev'] != self.activities['hhPersonID']

        self.activities['hhpid_next'] = self.activities['hhPersonID'].shift(-1, fill_value=0)
        self.activities['isLastActivity'] = self.activities['hhpid_next'] != self.activities['hhPersonID']

        newIndex = self.activities.index[self.activities.isLastActivity]
        dfAdd = self.activities.loc[newIndex, :]
        dfAdd['tripID'] = pd.NA
        self.activities.loc[newIndex, 'isLastActivity'] = False
        dfAdd['parkID'] = self.activities.loc[newIndex, 'tripID'] + 1
        self.activities = pd.concat([self.activities, dfAdd]).sort_index()
        # self.activities.reset_index(inplace=True)

        # Set tripDistance values to zero where tripID == NaN (i.e. for parking activities)
        self.activities.loc[self.activities['tripID'].isna(), 'tripDistance'] = pd.NA
        
        self.activities['colFromIndex'] = self.activities.index
        self.activities = self.activities.sort_values(by=['colFromIndex', 'tripID'])

        # Clean-up of temporary redundant columns
        self.activities.drop(columns=[
            'isMIVDriver', 'tripStartClock', 'tripEndClock', 'tripStartYear', 'tripStartMonth',
            'tripStartWeek', 'tripStartHour', 'tripStartMinute', 'tripEndHour', 'tripEndMinute', 'hhpid_prev',
            'hhpid_next', 'colFromIndex'], inplace=True)

        # Checking for trips across day-limit and removing respective parking activities
        indexMulti = (self.activities['isLastActivity'] & self.activities['tripEndNextDay'])
        indexMulti = indexMulti.loc[indexMulti]
        self.activities.loc[indexMulti.index, 'isLastActivity'] = True
        self.activities = self.activities.reset_index()
        indexMultiDayActivity = (self.activities['isLastActivity'] &
                                 self.activities['tripEndNextDay'] &
                                 self.activities['parkID'])
        self.activities = self.activities.loc[~indexMultiDayActivity, :]

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
        # Q to Ben: Why does vectorized replace of timestamp hour and minute not work?
        idxActs = self.activities['parkID'].fillna(0).astype(bool) & self.activities['isFirstActivity']
        self.activities.loc[idxActs, 'timestampStart'] = self.activities.loc[
            idxActs, 'timestampEnd'].apply(lambda x: x.replace(hour=0, minute=0))

        # Updating park end timestamps for last activity
        idxActs = self.activities['parkID'].fillna(0).astype(bool) & self.activities['isLastActivity']
        self.activities.loc[idxActs, 'timestampEnd'] = self.activities.loc[idxActs, 'timestampStart'].apply(
            lambda x: x.replace(hour=0, minute=0) + pd.Timedelta(1, 'd')
        )

        # FIXME: Optionally adjust end timestamp of last activity trips, add additional trips before first parking
        # in the future if needed
        # idxActs = self.activities['tripID'].fillna(0).astype(bool) & self.activities['isLastActivity']

        # Add timedelta column
        self.activities['timedelta'] = self.activities['timestampEnd'] - self.activities['timestampStart']
        print(f'Finished activity composition with {self.activities["tripID"].fillna(0).astype(bool).sum()} trips '
              f'and {self.activities["parkID"].fillna(0).astype(bool).sum()} parking activites')

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
        self.columns = self.compileVariableList()

    def compileVariableList(self) -> list:
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
        self.removeNA(variables)
        return variables

    def removeNA(self, variables: list):
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

    def selectColumns(self):
        """
        Function to filter the rawData for only relevant columns as specified
        by parseConfig and cleaned in self.compileVariablesList().
        Stores the subset of data in self.data

        :return: None
        """
        self.data = self.rawData.loc[:, self.columns]

    def filterConsistentHours(self):
        """
        Filtering out records where starting hour is after end hour but trip
        takes place on the same day.
        These observations are data errors.

        :return: No returns, operates only on the class instance
        """
        if self.datasetID in ["MiD17", "MiD08"]:
            dat = self.data
            self.data = dat.loc[
                (dat["tripStartClock"] <= dat["tripEndClock"])
                | (dat["tripEndNextDay"] == 1),
                :,
            ]
            # If we want to get rid of tripStartClock and tripEndClock
            # (they are redundant variables)
            # self.data = dat.loc[pd.to_datetime(dat.loc[:, 'tripStartHour'])
            # <= pd.to_datetime(dat.loc[:,
            # 'tripEndHour']) | (dat['tripEndNextDay'] == 1), :]
            filters = (
                (
                    self.data.loc[:, "tripStartHour"]
                    == self.data.loc[:, "tripEndHour"]
                )
                & (
                    self.data.loc[:, "tripStartMinute"]
                    == self.data.loc[:, "tripEndMinute"]
                )
                & (self.data.loc[:, "tripEndNextDay"])
            )

            self.data = self.data.loc[~filters, :]

    def addStrColumnFromVariable(self, colName: str, varName: str):
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

    def composeTimestamp(
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

    def composeStartAndEndTimestamps(self):
        """
        :return: Returns start and end time of a trip
        """
        self.composeTimestamp(data=self.data)  # Starting timestamp
        self.composeTimestamp(
            data=self.data,  # Ending timestamps
            colHour="tripEndHour",
            colMin="tripEndMinute",
            colName="timestampEnd",
        )

    def harmonizeVariablesGenericIdNames(self):
        """

        """
        self.data["genericID"] = self.data[
            str(self.parseConfig["IDVariablesNames"][self.datasetID])
        ]
        # self.data['tripDistance'] = 0.62 * self.data['tripDistance']
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

    def harmonizeVariables(self):
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
        if self.datasetID == "MiD08":
            dataRenamed["hhPersonID"] = (
                dataRenamed["hhID"].astype("string")
                + dataRenamed["personID"].astype("string")
            ).astype("int")
        self.data = dataRenamed
        print("Finished harmonization of variables")

    def convertTypes(self):
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
        keys = {
            iCol for iCol in conversionDict.keys() if iCol in self.data.columns
        }
        self.varDataTypeDict = {
            key: conversionDict[key] for key in conversionDict.keys() & keys
        }
        self.data = self.data.astype(self.varDataTypeDict)

    def addStrColumns(self, weekday=True, purpose=True):
        """
        Adds string columns for either weekday or purpose.

        :param weekday: Boolean identifier if weekday string info should be
                        added in a separate column
        :param purpose: Boolean identifier if purpose string info should be
                        added in a separate column
        :return: None
        """

        if weekday:
            self.addStrColumnFromVariable(
                colName="weekdayStr", varName="tripStartWeekday"
            )
        if purpose:
            self.addStrColumnFromVariable(
                colName="purposeStr", varName="tripPurpose"
            )

    def updateEndTimestamp(self):
        """
        :return:
        """
        endsFollowingDay = self.data["tripEndNextDay"] == 1
        self.data.loc[endsFollowingDay, "timestampEnd"] = self.data.loc[
            endsFollowingDay, "timestampEnd"
        ] + pd.offsets.Day(1)

    def process(self):
        """
        Wrapper function for harmonising and filtering the dataset.
        """
        self.selectColumns()
        self.harmonizeVariables()
        self.convertTypes()
        self.checkFilterDict(self.filterDict)
        self.filter(self.filterDict)
        self.filterConsistentHours()
        self.addStrColumns()
        self.composeStartAndEndTimestamps()
        self.updateEndTimestamp()
        self.harmonizeVariablesGenericIdNames()
        self.addParkingRows()
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

    def loadData(self):
        """
        rawDataPathTrip, unlike for other MiD classes is taken from the MiD B1 dataset
        rawDataPathVehicles is an internal dataset from VF
        """
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

        rawDataVehicles = pd.read_csv(
            rawDataPathVehicles,
            encoding="ISO-8859-1"
        )
        rawDataVehicles = rawDataVehicles.drop(columns=['Unnamed: 0'])
        rawDataVehicles = rawDataVehicles.drop_duplicates(subset=['HP_ID'], keep='first')
        rawDataVehicles.set_index("HP_ID", inplace=True)
        rawData = rawDataTrips.join(rawDataVehicles, on="HP_ID", rsuffix="VF")
        self.rawData = rawData
        print(
            f"Finished loading {len(self.rawData)} "
            f"rows of raw data of type .dta"
        )

    def harmonizeVariables(self):
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
        if self.datasetID == "MiD08":
            dataRenamed["hhPersonID"] = (
                dataRenamed["hhID"].astype("string")
                + dataRenamed["personID"].astype("string")
            ).astype("int")
        self.data = dataRenamed
        print("Finished harmonization of variables")

    def convertTypes(self):
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
        keys = {
            iCol for iCol in conversionDict.keys() if iCol in self.data.columns
        }
        self.varDataTypeDict = {
            key: conversionDict[key] for key in conversionDict.keys() & keys
        }
        self.data = self.data.astype(self.varDataTypeDict)

    def addStrColumns(self, weekday=True, purpose=True):
        """
        Adds string columns for either weekday or purpose.

        :param weekday: Boolean identifier if weekday string info should be
                        added in a separate column
        :param purpose: Boolean identifier if purpose string info should be
                        added in a separate column
        :return: None
        """

        if weekday:
            self.addStrColumnFromVariable(
                colName="weekdayStr", varName="tripStartWeekday"
            )
        if purpose:
            self.addStrColumnFromVariable(
                colName="purposeStr", varName="tripPurpose"
            )

    def updateEndTimestamp(self):
        """
        :return:
        """
        endsFollowingDay = self.data["tripEndNextDay"] == 1
        self.data.loc[endsFollowingDay, "timestampEnd"] = self.data.loc[
            endsFollowingDay, "timestampEnd"
        ] + pd.offsets.Day(1)

    def copyOverTripNextDay(self):
        pass

    def process(self):
        """
        Wrapper function for harmonising and filtering the dataset.
        """
        self.selectColumns()
        self.harmonizeVariables()
        self.convertTypes()
        self.checkFilterDict(self.filterDict)
        self.filter(self.filterDict)
        self.filterConsistentHours()
        self.addStrColumns()
        self.composeStartAndEndTimestamps()
        self.updateEndTimestamp()
        # self.copyOverTripNextDay()
        self.harmonizeVariablesGenericIdNames()
        self.addParkingRows()
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

    def loadData(self):
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

    def addStrColumns(self, weekday=True, purpose=True):
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
            self.addStrColumnFromVariable(colName="weekdayStr", varName="tripStartWeekday")
        if purpose:
            self.addStrColumnFromVariable(colName="purposeStr", varName="tripPurpose")

    def convertTypes(self):
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

    def updateEndTimestamp(self):
        """
        :return:
        """
        self.data["tripEndNextDay"] = np.where(
            self.data["timestampEnd"].dt.day
            > self.data["timestampStart"].dt.day,
            1,
            0,
        )
        endsFollowingDay = self.data["tripEndNextDay"] == 1
        self.data.loc[endsFollowingDay, "timestampEnd"] = self.data.loc[
            endsFollowingDay, "timestampEnd"
        ] + pd.offsets.Day(1)

    def excludeHours(self):
        """
        Removes trips where both start and end trip time are missing.
        """
        self.data = self.data.loc[(self.data["tripStartClock"] != "-1:-1") & (self.data["tripEndClock"] != "-1:-1"), :]

    def process(self):
        """
        Wrapper function for harmonising and filtering the dataset.
        """
        self.selectColumns()
        self.harmonizeVariables()
        self.convertTypes()
        self.checkFilterDict(self.filterDict)
        self.filter(self.filterDict)
        self.excludeHours()
        self.filterConsistentHours()
        self.addStrColumns()
        self.composeStartAndEndTimestamps()
        self.updateEndTimestamp()
        self.harmonizeVariablesGenericIdNames()
        self.addParkingRows()
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
