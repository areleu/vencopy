__version__ = '0.1.0'
__maintainer__ = 'Niklas Wulff 31.12.2019'
__contributors__ = 'Fabia Miorelli, Parth Butte'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '31.12.2019'
__status__ = 'dev'  # options are: dev, test, prod

import pprint
import pandas as pd
import numpy as np
import warnings
from pathlib import Path
import yaml
from zipfile import ZipFile


class DataParser:
    def __init__(self, configDict: dict, datasetID: str, loadEncrypted=True):
        """
        Basic class for parsing a mobility survey trip data set. Currently the both German travel surveys MiD 2008 and
        MiD 2017 are pre-configured and one of the two can be given (default: MiD 2017).
        The data set can be provided from an encrypted file on a server in which case the link to the ZIP-file as well
        as a link to the file within the ZIP-file have to be supplied in the globalConfig and a password has to be
        supplied in the parseConfig.
        Columns relevant for the EV simulation are selected from the entirety of the data and renamed to VencoPy
        internal variable names given in the dictionary parseConfig['dataVariables'] for the respective survey data set.
        Manually configured exclude, include, greaterThan and smallerThan filters are applied as they are specified in
        parseConfig. For some columns, raw data is transferred to human readable strings and respective columns are
        added. Pandas timestamp columns are synthesized from the given trip start and trip end time information.

        :param parseConfig: A yaml config file holding a dictionary with the keys 'pathRelative'
        :param globalConfig: A yaml config file holding mainly filenames and file labels
        :param localPathConfig: A config that is not synchronized holding local paths
        :param datasetID: Currently, MiD08 and MiD17 are implemented as travel survey data sets
        :param loadEncrypted: If True, load an encrypted ZIP file as specified in parseConfig
        """
        self.parseConfig = configDict['parseConfig']
        self.globalConfig = configDict['globalConfig']
        self.localPathConfig = configDict['localPathConfig']
        self.datasetID = self.checkDatasetID(datasetID, self.parseConfig)
        self.rawDataPath = Path(self.localPathConfig['pathAbsolute'][self.datasetID]) / self.globalConfig['files'][self.datasetID]['tripsDataRaw']
        self.subDict = {}
        self.rawData = None
        self.data = None
        self.__filterDict = {}
        self.columns = self.compileVariableList()
        self.filterDictNameList = ['include', 'exclude', 'greaterThan', 'smallerThan']
        self.updateFilterDict()
        print('Parsing properties set up')
        if loadEncrypted:
            print(f"Starting to retrieve encrypted data file from "
                  f"{self.globalConfig['pathAbsolute']['encryptedZipfile']}")
            self.loadEncryptedData(pathToZip=Path(self.globalConfig['pathAbsolute']['encryptedZipfile']) /
                                             self.globalConfig['files'][self.datasetID]['encryptedZipFileB2'],
                                   pathInZip=self.globalConfig['files'][self.datasetID]['tripDataZipFileRaw'])
        else:
            print(f"Starting to retrieve local data file from {self.rawDataPath}")
            self.loadData()
        self.selectColumns()
        self.harmonizeVariables()
        self.convertTypes()
        self.checkFilterDict()
        self.filter()
        self.filterConsistentHours()
        self.addStrColumns()
        self.composeStartAndEndTimestamps()
        self.updateEndTimestamps()
        self.harmonizeVariablesGenericIdNames()
        print('Parsing completed')

    def updateFilterDict(self) -> None:
        """
        Internal function to parse the filter dictionary of a specified data set from parseConfig.yaml

        :return: None
        """
        self.__filterDict[self.datasetID] = self.parseConfig['filterDicts'][self.datasetID]
        self.__filterDict[self.datasetID] = {iKey: iVal for iKey, iVal in self.__filterDict[self.datasetID].items() if self.__filterDict[self.datasetID][iKey] is not
                             None}

    def checkDatasetID(self, datasetID: str, parseConfig: dict) -> str:
        """
        General check if data set ID is defined in parseConfig.yaml

        :param datasetID: list of strings declaring the datasetIDs to be read in
        :param parseConfig: A yaml config file holding a dictionary with the keys 'pathRelative' and 'pathAbsolute'
        :return: Returns a string value of a mobility data
        """
        availableDatasetIDs = parseConfig['dataVariables']['datasetID']
        assert datasetID in availableDatasetIDs, \
            f'Defined datasetID {datasetID} not specified under dataVariables in parseConfig. Specified datasetIDs ' \
            f'are {availableDatasetIDs}'
        return datasetID

    def compileVariableList(self) -> list:
        """
        Clean up the replacement dictionary of raw data file variable (column) names. This has to be done because some
        variables that may be relevant for the analysis later on are only contained in one raw data set while not
        contained in another one. E.g. if a trip is an intermodal trip was only assessed in the MiD 2017 while it wasn't
        in the MiD 2008. This has to be mirrored by the filter dict for the respective data set.

        :return: List of variables
        """
        listIndex = self.parseConfig['dataVariables']['datasetID'].index(self.datasetID)
        variables = [val[listIndex] if not val[listIndex] == 'NA' else 'NA' for key, val in
                     self.parseConfig['dataVariables'].items()]
        variables.remove(self.datasetID)
        self.removeNA(variables)
        return variables

    def removeNA(self, variables: list):
        """
        Removes all strings that can be capitalized to 'NA' from the list of variables

        :param variables: List of variables of the mobility dataset
        :return: Returns a list with non NA values
        """
        vars = [iVar.upper() for iVar in variables]
        counter = 0
        for idx, iVar in enumerate(vars):
            if iVar == 'NA':
                del variables[idx - counter]
                counter += 1

    def loadData(self):
        """
        Loads data specified in self.rawDataPath and stores it in self.rawData. Raises an exception if a invalid suffix
        is specified in self.rawDataPath. READ IN OF CSV HAS NOT BEEN EXTENSIVELY TESTED BEFORE BETA RELEASE.

        :return: None
        """
        # Future releases: Are potential error messages (.dta not being a stata file even as the ending matches)
        # readable for the user? Should we have a manual error treatment here?
        if self.rawDataPath.suffix == '.dta':
            self.rawData = pd.read_stata(self.rawDataPath, convert_categoricals=False, convert_dates=False,
                                      preserve_dtypes=False)

        # This has not been tested before the beta release
        elif self.rawDataPath.suffix == '.csv':
            self.rawData = pd.read_csv(self.rawDataPath)
        else:
            Exception(f"Data type {self.rawDataPath.suffix} not yet specified. Available types so far are .dta and "
                      f".csv")

        print(f'Finished loading {len(self.rawData)} rows of raw data of type {self.rawDataPath.suffix}')

    def loadEncryptedData(self, pathToZip, pathInZip):
        """
        Since the MiD data sets are only accessible by an extensive data security contract, VencoPy provides the
        possibility to access encrypted zip files. An encryption password has to be given in parseConfig.yaml in order
        to access the encrypted file. Loaded data is stored in self.rawData

        :param pathToZip: path from current working directory to the zip file or absolute path to zipfile
        :param pathInZip: Path to trip data file within the encrypted zipfile
        :return: None
        """
        with ZipFile(pathToZip) as myzip:
            if '.dta' in pathInZip:
                self.rawData = pd.read_stata(myzip.open(pathInZip, pwd=bytes(self.parseConfig['encryptionPW'],
                                                                             encoding='utf-8')),
                                             convert_categoricals=False, convert_dates=False, preserve_dtypes=False)
            else:  # if '.csv' in pathInZip:
                self.rawData = pd.read_csv(myzip.open(pathInZip, pwd=bytes(self.parseConfig['encryptionPW'],
                                                                           encoding='utf-8')), sep=';', decimal=',')

        print(f'Finished loading {len(self.rawData)} rows of raw data of type {self.rawDataPath.suffix}')

    def selectColumns(self):
        """
        Function to filter the rawData for only relevant columns as specified by parseConfig and cleaned in
        self.compileVariablesList(). Stores the subset of data in self.data

        :return: None
        """

        self.data = self.rawData.loc[:, self.columns]
        
    def harmonizeVariables(self):
        """
        Harmonizes the input data variables to match internal VencoPy names given as specified in the mapping in
        parseConfig['dataVariables']. So far mappings for MiD08 and MiD17 are given. Since the MiD08 doesn't provide
        a combined household and person unique identifier, it is synthesized of the both IDs.

        :return: None
        """
        replacementDict = self.createReplacementDict(self.datasetID, self.parseConfig['dataVariables'])
        dataRenamed = self.data.rename(columns=replacementDict)
        if self.datasetID == 'MiD08':
            dataRenamed['hhPersonID'] = (dataRenamed['hhID'].astype('string') +
                                         dataRenamed['personID'].astype('string')).astype('int')
        self.data = dataRenamed
        print('Finished harmonization of variables')

    def createReplacementDict(self, datasetID: str, dictRaw: dict) -> dict:
        """
        Creates the mapping dictionary from raw data variable names to VencoPy internal variable names as specified
        in parseConfig.yaml for the specified data set.

        :param datasetID: list of strings declaring the datasetIDs to be read in
        :param dictRaw: Contains dictionary of the raw data
        :return: Dictionary with internal names as keys and raw data column names as values.
        """

        if datasetID in dictRaw['datasetID']:
            listIndex = dictRaw['datasetID'].index(datasetID)
            return {val[listIndex]: key for (key, val) in dictRaw.items()}
        else:
            raise ValueError(f'Data set {datasetID} not specified in parseConfig variable dictionary.')

    def convertTypes(self):
        """
        Convert raw column types to predefined python types as specified in parseConfig['inputDTypes'][datasetID]. This is mainly
        done for performance reasons. But also in order to avoid index values that are of type int to be cast to float.
        The function operates only on self.data and writes back changes to self.data

        :return: None
        """

        # Filter for dataset specific columns
        if self.datasetID == 'KiD':
            conversionDict = self.parseConfig['inputDTypes'][self.datasetID]
            keys = {iCol for iCol in conversionDict.keys() if iCol in self.data.columns}
            self.subDict = {key: conversionDict[key] for key in conversionDict.keys() & keys}
            # German df has commas instead of dots in floats
            for i, x in enumerate(list(self.data.tripDistance)):
                self.data.at[i, 'tripDistance'] = x.replace(',', '.')
            for i, x in enumerate(list(self.data.tripWeight)):
                self.data.at[i, 'tripWeight'] = x.replace(',', '.')
            self.data = self.data.astype(self.subDict)
        else:
            conversionDict = self.parseConfig['inputDTypes'][self.datasetID]
            keys = {iCol for iCol in conversionDict.keys() if iCol in self.data.columns}
            self.subDict = {key: conversionDict[key] for key in conversionDict.keys() & keys}
            self.data = self.data.astype(self.subDict)


    def returnDictBottomValues(self, baseDict: dict, lst: list = []) -> list:
        """
        Returns a list of all dictionary values of the last dictionary level (the bottom) of baseDict. The parameter
        lst is used as an interface between recursion levels.

        :param baseDict: Dictionary of variables
        :param lst: empty list, is used as interface to next recursion
        :return: Returns a list with all the bottom dictionary values
        """
        for iKey, iVal in baseDict.items():
            if isinstance(iVal, dict):
                lst = self.returnDictBottomValues(iVal, lst)
            else:
                if iVal is not None:
                    lst.append(iVal)
        return lst

    def checkFilterDict(self):
        """
        Checking if all values of filter dictionaries are of type list.  Currently only checking if list of list str
        not typechecked all(map(self.__checkStr, val). Conditionally triggers an assert.

        :return: None
        """

        assert all(isinstance(val, list) for val in self.returnDictBottomValues(self.__filterDict[self.datasetID])), \
            f'All values in filter dictionaries have to be lists, but are not'

    def returnDictBottomKeys(self, baseDict: dict, lst: list = None) -> list:
        """
        Returns the lowest level keys of baseDict and returns all of them as a list. The parameter lst is used as
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
            else:
                if iVal is not None:
                    lst.append(iKey)
        return lst

    def filter(self):
        """
        Wrapper function to carry out filtering for the four filter logics of including, excluding, greaterThan and
        smallerThan. If a filterDict is defined with a different key, a warning is thrown. The function operates on
        self.data class-internally.

        :return: None
        """

        print(f'Starting filtering, applying {len(self.returnDictBottomKeys(self.__filterDict[self.datasetID]))} filters.')
        ret = pd.DataFrame(index=self.data.index)
        # Future releases: as discussed before we could indeed work here with a plug and pray approach.
        #  we would need to introduce a filter manager and a folder structure where to look for filters.
        #  this is very similar code than the one from ioproc. If we want to go down this route we should
        #  take inspiration from the code there. It was not easy to get it right in the first place. This
        #  might be easy to code but hard to implement correctly.
        for iKey, iVal in self.__filterDict[self.datasetID].items():
            if iKey == 'include':
                ret = ret.join(self.setIncludeFilter(iVal, self.data.index))
            elif iKey == 'exclude':
                ret = ret.join(self.setExcludeFilter(iVal, self.data.index))
            elif iKey == 'greaterThan':
                ret = ret.join(self.setGreaterThanFilter(iVal, self.data.index))
            elif iKey == 'smallerThan':
                ret = ret.join(self.setSmallerThanFilter(iVal, self.data.index))
            else:
                warnings.warn(f'A filter dictionary was defined in the parseConfig with an unknown filtering key. '
                              f'Current filtering keys comprise include, exclude, smallerThan and greaterThan.'
                              f'Continuing with ignoring the dictionary {iKey}')
        self.data = self.data[ret.all(axis='columns')]
        self.filterAnalysis(ret)

    def setIncludeFilter(self, includeFilterDict: dict, dataIndex) -> pd.DataFrame:
        """
        Read-in function for include filter dict from parseConfig.yaml

        :param includeFilterDict: Dictionary of include filters defined in parseConfig.yaml
        :param dataIndex: Index for the data frame
        :return: Returns a data frame with individuals using car as a mode of transport
        """

        incFilterCols = pd.DataFrame(index=dataIndex, columns=includeFilterDict.keys())
        for incCol, incElements in includeFilterDict.items():
            incFilterCols[incCol] = self.data[incCol].isin(incElements)
        return incFilterCols

    def setExcludeFilter(self, excludeFilterDict: dict, dataIndex) -> pd.DataFrame:
        """
        Read-in function for exclude filter dict from parseConfig.yaml

        :param excludeFilterDict: Dictionary of exclude filters defined in parseConfig.yaml
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

        :param greaterThanFilterDict: Dictionary of greater than filters defined in parseConfig.yaml
        :param dataIndex: Index for the data frame
        :return:
        """
        greaterThanFilterCols = pd.DataFrame(index=dataIndex, columns=greaterThanFilterDict.keys())
        for greaterCol, greaterElements in greaterThanFilterDict.items():
            greaterThanFilterCols[greaterCol] = self.data[greaterCol] >= greaterElements.pop()
            if len(greaterElements) > 0:
                warnings.warn(f'You specified more than one value as lower limit for filtering column {greaterCol}.'
                              f'Only considering the last element given in the parseConfig.')
        return greaterThanFilterCols

    def setSmallerThanFilter(self, smallerThanFilterDict: dict, dataIndex) -> pd.DataFrame:
        """
        Read-in function for smallerThan filter dict from parseConfig.yaml

        :param smallerThanFilterDict: Dictionary of smaller than filters defined in parseConfig.yaml
        :param dataIndex: Index for the data frame
        :return: Returns a data frame of trips covering a distance of less than 1000 km
        """
        smallerThanFilterCols = pd.DataFrame(index=dataIndex, columns=smallerThanFilterDict.keys())
        for smallerCol, smallerElements in smallerThanFilterDict.items():
            smallerThanFilterCols[smallerCol] = self.data[smallerCol] <= smallerElements.pop()
            if len(smallerElements) > 0:
                warnings.warn(f'You specified more than one value as upper limit for filtering column {smallerCol}.'
                              f'Only considering the last element given in the parseConfig.')
        return smallerThanFilterCols

    def filterAnalysis(self, filterData: pd.DataFrame):
        """
        Function supplies some aggregate info of the data after filtering to the user Function does not change any
        class attributes

        :param filterData:
        :return: None
        """

        lenData = sum(filterData.all(axis='columns'))
        boolDict = {iCol: sum(filterData[iCol]) for iCol in filterData}
        print(f'The following values were taken into account after filtering:')
        pprint.pprint(boolDict)
        print(f"All filters combined yielded a total of {lenData} was taken into account")
        print(f'This corresponds to {lenData / len(filterData)* 100} percent of the original data')

    def filterConsistentHours(self):
        """
        Filtering out records where starting hour is after end hour but trip takes place on the same day.
        These observations are data errors.

        :return: No returns, operates only on the class instance
        """

        if self.datasetID == 'MiD17' or self.datasetID == 'MiD08':
            dat = self.data
            self.data = dat.loc[(dat['tripStartClock'] <= dat['tripEndClock']) | (dat['tripEndNextDay'] == 1), :]
            # If we want to get rid of tripStartClock and tripEndClock (they are redundant variables)
            # self.data = dat.loc[pd.to_datetime(dat.loc[:, 'tripStartHour']) <= pd.to_datetime(dat.loc[:, 'tripEndHour']) |
            #                     (dat['tripEndNextDay'] == 1), :]

    def addStrColumnFromVariable(self, colName: str, varName: str):
        """
        Replaces each occurence of a MiD/KiD variable e.g. 1,2,...,7 for weekdays with an explicitly mapped string e.g.
        'MON', 'TUE',...,'SUN'.

        :param colName: Name of the column in self.data where the explicit string info is stored
        :param varName: Name of the VencoPy internal variable given in config/parseConfig['dataVariables']
        :return: None
        """
        self.data.loc[:, colName] \
            = self.data.loc[:, varName].replace(self.parseConfig['Replacements'][self.datasetID][varName])

    def addStrColumns(self, weekday=True, purpose=True):
        """
        Adds string columns for either weekday or purpose.

        :param weekday: Boolean identifier if weekday string info should be added in a separate column
        :param purpose: Boolean identifier if purpose string info should be added in a separate column
        :return: None
        """

        if self.datasetID == 'MiD17' or self.datasetID == 'MiD08':
            if weekday:
                self.addStrColumnFromVariable(colName='weekdayStr', varName='tripStartWeekday')
            if purpose:
                self.addStrColumnFromVariable(colName='purposeStr', varName='tripPurpose')

        if self.datasetID == 'KiD':
            # from tripStartDate retrieve tripStartWeekday, tripStartWeek, tripStartYear, tripStartMonth, tripStartDay
            # from tripStartClock retrieve tripStartHour, tripStartMinute
            # from tripEndClock retrieve tripEndHour, tripEndMinute
            self.data['tripStartDate'] = pd.to_datetime(self.data['tripStartDate'], format='%d.%m.%Y')
            self.data['tripStartYear'] = self.data['tripStartDate'].dt.year
            self.data['tripStartMonth'] = self.data['tripStartDate'].dt.month
            self.data['tripStartDay'] = self.data['tripStartDate'].dt.day
            self.data['tripStartWeekday'] = self.data['tripStartDate'].dt.weekday
            self.data['tripStartWeek'] = self.data['tripStartDate'].dt.isocalendar().week
            self.data['tripStartHour'] = pd.to_datetime(self.data['tripStartClock'], format='%H:%M').dt.hour
            self.data['tripStartMinute'] = pd.to_datetime(self.data['tripStartClock'], format='%H:%M').dt.minute
            self.data['tripEndHour'] = pd.to_datetime(self.data['tripEndClock'], format='%H:%M').dt.hour
            self.data['tripEndMinute'] = pd.to_datetime(self.data['tripEndClock'], format='%H:%M').dt.minute


            if weekday:
                self.addStrColumnFromVariable(colName='weekdayStr', varName='tripStartWeekday')
            if purpose:
                self.addStrColumnFromVariable(colName='purposeStr', varName='tripPurpose')



    def composeTimestamp(self, data: pd.DataFrame = None,
                         colYear: str = 'tripStartYear',
                         colWeek: str = 'tripStartWeek',
                         colDay: str = 'tripStartWeekday',
                         colHour: str = 'tripStartHour',
                         colMin: str = 'tripStartMinute',
                         colName: str = 'timestampStart') -> np.datetime64:
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
        data[colName] = pd.to_datetime(data.loc[:, colYear], format='%Y') + \
                        pd.to_timedelta(data.loc[:, colWeek] * 7, unit='days') + \
                        pd.to_timedelta(data.loc[:, colDay], unit='days') + \
                        pd.to_timedelta(data.loc[:, colHour], unit='hour') + \
                        pd.to_timedelta(data.loc[:, colMin], unit='minute')
        # return data

    def composeStartAndEndTimestamps(self):
        """
        :return: Returns start and end time of a trip
        """
        self.composeTimestamp(data=self.data)  # Starting timestamp
        self.composeTimestamp(data=self.data,  # Ending timestamps
                              colHour='tripEndHour',
                              colMin='tripEndMinute',
                              colName='timestampEnd')

    def updateEndTimestamp(self):
        """
        :return:
        """
        if self.datasetID == 'MiD17' or self.datasetID == 'MiD08':
            endsFollowingDay = self.data['tripEndNextDay'] == 1
            self.data.loc[endsFollowingDay, 'timestampEnd'] = self.data.loc[endsFollowingDay,
                                                                        'timestampEnd'] + pd.offsets.Day(1)
        if self.datasetID == 'KiD':
            self.data['tripEndNextDay'] = np.where(self.data['timestampEnd'].dt.day > self.data['timestampStart'].dt.day, 1, 0)
            endsFollowingDay = self.data['tripEndNextDay'] == 1
            self.data.loc[endsFollowingDay, 'timestampEnd'] = self.data.loc[endsFollowingDay,
                                                                            'timestampEnd'] + pd.offsets.Day(1)


    def updateEndTimestamps(self) -> np.datetime64:
        """
        :return: Returns start and end time of a trip
        """
        self.updateEndTimestamp()

    def harmonizeVariablesGenericIdNames(self):
        """

        """
        self.data['genericID'] = self.data[str(self.parseConfig['IDVariablesNames'][self.datasetID])]
        print('Finished harmonization of ID variables')


class ParseMID(DataParser):
    # Inherited data class to differentiate between abstract interfaces such as vencopy internal
    # variable namings and data set specific functions such as filters etc. Currently not used (06/14/2021)
    pass

if __name__ == '__main__':
    from vencopy.scripts.globalFunctions import loadConfigDict
    configNames = ('globalConfig', 'localPathConfig', 'parseConfig', 'tripConfig', 'gridConfig', 'flexConfig', 'evaluatorConfig')
    configDict = loadConfigDict(configNames)

    #datasetID = 'MiD17' #options are MiD08, MiD17, KiD
    datasetID = 'KiD'
    vpData = DataParser(configDict=configDict, loadEncrypted=False, datasetID=datasetID)
