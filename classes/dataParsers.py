__version__ = '0.0.9'
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
    # Separate datasets that know each other
    # @profile(immediate=True)
    def __init__(self, parseConfig: dict, globalConfig: dict, localPathConfig: dict, datasetID: str = 'MiD17',
                 loadEncrypted=True):
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

        :param parseConfig: A yaml config file holding a dictionary with the keys 'pathRelative' and 'pathAbsolute'
        :param globalConfig:
        :param localPathConfig:
        :param datasetID:
        :param loadEncrypted:
        """
        self.datasetID = self.checkDatasetID(datasetID, parseConfig)
        self.parseConfig = parseConfig
        self.globalConfig = globalConfig
        self.rawDataPath = Path(localPathConfig['pathAbsolute'][self.datasetID]) / globalConfig['files'][self.datasetID]['tripsDataRaw']
        self.subDict = {}
        self.rawData = None
        self.data = None
        self.__filterDict = {}
        self.columns = self.compileVariableList()
        self.filterDictNameList = ['include', 'exclude', 'greaterThan', 'smallerThan']
        self.updateFilterDict()
        # review: Why do we not use the standard logging library of python for these output. It would empower
        #  the user to configure the output level it desires.
        print('Parsing properties set up')
        if loadEncrypted:
            print(f"Starting to retrieve encrypted data file from {self.globalConfig['pathAbsolute']['encryptedZipfile']}")
            self.loadEncryptedData(pathToZip=Path(self.globalConfig['pathAbsolute']['encryptedZipfile']) / self.globalConfig['files'][self.datasetID]['encryptedZipFileB2'],
                                   pathInZip=globalConfig['files'][self.datasetID]['tripDataZipFileRaw'])
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
        print('Parsing completed')

    def updateFilterDict(self) -> None:
        self.__filterDict = self.parseConfig['filterDicts'][self.datasetID]
        self.__filterDict = {iKey: iVal for iKey, iVal in self.__filterDict.items() if self.__filterDict[iKey] is not
                             None}

    def checkDatasetID(self, datasetID: str, parseConfig: dict) -> str:
        """
        :param datasetID: list of strings declaring the datasets to be read in
        :param parseConfig: A yaml config file holding a dictionary with the keys 'pathRelative' and 'pathAbsolute'
        :return: Returns a string value of a mobility data
        """
        availableDatasetIDs = parseConfig['dataVariables']['datasetID']
        assert datasetID in availableDatasetIDs, \
            f'Defined datasetID {datasetID} not specified under dataVariables in parseConfig. Specified datasetIDs are ' \
                f'{availableDatasetIDs}'
        return datasetID

    def compileVariableList(self) -> list:
        listIndex = self.parseConfig['dataVariables']['datasetID'].index(self.datasetID)
        variables = [val[listIndex] if not val[listIndex] == 'NA' else 'NA' for key, val in self.parseConfig['dataVariables'].items()]
        variables.remove(self.datasetID)
        if 'NA' in variables:
            self.removeNA(variables)
        return variables

    def removeNA(self, variables: list):
        """
        :param variables: List of variables of the mobility dataset
        :return: Returns a list with non NA values
        """
        variables.remove('NA')
        if 'NA' in variables:
            self.removeNA(variables)

        # review: is this maybe to exact a filter? Should we not also accept "na" as a synonym for NA?
        #  If yes, we would need to loop once over the values in variables and capitalize them before filtering.
        #  Or are uppercase NA guaranteed?

    def loadData(self):
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
        :param pathToZip:
        :param pathInZip:
        :return:
        """
        # review: What happens wif pathToZip is ot on the hard drive? Do we get an error or
        #  is a file created?
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

    def createReplacementDict(self, datasetID : str, dictRaw : dict) -> None:
        """
        :param datasetID: list of strings declaring the datasets to be read in
        :param dictRaw: Contains dictionary of the raw data
        :return:
        """
        if datasetID in dictRaw['datasetID']:
            listIndex = dictRaw['datasetID'].index(datasetID)
            return {val[listIndex]: key for (key, val) in dictRaw.items()}
        else:
            raise ValueError(f'Data set {datasetID} not specified in MiD variable dictionary.')

    def convertTypes(self):
        # Filter for dataset specific columns
        conversionDict = self.parseConfig['inputDTypes']
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
        # review: It is unclear to me which concept is referenced by bottom dict.
        #  I suggest we add some explanation about the concept of bottomDict to the doc string
        for iKey, iVal in baseDict.items():
            if isinstance(iVal, dict):
                lst = self.returnDictBottomValues(iVal, lst)
            else:
                if iVal is not None:
                    lst.append(iVal)
        return lst

    def checkFilterDict(self):
        """
        :return: Returns a filter dictionary containing a list from BottomDictValues
        """
        # Currently only checking if list of list str not typechecked all(map(self.__checkStr, val)
        assert all(isinstance(val, list) for val in self.returnDictBottomValues(self.__filterDict)), \
            f'All values in filter dictionaries have to be lists, but are not'

    def returnDictBottomKeys(self, baseDict: dict, lst: list = None) -> list:
        """
        Returns the lowest level keys of baseDict and returns all of them as a list. The parameter lst is used as
        interface between recursion levels.

        :param baseDict: Dictionary of variables
        :param lst: empty list
        :return: Returns a list with all the bottom dictionary keys
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
        print(f'Starting filtering, applying {len(self.returnDictBottomKeys(self.__filterDict))} filters.')
        ret = pd.DataFrame(index=self.data.index)
        # Future releases: as discussed before we could indeed work here with a plug and pray approach.
        #  we would need to introduce a filter manager and a folder structure where to look for filters.
        #  this is very similar code than the one from ioproc. If we want to go down this route we should
        #  take inspiration from the code there. It was not easy to get it right in the first place. This
        #  might be easy to code but hard to implement correctly.
        for iKey, iVal in self.__filterDict.items():
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
        :param filterData:
        :return:
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
        dat = self.data
        self.data = dat.loc[(dat['tripStartClock'] <= dat['tripEndClock']) | (dat['tripEndNextDay'] == 1), :]
        # If we want to get rid of tripStartClock and tripEndClock (they are redundant variables)
        # self.data = dat.loc[pd.to_datetime(dat.loc[:, 'tripStartHour']) <= pd.to_datetime(dat.loc[:, 'tripEndHour']) |
        #                     (dat['tripEndNextDay'] == 1), :]

    def addStrColumns(self, weekday=True, purpose=True):
        """
        Adds string columns for either weekday or purpose.

        :param weekday: Boolean identifier if weekday string info should be added in a separate column
        :param purpose: Boolean identifier if purpose string info should be added in a separate column
        :return: None
        """

        if weekday:
            self.addStrColumnFromMidVariable(colName='weekdayStr', varName='tripStartWeekday')
        if purpose:
            self.addStrColumnFromMidVariable(colName='purposeStr', varName='tripPurpose')

    def addStrColumnFromMidVariable(self, colName: str, varName: str):
        """
        Replaces each occurence of a MID variable e.g. 1,2,...,7 for weekdays with an explicitly mapped string e.g.
        'MON', 'TUE',...,'SUN'.

        :param colName: Name of the column in self.data where the explicit string info is stored
        :param varName: Name of the VencoPy internal variable given in config/parseConfig['dataVariables']
        :return: None
        """
        self.data.loc[:, colName] \
            = self.data.loc[:, varName].replace(self.parseConfig['midReplacements'][varName])

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

    def composeStartAndEndTimestamps(self) -> np.datetime64:
        """
        :return: Returns start and end time of a trip
        """
        self.composeTimestamp(data=self.data)  # Starting timestamp
        self.composeTimestamp(data=self.data,  # Ending timestamps
                              colHour='tripEndHour',
                              colMin='tripEndMinute',
                              colName='timestampEnd')
        self.updateEndTimestamp()

    def updateEndTimestamp(self) -> np.datetime64:
        """
        :return:
        """
        endsFollowingDay = self.data['tripEndNextDay'] == 1
        self.data.loc[endsFollowingDay, 'timestampEnd'] = self.data.loc[endsFollowingDay,
                                                                        'timestampEnd'] + pd.offsets.Day(1)

    def calcNTripsPerDay(self):
        """
        :return: Returns number of trips trips per household person per day
        """
        return self.data['hhPersonID'].value_counts().mean()

    def calcDailyTravelDistance(self):
        """
        :return: Returns daily travel distance per household
        """
        dailyDistances = self.data.loc[:, ['hhPersonID', 'tripDistance']].groupby(by=['hhPersonID']).sum()
        return dailyDistances.mean()

    def calcDailyTravelTime(self):
        """
        :return: Returns daily travel time per household person
        """
        travelTime = self.data.loc[:, ['hhPersonID', 'travelTime']].groupby(by=['hhPersonID']).sum()
        return travelTime.mean()

    def calcAverageTripDistance(self):
        """
        :return: Returns daily average trip distance
        """
        return self.data.loc[:, 'tripDistance'].mean()


class ParseMID(DataParser):
    # Inherited data class to at some point differentiate between abstract interfaces such as vencopz internal
    # variable namings and data set specific functions such as filters etc. Currently not used (06/14/2021)
    pass



if __name__ == '__main__':
    pathLocalPathConfig = Path.cwd().parent / 'config' / 'localPathConfig.yaml'
    with open(pathLocalPathConfig) as ipf:
        localPathConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    pathParseConfig = Path.cwd().parent / 'config' / 'parseConfig.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    with open(pathParseConfig) as ipf:
        parseConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    pathGlobalConfig = Path.cwd().parent / 'config' / 'globalConfig.yaml'
    with open(pathGlobalConfig) as ipf:
        globalConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    p = DataParser(localPathConfig=localPathConfig, parseConfig=parseConfig, globalConfig=globalConfig,
                   loadEncrypted=False, datasetID='MiD17')
    print(p.data.head())
    print('end')