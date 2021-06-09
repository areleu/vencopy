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
    def __init__(self, config: dict, globalConfig: dict,  datasetID: str = 'MiD17', loadEncrypted=True):
        # review: This doc string could do with some love. It is currently not saying much helpful
        #  in the description.
        """
        This is some explanation

        :param config: A yaml config file holding a dictionary with the keys 'pathRelative' and 'pathAbsolute'
        :param globalConfig:
        """
        self.datasetID = self.checkDatasetID(datasetID, config)
        self.config = config
        self.globalConfig = globalConfig
        self.rawDataPath = Path(globalConfig['pathAbsolute'][self.datasetID]) / globalConfig['files'][self.datasetID]['tripsDataRaw']
        self.subDict = {}
        self.rawData = None
        self.data = None
        self.columns = self.compileVariableList()
        self.filterDictNameList = ['include', 'exclude', 'greaterThan', 'smallerThan']
        self.updateFilterDict()
        # review: Why do we not use the standard logging library of python for these output. It would empower
        #  the user to configure the output level it desires.
        print('Parsing properties set up')
        if loadEncrypted:
            # review: I am unsure if the config field should be called "pathAbsolute" as paths have a
            #  plethora of meanings. Could we make it more clear that we talk about files on disk here?
            #  Something like filePaths or dataPaths?
            print(f"Starting to retrieve encrypted data file from {self.globalConfig['pathAbsolute']['encryptedZipfile']}")
            self.loadEncryptedData(pathToZip=Path(self.globalConfig['pathAbsolute']['encryptedZipfile']) / self.globalConfig['files'][self.datasetID]['encryptedZipFileB2'],
                                   pathInZip=globalConfig['files'][self.datasetID]['tripDataZipFileRaw'])
        else:
            print(f"Starting to retrieve local data file from {self.rawDataPath}")
            self.loadData()
        self.selectColumns()
        self.harmonizeVariables()
        self.convertTypes()
        # review: Why do we provide the local information to the checkFilterDict method?
        #  Could we switch to a default behaviour, so that checkFilterDict by default (so without
        #  input arguments) accesses self.__filterDict and only uses an overwrite when provided?
        #  For me this would clarify this rather unintuitive code snipped quite a bit.
        self.checkFilterDict(self.__filterDict)
        self.filter()
        self.filterConsistentHours()
        self.addStrColumns()
        # review: this is obsolete code I presume?
        # self.composeIndex()  Method to compose a unique index from hhID and personID if needed
        # self.setIndex(col='hhPersonID')
        self.composeStartAndEndTimestamps()
        print('Parsing completed')

    def updateFilterDict(self) -> None:
        self.__filterDict = self.config['filterDicts'][self.datasetID]
        self.__filterDict = {iKey: iVal for iKey, iVal in self.__filterDict.items() if self.__filterDict[iKey] is not None}

    def checkDatasetID(self, datasetID: str, config: dict) -> str:
        """
        :param datasetID: list of strings declaring the datasets to be read in
        :param config: A yaml config file holding a dictionary with the keys 'pathRelative' and 'pathAbsolute'
        :return: Returns a string value of a mobility data
        """
        availableDatasetIDs = config['dataVariables']['datasetID']
        assert datasetID in availableDatasetIDs, \
            f'Defined datasetID {datasetID} not specified under dataVariables in config. Specified datasetIDs are ' \
                f'{availableDatasetIDs}'
        return datasetID

    def compileVariableList(self) -> list:
        listIndex = self.config['dataVariables']['datasetID'].index(self.datasetID)
        variables = [val[listIndex] if not val[listIndex] == 'NA' else 'NA' for key, val in self.config['dataVariables'].items()]
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
        # review: Are potential error messages (.dta not being a stata file even as the ending matches)
        #  readable for the user? Should we have a manual error treatment here?
        if self.rawDataPath.suffix == '.dta':
            self.rawData = pd.read_stata(self.rawDataPath, convert_categoricals=False, convert_dates=False,
                                      preserve_dtypes=False)
        else:  # self.rawDataFileType == '.csv':
            self.rawData = pd.read_csv(self.rawDataPath)

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
                self.rawData = pd.read_stata(myzip.open(pathInZip, pwd=bytes(self.config['encryptionPW'],
                                                                             encoding='utf-8')),
                                             convert_categoricals=False, convert_dates=False, preserve_dtypes=False)
            else:  # if '.csv' in pathInZip:
                self.rawData = pd.read_csv(myzip.open(pathInZip, pwd=bytes(self.config['encryptionPW'],
                                                                           encoding='utf-8')), sep=';', decimal=',')

        print(f'Finished loading {len(self.rawData)} rows of raw data of type {self.rawDataPath.suffix}')

    def selectColumns(self):
        self.data = self.rawData.loc[:, self.columns]

    def harmonizeVariables(self):
        """
        :return: Returns the variable names of 2008 MiD data harmonized with the variable names for 2017 MiD data
        """
        replacementDict = self.createReplacementDict(self.datasetID, self.config['dataVariables'])
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
        conversionDict = self.config['inputDTypes']
        keys = {iCol for iCol in conversionDict.keys() if iCol in self.data.columns}
        self.subDict = {key: conversionDict[key] for key in conversionDict.keys() & keys}
        self.data = self.data.astype(self.subDict)

    def returnBottomDictValues(self, baseDict: dict, lst: list = []) -> list:
        """
        :param baseDict: Dictionary of variables
        :param lst: empty list
        :return: Returns a list with all the bottom dictionary values
        """
        # review: It is unclear to me which concept is referenced by bottom dict.
        #  I suggest we add some explanation about the concept of bottomDict to the doc string
        for iKey, iVal in baseDict.items():
            if isinstance(iVal, dict):
                lst = self.returnBottomDictValues(iVal, lst)
            else:
                if iVal is not None:
                    lst.append(iVal)
        return lst

    def checkFilterDict(self, filterDict: dict):
        """
        :return: Returns a filter dictionary containing a list from BottomDictValues
        """
        # Currently only checking if list of list str not typechecked all(map(self.__checkStr, val)
        assert all(isinstance(val, list) for val in self.returnBottomDictValues(filterDict)), \
            f'All values in filter dictionaries have to be lists, but are not'

    def returnBottomDictKeys(self, baseDict: dict, lst: list = None) -> list:
        """
        :param baseDict: Dictionary of variables
        :param lst: empty list
        :return: Returns a list with all the bottom dictionary keys
        """
        if lst is None:
            lst = []
        for iKey, iVal in baseDict.items():
            if isinstance(iVal, dict):
                lst = self.returnBottomDictKeys(iVal, lst)
            else:
                if iVal is not None:
                    lst.append(iKey)
        return lst

    def filter(self):
        print(f'Starting filtering, applying {len(self.returnBottomDictKeys(self.__filterDict))} filters.')
        ret = pd.DataFrame(index=self.data.index)
        # review: as discussed before we could indeed work here with a plug and pray approach.
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
                warnings.warn(f'A filter dictionary was defined in the config with an unknown filtering key. '
                              f'Current filtering keys comprise include, exclude, smallerThan and greaterThan.'
                              f'Continuing with ignoring the dictionary {iKey}')
        self.data = self.data[ret.all(axis='columns')]
        self.filterAnalysis(ret)

    def setIncludeFilter(self, includeFilterDict: dict, dataIndex) -> pd.DataFrame:
        """
        :param includeFilterDict: Dictionary of include filters defined in config.yaml
        :param dataIndex: Index for the data frame
        :return: Returns a data frame with individuals using car as a mode of transport
        """
        incFilterCols = pd.DataFrame(index=dataIndex, columns=includeFilterDict.keys())
        for incCol, incElements in includeFilterDict.items():
            incFilterCols[incCol] = self.data[incCol].isin(incElements)
        return incFilterCols

    def setExcludeFilter(self, excludeFilterDict: dict, dataIndex) -> pd.DataFrame:
        """
        :param excludeFilterDict: Dictionary of exclude filters defined in config.yaml
        :param dataIndex: Index for the data frame
        :return: Returns a filtered data frame with exclude filters
        """
        exclFilterCols = pd.DataFrame(index=dataIndex, columns=excludeFilterDict.keys())
        for excCol, excElements in excludeFilterDict.items():
            exclFilterCols[excCol] = ~self.data[excCol].isin(excElements)
        return exclFilterCols

    def setGreaterThanFilter(self, greaterThanFilterDict: dict, dataIndex):
        """
        :param greaterThanFilterDict: Dictionary of greater than filters defined in config.yaml
        :param dataIndex: Index for the data frame
        :return:
        """
        greaterThanFilterCols = pd.DataFrame(index=dataIndex, columns=greaterThanFilterDict.keys())
        for greaterCol, greaterElements in greaterThanFilterDict.items():
            greaterThanFilterCols[greaterCol] = self.data[greaterCol] >= greaterElements.pop()
            if len(greaterElements) > 0:
                warnings.warn(f'You specified more than one value as lower limit for filtering column {greaterCol}.'
                              f'Only considering the last element given in the config.')
        return greaterThanFilterCols

    def setSmallerThanFilter(self, smallerThanFilterDict: dict, dataIndex) -> pd.DataFrame:
        """
        :param smallerThanFilterDict: Dictionary of smaller than filters defined in config.yaml
        :param dataIndex: Index for the data frame
        :return: Returns a data frame of trips covering a distance of less than 1000 km
        """
        smallerThanFilterCols = pd.DataFrame(index=dataIndex, columns=smallerThanFilterDict.keys())
        for smallerCol, smallerElements in smallerThanFilterDict.items():
            smallerThanFilterCols[smallerCol] = self.data[smallerCol] <= smallerElements.pop()
            if len(smallerElements) > 0:
                warnings.warn(f'You specified more than one value as upper limit for filtering column {smallerCol}.'
                              f'Only considering the last element given in the config.')
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
        if weekday:
            self.addWeekdayStrColumn()
        if purpose:
            self.addPurposeStrColumn()

        # review: just a comment here for the general granularity of the code:
        #  a split into two submethods is not necessary for readability or decoupling.
        #  One would opt for this high granularity iff addWekdayStrColumn is also used outside of
        #  the method addStrColumn as it contains only one instruction. This by no means means that we
        #  need to refactor it back into one method. This is more like a reminder, that we are too granular
        #  at this point in the code for future guidance.

    def addWeekdayStrColumn(self):
        self.data.loc[:, 'weekdayStr'] \
            = self.data.loc[:, 'tripStartWeekday'].replace(self.config['midReplacements']['tripStartWeekday'])

    def addPurposeStrColumn(self):
        self.data.loc[:, 'purposeStr'] \
            = self.data.loc[:, 'tripPurpose'].replace(self.config['midReplacements']['tripPurpose'])

    def composeIndex(self):
        # review: pycharm complains that this method is never used. I am not totally convinced. If true we
        #  should remove it.
        self.data['idxCol'] = self.data['hhPersonID'].astype('string') + '__' + self.data['tripID'].astype('string')

    def setIndex(self, col):
        # review: pycharm complains that this method is never used. I am not totally convinced. If true we
        #  should remove it.
        self.data.set_index(col, inplace=True, drop=True)

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
    # review: This is actually not future fail prove as parameters to DataParser are not
    #  forwarded in the super call. I suggest to use just and simply the syntax:
    #  class ParseMID(DataParser):
    #      pass
    #
    # Also this begs the question how ParseMID is actually different from all other parsers?

    # review: The class name is misleading, as it implies a function, since by convention verbs are only used
    #  for methods. Classes are more like entities and hence are named after nouns. In this case MIDParser would
    #  be a better name.
    def __init__(self):
        super().__init__()



if __name__ == '__main__':
    pathParseConfig = Path.cwd().parent / 'config' / 'parseConfig.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    parseConfig = yaml.load(open(pathParseConfig), Loader=yaml.SafeLoader)
    pathGlobalConfig = Path.cwd().parent / 'config' / 'globalConfig.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    globalConfig = yaml.load(open(pathGlobalConfig), Loader=yaml.SafeLoader)
    p = DataParser(config=parseConfig, globalConfig=globalConfig, loadEncrypted=False)
    print(p.data.head())
    print('end')