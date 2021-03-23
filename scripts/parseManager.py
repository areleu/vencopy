__version__ = '0.0.0'
__maintainer__ = 'Niklas Wulff 31.12.2019'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '31.12.2019'
__status__ = 'dev'  # options are: dev, test, prod

import time
import pprint
import pandas as pd
import warnings
from pathlib import Path
from profilehooks import profile
import yaml
from scripts.libInput import *

class ParseData:
    # Separate datasets that know each other
    # @profile(immediate=True)
    def __init__(self, datasetID: str, config: dict, strColumns: bool = False, loadEncrypted=True):
        self.datasetID = self.checkDatasetID(datasetID, config)
        self.config = config
        self.rawDataPath = Path(config['linksAbsolute'][self.datasetID]) / config['files'][self.datasetID]['tripsDataRaw']
        self.data = None
        self.columns = self.compileVariableList()
        self.filterDictNameList = ['include', 'exclude', 'greaterThan', 'smallerThan']
        self.updateFilterDict()
        print('Parsing properties set up')
        if loadEncrypted:
            print(f"Starting to retrieve encrypted data file from {self.config['linksAbsolute']['encryptedZipfile']}")
            self.loadEncryptedData(linkToZip=Path(self.config['linksAbsolute']['encryptedZipfile']) / self.config['files'][self.datasetID]['enryptedZipFileB2'],
                                   linkInZip=config['files'][self.datasetID]['tripDataZipFileRaw'])
        else:
            print(f"Starting to retrieve local data file from {self.rawDataPath}")
            self.loadData()
        self.harmonizeVariables()
        self.convertTypes()
        self.checkFilterDict(self.__filterDict)
        self.filter()
        self.filterConsistentHours()
        if strColumns:
            self.addStrColumns()
        # self.addIndex()
        self.composeStartAndEndTimestamps()
        print('Parsing completed')

    def updateFilterDict(self):
        self.__filterDict = self.config['filterDicts'][self.datasetID]
        self.__filterDict = {iKey: iVal for iKey, iVal in self.__filterDict.items() if self.__filterDict[iKey] is not None}

    def checkDatasetID(self, dataset: str, config: dict):
        availableDatasetIDs = config['dataVariables']['dataset']
        assert dataset in availableDatasetIDs, \
            f'Defined dataset {dataset} not specified under dataVariables in config. Specified datasetIDs are ' \
                f'{availableDatasetIDs}'
        return dataset

    def compileVariableList(self):
        listIndex = self.config['dataVariables']['dataset'].index(self.datasetID)
        variables = [val[listIndex] if not val[listIndex] == 'NA' else 'NA' for key, val in self.config['dataVariables'].items()]
        variables.remove(self.datasetID)
        if 'NA' in variables:
            self.removeNA(variables)
        return variables

    def removeNA(self, variables: list):
        variables.remove('NA')
        if 'NA' in variables:
            self.removeNA(variables)

    def loadData(self):
        if self.rawDataPath.suffix == '.dta':
            self.data = pd.read_stata(self.rawDataPath,
                                      columns=self.columns, convert_categoricals=False, convert_dates=False,
                                      preserve_dtypes=False)
        else:  # self.rawDataFileType == '.csv':
            self.data = pd.read_csv(self.rawDataPath, usecols=self.columns)

        print(f'Finished loading {len(self.columns)} columns and {len(self.data)} rows of raw data '
              f'of type {self.rawDataPath.suffix}')

    def loadEncryptedData(self, linkToZip, linkInZip):
        with ZipFile(linkToZip) as myzip:
            if '.dta' in linkInZip:
                self.data = pd.read_stata(myzip.open(linkInZip, pwd=bytes(self.config['encryptionPW'], encoding='utf-8')),
                                          columns=self.columns, convert_categoricals=False, convert_dates=False,
                                          preserve_dtypes=False)
            else:  # if '.csv' in linkInZip:
                self.data = pd.read_csv(myzip.open(linkInZip, pwd=bytes('Eveisnotonlyanicename!', encoding='utf-8')),
                                        sep=';', decimal=',', usecols=self.columns)

        print(f'Finished loading {len(self.columns)} columns and {len(self.data)} rows of raw data '
              f'of type {self.rawDataPath.suffix}')

    def harmonizeVariables(self):
        replacementDict = self.createReplacementDict(self.datasetID, self.config['dataVariables'])
        dataRenamed = self.data.rename(columns=replacementDict)
        if self.datasetID == 'MiD08':
            dataRenamed['hhPersonID'] = (dataRenamed['hhID'].astype('string') + dataRenamed['personID'].astype('string')).astype('int')
        self.data = dataRenamed
        print('Finished harmonization of variables')

    def createReplacementDict(self, dataset, dictRaw):
        if dataset in dictRaw['dataset']:
            listIndex = dictRaw['dataset'].index(dataset)
            return {val[listIndex]: key for (key, val) in dictRaw.items()}
        else:
            raise ValueError(f'Data set {dataset} not specified in MiD variable dictionary.')

    def convertTypes(self):
        # Filter for dataset specific columns
        conversionDict = self.config['inputDTypes']
        keys = {iCol for iCol in conversionDict.keys() if iCol in self.data.columns}
        subDict = {key: conversionDict[key] for key in conversionDict.keys() & keys}
        self.data = self.data.astype(subDict)

    def checkFilterDict(self, filterDict: dict):
        # Currently only checking if list of list str not typechecked all(map(self.__checkStr, val)
        assert all(isinstance(val, list) for val in returnBottomDictValues(filterDict)), \
            f'All values in filter dictionaries have to be lists, but are not'

    def filter(self):
        print(f'Starting filtering, applying {len(returnBottomDictKeys(self.__filterDict))} filters.')
        ret = pd.DataFrame(index=self.data.index)
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

    def setIncludeFilter(self, includeFilterDict: dict, dataIndex):
        incFilterCols = pd.DataFrame(index=dataIndex, columns=includeFilterDict.keys())
        for incCol, incElements in includeFilterDict.items():
            incFilterCols[incCol] = self.data[incCol].isin(incElements)
        return incFilterCols

    def setExcludeFilter(self, excludeFilterDict: dict, dataIndex):
        exclFilterCols = pd.DataFrame(index=dataIndex, columns=excludeFilterDict.keys())
        for excCol, excElements in excludeFilterDict.items():
            exclFilterCols[excCol] = ~self.data[excCol].isin(excElements)
        return exclFilterCols

    def setGreaterThanFilter(self, greaterThanFilterDict: dict, dataIndex):
        greaterThanFilterCols = pd.DataFrame(index=dataIndex, columns=greaterThanFilterDict.keys())
        for greaterCol, greaterElements in greaterThanFilterDict.items():
            greaterThanFilterCols[greaterCol] = self.data[greaterCol] >= greaterElements.pop()
            if len(greaterElements) > 0:
                warnings.warn(f'You specified more than one value as lower limit for filtering column {greaterCol}.'
                              f'Only considering the last element given in the config.')
        return greaterThanFilterCols

    def setSmallerThanFilter(self, smallerThanFilterDict: dict, dataIndex):
        smallerThanFilterCols = pd.DataFrame(index=dataIndex, columns=smallerThanFilterDict.keys())
        for smallerCol, smallerElements in smallerThanFilterDict.items():
            smallerThanFilterCols[smallerCol] = self.data[smallerCol] <= smallerElements.pop()
            if len(smallerElements) > 0:
                warnings.warn(f'You specified more than one value as upper limit for filtering column {smallerCol}.'
                              f'Only considering the last element given in the config.')
        return smallerThanFilterCols

    def filterAnalysis(self, filterData):
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

    def addWeekdayStrColumn(self):
        self.data.loc[:, 'weekdayStr'] \
            = self.data.loc[:, 'tripStartWeekday'].replace(self.config['midReplacements']['tripStartWeekday'])

    def addPurposeStrColumn(self):
        self.data.loc[:, 'purposeStr'] \
            = self.data.loc[:, 'tripPurpose'].replace(self.config['midReplacements']['tripPurpose'])

    def addIndex(self):
        self.data['idxCol'] = self.data['hhPersonID'].astype('string') + '__' + self.data['tripID'].astype('string')
        self.data.set_index('idxCol', inplace=True, drop=True)

    def composeTimestamp(self, data: pd.DataFrame = None,
                         colYear: str = 'tripStartYear',
                         colWeek: str = 'tripStartWeek',
                         colDay: str = 'tripStartWeekday',
                         colHour: str = 'tripStartHour',
                         colMin: str = 'tripStartMinute',
                         colName: str = 'timestampStart'):
        data[colName] = pd.to_datetime(data.loc[:, colYear], format='%Y') + \
                         pd.to_timedelta(data.loc[:, colWeek] * 7, unit='days') + \
                         pd.to_timedelta(data.loc[:, colDay], unit='days') + \
                         pd.to_timedelta(data.loc[:, colHour], unit='hour') + \
                         pd.to_timedelta(data.loc[:, colMin], unit='minute')
        # return data

    def composeStartAndEndTimestamps(self):
        self.composeTimestamp(data=self.data)  # Starting timestamp
        self.composeTimestamp(data=self.data,  # Ending timestamps
                              colHour='tripEndHour',
                              colMin='tripEndMinute',
                              colName='timestampEnd')
        self.updateEndTimestamp()

    def updateEndTimestamp(self):
        endsFollowingDay = self.data['tripEndNextDay'] == 1
        self.data.loc[endsFollowingDay, 'timestampEnd'] = self.data.loc[endsFollowingDay,
                                                                        'timestampEnd'] + pd.offsets.Day(1)

    def calcNTripsPerDay(self):
        return self.data['hhPersonID'].value_counts().mean()

    def calcDailyTravelDistance(self):
        dailyDistances = self.data.loc[:, ['hhPersonID', 'tripDistance']].groupby(by=['hhPersonID']).sum()
        return dailyDistances.mean()

    def calcDailyTravelTime(self):
        travelTime = self.data.loc[:, ['hhPersonID', 'travelTime']].groupby(by=['hhPersonID']).sum()
        return travelTime.mean()

    def calcAverageTripDistance(self):
        return self.data.loc[:, 'tripDistance'].mean()


class ParseMID(ParseData):
    def __init__(self):
        super().__init__()



if __name__ == '__main__':
    linkConfig = Path.cwd().parent / 'config' / 'config.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    config = yaml.load(open(linkConfig), Loader=yaml.SafeLoader)
    p = ParseData(datasetID='MiD08', config=config, loadEncrypted=False, strColumns=True)
    print(p.data.head())
    print('end')