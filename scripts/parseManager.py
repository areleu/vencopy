__version__ = '0.0.0'
__maintainer__ = 'Niklas Wulff 31.12.2019'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '31.12.2019'
__status__ = 'dev'  # options are: dev, test, prod

import time
import pprint
import pandas as pd
import profile
import cProfile
from pathlib import Path
import yaml

class ParseData:
    # Separate datasets that know each other
    def __init__(self, datasetID: str, config: dict):
        self.datasetID = datasetID
        self.config = config
        self.rawDataPath = Path(config['linksAbsolute'][self.datasetID]) / config['files'][self.datasetID]['tripsDataRaw']
        self.data = None
        self.columns = self.compileVariableList()
        self.__includeFilterDict = config['filterDicts'][self.datasetID]['include']  # columnName: elements
        self.__excludeFilterDict = config['filterDicts'][self.datasetID]['exclude']  # columnName: elements
        self.loadData()
        self.harmonizeVariables()
        self.convertTypes()
        self.checkFilterDict(self.__includeFilterDict)
        self.checkFilterDict(self.__excludeFilterDict)
        self.filter()

    def compileVariableList(self):
        listIndex = self.config['midVariables']['dataset'].index(self.datasetID)
        variables = [val[listIndex] if not val[listIndex] == 'NA' else 'NA' for key, val in config['midVariables'].items()]
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
            return pd.read_csv(self.rawDataPath, usecols=self.columns)
        print(f'Finished loading {len(self.columns)} columns and {len(self.data)} rows of raw data '
              f'of type {self.rawDataPath.suffix}')

    def harmonizeVariables(self):
        replacementDict = self.createReplacementDict(self.datasetID, self.config['midVariables'])
        dataRenamed = self.data.rename(columns=replacementDict)
        if self.datasetID == 'MiD08':
            dataRenamed['hhPersonID'] = dataRenamed['hhid'].astype('string') + '__' + \
                                        dataRenamed['hhPersonID'].astype('string')
        self.data = dataRenamed
        print('Finished harmonization of variables')

    def createReplacementDict(self, dataset, dictRaw):
        if dataset in dictRaw['dataset']:
            listIndex = dictRaw['dataset'].index(dataset)
            return {val[listIndex]: key for (key, val) in dictRaw.items()}
        else:
            raise ValueError(f'Data set {dataset} not specified in MiD variable dictionary.')

    def convertTypes(self):
        self.data = self.data.astype(self.config['inputDTypes'])

    def checkFilterDict(self, filterDict: dict):
        # Currently only checking if list of list str not typechecked all(map(self.__checkStr, val)
        assert all(isinstance(val, list) for val in filterDict.values()), \
            f'All values in filter dictionaries have to be lists, but are not'

    def filter(self):
        print(f'Starting filtering, applying {len(self.__includeFilterDict) + len(self.__excludeFilterDict)} filters.')
        ret = pd.DataFrame(index=self.data.index,
                           columns=set(self.__includeFilterDict.keys()) | set(self.__excludeFilterDict.keys()))
        for incCol, incEl in self.__includeFilterDict.items():
            ret[incCol] = self.data[incCol].isin(incEl)
        for excCol, excEl in self.__excludeFilterDict.items():
            ret[excCol] = ~self.data[excCol].isin(excEl)
        self.data = self.data[ret.all(axis='columns')]
        self.filterAnalysis(ret)

    def filterAnalysis(self, filterData):
        lenData = sum(filterData.all(axis='columns'))
        boolDict = {iCol: sum(filterData[iCol]) for iCol in filterData}
        print(f'The following values were taken into account after filtering:')
        pprint.pprint(boolDict)
        print(f"A total of {lenData} was taken into account")
        print(f'This corresponds to {lenData / len(filterData)* 100} percent of the original data')


class ParseMID(ParseData):
    def __init__(self):
        super().__init__()


if __name__ == '__main__':
    linkConfig = Path.cwd().parent / 'config' / 'config.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    config = yaml.load(open(linkConfig), Loader=yaml.SafeLoader)
    p = ParseData(datasetID='MiD17', config=config)
