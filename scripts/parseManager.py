__version__ = '0.0.0'
__maintainer__ = 'Niklas Wulff 31.12.2019'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '31.12.2019'
__status__ = 'dev'  # options are: dev, test, prod

import time
import pandas as pd
from pathlib import Path
import yaml
import utilsParsing


class ParseData:
    def __init__(self, columnList: list, dataPath: Path):
        self.rawDataPath = dataPath
        self.rawDataFileType = self.rawDataPath.suffix
        self.columns = columnList
        self.__isInFilterDict = {}  # columnName: elements
        self.__isNotInFilterDict = {}  # columnName: elements

    def addFilter(self, filterDict: dict, filterType='isIn'):
        assert all(isinstance(val, list) for val in filterDict.values()) # Currently only checking if list of list str not typechecked all(map(self.__checkStr, val)
        assert filterType in ['isIn', 'isNotIn'], f'filterType has to be either "isIn" or "isNotIn", was {filterType}'
        if filterType == 'isIn':
            self.__isInFilterDict = filterDict
        elif filterType == 'isNotIn':
            self.__isNotInFilterDict = filterDict

    def parse(self):
        def filter(data, col, elements):
            return data.loc[data.loc[:, col] in elements, :]

        if self.rawDataFileType == '.dta':
            self.data = pd.read_stata(self.rawDataPath,
                      columns=self.columns, convert_categoricals=False, convert_dates=False, preserve_dtypes=False)
        else:  # self.rawDataFileType == '.csv':
            self.data = pd.read_csv(self.rawDataPath, usecols=self.columns)
        for iCol, iElements in self.__isInFilterDict.items():
            self.data = self.data.loc[self.data[iCol].isin(iElements), :]
        return self.data  # not tested yet


class ParseMID(ParseData):
    def __init__(self):
        super().__init__()


if __name__ == '__main__':
    linkConfig = Path.cwd().parent / 'config' / 'config.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    config = yaml.load(open(linkConfig), Loader=yaml.SafeLoader)
    colNames = [iVal[1] for (iKey, iVal) in config['midVariables'].items()]
    colNames.pop(0)
    p = ParseData(dataPath=Path(config['linksAbsolute']['MiD17']) / config['files']['MiD17']['tripsDataRaw'],
                  columnList=colNames)
    p.parse()
    data = p.data
    print(data.head())