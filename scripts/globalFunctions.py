__version__ = '0.0.9'
__maintainer__ = 'Niklas Wulff 31.12.2019'
__contributors__ = 'Fabia Miorelli, Parth Butte'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '31.12.2019'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'

import pandas as pd
import yaml
import pathlib


def createFileString(globalConfig: dict, fileKey: str, datasetID: str=None, manualLabel: str = '',
                     filetypeStr: str = 'csv'):
    #pathGlobalConfig = pathlib.Path.cwd().parent / 'config' / 'globalConfig.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    #globalConfig = yaml.load(open(pathGlobalConfig), Loader=yaml.SafeLoader)
    if datasetID is None:

        return "%s_%s%s.%s" % (globalConfig['files'][fileKey],
                               globalConfig['labels']['runLabel'],
                               manualLabel,
                               filetypeStr)
    return "%s_%s%s_%s.%s" % (globalConfig['files'][datasetID][fileKey],
                              globalConfig['labels']['runLabel'],
                              manualLabel,
                              datasetID,
                              filetypeStr)


def mergeVariables(data, variableData, variables):
    variableDataUnique = variableData.loc[~variableData['hhPersonID'].duplicated(), :]
    variables.append('hhPersonID')
    variableDataMerge = variableDataUnique.loc[:, variables].set_index('hhPersonID')
    if 'hhPersonID' not in data.index.names:
        data.set_index('hhPersonID', inplace=True, drop=True)
    mergedData = pd.concat([variableDataMerge, data], axis=1)
    mergedData.reset_index(inplace=True)
    return mergedData


def mergeDataToWeightsAndDays(diaryData, ParseData):
    return mergeVariables(data=diaryData, variableData=ParseData.data, variables=['tripStartWeekday', 'tripWeight'])


def calculateWeightedAverage(col, weightCol):
    return sum(col * weightCol) / sum(weightCol)
