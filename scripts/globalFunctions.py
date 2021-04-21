__version__ = '0.0.9'
__maintainer__ = 'Niklas Wulff 31.12.2019'
__contributors__ = 'Fabia Miorelli, Parth Butte'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '31.12.2019'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'

import pandas as pd
import numpy as np
import warnings

def createFileString(config: dict, fileKey: str, dataset: str = None, manualLabel: str = '',
                     filetypeStr: str = 'csv'):
    if dataset is None:
        return "%s_%s%s.%s" % (config['files'][fileKey],
                               config['labels']['runLabel'],
                               manualLabel,
                               filetypeStr)
    return "%s_%s%s_%s.%s" % (config['files'][dataset][fileKey],
                              config['labels']['runLabel'],
                              manualLabel,
                              dataset,
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