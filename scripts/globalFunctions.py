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
    """
    Generic method used for fileString compilation throughout the VencoPy framework. This method does not write any
    files but just creates the file name including the filetype suffix.

    :param globalConfig: global config file for paths
    :param fileKey: Manual specification of fileKey
    :param datasetID: Manual specification of data set ID e.g. 'MiD17'
    :param manualLabel: Optional manual label to add to filename
    :param filetypeStr: filetype to be written to hard disk
    :return: Full name of file to be written.
    """

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
    """
    Global VencoPy function to merge MiD variables to trip distance, purpose or grid connection data.

    :param data: trip diary data as given by tripDiaryBuilder and gridModeler
    :param variableData: Survey data that holds specific variables for merge
    :param variables: Name of variables that will be merged
    :return: The merged data
    """

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


def writeProfilesToCSV(profileDictOut, globalConfig: dict, singleFile=True, datasetID='MiD17'):
    """
    Function to write VencoPy profiles to either one or five .csv files in the output folder specified in outputFolder.

    :param outputFolder: path to output folder
    :param profileDictOut: Dictionary with profile names in keys and profiles as pd.Series containing a VencoPy
    profile each to be written in value
    :param singleFile: If True, all profiles will be appended and written to one .csv file. If False, five files are
    written
    :param strAdd: String addition for filenames
    :return: None
    """

    if singleFile:
        dataOut = pd.DataFrame(profileDictOut)
        dataOut.to_csv(pathlib.Path(globalConfig['pathRelative']['dataOutput']) /
                       createFileString(globalConfig=globalConfig, fileKey='vencoPyOutput',
                                        datasetID=datasetID), header=True)
    else:
        for iName, iProf in profileDictOut.items():
            iProf.to_csv(pathlib.Path(globalConfig['pathRelative']['dataOutput']) /
                         pathlib.Path(r'vencoPyOutput_' + iName + datasetID + '.csv'), header=True)