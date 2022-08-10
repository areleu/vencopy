__version__ = '0.1.X'
__maintainer__ = 'Niklas Wulff'
__contributors__ = 'Fabia Miorelli, Parth Butte'
__email__ = 'niklas.wulff@dlr.de'
__birthdate__ = '31.12.2019'
__status__ = 'prod'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'

import pandas as pd
import yaml
from pathlib import Path
import os


def loadConfigDict(configNames: tuple, basePath):
    # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    """
    Generic function to load and open yaml config files

    :param configNames: Tuple containing names of config files to be loaded
    :return: Dictionary with opened yaml config files
    """
    configPath = basePath / 'config'
    configDict = {}
    for configName in configNames:
        filePath = (configPath / configName).with_suffix('.yaml')
        with open(filePath) as ipf:
            configDict[configName] = yaml.load(ipf, Loader=yaml.SafeLoader)
    return configDict


def createOutputFolders(configDict: dict):
    """
    Function to crete vencopy output folder and subfolders

    :param: config dictionary
    :return: None
    """
    root = Path(configDict['localPathConfig']['pathAbsolute']['vencoPyRoot'])
    mainDir = 'output'
    if not os.path.exists(Path(root / mainDir)):
        os.mkdir(Path(root / mainDir))
    subDirs = ('dataParser', 'diaryBuilder', 'gridModeler', 'flexEstimator', 'profileAggregator', 'evaluator')
    for subDir in subDirs:
        if not os.path.exists(Path(root / mainDir / subDir)):
            os.mkdir(Path(root / mainDir / subDir))


def createFileString(globalConfig: dict, fileKey: str, datasetID: str, manualLabel: str = '',
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
        return f"{globalConfig['files'][fileKey]}_{globalConfig['labels']['runLabel']}_{manualLabel}.{filetypeStr}"
    return f"{globalConfig['files'][datasetID][fileKey]}_{globalConfig['labels']['runLabel']}_{manualLabel}" \
           f"{datasetID}.{filetypeStr}"


def mergeVariables(data, variableData, variables):
    """
    Global VencoPy function to merge MiD variables to trip distance, purpose or grid connection data.

    :param data: trip diary data as given by tripDiaryBuilder and gridModeler
    :param variableData: Survey data that holds specific variables for merge
    :param variables: Name of variables that will be merged
    :return: The merged data
    """

    variableDataUnique = variableData.loc[~variableData['genericID'].duplicated(), :]
    variables.append('genericID')
    variableDataMerge = variableDataUnique.loc[:, variables].set_index('genericID')
    if 'genericID' not in data.index.names:
        data.set_index('genericID', inplace=True, drop=True)
    mergedData = pd.concat([variableDataMerge, data], axis=1, join='inner')
    mergedData.reset_index(inplace=True)
    return mergedData


def mergeDataToWeightsAndDays(diaryData, ParseData):
    return mergeVariables(data=diaryData, variableData=ParseData.data, variables=['tripStartWeekday', 'tripWeight'])


def calculateWeightedAverage(col, weightCol):
    return sum(col * weightCol) / sum(weightCol)


def writeProfilesToCSV(profileDictOut, globalConfig: dict, localPathConfig: dict, singleFile=True, datasetID='MiD17'):
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
        dataOut.to_csv(Path(localPathConfig['pathAbsolute']['vencoPyRoot']) / globalConfig['pathRelative']['flexOutput'] /
                       createFileString(globalConfig=globalConfig, fileKey='output',
                                        manualLabel=globalConfig['labels']['technologyLabel'], datasetID=datasetID),
                       header=True)
    else:
        for iName, iProf in profileDictOut.items():
            iProf.to_csv(Path(localPathConfig['pathAbsolute']['vencoPyRoot']) / globalConfig['pathRelative']['flexOutput'] /
                         Path(f'vencopy_{iName}_{datasetID}.csv'), header=True)


def writeOut(dataset, outputFolder, fileKey, manualLabel, datasetID, localPathConfig, globalConfig):
    dataset.to_csv(Path(localPathConfig['pathAbsolute']['vencoPyRoot']) / globalConfig['pathRelative'][outputFolder] /
                   createFileString(globalConfig=globalConfig, manualLabel=manualLabel, fileKey=fileKey,
                                    datasetID=datasetID))
    print(('Dataset written to ' + str(createFileString(globalConfig=globalConfig,
                                                        fileKey=fileKey,
                                                        datasetID=datasetID))))
