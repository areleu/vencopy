__version__ = "0.1.X"
__maintainer__ = "Niklas Wulff"
__contributors__ = "Fabia Miorelli, Parth Butte"
__email__ = "niklas.wulff@dlr.de"
__birthdate__ = "31.12.2019"
__status__ = "prod"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"

import pandas as pd
import yaml
from pathlib import Path
import os


def loadConfigDict(basePath):
    # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    """
    Generic function to load and open yaml config files.

    :param configNames: Tuple containing names of config files to be loaded
    :return: Dictionary with opened yaml config files
    """
    configNames = ("user_config", "dev_config")
    configPath = basePath / "config"
    configDict = {}
    for configName in configNames:
        filePath = (configPath / configName).with_suffix(".yaml")
        with open(filePath) as ipf:
            configDict[configName] = yaml.load(ipf, Loader=yaml.SafeLoader)
    return configDict


def returnDictBottomKeys(baseDict: dict, lst: list = None) -> list:
    """
    Returns the lowest level keys of baseDict and returns all of them
    as a list. The parameter lst is used as
    interface between recursion levels.

    :param baseDict: Dictionary of variables
    :param lst: empty list, used as interface between recursion levels
    :return: Returns a list with all the bottom level dictionary keys
    """
    if lst is None:
        lst = []
    for iKey, iVal in baseDict.items():
        if isinstance(iVal, dict):
            lst = returnDictBottomKeys(iVal, lst)
        elif iVal is not None:
            lst.append(iKey)
    return lst


def returnDictBottomValues(baseDict: dict, lst: list = None) -> list:
    """
    Returns a list of all dictionary values of the last dictionary level
    (the bottom) of baseDict. The parameter
    lst is used as an interface between recursion levels.

    :param baseDict: Dictionary of variables
    :param lst: empty list, is used as interface to next recursion
    :return: Returns a list with all the bottom dictionary values
    """
    if lst is None:
        lst = []
    # iKey not used in returndictBottomValues but used in checkFilterDict to have lists
    for iKey, iVal in baseDict.items():
        if isinstance(iVal, dict):
            lst = returnDictBottomValues(iVal, lst)
        elif iVal is not None:
            lst.append(iVal)
    return lst


def replace_vec(series, year=None, month=None, day=None, hour=None, minute=None):
    return pd.to_datetime(
        {
            "year": series.dt.year if year is None else year,
            "month": series.dt.month if month is None else month,
            "day": series.dt.day if day is None else day,
            "hour": series.dt.hour if hour is None else hour,
            "minute": series.dt.minute if minute is None else minute,
        }
    )


def createOutputFolders(configDict: dict):
    """
    Function to crete vencopy output folder and subfolders

    :param: config dictionary
    :return: None
    """
    root = Path(configDict["user_config"]["global"]["pathAbsolute"]["vencopyRoot"])
    mainDir = "output"
    if not os.path.exists(Path(root / mainDir)):
        os.mkdir(Path(root / mainDir))
    subDirs = ("dataParser", "diaryBuilder", "gridModeler", "flexEstimator", "profileAggregator", "postProcessing")
    for subDir in subDirs:
        if not os.path.exists(Path(root / mainDir / subDir)):
            os.mkdir(Path(root / mainDir / subDir))


def createFileName(
    dev_config: dict, user_config: dict, manualLabel: str, fileNameID: str, datasetID: str, suffix: str = "csv"
):
    """
    Generic method used for fileString compilation throughout the VencoPy framework. This method does not write any
    files but just creates the file name including the filetype suffix.

    :param user_config: user config file for paths
    :param fileNameID: ID of respective data file as specified in global config
    :param datasetID: Manual specification of data set ID e.g. 'MiD17'
    :param manualLabel: Optional manual label to add to filename
    :param filetypeStr: filetype to be written to hard disk
    :return: Full name of file to be written.
    """
    if datasetID is None:
        return f"{dev_config['global']['diskFileNames'][fileNameID]}_{user_config['global']['runLabel']}_{manualLabel}.{suffix}"
    return f"{dev_config['global']['diskFileNames'][fileNameID]}_{user_config['global']['runLabel']}_{manualLabel}_{datasetID}.{suffix}"


def mergeVariables(data, variableData, variables):
    """
    Global VencoPy function to merge MiD variables to trip distance, purpose or grid connection data.

    :param data: trip diary data as given by tripDiaryBuilder and gridModeler
    :param variableData: Survey data that holds specific variables for merge
    :param variables: Name of variables that will be merged
    :return: The merged data
    """

    variableDataUnique = variableData.loc[~variableData["genericID"].duplicated(), :]
    variables.append("genericID")
    variableDataMerge = variableDataUnique.loc[:, variables].set_index("genericID")
    if "genericID" not in data.index.names:
        data.set_index("genericID", inplace=True, drop=True)
    mergedData = pd.concat([variableDataMerge, data], axis=1, join="inner")
    mergedData.reset_index(inplace=True)
    return mergedData


def mergeDataToWeightsAndDays(diaryData, ParseData):
    return mergeVariables(data=diaryData, variableData=ParseData.data, variables=["tripStartWeekday", "tripWeight"])


def calculateWeightedAverage(col, weightCol):
    return sum(col * weightCol) / sum(weightCol)


def writeOut(data: pd.DataFrame, path: Path):
    data.to_csv(path)
    print(f"Dataset written to {path}.")


def dumpReferenceData(data: pd.DataFrame, tag: str, path: Path):
    """_summary_

    Args:
        data (pd.DataFrame): _description_
        path (Path): _description_
    """
    path.mkdir(exist_ok=True, parents=True)
    data.to_hdf(path / f"{tag}.h5f", key=tag)
