# -*- coding:utf-8 -*-

__version__ = '0.0.8'
__maintainer__ = 'Niklas Wulff 04.09.2020'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '24.02.2020'
__status__ = 'test'  # options are: dev, test, prod

# This file holds the function definitions for VencoPy input functions.

import io
import yaml
import pathlib
import getpass
import pandas as pd
import numpy as np
from enum import Enum, auto
from zipfile import ZipFile

from .libLogging import logit
from .libLogging import logger
from .utilsParsing import createFileString


@logit
def initializeLinkMgr(config, dataset):
    """
    Setup link manager based on a VencoPy config file.

    :param config: Config file initiated by a yaml-loader

    :return: Returns link dictionary with relative links to input data and output folders.
    """
    linkDict = {'linkScalars': pathlib.Path(config['linksRelative']['input']) /
                               pathlib.Path(config['files']['inputDataScalars']),
                'linkDriveProfiles': pathlib.Path(config['linksRelative']['input']) /
                                     pathlib.Path(createFileString(config=config, fileKey='inputDataDriveProfiles',
                                                                   dataset=dataset)),
                'linkPlugProfiles': pathlib.Path(config['linksRelative']['input']) /
                                    pathlib.Path(createFileString(config=config, fileKey='inputDataPlugProfiles',
                                                                  dataset=dataset)),
                'linkOutputConfig': pathlib.Path(config['linksRelative']['outputConfig']),
                'linkOutputAnnual': pathlib.Path(config['linksRelative']['resultsAnnual']),
                'linkPlots': pathlib.Path(config['linksRelative']['plots']),
                'linkOutput': pathlib.Path(config['linksRelative']['resultsDaily'])}
    return linkDict


class Assumptions(Enum):
    minDailyMileage = auto()
    batteryCapacity = auto()
    electricConsumption = auto()
    fuelConsumption = auto()
    electricConsumptionCorr = auto()
    fuelConsumptionCorr = auto()
    maximumSOC = auto()
    minimumSOC = auto()
    powerChargingStation = auto()
    isBEV = auto()


@logit
def readInputScalar(filePath):
    """
    Method that gets the path to a venco scalar input file specifying technical assumptions such as battery capacity
    specific energy consumption, usable battery capacity share for load shifting and charge power.

    :param filePath: The relative file path to the input file
    :return: Returns a dataframe with an index column and two value columns. The first value column holds numbers the
        second one holds units.
    """

    scalarInput = Assumptions
    inputRaw = pd.read_excel(filePath,
                             header=5,
                             usecols='A:C',
                             skiprows=0)
    scalarsOut = inputRaw.set_index('parameter')
    return scalarsOut


@logit
def readInputCSV(filePath):
    """
    Reads input and cuts out value columns from a given CSV file.

    :param filePath: Relative file path to CSV file
    :return: Pandas dataframe with raw input from CSV file
    """
    inputRaw = pd.read_csv(filePath, header=0)
    inputData = inputRaw.loc[:, ~inputRaw.columns.str.match('Unnamed')]
    return inputData


@logit
def stringToBoolean(df):
    """
    Replaces given strings with python values for true or false.
    FixMe: Foreseen to be more flexible in next release.

    :param df: Dataframe holding strings defining true or false values
    :return: Dataframe holding true and false
    """

    dictBol = {'WAHR': True,
               'FALSCH': False}
    outBool = df.replace(to_replace=dictBol, value=None)
    return outBool


@logit
def readInputBoolean(filePath):
    """
    Wrapper function for reading boolean data from CSV.

    :param filePath: Relative path to CSV file
    :return: Returns a dataframe with boolean values
    """

    inputRaw = readInputCSV(filePath)
    inputData = stringToBoolean(inputRaw)
    return inputData


@logit
def readVencoInput(config, dataset):
    """
    Initializing action for VencoPy-specific config-file, link dictionary and data read-in. The config file has
    to be a dictionary in a .yaml file containing three categories: linksRelative, linksAbsolute and files. Each
    category must contain itself a dictionary with the linksRelative to data, functions, plots, scripts, config and
    tsConfig. Absolute links should contain the path to the output folder. Files should contain a link to scalar input
    data, and the two timeseries files inputDataDriveProfiles and inputDataPlugProfiles.

    :param config: A yaml config file holding a dictionary with the keys 'linksRelative' and 'linksAbsolute'
    :return: Returns four dataframes: A link dictionary, scalars, drive profile data and plug profile
    data, the latter three ones in a raw data format.
    """

    linkDict = initializeLinkMgr(config, dataset)

    # review: have you considered using the logging module for these kind of outputs?
    print('Reading Venco input scalars, drive profiles and boolean plug profiles')

    scalars = readInputScalar(linkDict['linkScalars'])
    driveProfiles_raw = readInputCSV(linkDict['linkDriveProfiles'])
    plugProfiles_raw = readInputBoolean(linkDict['linkPlugProfiles'])

    print('There are ' + str(len(driveProfiles_raw)) + ' drive profiles and ' +
          str(len(driveProfiles_raw)) + ' plug profiles.')

    return linkDict, scalars, driveProfiles_raw, plugProfiles_raw


def readZipData(filePath):
    """
    Opening the zip file in READ mode and transform scalars.csv to data frame
    :param filePath: path to zip-file
    :return: data frame with scalars.csv content
    """
    with ZipFile(filePath.as_posix(), 'r') as zip:
        scalars = None
        for i in zip.namelist():
            if i.endswith('Scalars.csv'):
                scalars = i
                break
        print('Reading', scalars)
        if scalars is None:
            print('No scalars file exists in zip file!')
            return pd.DataFrame()
        scalars = zip.read(scalars)
        # allow colon and semicolon as separators
        df = pd.read_csv(io.BytesIO(scalars), sep=',|;')
    return df


def readEncryptedFile(filePath, fileName):
    print('Starting extraction of encrypted zipfile. Password required')
    pw = getpass.getpass(stream=None)
    with ZipFile(filePath, 'r') as zip:
        for iFile in zip.namelist():
            if iFile.endswith(fileName):
                #trips = zip.read(iFile, pwd=bytes(pw, 'utf-8'))
                trips = zip.read(iFile, pwd=pw)
                if fileName.endswith('.csv'):
                    return pd.read_csv(io.BytesIO(trips))
                elif fileName.endswith('.dta'):
                    return pd.read_stata(io.BytesIO(trips))


def returnBottomDictValues(baseDict: dict, lst: list = []):
    for iKey, iVal in baseDict.items():
        if isinstance(iVal, dict):
            lst = returnBottomDictValues(iVal, lst)
        else:
            if iVal is not None:
                lst.append(iVal)
    return lst


def returnBottomDictKeys(baseDict: dict, lst: list = None):
    if lst is None:
        lst = []
    for iKey, iVal in baseDict.items():
        if isinstance(iVal, dict):
            lst = returnBottomDictKeys(iVal, lst)
        else:
            if iVal is not None:
                lst.append(iKey)
    return lst


def returnBottomKeys(self, baseDict: dict, lst: list = []):
    for iKey, iVal in baseDict.items():
        if isinstance(iVal, dict):
            self.returnValueList(iVal, lst)
        else:
            lst.append(iKey)
    return lst