# -*- coding:utf-8 -*-

__version__ = '0.0.8'
__maintainer__ = 'Niklas Wulff 04.09.2020'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '24.02.2020'
__status__ = 'test'  # options are: dev, test, prod

# This file holds the function definitions for VencoPy input functions

import yaml
import pandas as pd
from enum import Enum, auto
import pathlib

def initializeLinkMgr(config):
    """
    Setup link manager based on a VencoPy config file.

    :param config: Config file initiated by a yaml-loader

    :return: Returns link dictionary with relative links to input data and output folders.
    """
    linkDict = {'linkScalars': pathlib.Path(config['linksRelative']['input']) /
                               pathlib.Path(config['files']['inputDataScalars']),
                'linkDriveProfiles': pathlib.Path(config['linksRelative']['input']) /
                                     pathlib.Path(config['files']['inputDataDriveProfiles']),
                'linkPlugProfiles': pathlib.Path(config['linksRelative']['input']) /
                                    pathlib.Path(config['files']['inputDataPlugProfiles']),
                'linkOutputConfig': pathlib.Path(config['linksRelative']['outputConfig']),
                'linkOutputAnnual': pathlib.Path(config['linksRelative']['resultsAnnual']),
                'linkPlots': pathlib.Path(config['linksRelative']['plots']),
                'linkOutput': pathlib.Path(config['linksRelative']['resultsDaily'])}
    return linkDict


def readInputScalar(filePath):
    """
    Method that gets the path to a venco scalar input file specifying technical assumptions such as battery capacity
    specific energy consumption, usable battery capacity share for load shifting and charge power.

    :param filePath: The relative file path to the input file
    :return: Returns a dataframe with an index column and two value columns. The first value column holds numbers the
        second one holds units.
    """

    inputRaw = pd.read_excel(filePath,
                              header=5,
                              usecols='A:C',
                              skiprows=0)
    scalarsOut = inputRaw.set_index('parameter')
    return scalarsOut


def readInputCSV(filePath):
    """
    Reads input and cuts out value columns from a given CSV file.

    :param filePath: Relative file path to CSV file
    :return: Pandas dataframe with raw input from CSV file
    """
    inputRaw = pd.read_csv(filePath, header=0)
    inputData = inputRaw.loc[:, ~inputRaw.columns.str.match('Unnamed')]
    return inputData


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
    return (outBool)


def readInputBoolean(filePath):
    """
    Wrapper function for reading boolean data from CSV.

    :param filePath: Relative path to CSV file
    :return: Returns a dataframe with boolean values
    """

    inputRaw = readInputCSV(filePath)
    inputData = stringToBoolean(inputRaw)
    return inputData


def readVencoInput(config):
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

    linkDict = initializeLinkMgr(config)

    # review: have you considered using the logging module for these kind of outputs?
    print('Reading Venco input scalars, drive profiles and boolean plug profiles')

    scalars = readInputScalar(linkDict['linkScalars'])
    driveProfiles_raw = readInputCSV(linkDict['linkDriveProfiles'])
    plugProfiles_raw = readInputBoolean(linkDict['linkPlugProfiles'])

    print('There are ' + str(len(driveProfiles_raw)) + ' drive profiles and ' +
                    str(len(driveProfiles_raw)) + ' plug profiles.')

    return linkDict, scalars, driveProfiles_raw, plugProfiles_raw