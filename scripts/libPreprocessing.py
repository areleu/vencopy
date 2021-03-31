# -*- coding:utf-8 -*-

__version__ = '0.0.8'
__maintainer__ = 'Niklas Wulff 24.02.2020'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '24.02.2020'
__status__ = 'test'  # options are: dev, test, prod

# This file holds the function definitions for preprocessing after data input for VencoPy.

import warnings
from .libLogging import logit
from .libLogging import logger
import pandas as pd


def indexWeights(weights: pd.DataFrame) -> pd.DataFrame:
    weights = weights.convert_dtypes()
    return weights.set_index(['hhPersonID', 'tripStartWeekday'], drop=True)


def indexDriveAndPlugData(driveData: pd.DataFrame, plugData: pd.DataFrame, dropIdxLevel: str, nHours: int):
    driveProfiles = indexProfile(driveData, nHours)
    plugProfiles = indexProfile(plugData, nHours)
    return driveProfiles.droplevel(dropIdxLevel), plugProfiles.droplevel(dropIdxLevel)

@logit
def indexProfile(data, nHours):
    """
    Takes raw data as input and indices different profiles with the specified index columns und an unstacked form.

    :param driveProfiles_raw: Dataframe of raw drive profiles in km with as many index columns as elements
        of the list in given in indices. One column represents one timestep, e.g. hour.
    :param plugProfiles_raw: Dataframe of raw plug profiles as boolean values with as many index columns
        as elements of the list in given in indices. One column represents one timestep e.g. hour.
    :param indices: List of column names given as strings.
    :return: Two indexed dataframes with index columns as given in argument indices separated from data columns
    """

    indexCols = findIndexCols(data, nHours)
    data = data.convert_dtypes()  # Reduce column data types if possible (specifically hhPersonID column to int)
    dataIndexed = data.set_index(list(indexCols))

    # Typecast column indices to int for later looping over a range
    dataIndexed.columns = dataIndexed.columns.astype(int)
    return dataIndexed

def findIndexCols(data, nHours):
    dataCols = [str(i) for i in range(0, nHours+1)]
    return data.columns[~data.columns.isin(dataCols)]

@logit
def procScalars(driveProfiles_raw, plugProfiles_raw, driveProfiles, plugProfiles):
    """
    Calculates some scalars from the input data such as the number of hours of drive and plug profiles, the number of
    profiles etc.

    :param driveProfiles: Input drive profile input data frame with timestep specific driving distance in km
    :param plugProfiles: Input plug profile input data frame with timestep specific boolean grid connection values
    :return: Returns a dataframe of processed scalars including number of profiles and number of hours per profile
    """

    noHoursDrive = len(driveProfiles.columns)
    noHoursPlug = len(plugProfiles.columns)
    noDriveProfilesIn = len(driveProfiles)
    noPlugProfilesIn = len(plugProfiles)
    scalarsProc = {'noHoursDrive': noHoursDrive,
                   'noHoursPlug': noHoursPlug,
                   'noDriveProfilesIn': noDriveProfilesIn,
                   'noPlugProfilesIn': noPlugProfilesIn}
    if noHoursDrive == noHoursPlug:
        scalarsProc['noHours'] = noHoursDrive
    else:
        warnings.warn('Length of drive and plug input data differ! This will at the latest crash in calculating '
                      'profiles for SoC max')
    return scalarsProc

