__version__ = '0.0.1'
__maintainer__ = 'Niklas Wulff 31.12.2019'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '31.12.2019'
__status__ = 'dev'  # options are: dev, test, prod


import pandas as pd
import numpy as np
from scripts.utilsTimestamp import *

def assignMultiColToDType(dataFrame, cols, dType):
    dictDType = dict.fromkeys(cols, dType)
    dfOut = dataFrame.astype(dictDType)
    return(dfOut)


def assignTSToColViaDay(df, colYear, colMonth, colDay, colHour, colMin, colName):
    dfOut = df.copy()
    dfOut[colName] = [pd.Timestamp(year=dfOut.loc[x, colYear],
                                           month=dfOut.loc[x, colMonth],
                                           day=dfOut.loc[x, colDay],
                                           hour=dfOut.loc[x, colHour],
                                           minute=dfOut.loc[x, colMin]) for x in dfOut.index]
    return(dfOut)

def assignTSToColViaCWeek(df, colYear, colWeek, colDay, colHour, colMin, colName):
    dfOut = df.copy()
    dfOut[colName] = pd.to_datetime(df.loc[:, colYear], format='%Y') + \
                     pd.to_timedelta(df.loc[:, colWeek] * 7, unit='days') + \
                     pd.to_timedelta(df.loc[:, colDay], unit='days') + \
                     pd.to_timedelta(df.loc[:, colHour], unit='hour') + \
                     pd.to_timedelta(df.loc[:, colMin], unit='minute')
    return dfOut

def calcHourlyShares(data, ts_st, ts_en):
    duration = tripDuration(data.loc[:, ts_st], data.loc[:, ts_en])
    data.loc[:, 'shareStartHour'], data.loc[:, 'shareEndHour'] = calcDistanceShares(data, duration)
    data.loc[:, 'noOfFullHours'] = numberOfFullHours(data.loc[:, ts_st], data.loc[:, ts_en])
    data.loc[:, 'fullHourTripLength'] = calcFullHourTripLength(duration, data.loc[:, 'noOfFullHours'],
                                                               data.loc[:, 'wegkm'])
    return data


def fillDataframe(data, hourlyArray):
    hourlyArray.loc[:, data.loc[:, 'W_SZS']] = data.loc[:, 'shareStartHour'] * data.loc[:, 'wegkm']
    hourlyArray.loc[:, data.loc[:, 'W_AZS']] = data.loc[:, 'shareStartEnd'] * data.loc[:, 'wegkm']
    fullHourRange = data.apply(range(start=data.loc[:, 'W_SZS'] + 1, stop=data.loc[:, 'W_AZS']), axis=1)
    hourlyArray.loc[:, fullHourRange] = data.loc[:,]


def fillInHourlyTrips(dfData, dfZeros, colVal='wegkm_k', nHours=24):
    """
    Fills in an array with hourly columns in a given dfZeros with values from dfData's column colVal.

    :param dfData: Dataframe containing travel survey data
    :param dfZeros: Dataframe based on dfData with the same length but only limited columns containing id Data and hour columns
    :param colVal: Column name to retrieve values for columns from. Default:
    :param nHours: Number of hour columns to loop over for filling. If
    :return:
    """
    dfZerosOut = dfZeros.copy()
    for hour in range(nHours):
        rowsHourTrip = dfData.loc[:, 'st_std'] == hour
        rowsSameDayStart = rowsHourTrip & dfData.loc[:, 'st_dat'] == 0
        dfZerosOut.loc[rowsSameDayStart, hour] = dfData.loc[rowsSameDayStart, colVal]
        if hour + 24 < nHours:
            rowsNextDayStart = rowsHourTrip & dfData.loc[:, 'st_dat'] == 1
            dfZerosOut.loc[rowsNextDayStart, hour + 24] = dfData.loc[rowsNextDayStart, colVal]

    return(dfZerosOut)


def fillInMultiHourTrips(dfFill, dfData):
    pass
    for idx in dfFill.index:
        if dfFill.loc[idx, 'duration'] > pd.Timedelta(Hours=1):
            distance = dfFill.loc[idx, 'wegkm_k']
            durationInHours = dfFill.loc[idx, 'duration'] / pd.Timedelta(hours=1)
            startHour =  dfFill.loc[idx, 'st_std']
            stopFullHour = dfFill.loc[idx, 'st_std'] + round(durationInHours, 0)
            stopLastHour = stopFullHour + 1
            for hour in range(startHour, stopFullHour):
                dfFill.loc[idx, hour] = distance / round(durationInHours, 0)

            dfFill.loc[idx, stopLastHour] = distance / (durationInHours - round(durationInHours, 0))
