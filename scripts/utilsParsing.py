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

def filterOutTripsBelongingToMultiModalDays(data):
    hpIDs = pd.Series(data.loc[:, 'HP_ID_Reg'].unique())
    idsWFirstTrip = data.loc[data.loc[:, 'W_ID'] == 1, 'HP_ID_Reg']
    return data.loc[data.loc[:, 'HP_ID_Reg'].isin(idsWFirstTrip), :]

def replaceDayNumbersByStrings(data):
    dict = {1: 'MON',
            2: 'TUE',
            3: 'WED',
            4: 'THU',
            5: 'FRI',
            6: 'SAT',
            7: 'SUN'}
    return data.replace(dict)

def replacePurposes(dataSeries, replacementDict):
    return dataSeries.replace(replacementDict)

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

def updateEndTimestamp(df):
    endsFollowingDay = df['W_FOLGETAG'] == 1
    df.loc[endsFollowingDay, 'timestamp_en'] = df.loc[endsFollowingDay, 'timestamp_en'] + pd.offsets.Day(1)

def calcHourlyShares(data, ts_st, ts_en):
    duration = tripDuration(data.loc[:, ts_st], data.loc[:, ts_en])
    data.loc[:, 'shareStartHour'], data.loc[:, 'shareEndHour'] = calcDistanceShares(data, duration)
    data.loc[:, 'noOfFullHours'] = numberOfFullHours(data.loc[:, ts_st], data.loc[:, ts_en])
    data.loc[:, 'fullHourTripLength'] = calcFullHourTripLength(duration, data.loc[:, 'noOfFullHours'],
                                                               data.loc[:, 'wegkm'])
    return data

def initiateColRange(row):
    if row['W_SZS'] + 1 < row['W_AZS']:
        return range(row['W_SZS']+1, row['W_AZS'])  # The hour of arrival (W_AZS) will not be indexed further below but is part of the range() object
    else:
        return None

class FillHourValues:
    def __init__(self, data, rangeFunction):
        # self.data = data
        self.startHour = data['W_SZS']
        self.distanceStartHour = data['shareStartHour'] * data['wegkm']
        self.endHour = data['W_AZS']
        self.distanceEndHour = data['shareEndHour'] * data['wegkm']
        self.fullHourCols = data.apply(rangeFunction, axis=1)
        self.fullHourRange = data['fullHourTripLength']

    def __call__(self, row):
        idx = row.name
        row[self.startHour[idx]] = self.distanceStartHour[idx]
        if self.endHour[idx] != self.startHour[idx]:
            row[self.endHour[idx]] = self.distanceEndHour[idx]
        if isinstance(self.fullHourCols[idx], range):
            row[self.fullHourCols[idx]] = self.fullHourRange[idx]
        return row

def fillDataframe(hourlyArray, fillFunction):
    hourlyArray = hourlyArray.apply(fillFunction, axis=1)
    return hourlyArray

def mergeTrips(tripData):
    # uniqueHHPersons = tripData.loc[:, 'HP_ID_Reg'].unique()
    # dataDay = pd.DataFrame(index=uniqueHHPersons, columns=tripData.columns)
    dataDay = tripData.groupby(['HP_ID_Reg']).sum()
    dataDay = dataDay.drop('W_ID', axis=1)
    return dataDay

def mergeVariables(data, variableData, variables):
    variableDataUnique = variableData.loc[~variableData['HP_ID_Reg'].duplicated(), :]
    variables.append('HP_ID_Reg')
    variableDataMerge = variableDataUnique.loc[:, variables].set_index('HP_ID_Reg')
    mergedData = pd.concat([variableDataMerge, data], axis=1)
    return mergedData

def replaceTripsBeforeFirstTrips(driveData, replacement:str):
    firstTripIdx = driveData.loc[~driveData.isnull()].index[0]
    driveData.loc[range(0, firstTripIdx)] = replacement
    return driveData

def assignDriving(driveData):
    # assign hours where drivData != 0/NA to 'driving'
    locationData = driveData.copy()
    locationData = locationData.where(locationData == 0, other='DRIVING')
    return locationData

def assignHome(driveData):
    pass

def assignPurpose(driveData, tripData):
    firstHour = tripData['W_SZS']
    lastHour = tripData['W_AZS']
    tripPurpose = tripData['zweck']


class FillTripPurposes:
    def __init__(self, tripData, mergedDayTrips, rangeFunction=initiateColRange):
        self.startHour = tripData['W_SZS']
        self.endHour = tripData['W_AZS']
        self.tripHourCols = tripData.apply(rangeFunction, axis=1)
        self.purpose = tripData['zweck']
        self.tripDict = tripData.loc[:, ['HP_ID_Reg', 'W_ID']].groupby(['HP_ID_Reg']).list()

    def __call__(self, row):
        idx = row.name
        row[self.startHour[idx]] = 'DRIVING'
        if self.endHour[idx] != self.startHour[idx]:
            row[self.endHour[idx]] = self.distanceEndHour[idx]
        if isinstance(self.fullHourCols[idx], range):
            row[self.fullHourCols[idx]] = self.fullHourRange[idx]
        return row


def fillDayPurposes(tripData, purposeDataDays):
    tripData.loc[:, 'W_SZ_datetime'] = pd.to_datetime(tripData.loc[:, 'W_SZ'])
    tripData.loc[:, 'W_AZ_datetime'] = pd.to_datetime(tripData.loc[:, 'W_AZ'])
    hpID = str()
    for idx in tripData.index:
        isSameHPID = hpID == tripData.loc[idx, 'HP_ID_Reg']
        if not isSameHPID:
            hpID = tripData.loc[idx, 'HP_ID_Reg']
            allWIDs = list(tripData.loc[tripData['HP_ID_Reg'] == hpID, 'W_ID'])

        if tripData.loc[idx, 'W_ID'] == 1:  # Differentiate if trip starts in first half hour or not
            if tripData.loc[idx, 'W_SZ_datetime'].minute <= 30:
                purposeDataDays.loc[hpID, range(0, tripData.loc[idx, 'W_SZS'])] = 'HOME'
            else:
                purposeDataDays.loc[hpID, range(0, tripData.loc[idx, 'W_SZS'] + 1)] = 'HOME'
            if tripData.loc[idx, 'W_ID'] == max(allWIDs):
                if tripData.loc[idx, 'W_AZ_datetime'].minute <= 30:
                    purposeDataDays.loc[hpID, range(tripData.loc[idx, 'W_AZS'], len(purposeDataDays.columns))] = 'HOME'
                else:
                    purposeDataDays.loc[hpID, range(tripData.loc[idx, 'W_AZS'] + 1, len(purposeDataDays.columns))] = 'HOME'
        else:
            purposeHourStart = determinePurposeStartHour(tripData.loc[idxOld, 'W_SZ_datetime'], tripData.loc[idxOld, 'W_AZ_datetime'])
            if tripData.loc[idx, 'W_SZ_datetime'].minute <= 30:
                hoursBetween = range(purposeHourStart, tripData.loc[idx, 'W_SZS'])  # FIXME: case differentiation on arrival hour
            else:
                hoursBetween = range(purposeHourStart,
                                     tripData.loc[idx, 'W_SZS']+1)
            purposeDataDays.loc[hpID, hoursBetween] = tripData.loc[idxOld, 'zweck_str']
            if tripData.loc[idx, 'W_ID'] == max(allWIDs):
                if tripData.loc[idx, 'W_AZ_datetime'].minute <= 30:
                    purposeDataDays.loc[hpID, range(tripData.loc[idx, 'W_AZS'], len(purposeDataDays.columns))] = 'HOME'
                else:
                    purposeDataDays.loc[hpID, range(tripData.loc[idx, 'W_AZS'] + 1, len(purposeDataDays.columns))] = 'HOME'

        idxOld = idx
    return purposeDataDays




# SANDBOX / OLD FUNCTION
#==================================
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
