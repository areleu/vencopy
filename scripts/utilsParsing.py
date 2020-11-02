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

def harmonizeVariables(data, dataset, config):
    replacementDict = createReplacementDict(dataset, config['midVariables'])
    dataRenamed = data.rename(columns=replacementDict)
    if dataset == 'MiD08':
        dataRenamed['hhPersonID'] = dataRenamed['hhid'].astype('string') + '__' + \
                                    dataRenamed['hhPersonID'].astype('string')
    return dataRenamed

def createReplacementDict(dataset, dictRaw):
    if dataset in dictRaw['dataset']:
        listIndex = dictRaw['dataset'].index(dataset)
        return {val[listIndex]: key for (key, val) in dictRaw.items()}
    else:
        raise ValueError(f'Dataset {dataset} not specified in MiD variable dictionary.')

def removeNA(variables:list):
    variables.remove('NA')
    if 'NA' in variables:
        removeNA(variables)

def createListOfVariables(dataset:str, config):
    listIndex = config['midVariables']['dataset'].index(dataset)
    variables = [key if not val[listIndex] == 'NA' else 'NA' for key, val in config['midVariables'].items()]
    variables.remove('dataset')
    if 'NA' in variables:
        removeNA(variables)
    return variables

def filterOutTripsBelongingToMultiModalDays(data):
    hpIDs = pd.Series(data.loc[:, 'hhPersonID'].unique())
    idsWFirstTrip = data.loc[data.loc[:, 'tripID'] == 1, 'hhPersonID']
    return data.loc[data.loc[:, 'hhPersonID'].isin(idsWFirstTrip), :]

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
    endsFollowingDay = df['tripEndNextDay'] == 1
    df.loc[endsFollowingDay, 'timestamp_en'] = df.loc[endsFollowingDay, 'timestamp_en'] + pd.offsets.Day(1)

def calcHourlyShares(data, ts_st, ts_en):
    duration = tripDuration(data.loc[:, ts_st], data.loc[:, ts_en])
    data.loc[:, 'shareStartHour'], data.loc[:, 'shareEndHour'] = calcDistanceShares(data, duration)
    data.loc[:, 'noOfFullHours'] = numberOfFullHours(data.loc[:, ts_st], data.loc[:, ts_en])
    data.loc[:, 'fullHourTripLength'] = calcFullHourTripLength(duration, data.loc[:, 'noOfFullHours'],
                                                               data.loc[:, 'tripDistance'])
    return data

def initiateColRange(row):
    if row['tripStartHour'] + 1 < row['tripEndHour']:
        return range(row['tripStartHour'] + 1, row['tripEndHour'])  # The hour of arrival (tripEndHour) will not be indexed further below but is part of the range() object
    else:
        return None

class FillHourValues:
    def __init__(self, data, rangeFunction):
        # self.data = data
        self.startHour = data['tripStartHour']
        self.distanceStartHour = data['shareStartHour'] * data['tripDistance']
        self.endHour = data['tripEndHour']
        self.distanceEndHour = data['shareEndHour'] * data['tripDistance']
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
    # uniqueHHPersons = tripData.loc[:, 'hhPersonID'].unique()
    # dataDay = pd.DataFrame(index=uniqueHHPersons, columns=tripData.columns)
    dataDay = tripData.groupby(['hhPersonID']).sum()
    dataDay = dataDay.drop('tripID', axis=1)
    return dataDay

def mergeVariables(data, variableData, variables):
    variableDataUnique = variableData.loc[~variableData['hhPersonID'].duplicated(), :]
    variables.append('hhPersonID')
    variableDataMerge = variableDataUnique.loc[:, variables].set_index('hhPersonID')
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
    firstHour = tripData['tripStartHour']
    lastHour = tripData['tripEndHour']
    tripPurpose = tripData['tripPurpose']


class FillTripPurposes:
    def __init__(self, tripData, mergedDayTrips, rangeFunction=initiateColRange):
        self.startHour = tripData['tripStartHour']
        self.endHour = tripData['tripEndHour']
        self.tripHourCols = tripData.apply(rangeFunction, axis=1)
        self.purpose = tripData['tripPurpose']
        self.tripDict = tripData.loc[:, ['hhPersonID', 'tripID']].groupby(['hhPersonID']).list()

    def __call__(self, row):
        idx = row.name
        row[self.startHour[idx]] = 'DRIVING'
        if self.endHour[idx] != self.startHour[idx]:
            row[self.endHour[idx]] = self.distanceEndHour[idx]
        if isinstance(self.fullHourCols[idx], range):
            row[self.fullHourCols[idx]] = self.fullHourRange[idx]
        return row


def hoursToDatetime(tripData):
    tripData.loc[:, 'W_SZ_datetime'] = pd.to_datetime(tripData.loc[:, 'tripStartClock'])
    tripData.loc[:, 'W_AZ_datetime'] = pd.to_datetime(tripData.loc[:, 'tripEndClock'])
    return tripData

def fillDayPurposes(tripData, purposeDataDays):  #FixMe: Ask Ben for performance improvements
    hpID = str()
    maxWID = int()
    maxHour = len(purposeDataDays.columns)
    for idx in tripData.index:
        isSameHPID = hpID == tripData.loc[idx, 'hhPersonID']
        if not isSameHPID:
            hpID = tripData.loc[idx, 'hhPersonID']
            allWIDs = list(tripData.loc[tripData['hhPersonID'] == hpID, 'tripID'])
            maxWID = max(allWIDs)
        if tripData.loc[idx, 'tripID'] == 1:  # Differentiate if trip starts in first half hour or not
            if tripData.loc[idx, 'timestamp_st'].minute <= 30:
                purposeDataDays.loc[hpID, range(0, tripData.loc[idx, 'tripStartHour'])] = 'HOME'
            else:
                purposeDataDays.loc[hpID, range(0, tripData.loc[idx, 'tripStartHour'] + 1)] = 'HOME'
            if tripData.loc[idx, 'tripID'] == maxWID:
                if tripData.loc[idx, 'timestamp_en'].minute <= 30:
                    purposeDataDays.loc[hpID, range(tripData.loc[idx, 'tripEndHour'], maxHour)] = 'HOME'
                else:
                    purposeDataDays.loc[hpID, range(tripData.loc[idx, 'tripEndHour'] + 1, maxHour)] = 'HOME'
        else:
            purposeHourStart = determinePurposeStartHour(tripData.loc[idxOld, 'timestamp_st'],
                                                         tripData.loc[idxOld, 'timestamp_en'])
            if tripData.loc[idx, 'timestamp_st'].minute <= 30:
                hoursBetween = range(purposeHourStart, tripData.loc[idx, 'tripStartHour'])  # FIXME: case differentiation on arrival hour
            else:
                hoursBetween = range(purposeHourStart,
                                     tripData.loc[idx, 'tripStartHour'] + 1)
            purposeDataDays.loc[hpID, hoursBetween] = tripData.loc[idxOld, 'zweck_str']
            if tripData.loc[idx, 'tripID'] == maxWID:
                if tripData.loc[idx, 'timestamp_en'].minute <= 30:
                    purposeDataDays.loc[hpID, range(tripData.loc[idx, 'tripEndHour'], maxHour)] = 'HOME'
                else:
                    purposeDataDays.loc[hpID, range(tripData.loc[idx, 'tripEndHour'] + 1, maxHour)] = 'HOME'

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
