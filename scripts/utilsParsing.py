__version__ = '0.0.1'
__maintainer__ = 'Niklas Wulff 31.12.2019'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '31.12.2019'
__status__ = 'dev'  # options are: dev, test, prod


import pandas as pd
import numpy as np
from scripts.utilsTimestamp import *
from pathlib import Path

def createFileString(config, fileKey, dataset, filetypeStr='csv'):
    return "%s_%s_%s.%s" % (config['files'][dataset][fileKey],
                        config['labels']['runLabel'],
                        dataset,
                        filetypeStr)

def assignMultiColToDType(dataFrame, cols, dType):
    dictDType = dict.fromkeys(cols, dType)
    dfOut = dataFrame.astype(dictDType)
    return(dfOut)

def harmonizeVariables(data, dataset, config):
    replacementDict = createReplacementDict(dataset, config['dataVariables'])
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
    listIndex = config['dataVariables']['dataset'].index(dataset)
    variables = [key if not val[listIndex] == 'NA' else 'NA' for key, val in config['dataVariables'].items()]
    variables.remove('dataset')
    if 'NA' in variables:
        removeNA(variables)
    return variables

def filterOutTripsBelongingToMultiModalDays(data):
    idsWFirstTrip = data.loc[data.loc[:, 'tripID'] == 1, 'hhPersonID']
    return data.loc[data.loc[:, 'hhPersonID'].isin(idsWFirstTrip), :]

def calcHourlyShares(data, ts_st, ts_en):
    duration = tripDuration(data.loc[:, ts_st], data.loc[:, ts_en])
    data.loc[:, 'shareStartHour'], data.loc[:, 'shareEndHour'] = calcDistanceShares(data, duration, ts_st, ts_en)
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
    for idx, iRow in tripData.iterrows():
        isSameHPID = hpID == iRow['hhPersonID']
        if not isSameHPID:
            hpID = iRow['hhPersonID']
            allWIDs = list(tripData.loc[tripData['hhPersonID'] == hpID, 'tripID'])
            minWID = min(allWIDs)
            maxWID = max(allWIDs)
        if iRow['tripID'] == 1:  # Differentiate if trip starts in first half hour or not
            if iRow['timestampStart'].minute <= 30:
                purposeDataDays.loc[hpID, range(0, iRow['tripStartHour'])] = 'HOME'
            else:
                purposeDataDays.loc[hpID, range(0, iRow['tripStartHour'] + 1)] = 'HOME'
            if iRow['tripID'] == maxWID:
                if iRow['timestampEnd'].minute <= 30:
                    purposeDataDays.loc[hpID, range(iRow['tripEndHour'], maxHour)] = 'HOME'
                else:
                    purposeDataDays.loc[hpID, range(iRow['tripEndHour'] + 1, maxHour)] = 'HOME'
        elif iRow['tripID'] == minWID:
            if iRow['timestampStart'].minute <= 30:
                purposeDataDays.loc[hpID, range(0, iRow['tripStartHour'])] = 'HOME'
            else:
                purposeDataDays.loc[hpID, range(0, iRow['tripStartHour'] + 1)] = 'HOME'
            if iRow['tripID'] == maxWID:
                purposeDataDays.loc[hpID, range(iRow['tripEndHour'] + 1, maxHour)] = 'HOME'
        else:
            purposeHourStart = determinePurposeStartHour(tripData.loc[idxOld, 'timestampStart'],
                                                         tripData.loc[idxOld, 'timestampEnd'])
            if iRow['timestampStart'].minute <= 30:
                hoursBetween = range(purposeHourStart, iRow['tripStartHour'])  # FIXME: case differentiation on arrival hour
            else:
                hoursBetween = range(purposeHourStart,
                                     iRow['tripStartHour'] + 1)
            purposeDataDays.loc[hpID, hoursBetween] = tripData.loc[idxOld, 'purposeStr']
            if iRow['tripID'] == maxWID:
                if iRow['timestampEnd'].minute <= 30:
                    purposeDataDays.loc[hpID, range(iRow['tripEndHour'], maxHour)] = 'HOME'
                else:
                    purposeDataDays.loc[hpID, range(iRow['tripEndHour'] + 1, maxHour)] = 'HOME'
        idxOld = idx
    return purposeDataDays

def writeOut(config, dataset, dataDrive, dataPurpose):
    dataDrive.to_csv(Path(config['linksRelative']['input']) /
                     createFileString(config=config, fileKey='inputDataDriveProfiles', dataset=dataset), na_rep=0)
    dataPurpose.to_csv(Path(config['linksRelative']['input']) /
                       createFileString(config=config, fileKey='purposesProcessed', dataset=dataset))


### EXPERIMENTAL SANDBOX PART
# def fillDayPurposesPerformant(tripData, purposeDataDays):  #FixMe: Ask Ben for performance improvements
#     # This is an adaptation of fillDayPurposes()
#
# def merger(tripData, purposeDataDays):
#     hpID = str()
#     maxWID = int()
#     maxHour = len(purposeDataDays.columns)
#     for idx, iRow in tripData.iterrows():
#         isSameHPID = hpID == iRow['hhPersonID']
#         if not isSameHPID:
#             hpID = iRow['hhPersonID']
#             allWIDs = list(tripData.loc[tripData['hhPersonID'] == hpID, 'tripID'])
#             minWID = min(allWIDs)
#             maxWID = max(allWIDs)
#
#         idxOld = idx
#     return purposeDataDays
#
# def filler(tripData, purposeDataDays):
#     for idx, iRow in tripData.iterrows():
#         if iRow['tripID'] == 1:  # Differentiate if trip starts in first half hour or not
#             if iRow['timestampStart'].minute <= 30:
#                 purposeDataDays.loc[hpID, range(0, iRow['tripStartHour'])] = 'HOME'
#             else:
#                 purposeDataDays.loc[hpID, range(0, iRow['tripStartHour'] + 1)] = 'HOME'
#             if iRow['tripID'] == maxWID:
#                 if iRow['timestampEnd'].minute <= 30:
#                     purposeDataDays.loc[hpID, range(iRow['tripEndHour'], maxHour)] = 'HOME'
#                 else:
#                     purposeDataDays.loc[hpID, range(iRow['tripEndHour'] + 1, maxHour)] = 'HOME'
#         elif iRow['tripID'] == minWID:
#             if iRow['timestampStart'].minute <= 30:
#                 purposeDataDays.loc[hpID, range(0, iRow['tripStartHour'])] = 'HOME'
#             else:
#                 purposeDataDays.loc[hpID, range(0, iRow['tripStartHour'] + 1)] = 'HOME'
#             if iRow['tripID'] == maxWID:
#                 purposeDataDays.loc[hpID, range(iRow['tripEndHour'] + 1, maxHour)] = 'HOME'
#         else:
#             purposeHourStart = determinePurposeStartHour(tripData.loc[idxOld, 'timestampStart'],
#                                                          tripData.loc[idxOld, 'timestampEnd'])
#             if iRow['timestampStart'].minute <= 30:
#                 hoursBetween = range(purposeHourStart, iRow['tripStartHour'])  # FIXME: case differentiation on arrival hour
#             else:
#                 hoursBetween = range(purposeHourStart,
#                                      iRow['tripStartHour'] + 1)
#             purposeDataDays.loc[hpID, hoursBetween] = tripData.loc[idxOld, 'purposeStr']
#             if iRow['tripID'] == maxWID:
#                 if iRow['timestampEnd'].minute <= 30:
#                     purposeDataDays.loc[hpID, range(iRow['tripEndHour'], maxHour)] = 'HOME'
#                 else:
#                     purposeDataDays.loc[hpID, range(iRow['tripEndHour'] + 1, maxHour)] = 'HOME'
#
# Basic ideas for more performant code:
#     - first set all columns for each trip (independent of hhPersonID) (vectorized)
#     - then merge