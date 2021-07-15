__version__ = '0.0.9'
__maintainer__ = 'Niklas Wulff 31.12.2019'
__contributors__ = 'Fabia Miorelli, Parth Butte'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '31.12.2019'
__status__ = 'dev'  # options are: dev, test, prod


import pandas as pd
import numpy as np
from typing import Callable
from pathlib import Path
import yaml
import os
from scripts.globalFunctions import createFileString



class TripDiaryBuilder:
    def __init__(self, tripConfig: dict, globalConfig: dict, ParseData, datasetID: str = 'MiD17'):
        self.tripConfig = tripConfig
        self.globalConfig = globalConfig
        self.parsedData = ParseData
        self.tripDataClean = None
        self.tripDistanceDiary = None
        self.tripPurposeDiary = None
        # self.tripDataClean = self.calculateConsistentHourlyShares(data=ParseData.data)
        # ONLY FOR DEBUGGING PURPOSES
        self.tripDataClean = self.calculateConsistentHourlyShares(data=ParseData.data.loc[0:2000, :])
        self.tripDistanceDiary = self.tripDistanceAllocation(globalConfig)
        self.tripPurposeAllocation()
        self.writeOut(globalConfig=globalConfig, datasetID=datasetID, dataDrive=self.tripDistanceDiary,
                 dataPurpose=self.tripPurposeDiary)

    def tripDuration(self, timestampStart: np.datetime64, timestampEnd: np.datetime64) -> np.datetime64:
        """
        :param timestampStart: start time of a trip
        :param timestampEnd:  end time of a trip
        :return: Returns trip duration
        """
        return timestampEnd - timestampStart

    def calcHourShareStart(self, timestampStart: np.datetime64, timestampEnd: np.datetime64, duration) -> (pd.Series,
                                                                                                           pd.Series):
        """
        :param timestampStart: start time of a trip
        :param timestampEnd:  end time of a trip
        :param duration: duration of a trip
        :return: Returns a data frame of share of individual trip for trips completed in an hour and more w.r.t start time of the trip
        """
        isSameHourTrip = timestampStart.dt.hour == timestampEnd.dt.hour
        shareSameHour = (timestampEnd.dt.minute - timestampStart.dt.minute) / (duration.dt.seconds / 60)
        shareSameHour[duration == pd.Timedelta(0)] = 1  # Set share of first hour to 1 for trips with duration of 0
        share = pd.Series(shareSameHour.where(isSameHourTrip,
                                              (60 - timestampStart.dt.minute) / (duration.dt.seconds / 60)))
        return share.copy(), isSameHourTrip

    def calcHourShareEnd(self, timestampEnd: np.datetime64, duration, isSameHourTrip: pd.DataFrame) -> pd.Series:
        """
        :param timestampEnd: end time of a trip
        :param duration: duration of a trip
        :param isSameHourTrip: data frame containing same start time of various trips
        :return: Returns a data frame of share of individual trip for trips completed in an hour and more w.r.t end time of the trip
        """
        share = timestampEnd.dt.minute / (duration.dt.seconds / 60)
        return share.where(~isSameHourTrip, 0)

    def calcDistanceShares(self, data: pd.DataFrame, duration, timestampSt: np.datetime64,
                           timestampEn: np.datetime64) -> tuple:
        """
        :param data: list of strings declaring the datasetIDs to be read in
        :param duration: duration of a trip
        :param timestampSt:  start time of a trip
        :param timestampEn:  end time of a trip
        :return: Return a data frame of distance covered by each trip in an hour or more
        """
        shareHourStart, isSameHourTrip = self.calcHourShareStart(timestampStart=data.loc[:, timestampSt],
                                                                 timestampEnd=data.loc[:, timestampEn],
                                                                 duration=duration)
        shareHourEnd = self.calcHourShareEnd(timestampEnd=data.loc[:, timestampEn], duration=duration,
                                             isSameHourTrip=isSameHourTrip)
        return shareHourStart.copy(), shareHourEnd.copy()

    def numberOfFullHours(self, timestampStart: np.datetime64, timestampEnd: np.datetime64) -> pd.DataFrame:
        """
        :param timestampStart:  start time of a trip
        :param timestampEnd: end time of a trip
        :return: Returns a data frame of number of unutilized hours of individual trip #need to confirm this
        """
        timedeltaTrip = timestampEnd - timestampStart
        numberOfHours = timedeltaTrip.apply(lambda x: x.components.hours)
        numberOfDays = timedeltaTrip.apply(lambda x: x.components.days)
        minLeftFirstHour = pd.to_timedelta(60 - timestampStart.dt.minute, unit='m')
        hasFullHourAfterFirstHour = (timedeltaTrip - minLeftFirstHour) >= pd.Timedelta(1, unit='h')
        numberOfHours = numberOfHours.where(hasFullHourAfterFirstHour, other=0)
        return numberOfHours.where(numberOfDays != -1, other=0)

    def calcFullHourTripLength(self, duration, numberOfFullHours: Callable, tripLength: pd.DataFrame) -> pd.DataFrame:
        """
        :param duration: duration of a trip
        :param numberOfFullHours:
        :param tripLength: data frame of individual trip length
        :return: Returns a data frame of hourly distance covered by each trip
        """
        fullHourTripLength = (numberOfFullHours / (duration.dt.seconds / 3600)) * tripLength
        fullHourTripLength.loc[duration == pd.Timedelta(0)] = 0  # set trip length to 0 that would otherwise be NaN
        return fullHourTripLength

    def calcHourlyShares(self, data: pd.DataFrame, ts_st: np.datetime64, ts_en: np.datetime64) -> pd.DataFrame:
        """
        :param data: list of strings declaring the datasetIDs to be read in
        :param ts_st: start time of a trip
        :param ts_en: end time of a trip
        :return: data frame consisting additional information regarding share of a trip, number of full hours and lenght of each trip
        """
        duration = self.tripDuration(data.loc[:, ts_st], data.loc[:, ts_en])
        data.loc[:, 'shareStartHour'], data.loc[:, 'shareEndHour'] = self.calcDistanceShares(data, duration, ts_st, ts_en)
        data.loc[:, 'noOfFullHours'] = self.numberOfFullHours(data.loc[:, ts_st], data.loc[:, ts_en])
        data.loc[:, 'fullHourTripLength'] = self.calcFullHourTripLength(duration, data.loc[:, 'noOfFullHours'],
                                                                   data.loc[:, 'tripDistance'])
        return data

    def calculateConsistentHourlyShares(self, data: pd.DataFrame):
        print('Calculating hourly shares')
        if not data._is_view:
            data = data.copy()  #FIXME: why is data._is_view False if we get a view
        tripDataWHourlyShares = self.calcHourlyShares(data, ts_st='timestampStart', ts_en='timestampEnd')

        # Filter out implausible hourly share combinations
        return tripDataWHourlyShares.loc[~((tripDataWHourlyShares['shareStartHour'] != 1) &
                                                       (tripDataWHourlyShares['shareEndHour'] == 0) &
                                                       (tripDataWHourlyShares['noOfFullHours'] == 0)), :]

    def initiateHourDataframe(self, indexCol, nHours: int) -> pd.DataFrame:
        """
        Sets up an empty dataframe to be filled with hourly data.

        :param indexCol: List of column names
        :param nHours: integer giving the number of columns that should be added to the dataframe
        :return: data frame with columns given and nHours additional columns appended with 0s
        """
        emptyDf = pd.DataFrame(index=indexCol, columns=range(nHours))
        return (emptyDf)

    def fillDataframe(self, hourlyArray: pd.DataFrame, fillFunction) -> pd.DataFrame:
        hourlyArray = hourlyArray.apply(fillFunction, axis=1)
        return hourlyArray

    def mergeTrips(self, tripData: pd.DataFrame) -> pd.DataFrame:
        dataDay = tripData.groupby(['hhPersonID']).sum()
        dataDay = dataDay.drop('tripID', axis=1)
        return dataDay

    def initiateColRange(self, row):
        if row['tripStartHour'] + 1 < row['tripEndHour']:
            return range(row['tripStartHour'] + 1, row['tripEndHour'])  # The hour of arrival (tripEndHour) will
            # not be indexed further below but is part of the range() object
        else:
            return None

    def tripDistanceAllocation(self, globalConfig : dict):
        print('Trip distance diary setup starting')
        self.formatDF = self.initiateHourDataframe(indexCol=self.tripDataClean.index, nHours=globalConfig['numberOfHours'])
        fillHourValues = FillHourValues(data=self.tripDataClean, rangeFunction=self.initiateColRange)
        driveDataTrips = self.fillDataframe(self.formatDF, fillFunction=fillHourValues)
        driveDataTrips.loc[:, ['hhPersonID', 'tripID']] = pd.DataFrame(self.tripDataClean.loc[:, ['hhPersonID',
                                                                                                  'tripID']])
        driveDataTrips = driveDataTrips.astype({'hhPersonID': int, 'tripID': int})
        return self.mergeTrips(driveDataTrips)
        print('Finished trip distance diary setup')

    def assignDriving(self, driveData: pd.DataFrame) -> pd.DataFrame:
        """
        Assign hours where driveData != 0/NA to 'driving'

        :param driveData: driving data
        :return: Returns driving data with 'driving' instead of hours having 0/NA
        """
        locationData = driveData.copy()
        locationData = locationData.where(locationData == 0, other='DRIVING')
        return locationData

    def determinePurposeStartHour(self, departure: np.datetime64, arrival: np.datetime64):
        """

        :param departure:  start time of a trip
        :param arrival: end time of a trip
        :return: Returns more specific start hour of a trip when departure hour and arrival hour of a trip are equal
        """

        if departure.hour == arrival.hour:
            if arrival.minute >= 30:  # Cases 3, 4, 5
                startHour = departure.hour + 1  # Cases 3,5
            else:  # arrival.minute < 30:
                startHour = departure.hour  # Case 4
        else:  # inter-hour trip
            if arrival.minute <= 30:
                startHour = arrival.hour  # Cases 1a and b
            else:  # arrival.minute > 30:
                startHour = arrival.hour + 1  # Cases 2a and b
        return startHour

    def fillDayPurposes(self, tripData: pd.DataFrame, purposeDataDays: pd.DataFrame) -> pd.DataFrame:  # FixMe: Ask Ben for performance improvements
        """
        :param tripData: data frame holding all the information about individual trip
        :param purposeDataDays:
        :return: Returns a data frame of individual trip with it's hourly activity or location of trip
        """
        hpID = str()
        maxWID = int()
        maxHour = len(purposeDataDays.columns)

        # # uniques = tripData['hhPersonID'].unique()
        #
        # for iSubData in tripData.groupby('hhPersonID'):
        #
        #     currentPerson = tripData['hhPersonID'] == hpID
        #     allWIDs = tripData.loc[currentPerson, 'tripID']  # FIXME perf
        #     minWID = allWIDs.min()  # FIXME perf
        #     maxWID = allWIDs.max()  # FIXME perf
        #
        #     isFirstTripID = iSubData['tripID'] == 1
        #
        #     isBelowHalfHour = iSubData['timestampStart'].dt.minute <= 30
        #
        #     isMaxWID = iSubData['tripID'] == maxWID
        #
        #     isMinWID = iSubData['tripID'] == minWID
        #
        #     arrivalEqualsDeparture = iSubData['timestampEnd'].dt.hour == iSubData['timestampStart'].dt.hour
        #
        #     arrivalIsBelowHalfHour = iSubData['timestampStart'].dt.hour <= 30

        # Solution 1: use enumerate in order to get rowNumber instead of index and then .iloc below
        # Solution 2: Rename columns
        for idx, iRow in tripData.iterrows():
            isSameHPID = hpID == iRow['hhPersonID']
            if not isSameHPID:
                hpID = iRow['hhPersonID']
                allWIDs = tripData.loc[tripData['hhPersonID'] == hpID, 'tripID']  # FIXME perf
                minWID = allWIDs.min()  # FIXME perf
                maxWID = allWIDs.max()  # FIXME perf

            if iRow['tripID'] == 1:  # Differentiate if trip starts in first half hour or not

                if iRow['timestampStart'].minute <= 30:
                    # purposeDataDays.loc[hpID, 0:iRow['tripStartHour']] = 'HOME'  # FIXME perf
                    purposeDataDays.loc[hpID, range(0, iRow['tripStartHour'])] = 'HOME'
                else:
                    purposeDataDays.loc[hpID, range(0, iRow['tripStartHour'] + 1)] = 'HOME'
                if iRow['tripID'] == maxWID:
                    if iRow['timestampEnd'].minute <= 30:
                        purposeDataDays.loc[hpID, range(iRow['tripEndHour'], maxHour)] = 'HOME'
                    else:
                        purposeDataDays.loc[hpID, range(iRow['tripEndHour']+1, maxHour)] = 'HOME'
            elif iRow['tripID'] == minWID:
                if iRow['timestampStart'].minute <= 30:
                    purposeDataDays.loc[hpID, range(0, iRow['tripStartHour'])] = 'HOME'
                else:
                    purposeDataDays.loc[hpID, range(0, iRow['tripStartHour'] + 1)] = 'HOME'
                if iRow['tripID'] == maxWID:
                    purposeDataDays.loc[hpID, range(iRow['tripEndHour'] + 1, maxHour)] = 'HOME'
            else:
                purposeHourStart = self.determinePurposeStartHour(tripData.loc[idxOld, 'timestampStart'],
                                                             tripData.loc[idxOld, 'timestampEnd'])  # FIXME perf?
                if iRow['timestampStart'].minute <= 30:
                    hoursBetween = range(purposeHourStart,
                                         iRow['tripStartHour'])  # FIXME: case differentiation on arrival hour
                else:
                    hoursBetween = range(purposeHourStart,
                                         iRow['tripStartHour'] + 1)
                purposeDataDays.loc[hpID, hoursBetween] = tripData.loc[idxOld, 'purposeStr']

                # NEW PERFORMANCE IMPROVED SNIPPET
                # if iRow['timestampStart'].minute <= 30:
                #     purposeDataDays.loc[hpID, range(purposeHourStart,
                #                                     iRow['tripStartHour'])] = tripData.loc[idxOld, 'purposeStr']
                #     # hoursBetween = range(purposeHourStart,
                #     #                      iRow['tripStartHour'])  # FIXME: case differentiation on arrival hour
                # else:
                #     # hoursBetween = range(purposeHourStart,
                #     #                      iRow['tripStartHour'] + 1)
                #     purposeDataDays.loc[hpID, purposeHourStart:iRow['tripStartHour'] + 1] = tripData.loc[idxOld, 'purposeStr']
                if iRow['tripID'] == maxWID:
                    if iRow['timestampEnd'].minute <= 30:
                        purposeDataDays.loc[hpID, range(iRow['tripEndHour'], maxHour)] = 'HOME'
                    else:
                        purposeDataDays.loc[hpID, range(iRow['tripEndHour'] + 1, maxHour)] = 'HOME'
            idxOld = idx
        return purposeDataDays

    def tripPurposeAllocation(self):
        print('Starting trip purpose diary setup')
        tripPurposesDriving = self.assignDriving(self.tripDistanceDiary)
        self.tripPurposeDiary = self.fillDayPurposes(self.tripDataClean, tripPurposesDriving)
        self.tripPurposeDiary.replace({'0.0': 'HOME'})  # Replace remaining non-allocated purposes with HOME
        print('Finished purpose replacements')
        print(f'There are {len(self.tripPurposeDiary)} daily trip diaries.')

    # improved purpose allocation approach
    def mapHHPIDToTripID(self, tripData):
        idCols = self.tripDataClean.loc[:, ['hhPersonID', 'tripID']]
        idCols.loc['nextTripID'] = idCols['tripID'].shift(-1, fill_value=0)
        tripDict = dict.fromkeys(set(idCols['hhPersonID']))
        for ihhpID in tripDict.keys():
            tripDict[ihhpID] = set(idCols.loc[idCols['hhPersonID'] == ihhpID, 'tripID'])
        return tripDict

    def writeOut(self, globalConfig:dict, dataDrive: pd.DataFrame, dataPurpose: pd.DataFrame, datasetID: str = 'MiD17'):
        dataDrive.to_csv(Path(globalConfig['pathRelative']['input']) /
                         createFileString(globalConfig=globalConfig, fileKey='inputDataDriveProfiles',
                                          datasetID=datasetID),
                         na_rep=0)
        dataPurpose.to_csv(Path(globalConfig['pathRelative']['input']) /
                          createFileString(globalConfig=globalConfig, fileKey='purposesProcessed', datasetID=datasetID))
        print(f"Drive data and trip purposes written to files "
              f"{createFileString(globalConfig=globalConfig, fileKey='inputDataDriveProfiles', datasetID=datasetID)} "
              f"and {createFileString(globalConfig=globalConfig, fileKey='purposesProcessed', datasetID=datasetID)}")


class FillHourValues:
    def __init__(self, data, rangeFunction):
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


if __name__ == '__main__':
    from classes.dataParsers import DataParser
    pathGlobalConfig = Path.cwd().parent / 'config' / 'globalConfig.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    with open(pathGlobalConfig) as ipf:
        globalConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    pathParseConfig = Path.cwd().parent / 'config' / 'parseConfig.yaml'
    with open(pathParseConfig) as ipf:
        parseConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    pathGlobalConfig = Path.cwd().parent / 'config' / 'globalConfig.yaml'
    with open(pathGlobalConfig) as ipf:
        globalConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    pathTripConfig = Path.cwd().parent / 'config' / 'tripConfig.yaml'
    with open(pathTripConfig) as ipf:
        tripConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    pathLocalPathConfig = Path.cwd().parent / 'config' / 'localPathConfig.yaml'
    with open(pathLocalPathConfig) as ipf:
        localPathConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    os.chdir(localPathConfig['pathAbsolute']['vencoPyRoot'])
    vpData = DataParser(parseConfig=parseConfig, globalConfig=globalConfig, localPathConfig=localPathConfig, loadEncrypted=False)
    vpDiary = TripDiaryBuilder(tripConfig=tripConfig, globalConfig=globalConfig, ParseData=vpData)