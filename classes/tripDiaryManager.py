__version__ = '0.0.9'
__maintainer__ = 'Niklas Wulff 31.12.2019'
__contributors__ = 'Fabia Miorelli, Parth Butte'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '31.12.2019'
__status__ = 'dev'  # options are: dev, test, prod


import pandas as pd
from pathlib import Path
import yaml
import os
from classes.parseManager import DataParser
from scripts.globalFunctions import createFileString

class TripDiaryBuilder:
    def __init__(self, config: dict, globalConfig: dict, ParseData: DataParser, datasetID: str = 'MiD17'):
        self.config = config
        self.globalConfig = globalConfig
        self.tripDataClean = None
        self.tripDistanceDiary = None
        self.tripPurposeDiary = None
        # self.calculateConsistentHourlyShares(data=ParseData.data)
        # ONLY FOR DEBUGGING PURPOSES
        self.calculateConsistentHourlyShares(data=ParseData.data.loc[0:2000, :])
        self.tripDistanceAllocation(globalConfig)
        # self.hhPersonMap = self.mapHHPIDToTripID(self.tripDataClean)
        self.tripPurposeAllocation()
        self.writeOut(globalConfig=globalConfig, datasetID=datasetID, dataDrive=self.tripDistanceDiary,
                 dataPurpose=self.tripPurposeDiary)

    def writeOut(self, globalConfig:dict, dataDrive, dataPurpose, datasetID: str = 'MiD17'):
        dataDrive.to_csv(Path(globalConfig['linksRelative']['input']) /
                         createFileString(globalConfig=globalConfig, fileKey='inputDataDriveProfiles', dataset=datasetID), na_rep=0)
        dataPurpose.to_csv(Path(globalConfig['linksRelative']['input']) /
                          createFileString(globalConfig=globalConfig, fileKey='purposesProcessed', dataset=datasetID))
        print(f"Drive data and trip purposes written to files "
              f"{createFileString(globalConfig=globalConfig, fileKey='inputDataDriveProfiles', dataset=datasetID)} and"
              f"{createFileString(globalConfig=globalConfig, fileKey='purposesProcessed', dataset=datasetID)}")

    def tripDuration(self, timestampStart, timestampEnd):
        """
        :param timestampStart: start time of a trip
        :param timestampEnd:  end time of a trip
        :return: Returns trip duration
        """
        return timestampEnd - timestampStart

    def calcHourShareStart(self, timestampStart, timestampEnd, duration):
        """
        :param timestampStart: start time of a trip
        :param timestampEnd:  end time of a trip
        :param duration: duration of a trip
        :return: Returns a timestamped data frame of the trips which started in the same hour
        """
        isSameHourTrip = timestampStart.dt.hour == timestampEnd.dt.hour
        shareSameHour = (timestampEnd.dt.minute - timestampStart.dt.minute) / (duration.dt.seconds / 60)
        shareSameHour[
            duration == pd.Timedelta(0)] = 1  # Set share of first hour to 1 for trips with reported duration of 0
        share = shareSameHour.where(isSameHourTrip, (60 - timestampStart.dt.minute) / (duration.dt.seconds / 60))
        return share, isSameHourTrip

    def calcHourShareEnd(self, timestampEnd, duration, isSameHourTrip):
        """
        :param timestampEnd: end time of a trip
        :param duration: duration of a trip
        :param isSameHourTrip: data frame containing same start time of various trips
        :return: Returns a timestamped data frame of the trips which ended in the same hour
        """
        share = timestampEnd.dt.minute / (duration.dt.seconds / 60)
        return share.where(~isSameHourTrip, 0)

    def calcDistanceShares(self, data, duration, timestampSt, timestampEn):
        """
        :param data:
        :param duration: duration of a trip
        :param timestampSt:  start time of a trip
        :param timestampEn:  end time of a trip
        :return: Return a data frame of distance covered by each trip in a particular hour
        """
        shareHourStart, isSameHourTrip = self.calcHourShareStart(data.loc[:, timestampSt], data.loc[:, timestampEn],
                                                            duration)
        shareHourEnd = self.calcHourShareEnd(data.loc[:, timestampEn], duration, isSameHourTrip=isSameHourTrip)
        return shareHourStart, shareHourEnd

    def numberOfFullHours(self, timestampStart, timestampEnd):
        """
        :param timestampStart:  start time of a trip
        :param timestampEnd: end time of a trip
        :return: Returns a data frame of number of full load hours of each trip(not sure about this)
        """
        timedeltaTrip = timestampEnd - timestampStart
        numberOfHours = timedeltaTrip.apply(lambda x: x.components.hours)
        numberOfDays = timedeltaTrip.apply(lambda x: x.components.days)
        minLeftFirstHour = pd.to_timedelta(60 - timestampStart.dt.minute, unit='m')
        hasFullHourAfterFirstHour = (timedeltaTrip - minLeftFirstHour) >= pd.Timedelta(1, unit='h')
        numberOfHours = numberOfHours.where(hasFullHourAfterFirstHour, other=0)
        return numberOfHours.where(numberOfDays != -1, other=0)

    def calcFullHourTripLength(self, duration, numberOfFullHours, tripLength):
        """
        :param duration: duration of a trip
        :param numberOfFullHours:
        :param tripLength: data frame of individual trip length
        :return: Returns a data frame of hourly distance covered by each trip
        """
        fullHourTripLength = (numberOfFullHours / (duration.dt.seconds / 3600)) * tripLength
        fullHourTripLength.loc[duration == pd.Timedelta(0)] = 0  # set trip length to 0 that would otherwise be NaN
        return fullHourTripLength

    def calcHourlyShares(self, data, ts_st, ts_en):
        duration = self.tripDuration(data.loc[:, ts_st], data.loc[:, ts_en])
        data.loc[:, 'shareStartHour'], data.loc[:, 'shareEndHour'] = self.calcDistanceShares(data, duration, ts_st, ts_en)
        data.loc[:, 'noOfFullHours'] = self.numberOfFullHours(data.loc[:, ts_st], data.loc[:, ts_en])
        data.loc[:, 'fullHourTripLength'] = self.calcFullHourTripLength(duration, data.loc[:, 'noOfFullHours'],
                                                                   data.loc[:, 'tripDistance'])
        return data

    def calculateConsistentHourlyShares(self, data):
        print('Calculating hourly shares')
        tripDataWHourlyShares = self.calcHourlyShares(data, ts_st='timestampStart', ts_en='timestampEnd')

        # Filter out implausible hourly share combinations
        self.tripDataClean = tripDataWHourlyShares.loc[~((tripDataWHourlyShares['shareStartHour'] != 1) &
                                                       (tripDataWHourlyShares['shareEndHour'] == 0) &
                                                       (tripDataWHourlyShares['noOfFullHours'] == 0)), :]

    def initiateHourDataframe(self, indexCol, nHours):
        """
        Sets up an empty dataframe to be filled with hourly data.

        :param indexCol: List of column names
        :param nHours: integer giving the number of columns that should be added to the dataframe
        :return: dataframe with columns given and nHours additional columns appended with 0s
        """
        emptyDf = pd.DataFrame(index=indexCol, columns=range(nHours))
        return (emptyDf)

    def fillDataframe(self, hourlyArray, fillFunction):
        hourlyArray = hourlyArray.apply(fillFunction, axis=1)
        return hourlyArray

    def mergeTrips(self, tripData):
        # uniqueHHPersons = tripData.loc[:, 'hhPersonID'].unique()
        # dataDay = pd.DataFrame(index=uniqueHHPersons, columns=tripData.columns)
        dataDay = tripData.groupby(['hhPersonID']).sum()
        dataDay = dataDay.drop('tripID', axis=1)
        return dataDay

    def initiateColRange(self, row):
        if row['tripStartHour'] + 1 < row['tripEndHour']:
            return range(row['tripStartHour'] + 1, row[
                'tripEndHour'])  # The hour of arrival (tripEndHour) will not be indexed further below but is part of the range() object
        else:
            return None

    def tripDistanceAllocation(self, globalConfig):
        print('Trip distance diary setup starting')
        self.formatDF = self.initiateHourDataframe(indexCol=self.tripDataClean.index, nHours=globalConfig['numberOfHours'])
        fillHourValues = FillHourValues(data=self.tripDataClean, rangeFunction=self.initiateColRange)
        driveDataTrips = self.fillDataframe(self.formatDF, fillFunction=fillHourValues)
        driveDataTrips.loc[:, ['hhPersonID', 'tripID']] = pd.DataFrame(self.tripDataClean.loc[:, ['hhPersonID',
                                                                                                  'tripID']])
        self.tripDistanceDiary = self.mergeTrips(driveDataTrips)
        print('Finished trip distance diary setup')

    def assignDriving(self, driveData):
        # assign hours where drivData != 0/NA to 'driving'
        """
        :param driveData: driving data
        :return: Returns driving data with 'driving' instead of hours having 0/NA
        """
        locationData = driveData.copy()
        locationData = locationData.where(locationData == 0, other='DRIVING')
        return locationData

    # def determinePurposeHourRange(self, departure, arrival):
    #     tripDuration = arrival - departure
    #     startHour = self.determinePurposeStartHour(departure, tripDuration)
    #     return range(startHour, self.endHour)

    def determinePurposeStartHour(self, departure, arrival):
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

    def fillDayPurposes(self, tripData, purposeDataDays):  # FixMe: Ask Ben for performance improvements
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
                purposeHourStart = self.determinePurposeStartHour(tripData.loc[idxOld, 'timestampStart'],
                                                             tripData.loc[idxOld, 'timestampEnd'])
                if iRow['timestampStart'].minute <= 30:
                    hoursBetween = range(purposeHourStart,
                                         iRow['tripStartHour'])  # FIXME: case differentiation on arrival hour
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


if __name__ == '__main__':
    linkGlobalConfig = Path.cwd().parent / 'config' / 'globalConfig.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    globalConfig = yaml.load(open(linkGlobalConfig), Loader=yaml.SafeLoader)
    linkTripConfig = Path.cwd().parent / 'config' / 'tripConfig.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    tripConfig = yaml.load(open(linkTripConfig), Loader=yaml.SafeLoader)
    linkParseConfig = Path.cwd().parent / 'config' / 'parseConfig.yaml'
    parseConfig = yaml.load(open(linkParseConfig), Loader=yaml.SafeLoader)
    os.chdir(globalConfig['linksAbsolute']['vencoPyRoot'])

    vpData = DataParser(config=parseConfig, globalConfig=globalConfig, loadEncrypted=False)
    vpDiary = TripDiaryBuilder(config=tripConfig, globalConfig=globalConfig, ParseData=vpData)