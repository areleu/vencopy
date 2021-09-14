__version__ = '0.1.0'
__maintainer__ = 'Niklas Wulff 31.12.2019'
__contributors__ = 'Fabia Miorelli, Parth Butte'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '31.12.2019'
__status__ = 'prod'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'

if __package__ is None or __package__ == '':
    import sys
    from os import path
    sys.path.append(path.dirname(path.dirname(path.dirname(__file__))))

import pandas as pd
import numpy as np
from pathlib import Path
from vencopy.scripts.globalFunctions import createFileString


class TripDiaryBuilder:
    def __init__(self, configDict: dict, ParseData, datasetID: str = 'MiD17', debug: bool=False):
        """
        Class to build diaries of trips and parking purposes ("purposes"). Currently a discretization of 1 hour is
        used and tested. Some assumptions are taken in order to build the diaries:
        1. The driven distance is allocated to all hours between start and end time of a trip depending on the time
        share of the respective hour.
        2. The purposes of a parking activity is always determined by the purpose of the previous trip. Purposes are
        taken from the MiD B2 "Wege" (German for trips) data set variable 38 - 'zweck' (German for purpose) and the
        additional purpose "DRIVING" is added.
        3. If an hour comprises multiple purposes (e.g. driving and home), the purpose with the higher share is taken.
        Potential ambiguities in the diaries are later corrected in the gridModel.

        :param configDict: A dictionary containing multiple yaml config files
        :param ParseData: Class instance of ParseData storing MiD data
        :param datasetID: Can be 'MiD17' or 'MiD08' depending on input data set
        :param debug: Boolean argument. If True, only the first 2000 trips are converted to diaries. This parameter is
        used in the beta since the performance of the current tripDiaryBuilder is quite low.
        """

        self.tripConfig = configDict['tripConfig']
        self.globalConfig = configDict['globalConfig']
        self.datasetID = datasetID
        self.parsedData = ParseData
        self.tripDataClean = None
        self.tripDistanceDiary = None
        self.tripPurposeDiary = None
        if debug:
            self.tripDataClean = self.calculateConsistentHourlyShares(data=ParseData.data.loc[0:2000, :])
        else:
            self.tripDataClean = self.calculateConsistentHourlyShares(data=ParseData.data)
        self.tripDistanceDiary = self.tripDistanceAllocation(globalConfig=self.globalConfig)
        self.tripPurposeAllocation()
        self.writeOut(globalConfig=self.globalConfig, datasetID=datasetID, dataDrive=self.tripDistanceDiary,
                      dataPurpose=self.tripPurposeDiary)

    def tripDuration(self, timestampStart: np.datetime64, timestampEnd: np.datetime64) -> np.datetime64:
        return timestampEnd - timestampStart

    def calcHourShareStart(self, timestampStart: pd.Series, timestampEnd: pd.Series, duration: pd.Series) -> (pd.Series,
                                                                                                           pd.Series):
        """
        :param timestampStart: start time of a trip
        :param timestampEnd:  end time of a trip
        :param duration: duration of a trip
        :return: Returns a data frame of share of individual trip for trips completed in an hour and more w.r.t start
                 time of the trip

        """
        isSameHourTrip = timestampStart.dt.hour == timestampEnd.dt.hour
        shareSameHour = (timestampEnd.dt.minute - timestampStart.dt.minute) / (duration.dt.seconds / 60)
        shareSameHour[duration == pd.Timedelta(0)] = 1  # Set share of first hour to 1 for trips with duration of 0
        share = pd.Series(shareSameHour.where(isSameHourTrip,
                                              (60 - timestampStart.dt.minute) / (duration.dt.seconds / 60)))
        return share.copy(), isSameHourTrip

    def calcHourShareEnd(self, timestampEnd: pd.Series, duration: pd.Series, isSameHourTrip: pd.Series) -> pd.Series:
        """
        :param timestampEnd: end time of a trip
        :param duration: duration of a trip
        :param isSameHourTrip: data frame containing same start time of various trips
        :return: Returns a data frame of share of individual trip for trips completed in an hour and more w.r.t end time
                 of the trip

        """
        share = timestampEnd.dt.minute / (duration.dt.seconds / 60)
        return share.where(~isSameHourTrip, 0)

    def calcDistanceShares(self, data: pd.DataFrame, duration: pd.Series, timestampSt: str,
                           timestampEn: str) -> tuple:
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

    def numberOfFullHours(self, timestampStart: pd.Series, timestampEnd: pd.Series) -> pd.DataFrame:
        """
        Calculates the number of full hours in each trip. E.g. a trip taking 1:42 has 1 full hour and a 30-minute trip
        has 0 full hours.

        :param timestampStart:  Start timestamps of trips
        :param timestampEnd: End times of trips
        :return: Returns a data frame of number of full hours of all trips
        """
        timedeltaTrip = timestampEnd - timestampStart
        numberOfHours = timedeltaTrip.apply(lambda x: x.components.hours)
        numberOfDays = timedeltaTrip.apply(lambda x: x.components.days)
        minLeftFirstHour = pd.to_timedelta(60 - timestampStart.dt.minute, unit='m')
        hasFullHourAfterFirstHour = (timedeltaTrip - minLeftFirstHour) >= pd.Timedelta(1, unit='h')
        numberOfHours = numberOfHours.where(hasFullHourAfterFirstHour, other=0)
        return numberOfHours.where(numberOfDays != -1, other=0)

    def calcFullHourTripLength(self, duration: pd.Series, numberOfFullHours: pd.Series, tripLength: pd.Series) -> \
            pd.Series:
        """
        Calculates the share of the full trip hours. E.g. the fullHourTripLength of a trip starting at 1:45 and ending
        at 4:30 is 120 minutes / 165 minutes ~ 0.73.

        :param duration: Series holding durations of all trips
        :param numberOfFullHours: Series holding the number of full hours of all trips
        :param tripLength: Series of trip lengths
        :return: Returns a Series of full hour trip lengths for all trips
        """

        fullHourTripLength = (numberOfFullHours / (duration.dt.seconds / 3600)) * tripLength
        fullHourTripLength.loc[duration == pd.Timedelta(0)] = 0  # set trip length to 0 that would otherwise be NaN
        return fullHourTripLength

    def calcHourlyShares(self, data: pd.DataFrame, ts_st: str, ts_en: str) -> pd.DataFrame:
        """
        Calculates the shares of first hour, the share of full hours and the full hour trip length.

        :param data: Trip data
        :param ts_st: String specifying the trip start column
        :param ts_en: String specifying the trip end column
        :return: data frame consisting additional information regarding share of a trip, number of full hours and
                 lenght of each trip
        """

        duration = self.tripDuration(data.loc[:, ts_st], data.loc[:, ts_en])
        data.loc[:, 'shareStartHour'], data.loc[:, 'shareEndHour'] = self.calcDistanceShares(data, duration, ts_st,
                                                                                             ts_en)
        data.loc[:, 'noOfFullHours'] = self.numberOfFullHours(data.loc[:, ts_st], data.loc[:, ts_en])
        data.loc[:, 'fullHourTripLength'] = self.calcFullHourTripLength(duration, data.loc[:, 'noOfFullHours'],
                                                                        data.loc[:, 'tripDistance'])
        return data

    def calculateConsistentHourlyShares(self, data: pd.DataFrame):
        """
        Wrapper around calcHourlyShares also filtering inconsistent trips.

        :param data: Trip data
        :return: Filtered trip data
        """

        print('Calculating hourly shares')
        if not data._is_view:
            data = data.copy()  # FIXME: why is data._is_view False if we get a view
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
        return pd.DataFrame(index=indexCol, columns=range(nHours))

    def fillDataframe(self, hourlyArray: pd.DataFrame, fillFunction) -> pd.DataFrame:
        return hourlyArray.apply(fillFunction, axis=1)

    def mergeTrips(self, tripData: pd.DataFrame) -> pd.DataFrame:
        """
        Merge multiple individual hourly trip distances into one diary consisting of multiple trips

        :param tripData: Input trip data with hourly distances of all hourly trips
        :return: Merged trip distance diaries
        """
        dataDay = tripData.groupby(['genericID']).sum()
        dataDay = dataDay.drop('tripID', axis=1)
        return dataDay

    def initiateColRange(self, row: pd.Series):
        """
        Returns a range object with start and end hour as limits

        :param row: A trip observation
        :return:
        """
        if row['tripStartHour'] + 1 < row['tripEndHour']:
            return range(row['tripStartHour'] + 1, row['tripEndHour'])  # The hour of arrival (tripEndHour) will
            # not be indexed further below but is part of the range() object
        else:
            return None

    def tripDistanceAllocation(self, globalConfig: dict) -> pd.DataFrame:
        """
        Wrapper function for the conversion of trip distance values (in a column) to hourly trip distance diaries.

        :param globalConfig: Dictionary holding relative paths, filenames and run labels
        :return: Trip distance diary as a pd.DataFrame
        """
        print('Trip distance diary setup starting')
        self.formatDF = self.initiateHourDataframe(indexCol=self.tripDataClean.index,
                                                   nHours=globalConfig['numberOfHours'])
        fillHourValues = FillHourValues(data=self.tripDataClean, rangeFunction=self.initiateColRange)
        driveDataTrips = self.fillDataframe(self.formatDF, fillFunction=fillHourValues)
        driveDataTrips.loc[:, ['genericID', 'tripID']] = pd.DataFrame(self.tripDataClean.loc[:, ['genericID',
                                                                                                 'tripID']])
        driveDataTrips = driveDataTrips.astype({'genericID': int, 'tripID': int})
        print('Finished trip distance diary setup')
        return self.mergeTrips(driveDataTrips)

    def assignDriving(self, driveData: pd.DataFrame) -> pd.DataFrame:
        """
        Assign hours where driveData != 0/NA to 'driving'

        :param driveData: driving data
        :return: Returns driving data with 'driving' instead of hours having 0/NA
        """
        locationData = driveData.copy()
        locationData = locationData.where(locationData == 0, other='DRIVING')
        return locationData

    def determinePurposeStartHour(self, departure: np.datetime64, arrival: np.datetime64) -> int:
        """
        Determines the start hour of a parking activity depending on previous trip end time and next trip start time

        :param departure: Start time of next trip after parking
        :param arrival: End time of previous trip
        :return: Returns start hour of a parking activity
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

    def fillDayPurposes(self, tripData: pd.DataFrame, purposeDataDays: pd.DataFrame) -> pd.DataFrame:
        # FixMe: Ask Ben for performance improvements
        """
        Main purpose diary builder function. Root of low performance of tripDiaryBuilder. Will be improved in future
        releases.

        :param tripData: data frame holding all the information about individual trip
        :param purposeDataDays: DataFrame with 24 (hour) columns holding 0s and 'DRIVING' for trip hours (for hours
               where majority of time is driving)
        :return: Returns a data frame of individual trip with it's hourly activity or parking purpose
        """
        hpID = str()
        maxWID = int()
        maxHour = len(purposeDataDays.columns)

        # # uniques = tripData['genericID'].unique()
        #
        # for iSubData in tripData.groupby('genericID'):
        #
        #     currentPerson = tripData['genericID'] == hpID
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
            isSameHPID = hpID == iRow['genericID']
            if not isSameHPID:
                hpID = iRow['genericID']
                allWIDs = tripData.loc[tripData['genericID'] == hpID, 'tripID']  # FIXME perf
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
                #     purposeDataDays.loc[hpID, purposeHourStart:iRow['tripStartHour'] + 1] = tripData.loc[idxOld,
                #     'purposeStr']
                if iRow['tripID'] == maxWID:
                    if iRow['timestampEnd'].minute <= 30:
                        purposeDataDays.loc[hpID, range(iRow['tripEndHour'], maxHour)] = 'HOME'
                    else:
                        purposeDataDays.loc[hpID, range(iRow['tripEndHour'] + 1, maxHour)] = 'HOME'
            idxOld = idx
        return purposeDataDays

    def tripPurposeAllocation(self):
        """
        Wrapper function for trip purpose allocation. Falsely non-allocated parking purposes are replaced by "HOME".

        :return: None
        """
        print('Starting trip purpose diary setup')
        tripPurposesDriving = self.assignDriving(self.tripDistanceDiary)
        self.tripPurposeDiary = self.fillDayPurposes(tripData=self.tripDataClean, purposeDataDays=tripPurposesDriving)
        self.tripPurposeDiary.replace({'0.0': 'HOME'})  # Replace remaining non-allocated purposes with HOME
        print('Finished purpose replacements')
        print(f'There are {len(self.tripPurposeDiary)} daily trip diaries.')

    # # improved purpose allocation approach
    # def mapHHPIDToTripID(self, tripData):
    #     idCols = self.tripDataClean.loc[:, ['genericID', 'tripID']]
    #     idCols.loc['nextTripID'] = idCols['tripID'].shift(-1, fill_value=0)
    #     tripDict = dict.fromkeys(set(idCols['genericID']))
    #     for ihhpID in tripDict.keys():
    #         tripDict[ihhpID] = set(idCols.loc[idCols['genericID'] == ihhpID, 'tripID'])
    #     return tripDict

    def writeOut(self, globalConfig: dict, dataDrive: pd.DataFrame, dataPurpose: pd.DataFrame, datasetID: str = 'MiD17'):
        """
        General writeout utility for tripDiaries

        :param globalConfig: global config storing relative paths, filenames and run labels
        :param dataDrive: Driving distance diary for each survey participant
        :param dataPurpose: Parking purpose diary for each survey participant
        :param datasetID: ID used for filenames
        :return: None
        """
        dataDrive.to_csv(Path(__file__).parent / globalConfig['pathRelative']['diaryOutput'] /
                         createFileString(globalConfig=globalConfig, fileKey='inputDataDriveProfiles',
                                          datasetID=datasetID), na_rep='0')
        dataPurpose.to_csv(Path(__file__).parent / globalConfig['pathRelative']['diaryOutput'] /
                           createFileString(globalConfig=globalConfig, fileKey='purposesProcessed',
                                            datasetID=datasetID))
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
    datasetID = 'MiD17' #options are MiD08, MiD17, KiD
    # datasetID = 'KiD'

    from vencopy.classes.dataParsers import DataParser
    from vencopy.scripts.globalFunctions import loadConfigDict

    configNames = ('globalConfig', 'localPathConfig', 'parseConfig', 'tripConfig', 'gridConfig', 'flexConfig', 'evaluatorConfig')
    configDict = loadConfigDict(configNames)
    vpData = DataParser(configDict=configDict, loadEncrypted=False, datasetID=datasetID)
    vpData.process()
    vpDiary = TripDiaryBuilder(configDict=configDict, ParseData=vpData, datasetID=datasetID, debug=True)
