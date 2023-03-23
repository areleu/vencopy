__version__ = '0.4.X'
__maintainer__ = 'Niklas Wulff'
__contributors__ = 'Fabia Miorelli'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '29.07.2022'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'

if __package__ is None or __package__ == '':
    import sys
    from os import path
    sys.path.append(path.dirname(path.dirname(path.dirname(__file__))))

import time
from pathlib import Path
import itertools

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from vencopy.core.dataParsers import ParseKiD, ParseMiD, ParseVF
from vencopy.core.diaryBuilders import DiaryBuilder
from vencopy.core.flexEstimators import FlexEstimator
from vencopy.core.gridModelers import GridModeler
from vencopy.utils.globalFunctions import (createFileName, createOutputFolders, loadConfigDict,
                                           writeOut)


class ProfileAggregator():
    def __init__(self, configDict: dict, datasetID: str,
                 activities: pd.DataFrame, profiles: DiaryBuilder):
        self.aggregatorConfig = configDict['aggregatorConfig']
        self.globalConfig = configDict['globalConfig']
        self.localPathConfig = configDict['localPathConfig']
        self.datasetID = datasetID
        self.activities = activities
        self.deltaTime = configDict['diaryConfig']['TimeDelta']
        self.timeIndex = list(pd.timedelta_range(start='00:00:00', end='24:00:00', freq=f'{self.deltaTime}T'))
        self.profiles = profiles
        self.weights = self.activities.loc[:, ['uniqueID', 'tripWeight']].drop_duplicates(
            subset=['uniqueID']).reset_index(drop=True).set_index('uniqueID')
        self.drain = profiles.drain
        self.chargingPower = profiles.chargingPower
        self.uncontrolledCharge = profiles.uncontrolledCharge
        self.maxBatteryLevel = profiles.maxBatteryLevel
        self.minBatteryLevel = profiles.minBatteryLevel

    def __createWeeklyProfiles(self):
        print('Aggregating all profiles to fleet level based on day of the week.')
        self.__aggregateWeightsAndWeekdays(byColumn="tripStartWeekday")

    def __aggregateWeightsAndWeekdays(self, byColumn: str) -> pd.Series:
        self.weekdayProfiles = pd.DataFrame(columns=self.profile.columns, index=range(1, 8))
        necessaryColumns = ['uniqueID', 'tripWeight'] + [byColumn]
        self.activitiesSubset = (
            self.activities[necessaryColumns].copy().drop_duplicates(subset=['uniqueID']).reset_index(drop=True))
        self.profile['uniqueID'] = self.profile.index
        self.activitiesWeekday = pd.merge(self.profile, self.activitiesSubset, on='uniqueID', how='inner')
        self.profile.drop('uniqueID', axis=1, inplace=True)
        self.activitiesWeekday = self.activitiesWeekday.set_index('uniqueID')
        # Compose weekly profile from 7 separate profiles
        if self.profileName in ('drain', 'uncontrolledCharge', 'chargingPower'):
            self.__calculateWeightedAverageFlowProfiles(byColumn=byColumn)
        else:
            self.__calculateWeightedAverageStateProfiles(byColumn=byColumn)
        self.__composeWeeklyProfile()
        self._writeOutput()

    def __calculateWeightedAverageFlowProfiles(self, byColumn):
        for idate in self.activitiesWeekday[byColumn].unique():
            weekdaySubset = self.activitiesWeekday[self.activitiesWeekday[byColumn] == idate].reset_index(drop=True)
            weekdaySubset = weekdaySubset.drop('tripStartWeekday', axis=1)
            # aggregate activitiesWeekday to one profile by multiplying by weights
            sumWeights = sum(weekdaySubset.tripWeight)
            weekdaySubsetW = weekdaySubset.apply(lambda x: x * weekdaySubset.tripWeight.values)
            weekdaySubsetW = weekdaySubsetW.drop('tripWeight', axis=1)
            weekdaySubsetWAgg = weekdaySubsetW.sum() / sumWeights
            self.weekdayProfiles.iloc[idate-1] = weekdaySubsetWAgg

    def __calculateWeightedAverageStateProfiles(self, byColumn, alpha=10):
        pass
        # for idate in self.activitiesWeekday[byColumn].unique():
        #     weekdaySubset = self.activitiesWeekday[self.activitiesWeekday[byColumn] == idate].reset_index(drop=True)
        #     weekdaySubset = weekdaySubset.drop('tripStartWeekday', axis=1)
        #     nProfiles = len(weekdaySubset)
        #     nProfilesFilter = int(alpha / 100 * nProfiles)
        #     for col in weekdaySubset:
        #         profileMin[col] = min(weekdaySubset[col].nlargest(nProfilesFilter))
        #     for col in weekdaySubset:
        #         profileMax[col] = max(weekdaySubset[col].nsmallest(nProfilesFilter))

    def __composeWeeklyProfile(self):
        # input is self.weekdayProfiles
        # check if any day of the week is not filled, copy line above in that case
        if self.weekdayProfiles.isna().any(axis=1).any():
            indexEmptyRows = self.weekdayProfiles[self.weekdayProfiles.isna().any(axis=1)].index - 1
            for emptyRow in indexEmptyRows:
                if emptyRow == 6:
                    self.weekdayProfiles.iloc[emptyRow] = self.weekdayProfiles.iloc[emptyRow - 1]
                else:
                    self.weekdayProfiles.iloc[emptyRow] = self.weekdayProfiles.iloc[emptyRow + 1]
        self.weekdayProfiles.index.name = 'weekday'
        self.weekdayProfiles = self.weekdayProfiles.stack().unstack(0)
        self.weeklyProfile = (
            pd.concat([self.weekdayProfiles[1], self.weekdayProfiles[2], self.weekdayProfiles[3],
                       self.weekdayProfiles[4], self.weekdayProfiles[5], self.weekdayProfiles[6],
                       self.weekdayProfiles[7]], ignore_index=True))

    def __createAnnualProfiles(self):
        startWeekday = 1  # (1: Monday, 7: Sunday)
        # shift input profiles to the right weekday and start with first bin of chosen weekday
        self.annualProfile = self.weeklyProfile.iloc[((startWeekday-1)*((len(list(self.timeIndex)))-1)):]
        self.annualProfile = self.annualProfile.append([self.weeklyProfile]*52, ignore_index=True)
        self.annualProfile.drop(
            self.annualProfile.tail(len(self.annualProfile)-((len(list(self.timeIndex)))-1)*365).index, inplace=True)

    def _writeOutput(self):
        root = Path(self.localPathConfig['pathAbsolute']['vencoPyRoot'])
        folder = self.globalConfig['pathRelative']['aggregatorOutput']
        fileName = createFileName(globalConfig=self.globalConfig, manualLabel='', file='outputProfileAggregator',
                                  datasetID=self.datasetID)
        writeOut(data=self.activities, path=root / folder / fileName)


    def createTimeseries(self):
        profiles = (vpDiary.drain, vpDiary.uncontrolledCharge, vpDiary.chargingPower, vpDiary.maxBatteryLevel, vpDiary.minBatteryLevel)
        profileNames = ('drain', 'chargingPower', 'uncontrolledCharge', 'maxBatteryLevel', 'minBatteryLevel')
        for profile, profileName in itertools.product(profiles, profileNames):
            self.profileName = profileName
            self.profile = profile
            self.__createWeeklyProfiles()
            self.__createAnnualProfiles()
        print('Run finished')


if __name__ == '__main__':

    startTime = time.time()
    basePath = Path(__file__).parent.parent
    configNames = ("globalConfig", "localPathConfig", "parseConfig", "diaryConfig",
                   "gridConfig", "flexConfig", "aggregatorConfig", "evaluatorConfig")
    configDict = loadConfigDict(configNames, basePath=basePath)
    createOutputFolders(configDict=configDict)

    datasetID = configDict["globalConfig"]["dataset"]
    if datasetID == "MiD17":
        vpData = ParseMiD(configDict=configDict, datasetID=datasetID, debug=False)
    elif datasetID == "KiD":
        vpData = ParseKiD(configDict=configDict, datasetID=datasetID, debug=False)
    elif datasetID == "VF":
        vpData = ParseVF(configDict=configDict, datasetID=datasetID, debug=False)
    vpData.process()

    vpGrid = GridModeler(configDict=configDict, datasetID=datasetID, activities=vpData.activities, gridModel='simple')
    vpGrid.assignGrid()

    vpFlex = FlexEstimator(configDict=configDict, datasetID=datasetID, activities=vpGrid.activities)
    vpFlex.estimateTechnicalFlexibility()

    vpDiary = DiaryBuilder(configDict=configDict, datasetID=datasetID, activities=vpFlex.activities)
    vpDiary.createDiaries()

    vpProfile = ProfileAggregator(configDict=configDict, datasetID=datasetID,
                                  activities=vpFlex.activities, profiles=vpDiary)
    vpProfile.createTimeseries()

    elapsedTime = time.time() - startTime
    print('Elapsed time:', elapsedTime)
