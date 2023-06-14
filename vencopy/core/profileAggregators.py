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

from pathlib import Path
import pandas as pd

from vencopy.core.diaryBuilders import DiaryBuilder
from vencopy.utils.globalFunctions import (createFileName, writeOut)


class ProfileAggregator():
    def __init__(self, configDict: dict, activities: pd.DataFrame,
                 profiles: DiaryBuilder):
        self.aggregatorConfig = configDict['aggregatorConfig']
        self.globalConfig = configDict['globalConfig']
        self.localPathConfig = configDict['localPathConfig']
        self.datasetID = configDict["globalConfig"]["dataset"]
        self.deltaTime = configDict['diaryConfig']['TimeDelta']
        self.weighted = self.aggregatorConfig['weightFlowProfiles']
        self.alpha = self.aggregatorConfig['alpha']

        self.activities = activities
        self.profiles = profiles

        self.timeIndex = list(pd.timedelta_range(
            start='00:00:00', end='24:00:00', freq=f'{self.deltaTime}T'))

        self.weights = self.activities.loc[
            :, ['uniqueID', 'tripWeight']].drop_duplicates(
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
        self.weekdayProfiles = pd.DataFrame(
            columns=self.profile.columns, index=range(1, 8))
        necessaryColumns = ['uniqueID', 'tripWeight'] + [byColumn]
        self.activitiesSubset = (
            self.activities[necessaryColumns].copy().drop_duplicates(subset=['uniqueID']).reset_index(drop=True))
        self.activitiesWeekday = pd.merge(
            self.profile, self.activitiesSubset, on='uniqueID', how='inner')
        # self.profile.drop('uniqueID', axis=1, inplace=True)
        self.activitiesWeekday = self.activitiesWeekday.set_index('uniqueID')
        # Compose weekly profile from 7 separate profiles
        if self.profileName in ('drain', 'uncontrolledCharge', 'chargingPower'):
            if self.weighted:
                self.__calculateWeightedAverageFlowProfiles(byColumn=byColumn)
            else:
                self.__calculateAverageFlowProfiles(byColumn=byColumn)
        else:
            self.__calculateAggregatedStateProfiles(
                byColumn=byColumn, alpha=self.alpha)
        self.__composeWeeklyProfile()

    def __calculateAverageFlowProfiles(self, byColumn):
        for idate in self.activitiesWeekday[byColumn].unique():
            weekdaySubset = self.activitiesWeekday[
                self.activitiesWeekday[byColumn] == idate].reset_index(
                drop=True)
            weekdaySubset = weekdaySubset.drop(columns=[
                'tripStartWeekday', 'tripWeight'], axis=1)
            weekdaySubsetAgg = weekdaySubset.mean(axis=0)
            self.weekdayProfiles.iloc[idate - 1] = weekdaySubsetAgg

    def __calculateWeightedAverageFlowProfiles(self, byColumn):
        for idate in self.activitiesWeekday[byColumn].unique():
            weekdaySubset = self.activitiesWeekday[
                self.activitiesWeekday[byColumn] == idate].reset_index(
                drop=True)
            weekdaySubset = weekdaySubset.drop('tripStartWeekday', axis=1)
            # aggregate activitiesWeekday to one profile by multiplying by weights
            sumWeights = sum(weekdaySubset.tripWeight)
            weekdaySubsetW = weekdaySubset.apply(
                lambda x: x * weekdaySubset.tripWeight.values)
            weekdaySubsetW = weekdaySubsetW.drop('tripWeight', axis=1)
            weekdaySubsetWAgg = weekdaySubsetW.sum() / sumWeights
            self.weekdayProfiles.iloc[idate - 1] = weekdaySubsetWAgg

    def __calculateAggregatedStateProfiles(self, byColumn: str, alpha: int = 10):
        """
        Selects the alpha (100 - alpha) percentile from maximum battery level 
        (minimum batttery level) profile for each hour. If alpha = 10, the 
        10%-biggest (10%-smallest) value is selected, all values beyond are 
        disregarded as outliers.

        :param byColumn: Currently tripWeekday
        :param alpha: Percentage, giving the amount of profiles whose mobility demand can not be
            fulfilled after selection.
        :return: No return. Result is written to self.weekdayProfiles with bins
            in the columns and weekday identifiers in the rows.
        """

        for idate in self.activitiesWeekday[byColumn].unique():
            levels = self.activitiesWeekday.copy()
            weekdaySubset = levels[levels[byColumn] == idate].reset_index(
                drop=True)
            weekdaySubset = weekdaySubset.drop(columns=[
                'tripStartWeekday', 'tripWeight'])
            weekdaySubset = weekdaySubset.convert_dtypes()
            if self.profileName == 'maxBatteryLevel':
                self.weekdayProfiles.iloc[idate-1] = weekdaySubset.quantile(
                    alpha / 100)
            elif self.profileName == 'minBatteryLevel':
                self.weekdayProfiles.iloc[idate-1] = weekdaySubset.quantile(
                    1 - (alpha / 100))
            else:
                raise NotImplementedError(f'An unknown profile {self.profileName} was selected.')

    def __composeWeeklyProfile(self):
        # input is self.weekdayProfiles
        # check if any day of the week is not filled, copy line above in that case
        if self.weekdayProfiles.isna().any(axis=1).any():
            indexEmptyRows = self.weekdayProfiles[
                self.weekdayProfiles.isna().any(axis=1)].index - 1
            for emptyRow in indexEmptyRows:
                if emptyRow == 6:
                    self.weekdayProfiles.iloc[
                        emptyRow] = self.weekdayProfiles.iloc[emptyRow - 1]
                else:
                    self.weekdayProfiles.iloc[
                        emptyRow] = self.weekdayProfiles.iloc[emptyRow + 1]
        self.weekdayProfiles.index.name = 'weekday'
        self.weekdayProfiles = self.weekdayProfiles.stack().unstack(0)
        self.weeklyProfile = (
            pd.concat([self.weekdayProfiles[1], self.weekdayProfiles[2], self.weekdayProfiles[3],
                       self.weekdayProfiles[4], self.weekdayProfiles[5], self.weekdayProfiles[6],
                       self.weekdayProfiles[7]], ignore_index=True))

    def __createAnnualProfiles(self):
        startWeekday = 1  # (1: Monday, 7: Sunday)
        # shift input profiles to the right weekday and start with first bin of chosen weekday
        self.annualProfile = self.weeklyProfile.iloc[(
            (startWeekday - 1) * ((len(list(self.timeIndex))) - 1)):]
        self.annualProfile = self.annualProfile.append(
            [self.weeklyProfile] * 52, ignore_index=True)
        self.annualProfile.drop(
            self.annualProfile.tail(
                len(self.annualProfile) - ((len(list(
                    self.timeIndex))) - 1) * 365).index, inplace=True)

    def _writeOutput(self):
        root = Path(self.localPathConfig['pathAbsolute']['vencoPyRoot'])
        folder = self.globalConfig['pathRelative']['aggregatorOutput']

        fileName = createFileName(globalConfig=self.globalConfig, manualLabel=(
            '_' + self.profileName + 'Week'),
            fileNameID='outputProfileAggregator', datasetID=self.datasetID)
        writeOut(data=self.weeklyProfile, path=root / folder / fileName)

        fileName = createFileName(globalConfig=self.globalConfig, manualLabel=(
            '_' + self.profileName + 'Annual'),
            fileNameID='outputProfileAggregator', datasetID=self.datasetID)
        writeOut(data=self.annualProfile, path=root / folder / fileName)

    def createTimeseries(self, pNames: (str) = None,
                         profiles: (pd.DataFrame) = None):
        if not pNames and not profiles:
            profiles = (self.drain, self.uncontrolledCharge, self.chargingPower,
                        self.maxBatteryLevel, self.minBatteryLevel)
            pNames = ('drain', 'uncontrolledCharge', 'chargingPower',
                      'maxBatteryLevel', 'minBatteryLevel')
        for profile, profileName in zip(profiles, pNames):
            self.profileName = profileName
            self.profile = profile
            self.__createWeeklyProfiles()
            self.__createAnnualProfiles()
            self._writeOutput()
        print('Run finished.')
