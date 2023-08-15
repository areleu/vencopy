__version__ = '0.4.X'
__maintainer__ = 'Niklas Wulff'
__contributors__ = 'Fabia Miorelli'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '29.07.2022'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'


import time
from pathlib import Path
import pandas as pd

from vencopy.core.diaryBuilders import DiaryBuilder
from vencopy.utils.globalFunctions import createFileName, writeOut


class ProfileAggregator():
    def __init__(self, configDict: dict, activities: pd.DataFrame,
                 profiles: DiaryBuilder):
        self.user_config = configDict['user_config']
        self.dev_config = configDict['dev_config']
        self.datasetID = self.user_config["global"]["dataset"]
        self.weighted = self.user_config["profileAggregators"]['weightFlowProfiles']
        self.alpha = self.user_config["profileAggregators"]['alpha']
        self.activities = activities
        self.profiles = profiles
        self.weights = self.activities.loc[
            :, ['uniqueID', 'tripWeight']].drop_duplicates(
            subset=['uniqueID']).reset_index(drop=True).set_index('uniqueID')
        self.drain = profiles.drain
        self.chargingPower = profiles.chargingPower
        self.uncontrolledCharge = profiles.uncontrolledCharge
        self.maxBatteryLevel = profiles.maxBatteryLevel
        self.minBatteryLevel = profiles.minBatteryLevel
        self.aggregator = Aggregator(activities=self.activities,
                                     datasetID=self.datasetID,
                                     user_config=self.user_config,
                                     dev_config=self.dev_config,
                                     weighted=self.weighted,
                                     alpha=self.alpha)

    def aggregateProfiles(self):
        self.drainWeekly = self.aggregator.performAggregation(profile=self.drain, profileName="drain", method="flow")
        self.chargingPowerWeekly = self.aggregator.performAggregation(profile=self.chargingPower, profileName="chargingPower", method="flow")
        self.uncontrolledChargeWeekly = self.aggregator.performAggregation(profile=self.uncontrolledCharge, profileName="uncontrolledCharge", method="flow")
        self.maxBatteryLevelWeekly = self.aggregator.performAggregation(profile=self.maxBatteryLevel, profileName="maxBatteryLevel", method="state")
        self.minBatteryLevelWeekly = self.aggregator.performAggregation(profile=self.minBatteryLevel, profileName="minBatteryLevel", method="state")
        print('Aggregation finished for all profiles.')


class Aggregator():
    def __init__(
            self,
            activities: pd.DataFrame,
            datasetID: str,
            alpha: int,
            user_config: dict,
            dev_config: dict,
            weighted: bool):
        self.datasetID = datasetID
        self.alpha = alpha
        self.activities = activities
        self.weighted = weighted
        self.user_config = user_config
        self.dev_config = dev_config

    def __basicAggregation(self, byColumn="tripStartWeekday") -> pd.Series:
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
        if self.method == 'flow':
            if self.weighted:
                self.__calculateWeightedAverageFlowProfiles(byColumn="tripStartWeekday")
            else:
                self.__calculateAverageFlowProfiles(byColumn="tripStartWeekday")
        elif self.method == 'state':
            self.__calculateAggregatedStateProfiles(
                byColumn="tripStartWeekday", alpha=self.alpha)

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
                self.weekdayProfiles.iloc[idate - 1] = weekdaySubset.quantile(
                    1 - (alpha / 100))
            elif self.profileName == 'minBatteryLevel':
                self.weekdayProfiles.iloc[idate - 1] = weekdaySubset.quantile(
                    alpha / 100)
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

    def _writeOutput(self):
        if self.user_config["global"]["writeOutputToDisk"]["aggregatorOutput"]:
            root = Path(self.user_config["global"]['pathAbsolute']['vencopyRoot'])
            folder = self.dev_config["global"]['pathRelative']['aggregatorOutput']
            fileName = createFileName(dev_config=self.dev_config, user_config=self.user_config, manualLabel=self.profileName, fileNameID='outputProfileAggregator',
                                      datasetID=self.datasetID)
            writeOut(data=self.activities, path=root / folder / fileName)

    def performAggregation(self, profile: pd.DataFrame, profileName: str, method: str):
        self.profile = profile
        self.profileName = profileName
        self.method = method
        print(f"Starting to aggregate {self.profileName} to fleet level based on day of the week.")
        startTimeAggregator = time.time()
        self.__basicAggregation()
        self.__composeWeeklyProfile()
        self._writeOutput()
        print(f"Aggregation finished for {self.profileName}.")
        elapsedTimeAggregator = time.time() - startTimeAggregator
        print(f"Needed time to aggregate {self.profileName}: {elapsedTimeAggregator}.")
        self.profile = None
        self.profileName = None
        self.method = None
        return self.weeklyProfile