import itertools

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

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from kneed import KneeLocator
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler
from vencopy.core.dataParsers import ParseKiD, ParseMiD, ParseVF
from vencopy.core.diaryBuilders import DiaryBuilder
from vencopy.core.flexEstimators import FlexEstimator
from vencopy.core.gridModelers import GridModeler
from vencopy.utils.globalFunctions import (createOutputFolders, loadConfigDict,
                                           writeOut)


class ProfileAggregator():
    def __init__(self, configDict: dict, datasetID: str,
                 activities: pd.DataFrame, profiles: DiaryBuilder, cluster=False):
        self.aggregatorConfig = configDict['aggregatorConfig']
        self.globalConfig = configDict['globalConfig']
        self.localPathConfig = configDict['localPathConfig']
        self.datasetID = datasetID
        self.activities = activities
        self.deltaTime = configDict['diaryConfig']['TimeDelta']
        self.timeIndex = list(pd.timedelta_range(start='00:00:00', end='24:00:00', freq=f'{self.deltaTime}T'))
        self.profiles = profiles
        self.weights = self.activities.loc[:, ['genericID', 'tripWeight']].drop_duplicates(
            subset=['genericID']).reset_index(drop=True).set_index('genericID')
        self.clusterBool = cluster
        self.nClusters = self.aggregatorConfig['clustering']['nClusters']
        self.drain = profiles.drain
        self.chargingPower = profiles.chargingPower
        # self.uncontrolledCharge = profiles.uncontrolledCharge
        # self.maxBatteryLevel = profiles.maxBatteryLevel
        # self.minBatteryLevel = profiles.minBatteryLevel

    def selectInputFeatures(self):
        # read from disk or from previous class
        self.datasetCleanup()
        self.dropDuplicateHH()

    def datasetCleanup(self):
        # timestamp start and end, unique ID, column to discretise - get read of additional columns
        necessaryColumns = ['genericID', 'economicStatus', 'areaType', 'bundesland']
        self.activitiesNoDuplicates = self.activities[necessaryColumns].copy()

    def dropDuplicateHH(self):
        # FIXME: generalise function by passing df and column
        if self.datasetID == 'MiD17':
            self.activitiesNoDuplicates = (
                self.activitiesNoDuplicates.drop_duplicates(subset=['genericID']).reset_index())

    def standardiseInputFeatures(self):
        self.features = self.activitiesNoDuplicates[["economicStatus", "areaType", "bundesland"]].to_numpy()
        scaler = StandardScaler()
        self.scaledFeatures = scaler.fit_transform(self.features)

    def cluster(self):
        kmeans = KMeans(init="random", n_clusters=self.nClusters, n_init=10, max_iter=300, random_state=42)
        kmeans.fit(self.scaledFeatures)
        self.predictedLabels = kmeans.labels_
        self.activitiesNoDuplicates['cluster'] = self.predictedLabels
        self._writeOutputActs()
        # inertia = kmeans.inertia_
        # centroids = kmeans.cluster_centers_
        # iterations = kmeans.n_iter_
        # self.plotClusters()
        # self.findOptimalClusterNumber()

    def plotClusters(self):
        clusterColours = ['#b4d2b1', '#568f8b', '#1d4a60', '#cd7e59', '#ddb247', '#d15252']
        plt.figure(figsize=(8, 8))
        scat = sns.scatterplot("component_1", "component_2", s=50, data=self.activitiesNoDuplicates,
                               hue="predicted_cluster", style="true_label", palette="Set2")
        scat.set_title("Clustering results")
        plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.0)
        plt.show()

    def findOptimalClusterNumber(self):
        self.kmeans_kwargs = {"init": "random", "n_init": 10, "max_iter": 300, "random_state": 42}
        self.sse = []  # list holds the SSE values for each k
        for k in range(1, 11):
            kmeans = KMeans(n_clusters=k, **self.kmeans_kwargs)
            kmeans.fit(self.scaledFeatures)
            self.sse.append(kmeans.inertia_)
        self.findClusterElbow()
        self.findSilhouetteCoeff()

    def findClusterElbow(self):
        kl = KneeLocator(range(1, 11), self.sse, curve="convex", direction="decreasing")
        self.elbow = kl.elbow
        self.plotElbow()

    def plotElbow(self):
        plt.style.use("fivethirtyeight")
        plt.plot(range(1, 11), self.sse)
        plt.xticks(range(1, 11))
        plt.xlabel("Number of Clusters")
        plt.ylabel("SSE")
        plt.show()

    def findSilhouetteCoeff(self):
        self.silhouetteCoefficients = []
        for k in range(2, 11):  # start with 2 clusters for silhouette coefficient
            kmeans = KMeans(n_clusters=k, **self.kmeans_kwargs)
            kmeans.fit(self.scaledFeatures)
            # self.predictedLabels = kmeans.labels_
            score = silhouette_score(self.scaledFeatures, kmeans.labels_)
            self.silhouetteCoefficients.append(score)
        self.plotSilhouette()

    def plotSilhouette(self):
        plt.style.use("fivethirtyeight")
        plt.plot(range(2, 11), self.silhouetteCoefficients)
        plt.xticks(range(2, 11))
        plt.xlabel("Number of Clusters")
        plt.ylabel("Silhouette Coefficient")
        plt.show()

    def clusterHH(self):
        # FIXME: custering should only happen once
        self.selectInputFeatures()
        self.standardiseInputFeatures()
        self.cluster()
        self.appendClusters()

    def appendClusters(self):
        self.activities['cluster'] = pd.NA
        self.activitiesNoDuplicates.set_index('genericID', inplace=True)
        for genericID in self.activitiesNoDuplicates.index:
            clusterValue = self.activitiesNoDuplicates.loc[genericID, 'cluster']
            self.activities.loc[self.activities.genericID == genericID, 'cluster'] = clusterValue

    def createWeeklyProfiles(self):
        if self.clusterBool:
            print('Aggregating profiles based on clustering of household specific characteristics.')
            self.clusterHH()
            print(f'Clustered household into {self.nClusters} clusters.')
            self.aggregateClusters()
        else:
            print('Aggregating all profiles to fleet level based on day of the week.')
            self.aggregateWeightsAndWeekdays(byColumn="tripStartWeekday")

    def aggregateClusters(self):
        # self.clustersProfiles = pd.DataFrame(columns=self.profile.columns, index=range(self.nClusters))
        necessaryColumns = ['genericID', 'tripWeight', 'tripStartWeekday', 'cluster']
        self.activitiesSubset = self.activities[necessaryColumns].copy().drop_duplicates(
            subset=['genericID']).reset_index(drop=True)
        # loop over clusters to aggregate over weekday and create weekly and annual profiles
        for clusterID in self.activitiesSubset.cluster.unique():
            print(f'Aggregating cluster {clusterID} based on day of the week.')
            self.clusterID = clusterID
            self.clusterSubset = (
                self.activitiesSubset[self.activitiesSubset.cluster == clusterID].reset_index(drop=True))
            # aggregateWeightsAndWeekdays takes self.profile and self.activities into account
            # self.aggregateWeightsAndWeekdays(byColumn="tripStartWeekday")

    def aggregateWeightsAndWeekdays(self, byColumn: str) -> pd.Series:
        self.weekdayProfiles = pd.DataFrame(columns=self.profile.columns, index=range(1, 8))
        necessaryColumns = ['genericID', 'tripWeight'] + [byColumn]
        if self.clusterBool:
            self.clusterSubset = (
                self.clusterSubset[necessaryColumns].copy().drop_duplicates(subset=['genericID']).reset_index(drop=True))
            self.profile['genericID'] = self.profile.index
            self.activitiesWeekday = pd.merge(self.profile, self.clusterSubset, on='genericID', how='inner')
        else:
            self.activitiesSubset = (
                self.activities[necessaryColumns].copy().drop_duplicates(subset=['genericID']).reset_index(drop=True))
            self.profile['genericID'] = self.profile.index
            self.activitiesWeekday = pd.merge(self.profile, self.activitiesSubset, on='genericID', how='inner')
        self.profile.drop('genericID', axis=1, inplace=True)
        self.activitiesWeekday = self.activitiesWeekday.set_index('genericID')
        # Compose weekly profile from 7 separate profiles
        if self.profileName in ('drain', 'uncontrolledCharge', 'chargingPower'):
            self.calculateWeightedAverageFlowProfiles(byColumn=byColumn)
        else:
            self.calculateWeightedAverageStateProfiles(byColumn=byColumn)
        self.composeWeeklyProfile()
        self.createAnnualProfiles()
        self._writeOutput()

    def calculateWeightedAverageFlowProfiles(self, byColumn):
        for idate in self.activitiesWeekday[byColumn].unique():
            weekdaySubset = self.activitiesWeekday[self.activitiesWeekday[byColumn] == idate].reset_index(drop=True)
            weekdaySubset = weekdaySubset.drop('tripStartWeekday', axis=1)
            # aggregate activitiesWeekday to one profile by multiplying by weights
            sumWeights = sum(weekdaySubset.tripWeight)
            weekdaySubsetW = weekdaySubset.apply(lambda x: x * weekdaySubset.tripWeight.values)
            weekdaySubsetW = weekdaySubsetW.drop('tripWeight', axis=1)
            weekdaySubsetWAgg = weekdaySubsetW.sum() / sumWeights
            self.weekdayProfiles.iloc[idate-1] = weekdaySubsetWAgg

    def calculateWeightedAverageStateProfiles(self, byColumn, alpha=10):
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

    def composeWeeklyProfile(self):
        # input is self.weekdayProfiles
        # check if any day of the week is not filled, copy line above in that case
        # FIXME: very ugly function below
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

    def createAnnualProfiles(self):
        startWeekday = 1  # (1: Monday, 7: Sunday)
        # shift input profiles to the right weekday and start with first bin of chosen weekday
        self.annualProfile = self.weeklyProfile.iloc[((startWeekday-1)*((len(list(self.timeIndex)))-1)):]
        self.annualProfile = self.annualProfile.append([self.weeklyProfile]*52, ignore_index=True)
        self.annualProfile.drop(
            self.annualProfile.tail(len(self.annualProfile)-((len(list(self.timeIndex)))-1)*365).index, inplace=True)

    def _writeOutputActs(self):
        writeOut(dataset=self.activities, outputFolder='aggregatorOutput', fileKey=('outputProfileAggregator'),
                 datasetID=self.datasetID, manualLabel=str('actvities_clusters'),
                 localPathConfig=self.localPathConfig, globalConfig=self.globalConfig)

    def _writeOutput(self):
        if self.clusterBool:
            writeOut(dataset=self.annualProfile, outputFolder='aggregatorOutput', fileKey=('outputProfileAggregator'),
                     datasetID=self.datasetID, manualLabel=str(self.profileName + '_cluster' + str(self.clusterID)),
                     localPathConfig=self.localPathConfig, globalConfig=self.globalConfig)
        else:
            writeOut(dataset=self.annualProfile, outputFolder='aggregatorOutput', datasetID=self.datasetID,
                     fileKey=('outputProfileAggregator'), manualLabel=str(self.profileName),
                     localPathConfig=self.localPathConfig, globalConfig=self.globalConfig)

    def createTimeseries(self):
        # profiles = (vpDiary.drain, vpDiary.uncontrolledCharge, vpDiary.chargingPower, vpDiary.maxBatteryLevel, vpDiary.minBatteryLevel)
        profiles = (self.drain, self.chargingPower)  # , self.uncontrolledCharge, self.maxBatteryLevel, self.minBatteryLevel)
        profileNames = ('drain', 'chargingPower')  # , 'uncontrolledCharge', 'maxBatteryLevel', 'minBatteryLevel')
        for profile, profileName in itertools.product(profiles, profileNames):
            self.profileName = profileName
            self.profile = profile
            self.createWeeklyProfiles()
        print('Run finished')


if __name__ == '__main__':

    startTime = time.time()
    datasetID = "MiD17"
    basePath = Path(__file__).parent.parent
    configNames = ("globalConfig", "localPathConfig", "parseConfig", "diaryConfig",
                   "gridConfig", "flexConfig", "aggregatorConfig", "evaluatorConfig")
    configDict = loadConfigDict(configNames, basePath=basePath)
    createOutputFolders(configDict=configDict)

    # if datasetID == "MiD17":
        # vpData = ParseMiD(configDict=configDict, datasetID=datasetID, debug=False)
    # elif datasetID == "KiD":
        # vpData = ParseKiD(configDict=configDict, datasetID=datasetID, debug=False)
    # elif datasetID == "VF":
        # vpData = ParseVF(configDict=configDict, datasetID=datasetID, debug=False)
    # vpData.process()

    # vpGrid = GridModeler(configDict=configDict, datasetID=datasetID, activities=vpData.activities, gridModel='simple')
    # vpGrid.assignGrid()

    # vpFlex = FlexEstimator(configDict=configDict, datasetID=datasetID, activities=vpGrid.activities)
    # vpFlex.estimateTechnicalFlexibility()

    # vpDiary = DiaryBuilder(configDict=configDict, datasetID=datasetID, activities=vpFlex.activities)
    # vpDiary.createDiaries()


    vpProfile = ProfileAggregator(configDict=configDict, datasetID=datasetID,
                                  activities=vpFlex.activities, profiles=vpDiary, cluster=True)
    vpProfile.createTimeseries()

    elapsedTime = time.time() - startTime
    print('Elapsed time:', elapsedTime)
