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

from pathlib import Path

import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
from kneed import KneeLocator
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler
from vencopy.core.dataParsers import ParseKiD, ParseMiD, ParseVF
from vencopy.core.diaryBuilders import DiaryBuilder
from vencopy.core.flexEstimators import FlexEstimator
from vencopy.core.gridModelers import GridModeler
from vencopy.utils.globalFunctions import loadConfigDict, writeOut


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
        # self.clusterHH()
        self.drain = profiles.drain
        self.uncontrolledCharge = profiles.uncontrolledCharge
        self.chargingPower = profiles.chargingPower
        self.maxBatteryLevel = profiles.maxBatteryLevel
        self.minBatteryLevel = profiles.minBatteryLevel

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
            self.activitiesNoDuplicates = self.activitiesNoDuplicates.drop_duplicates(subset=['genericID']).reset_index()

    def standardiseInputFeatures(self):
        self.features = self.activitiesNoDuplicates[["economicStatus", "areaType", "bundesland"]].to_numpy()
        # self.labels = self.activities[["genericID"]].to_numpy()
        scaler = StandardScaler()
        self.scaledFeatures = scaler.fit_transform(self.features)

    def cluster(self):
        kmeans = KMeans(init="random", n_clusters=self.nClusters, n_init=10, max_iter=300, random_state=42)
        kmeans.fit(self.scaledFeatures)
        self.predictedLabels = kmeans.labels_
        self.activitiesNoDuplicates['cluster'] = self.predictedLabels
        # inertia = kmeans.inertia_
        # centroids = kmeans.cluster_centers_
        # iterations = kmeans.n_iter_
        # self.plotClusters()
        self.findOptimalClusterNumber()

    def plotClusters(self):
        plt.style.use("fivethirtyeight")
        plt.figure(figsize=(8, 8))
        scat = sns.scatterplot("component_1", "component_2", s=50, data=self.activitiesNoDuplicates, hue="predicted_cluster", style="true_label", palette="Set2")
        scat.set_title("Clustering results from TCGA Pan-Cancer\nGene Expression Data")
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
        self.selectInputFeatures()
        self.standardiseInputFeatures()
        self.cluster()
        self.appendClusters()

    def appendClusters(self):
        # labels = pd.DataFrame(np.random.randint(0, 10, size=(len(self.activitiesNoDuplicates), 1)))
        # labels['genericID'] = self.activitiesNoDuplicates['genericID'].copy()
        for genericID in self.activitiesNoDuplicates.genericID.unique():
            idSubset = self.activities[self.activities.genericID == genericID].reset_index(drop=True)
            for irow in range(len(self.activities)):
                self.activities.loc[irow, (idSubset.loc[irow, 'genericID'])] = self.activitiesNoDuplicates.loc[genericID, 0]

    def createWeeklyProfiles(self):
        # FIXME: move if cluster to createTimeseries()
        if self.clusterBool == True:
            print('Aggregating profiles based on clustering of household specific characteristics.')
            self.aggregateClusters()
        else:
            print('Aggregating all profiles to fleet level based on day of the week.')
            # self.aggregate()
            self.aggregateWeightsAndWeekdays(byColumn="tripStartWeekday")

    def aggregateClusters(self):
        self.clustersProfiles = pd.DataFrame(columns=self.profile.columns, index=range(self.nClusters))
        necessaryColumns = ['genericID', 'tripWeight', 'cluster']
        self.activitiesSubset = self.activities[necessaryColumns].copy().drop_duplicates(subset=['genericID']).reset_index(drop=True)
        # for each cluster aggregate based on day of the week and weight
        # create new df where index equals cluster number
        for clusterID in self.activitiesSubset.cluster.unique():
            self.clusterSubset = self.activitiesSubset[self.activitiesSubset.cluster == clusterID].reset_index(drop=True)
            # aggregateWeightsAndWeekdays taks which df in input
            self.aggregateWeightsAndWeekdays()

    def aggregateProfileWeights(self, df, weights) -> pd.Series:
        # FIXME: unused function - might be deleted
        # profile = self.profile.loc[~self.profile.apply(lambda x: x.isna(), axis=0).any(axis=1), :]
        # weights = self.ac.loc[profile.index, :]  # Filtering weight data to equate lengths
        # self.weightedProfile = profile.apply(calculateWeightedAverage, weightCol=weights["tripWeight"])
        sumWeights = sum(weights.tripWeight)
        self.weightedProfile = df.apply(lambda x: x * weights.values / sumWeights)

    def aggregateWeightsAndWeekdays(self, byColumn: str) -> pd.Series:
        self.weekdayProfiles = pd.DataFrame(columns=self.profile.columns, index=range(1,8))
        necessaryColumns = ['genericID', 'tripWeight'] + [byColumn]
        self.activitiesSubset = self.activities[necessaryColumns].copy().drop_duplicates(subset=['genericID']).reset_index(drop=True)
        self.profile['genericID'] = self.profile.index
        self.activitiesWeekday = pd.merge(self.profile, self.activitiesSubset, on='genericID', how='inner')
        self.activitiesWeekday = self.activitiesWeekday.set_index('genericID')
        # Compose weekly profile from 7 separate profiles
        if self.profileName in ('drain', 'uncontrolledCharge', 'chargingPower'):
            self.calculateWeightedAverageFlowProfiles(byColumn=byColumn)
        else:
            self.calculateWeightedAverageStateProfiles(byColumn=byColumn)
        self.composeWeeklyProfile()
        self.createAnnualProfiles()
        # self._writeOutput()

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
        self.weekdayProfiles.stack()
        # format based on day of the week

    def createAnnualProfiles(self):
        indexYear = self.timeIndex / 7 * 365
        self.weeklyProfile = pd.DataFrame(columns=range(len(list(indexYear))))
        # multiply by 52 weeks and cutoff excessive days
        # input = weeklyProfiles
        # clone over year

    def _writeOutput(self):
        # FIXME: string to be passed for profile name
        writeOut(dataset=self.profile, outputFolder='profileAggregator', datasetID=self.datasetID,
                 fileKey=('outputProfileAggregator'), manualLabel=str(self.profile),
                 localPathConfig=self.localPathConfig, globalConfig=self.globalConfig)

    def createTimeseries(self):
        profiles = (self.drain, self.uncontrolledCharge, self.chargingPower, self.maxBatteryLevel, self.minBatteryLevel)
        profileNames = ('drain', 'uncontrolledCharge', 'chargingPower', 'maxBatteryLevel', 'minBatteryLevel')
        for profile, profileName in itertools.product(profiles, profileNames):
            self.profileName = profileName
            self.profile = profile
            self.createWeeklyProfiles()


if __name__ == '__main__':

    datasetID = "MiD17"
    basePath = Path(__file__).parent.parent
    configNames = ("globalConfig", "localPathConfig", "parseConfig", "diaryConfig",
                   "gridConfig", "flexConfig", "aggregatorConfig", "evaluatorConfig")
    configDict = loadConfigDict(configNames, basePath=basePath)

    if datasetID == "MiD17":
        vpData = ParseMiD(configDict=configDict, datasetID=datasetID, debug=True)
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

    # activities = pd.read_csv('C:\\work\\VencoPy\\vencopy_internal\\vencopy\\vencopy\\output\\flexEstimator\\vencopyOutputFlexEstimator_DEBUG_MiD17_clusters.csv')
    # vpProfile = ProfileAggregator(configDict=configDict, datasetID=datasetID,
    #                               activities=activities, profiles=activities, cluster=True)
    vpProfile = ProfileAggregator(configDict=configDict, datasetID=datasetID,
                                  activities=vpFlex.activities, profiles=vpDiary, cluster=False)
    vpProfile.createTimeseries()
