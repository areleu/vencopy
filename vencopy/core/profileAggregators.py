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
        self.activitiesNoDuplicates = None
        self.profiles = profiles
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
        self.activitiesNoDuplicates['cluster'] = self.predictedLabels
        # labels = pd.DataFrame(np.random.randint(0, 10, size=(len(self.activitiesNoDuplicates), 1)))
        # labels['genericID'] = self.activitiesNoDuplicates['genericID'].copy()
        for id in self.activitiesNoDuplicates.genericID.unique():
            idSubset = self.activities[self.activities.genericID == id].reset_index(drop=True)
            for irow in range(len(self.activities)):
                self.activities.loc[irow, (idSubset.loc[irow, 'genericID'])] = self.activitiesNoDuplicates.loc[id, 0]

    def createWeeklyProfiles(self):
        if self.clusterBool == True:
            print('Aggregating profiles based on clustering of household specific characteristics.')
            # use join()
            similarGenericIDs = None
        else:
            print('Aggregating all profiles to fleet level based on day of the week.')


    def createAnnualProfiles(self):
        pass

    def _writeOutput(self):
        writeOut(dataset=self.profile, outputFolder='profileAggregator', datasetID=self.datasetID,
                 fileKey=('outputProfileAggregator'), manualLabel=str(self.profile),
                 localPathConfig=self.localPathConfig, globalConfig=self.globalConfig)

    def createTimeseries(self):
        profiles = (self.drain, self.uncontrolledCharge, self.chargingPower, self.maxBatteryLevel, self.minBatteryLevel)
        for iprofile in profiles:
            self.profile = iprofile
            self.createWeeklyProfiles()
            self.createAnnualProfiles()
            self._writeOutput()


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
                                  activities=vpFlex.activities, profiles=vpDiary)
    vpProfile.createTimeseries()
