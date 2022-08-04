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

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from vencopy.core.dataParsers import ParseKiD, ParseMiD, ParseVF
from vencopy.core.diaryBuilders import DiaryBuilder
from vencopy.core.flexEstimators import FlexEstimator
from vencopy.core.gridModelers import GridModeler
from vencopy.utils.globalFunctions import loadConfigDict, writeOut


class ProfileAggregator():
    def __init__(self, configDict: dict, datasetID: str, activities: pd.DataFrame, profiles, cluster=False):
        self.aggregatorConfig = configDict['aggregatorConfig']
        self.globalConfig = configDict['globalConfig']
        self.localPathConfig = configDict['localPathConfig']
        self.datasetID = datasetID
        self.activities = activities
        self.activitiesNoDuplicates = None
        self.profiles = profiles
        self.clusterBool = cluster
        self.nClusters = self.aggregatorConfig['clustering']['nClusters']
        self.clusterHH()
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
        self.features = self.activitiesNoDuplicates[["economicStatus", "areaType"]].to_numpy()
        # self.labels = self.activities[["genericID"]].to_numpy()
        scaler = StandardScaler()
        self.scaledFeatures = scaler.fit_transform(self.features)



    def clusterHH(self):
        self.selectInputFeatures()
        self.standardiseInputFeatures()
        # self.cluster()
        # self.appendClusters()

    def appendClusters(self):
        # labels later result of clustering
        # dummy labels
        labels = pd.DataFrame(np.random.randint(0, 10, size=(len(self.activitiesNoDuplicates), 1)))
        labels['genericID'] = self.activitiesNoDuplicates['genericID'].copy()
        for id in labels.genericID.unique():
            idSubset = self.activities[self.activities.genericID == id].reset_index(drop=True)
            for irow in range(len(self.activities)):
                self.activities.loc[irow, (idSubset.loc[irow, 'genericID'])] = labels.loc[id, 0]






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

    # vpDiary = DiaryBuilder(configDict=configDict, datasetID=datasetID, activities=vpFlex.activities)
    # vpDiary.createDiaries()

    activities = pd.read_csv('C:\\work\\VencoPy\\vencopy_internal\\vencopy\\vencopy\\output\\flexEstimator\\vencopyOutputFlexEstimator_DEBUG_MiD17_clusters.csv')
    # vpProfile = ProfileAggregator(configDict=configDict, activities=vpFlex.activities, profiles=profiles)
    # activities = vpFlex.activities

    vpProfile = ProfileAggregator(configDict=configDict, datasetID=datasetID,
                                  activities=activities, profiles=activities, cluster=True)
    vpProfile.createTimeseries()
