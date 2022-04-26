__version__ = "0.4.X"
__maintainer__ = "Niklas Wulff"
__contributors__ = "Fabia Miorelli"
__email__ = "Niklas.Wulff@dlr.de"
__birthdate__ = "21.04.2022"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"

# ----- imports & packages ------
if __package__ is None or __package__ == "":
    import sys
    from os import path

    sys.path.append(path.dirname(path.dirname(path.dirname(__file__))))

import pandas as pd
from pathlib import Path
from vencopy.classes.dataParsers import ParseMiD, ParseVF, ParseKiD


class FlexEstimator:
    def __init__(
        self,
        configDict: dict,
        activityData: pd.DataFrame,
    ):
        self.flexConfig = configDict['flexConfig']
        self.nettoBatCap = self.flexConfig['Battery_capacity'] * self.flexConfig['Maximum_SOC']
        self.lowerBatLev = self.flexConfig['Battery_capacity'] * self.flexConfig['Minimum_SOC']
        self.activitiesIn = activityData
        self.activities = self.activitiesIn.copy()
        self.isTrip = self.activities['tripID'].fillna(0).astype(bool)
        self.isPark = self.activities['parkID'].fillna(0).astype(bool)
        self.isFirstAct = self.activities['isFirstActivity'].fillna(0).astype(bool)
        self.isLastAct = self.activities['isLastActivity'].fillna(0).astype(bool)
        # Dummy column to be able to work with numbers, get rid of this later
        self.activities.loc[self.isPark, 'ratedPower'] = 11

    def drain(self):
        self.activities['drain'] = self.activities['tripDistance'] * self.flexConfig['Electric_consumption'] / 100

    # FixME: Could not be tested yet, replace 'ratedPower' by charging station capacity column
    def maxChargeVolumePerParkingAct(self):
        self.activities.loc[self.isPark, 'maxChargeVolume'] = self.activities.loc[self.isPark, 'ratedPower'] * \
            self.activities.loc[self.isPark, 'timedelta'] / pd.Timedelta('1 hour')

    def maxBatteryLevel(self):
        # First activities - parking and trips
        self.activities.loc[self.activities['isFirstActivity'], 'maxBatteryLevelStart'] = self.nettoBatCap
        self.activities.loc[(self.isPark) & (self.isFirstAct),
                            'maxBatteryLevelEnd'] = self.activities['maxBatteryLevelStart']
        firstActTripIdx = (self.isTrip) & (self.isFirstAct)
        self.activities.loc[firstActTripIdx,
                            'maxBatteryLevelEnd'] = self.activities.loc[
                                firstActTripIdx, 'maxBatteryLevelStart'] - self.activities.loc[firstActTripIdx, 'drain']

        # All trips and parkings in between
        setActs = range(1, self.activities['parkID'].max())
        for act in setActs:
            self.calcMaxBatLevTrip(actID=act)
            self.calcMaxBatLevPark(actID=act)

        # Last trip (last parking was assigned above)
        lastTripIdx = (self.isTrip) & (self.isLastAct)
        lastTripIdx = lastTripIdx.loc[lastTripIdx].index
        self.activities.loc[lastTripIdx, 'maxBatteryLevelStart'] = self.activities.loc[lastTripIdx - 1,
                                                                                       'maxBatteryLevelEnd']
        theoBatLev = self.activities.loc[lastTripIdx, 'maxBatteryLevelStart'] - self.activities.loc[lastTripIdx,
                                                                                                    'drain']
        self.activities.loc[lastTripIdx, 'maxBatteryLevelEnd'] = theoBatLev.where(theoBatLev >= 0, other=0)

    def calcMaxBatLevTrip(self, actID: int):
        # Setting trip activity battery levels
        boolIdxTrips = (self.activities['tripID'] == actID) & (~self.activities['isFirstActivity'])
        self.activities.loc[boolIdxTrips, 'maxBatteryLevelStart'] = self.activities.loc[
            self.activities['parkID'] == actID, 'maxBatteryLevelEnd'].values
        # Can also be done for the first activity trips
        theoBatLev = self.activities.loc[self.activities['tripID'] == actID,
                                         'maxBatteryLevelStart'] - self.activities.loc[
            self.activities['tripID'] == actID, 'drain']
        self.activities.loc[self.activities['tripID'] == actID, 'maxBatteryLevelEnd'] = theoBatLev.where(
            theoBatLev >= 0, other=0)

    def calcMaxBatLevPark(self, actID: int):
        # Setting consecutive park activity battery levels
        self.activities.loc[(self.activities['parkID'] == actID + 1) & (~self.activities['isFirstActivity']),
                            'maxBatteryLevelStart'] = self.activities.loc[(self.activities['tripID'] == actID),
                                                                          'maxBatteryLevelEnd'].values

        theoBatLev = self.activities.loc[self.activities['parkID'] == actID + 1,
                                         'maxBatteryLevelStart'] + self.activities.loc[
                                             self.activities['parkID'] == actID + 1, 'maxChargeVolume']
        self.activities.loc[self.activities['parkID'] == actID + 1, 'maxBatteryLevelEnd'] = theoBatLev.where(
            theoBatLev <= self.nettoBatCap, other=self.nettoBatCap)

    def estimateTechnicalFlexibility(self):
        self.drain()
        self.maxChargeVolumePerParkingAct()
        self.maxBatteryLevel()


if __name__ == "__main__":
    from vencopy.scripts.globalFunctions import loadConfigDict
    basePath = Path(__file__).parent.parent
    configNames = ('globalConfig', 'localPathConfig', 'parseConfig', 'tripConfig', 'gridConfig', 'flexConfig',
                   'evaluatorConfig')
    configDict = loadConfigDict(configNames, basePath)

    datasetID = "MiD17"  # options are MiD08, MiD17, KiD
    if datasetID == "MiD17":
        vpData = ParseMiD(configDict=configDict, datasetID=datasetID)
    elif datasetID == "KiD":
        vpData = ParseKiD(configDict=configDict, datasetID=datasetID)
    elif datasetID == "VF":
        vpData = ParseVF(configDict=configDict, datasetID=datasetID)
    vpData.process()
    vpFlex = FlexEstimator(configDict=configDict, activityData=vpData.activities)
    vpFlex.estimateTechnicalFlexibility()
