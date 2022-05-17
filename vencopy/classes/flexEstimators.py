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
import numpy as np
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
        self.addNextAndPrevIDs()
        self.activities[['maxBatteryLevelStart', 'maxBatteryLevelEnd', 'minBatteryLevelStart',
                         'minBatteryLevelEnd', 'maxBatteryLevelEnd_unlimited', 'residualNeed',
                         'overshoot']] = None

        # Dummy column to be able to work with numbers, get rid of this later
        self.activities.loc[self.isPark, 'ratedPower'] = 11

    def addNextAndPrevIDs(self):
        self.activities.loc[~self.activities['tripID'].isna(), 'actID'] = self.activities['tripID']
        self.activities.loc[~self.activities['parkID'].isna(), 'actID'] = self.activities['parkID']
        self.activities.loc[~self.activities['isLastActivity'], 'nextActID'] = self.activities.loc[
            :, 'actID'].shift(-1)
        self.activities.loc[~self.activities['isFirstActivity'], 'prevActID'] = self.activities.loc[
            :, 'actID'].shift(1)

    def drain(self):
        self.activities['drain'] = self.activities['tripDistance'] * self.flexConfig['Electric_consumption'] / 100

    # FIXME: Could not be tested yet, replace 'ratedPower' by charging station capacity column
    def maxChargeVolumePerParkingAct(self):
        self.activities.loc[self.isPark, 'maxChargeVolume'] = self.activities.loc[self.isPark, 'ratedPower'] * \
            self.activities.loc[self.isPark, 'timedelta'] / pd.Timedelta('1 hour')

    def maxBatteryLevel(self):
        """
        Calculate the maximum battery level at the beginning and end of each activity. This represents the case of
        vehicle users always connecting when charging is available and charging as soon as possible as fast as possible
        until the maximum battery capacity is reached. 

        """

        # Calculate max battery start and end of all first activities
        firstActs = self.calcMaxBatFirstAct()
        actTemp = firstActs

        # Start and end for all trips and parkings in between
        # FIXME: Provide all previous trip/park data not just the last one
        setActs = range(1, self.activities['parkID'].max())
        for act in setActs:
            tripRows = (self.activities['tripID'] == act) & (~self.isFirstAct)  # & ~self.isLastAct  probably not needed
            parkRows = (self.activities['parkID'] == act) & (~self.isFirstAct)  # & ~self.isLastAct
            tripActs = self.activities.loc[tripRows, :]
            parkActs = self.activities.loc[parkRows, :]
            prevTripActs = actTemp.loc[~actTemp['tripID'].isna(), :]
            prevParkActs = actTemp.loc[~actTemp['parkID'].isna(), :]
            if act == 1:
                tripActsRes = self.calcMaxBatLevTrip(actID=act, tripActs=tripActs, prevParkActs=firstActs)
                parkActsRes = None
            else:
                parkActsRes = self.calcMaxBatLevPark(actID=act, parkActs=parkActs, prevTripActs=prevTripActs)
                tripActsRes = self.calcMaxBatLevTrip(actID=act, tripActs=tripActs, prevParkActs=prevParkActs),

            actTemp = pd.concat([actTemp, tripActsRes, parkActsRes])
            prevTripActs = tripActsRes

        # Last trip (last parking was assigned above)
        # actTemp.append(self.calcMaxBatLastAct())
        self.activities = pd.concat(actTemp, ignore_index=True)

    def calcMaxBatFirstAct(self):
        # First activities - parking and trips
        firstAct = self.activities.loc[self.isFirstAct, :]
        firstAct.loc[:, 'maxBatteryLevelStart'] = self.nettoBatCap
        firstAct.loc[self.isPark, 'maxBatteryLevelEnd'] = firstAct['maxBatteryLevelStart']
        firstAct.loc[self.isPark, 'overshoot'] = firstAct['maxChargeVolume']
        firstAct.loc[self.isTrip, 'maxBatteryLevelEnd_unlimited'] = firstAct.loc[
            self.isTrip, 'maxBatteryLevelStart'] - firstAct.loc[self.isTrip, 'drain']
        firstAct.loc[self.isTrip, 'maxBatteryLevelEnd'] = firstAct.loc[
            self.isTrip, 'maxBatteryLevelEnd_unlimited'].where(firstAct.loc[
                self.isTrip, 'maxBatteryLevelEnd_unlimited'] >= 0, other=0)
        firstAct.loc[self.isTrip, 'residualNeed'] = firstAct.loc[
            self.isTrip, 'maxBatteryLevelEnd_unlimited'].where(firstAct.loc[
                self.isTrip, 'maxBatteryLevelEnd_unlimited'] < 0, other=0)
        return firstAct

    def calcMaxBatLevTrip(self, actID: int, tripActs: pd.DataFrame, prevParkActs: pd.DataFrame):
        # Setting trip activity battery start level to battery end level of previous parking
        activeHHPersonIDs = tripActs.loc[:, 'hhPersonID']
        prevParkIDs = tripActs.loc[:, 'prevActID']
        multiIdxPark = [(id, None, act) for id, act in zip(activeHHPersonIDs, prevParkIDs)]
        multiIdxTrip = [(id, actID, None) for id in activeHHPersonIDs]

        # Index the previous park activity via integer index because loc park indices vary
        # intIdxPrevPark = activeTripRows.loc[activeTripRows].index - 1
        tripActsIdx = tripActs.set_index(['hhPersonID', 'tripID', 'parkID'])
        prevParkActsIdx = prevParkActs.set_index(['hhPersonID', 'tripID', 'parkID'])
        tripActsIdx.loc[multiIdxTrip, 'maxBatteryLevelStart'] = prevParkActsIdx.loc[multiIdxPark,
                                                                                    'maxBatteryLevelEnd'].values

        # Setting maximum battery end level for trip
        tripActsIdx.loc[multiIdxTrip, 'maxBatteryLevelEnd_unlimited'] = tripActsIdx.loc[
            multiIdxTrip, 'maxBatteryLevelStart'] - tripActsIdx.loc[multiIdxTrip, 'drain']
        tripActsIdx.loc[multiIdxTrip, 'maxBatteryLevelEnd'] = tripActsIdx.loc[
            multiIdxTrip, 'maxBatteryLevelEnd_unlimited'].where(tripActsIdx.loc[
                multiIdxTrip, 'maxBatteryLevelEnd_unlimited'] >= 0, other=0)
        tripActsIdx.loc[multiIdxTrip, 'residualNeed'] = tripActsIdx.loc[
            multiIdxTrip, 'maxBatteryLevelEnd_unlimited'].where(tripActsIdx.loc[
                multiIdxTrip, 'maxBatteryLevelEnd_unlimited'] < 0, other=0)
        return tripActsIdx.reset_index()

    def calcMaxBatLevPark(self, actID: int, parkActs: pd.DataFrame, prevTripActs: pd.DataFrame):
        # Setting next park activity battery start level to battery end level of current trip
        activeHHPersonIDs = parkActs.loc[:, 'hhPersonID']
        prevTripIDs = parkActs.loc[:, 'prevActID']

        # FIXME: Very ugly, still no solution for differing integer values for previous activity IDs since previous
        # values of batteries have to be given, but only the ones for the previous actID (and all before that) are
        # available.
        # Option 1:
        # Step 1: Write a function calculating the intersection between (id, act) in prev trip acts and prevTripIDs.
        # Step 2: Assign as per below
        # Step 3: Ex-post cope with the non-neighboring trip and park IDs

        # Option 2 (probably better):
        # Pass all previous trip (or park) activities concatenated to one dataframe. Should not be tooo heavy since
        # continuous appending is possible.
        multiIdxTrip = [(id, act, None) for id, act in zip(activeHHPersonIDs, prevTripIDs) if (
            id, act) in [(id2, act2) for id2, act2 in zip(activeHHPersonIDs, prevTripIDs)]]
        multiIdxPark = [(id, None, actID) for id in activeHHPersonIDs]

        # Index via integer index because loc park indices vary
        parkActsIdx = parkActs.set_index(['hhPersonID', 'tripID', 'parkID'])
        parkActsIdx.loc[multiIdxPark, 'maxBatteryLevelStart'] = prevTripActs.loc[multiIdxTrip,
                                                                                 'maxBatteryLevelEnd'].values

        parkActsIdx['maxBatteryLevelEnd_unlimited'] = parkActsIdx.loc[
            multiIdxPark, 'maxBatteryLevelStart'] + parkActsIdx.loc[multiIdxPark, 'maxChargeVolume']
        parkActsIdx.loc[multiIdxPark, 'maxBatteryLevelEnd'] = parkActsIdx['maxBatteryLevelEnd_unlimited'].where(
            parkActsIdx['maxBatteryLevelEnd_unlimited'] <= self.nettoBatCap, other=self.nettoBatCap)
        parkActsIdx['overshoot'] = parkActsIdx['maxBatteryLevelEnd_unlimited'] - parkActsIdx['maxBatteryLevelEnd']
        return parkActsIdx.reset_index()

    def calcMaxBatLastAct(self):
        """Calculate maximum battery levels for all last activities be it parking or trips
        """
        lastTripIdx = (self.isTrip) & (self.isLastAct)
        lastTripIdx = lastTripIdx.loc[lastTripIdx].index
        self.activities.loc[lastTripIdx, 'maxBatteryLevelStart'] = self.activities.loc[lastTripIdx - 1,
                                                                                       'maxBatteryLevelEnd']
        theoBatLev = self.activities.loc[lastTripIdx, 'maxBatteryLevelStart'] - self.activities.loc[lastTripIdx,
                                                                                                    'drain']
        self.activities.loc[lastTripIdx, 'maxBatteryLevelEnd'] = theoBatLev.where(theoBatLev >= 0, other=0)

    def uncontrolledCharging(self):
        pass

    def residualEnergy(self):
        pass

    def minBatteryLevel(self):
        pass

    def estimateTechnicalFlexibility(self):
        self.drain()
        self.maxChargeVolumePerParkingAct()
        self.maxBatteryLevel()
        self.uncontrolledCharging()
        self.residualEnergy()
        self.minBatteryLevel()


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
