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
from profilehooks import profile
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
                         'minBatteryLevelEnd', 'maxBatteryLevelEnd_unlimited', 'minBatteryLevelEnd_unlimited',
                         'maxResidualNeed', 'minResidualNeed', 'maxOvershoot', 'minOvershoot']] = None

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

    @profile(immediate=True)
    def batteryLevelMax(self):
        """

        Calculate the maximum battery level at the beginning and end of each activity. This represents the case of
        vehicle users always connecting when charging is available and charging as soon as possible as fast as possible
        until the maximum battery capacity is reached. 

        """

        # SUPPOSEDLY DEPRECATED GENERALIZED APPROACH
        # Calculate max battery start and end of all first activities
        # if type == 'max':
        #     firstActs = self.calcMaxBatFirstAct()
        # elif type == 'min':
        #     firstActs = self.calcMinBatFirstAct()
        # else:
        #     raise(ValueError(f'Specified value {type} not allowed, please select "min" or "max".'))

        print('Starting maximum battery level calculation')
        firstActs = self.calcMaxBatFirstAct()
        actTemp = firstActs

        # Start and end for all trips and parkings in between
        setActs = range(1, self.activities['parkID'].max() + 1)
        for act in setActs:
            print(f'Calculate maximum battery level for act {act}')
            tripRows = (self.activities['tripID'] == act) & (~self.isFirstAct)  # & ~self.isLastAct  probably not needed
            parkRows = (self.activities['parkID'] == act) & (~self.isFirstAct)  # & ~self.isLastAct
            tripActs = self.activities.loc[tripRows, :]
            parkActs = self.activities.loc[parkRows, :]
            prevTripActs = actTemp.loc[~actTemp['tripID'].isna(), :]

            if act == 1:
                tripActsRes = self.calcBatLevTrip(actID=act, tripActs=tripActs, prevParkActs=firstActs, type='max')
            else:
                parkActsRes = self.calcBatLevPark(actID=act, parkActs=parkActs, prevTripActs=prevTripActs, type='max')
                actTemp = pd.concat([actTemp, parkActsRes], ignore_index=True)
                prevParkActs = actTemp.loc[~actTemp['parkID'].isna(), :]
                tripActsRes = self.calcBatLevTrip(actID=act, tripActs=tripActs, prevParkActs=prevParkActs, type='max')

            actTemp = pd.concat([actTemp, tripActsRes], ignore_index=True)
            prevTripActs = tripActsRes

        self.activities = actTemp.sort_values(by=['hhPersonID', 'actID', 'parkID'])

    @profile(immediate=True)
    def batteryLevelMin(self):
        """
        Calculate the minimum battery level at the beginning and end of each activity. This represents the case of
        vehicles just being charged for the energy required for the next trip and as late as possible. The loop works
        exactly inverted to the batteryLevelMax() function since later trips influence the energy that has to be 
        charged in parking activities before. Thus, activities are looped over from the last activity to first. 
        TBD: The prefix "prev" is still used for previous in the loop (not in the activity-time-domain), however with 
        lastAct refers to the last activity in the activity-time-domain.

        """

        print('Starting minimum battery level calculation')

        lastActs = self.calcMinBatLastAct()
        actTemp = lastActs

        # Start and end for all trips and parkings starting from the last activities, then looping to earlier acts
        setActs = range(self.activities['parkID'].max(), 0, -1)
        for act in setActs:
            print(f'Calculate maximum battery level for act {act}')
            tripRows = (self.activities['tripID'] == act) & (~self.activities['isLastActivity'])
            parkRows = (self.activities['parkID'] == act) & (~self.activities['isLastActivity'])
            tripActs = self.activities.loc[tripRows, :]
            parkActs = self.activities.loc[parkRows, :]
            nextTripActs = actTemp.loc[~actTemp['tripID'].isna(), :]

            if act == self.activities['parkID'].max() - 1:
                tripActsRes = self.calcBatLevTrip(actID=act, tripActs=tripActs, nextParkActs=lastActs, type='min')
                actTemp = pd.concat([actTemp, tripActsRes], ignore_index=True)
                nextTripActs = actTemp.loc[~actTemp['tripID'].isna(), :]
                parkActsRes = self.calcBatLevPark(actID=act, parkActs=parkActs, nextTripActs=nextTripActs, type='min')  
            else:
                tripActsRes = self.calcBatLevTrip(actID=act, parkActs=parkActs, nextParkActs=nextParkActs, type='min')

            actTemp = pd.concat([actTemp, tripActsRes], ignore_index=True)
            nextTripActs = actTemp.loc[~actTemp['tripID'].isna(), :]
            parkActsRes = self.calcBatLevPark(actID=act, parkActs=parkActs, nextTripActs=nextTripActs, type='min')

            actTemp = pd.concat([actTemp, parkActsRes], ignore_index=True)
            nextParkActs = parkActsRes

        self.activities = actTemp.sort_values(by=['hhPersonID', 'actID', 'parkID'])

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

    def calcMinBatLastAct(self):
        # First activities - parking and trips
        lastAct = self.activities.loc[self.activities['isLastActivity'], :]
        lastAct.loc[self.isPark, 'minBatteryLevelEnd'] = self.lowerBatLev
        lastAct.loc[self.isPark, 'minBatteryLevelStart'] = self.lowerBatLev
        lastAct.loc[self.isTrip, 'minBatteryLevelEnd'] = self.lowerBatLev
        lastAct.loc[self.isTrip, 'minBatteryLevelStart'] = lastAct.loc[self.isTrip, 'drain'].where(
            lastAct.loc[self.isTrip, 'drain'] >= self.nettoBatCap, other=self.nettoBatCap)
        resNeed = lastAct.loc[self.isTrip, 'drain'] - self.nettoBatCap
        lastAct.loc[self.isTrip, 'residualNeed'] = resNeed.where(resNeed >= self.lowerBatLev, other=self.lowerBatLev)

        # SUPPOSEDLY DEPRECATED
        # lastAct.loc[self.isTrip, 'maxBatteryLevelEnd_unlimited'] = lastAct.loc[
        #     self.isTrip, 'maxBatteryLevelStart'] - lastAct.loc[self.isTrip, 'drain']
        # lastAct.loc[self.isTrip, 'maxBatteryLevelEnd'] = lastAct.loc[
        #     self.isTrip, 'maxBatteryLevelEnd_unlimited'].where(lastAct.loc[
        #         self.isTrip, 'maxBatteryLevelEnd_unlimited'] >= self.lowerBatLev, other=self.lowerBatLev)
        # resNeed = self.isTrip, 'maxBatteryLevelEnd_unlimited'] -
        # lastAct.loc[self.isTrip, 'residualNeed'] = lastAct.loc[
        #     self.isTrip, 'maxBatteryLevelEnd_unlimited'].where(lastAct.loc[
        #         self.isTrip, 'maxBatteryLevelEnd_unlimited'] < self.lowerBatLev, other=0)
        return lastAct

    def calcBatLevTrip(
            self, actID: int, tripActs: pd.DataFrame, type: str, prevParkActs: pd.DataFrame = None,
            nextParkActs: pd.DataFrame = None):
        # Setting trip activity battery start level to battery end level of previous parking
        activeHHPersonIDs = tripActs.loc[:, 'hhPersonID']
        multiIdxTrip = [(id, actID, None) for id in activeHHPersonIDs]

        # Index the previous park activity via integer index because loc park indices vary
        tripActsIdx = tripActs.set_index(['hhPersonID', 'tripID', 'parkID'])

        if type == 'max' and prevParkActs is not None:
            prevParkIDs = tripActs.loc[:, 'prevActID']
            multiIdxPark = [(id, None, act) for id, act in zip(activeHHPersonIDs, prevParkIDs)]
            prevParkActsIdx = prevParkActs.set_index(['hhPersonID', 'tripID', 'parkID'])
            tripActsIdx.loc[multiIdxTrip, 'maxBatteryLevelStart'] = prevParkActsIdx.loc[multiIdxPark,
                                                                                        'maxBatteryLevelEnd'].values
            # Setting maximum battery end level for trip
            tripActsIdx.loc[multiIdxTrip, 'maxBatteryLevelEnd_unlimited'] = tripActsIdx.loc[
                multiIdxTrip, 'maxBatteryLevelStart'] - tripActsIdx.loc[multiIdxTrip, 'drain']
            tripActsIdx.loc[multiIdxTrip, 'maxBatteryLevelEnd'] = tripActsIdx.loc[
                multiIdxTrip, 'maxBatteryLevelEnd_unlimited'].where(tripActsIdx.loc[
                    multiIdxTrip, 'maxBatteryLevelEnd_unlimited'] >= 0, other=0)
            tripActsIdx.loc[multiIdxTrip, 'maxResidualNeed'] = tripActsIdx.loc[
                multiIdxTrip, 'maxBatteryLevelEnd_unlimited'].where(tripActsIdx.loc[
                    multiIdxTrip, 'maxBatteryLevelEnd_unlimited'] < 0, other=0)
        elif type == 'min' and nextParkActs is not None:
            nextParkIDs = tripActs.loc[:, 'nextActID']
            multiIdxPark = [(id, None, act) for id, act in zip(activeHHPersonIDs, nextParkIDs)]
            nextParkActsIdx = nextParkActs.set_index(['hhPersonID', 'tripID', 'parkID'])
            tripActsIdx.loc[multiIdxTrip, 'minBatteryLevelEnd'] = nextParkActsIdx.loc[multiIdxPark,
                                                                                      'minBatteryLevelStart'].values

            # Setting minimum battery end level for trip
            tripActsIdx.loc[multiIdxTrip, 'minBatteryLevelStart_unlimited'] = tripActsIdx.loc[
                multiIdxTrip, 'minBatteryLevelEnd'] + tripActsIdx.loc[multiIdxTrip, 'drain']
            tripActsIdx.loc[multiIdxTrip, 'minBatteryLevelEnd'] = tripActsIdx.loc[
                multiIdxTrip, 'minBatteryLevelStart_unlimited'].where(tripActsIdx.loc[
                    multiIdxTrip, 'minBatteryLevelStart_unlimited'] <= self.nettoBatCap, other=self.nettoBatCap)
            resNeed = tripActsIdx.loc[multiIdxTrip, 'minBatteryLevelStart_unlimited'] - self.nettoBatCap
            tripActsIdx.loc[multiIdxTrip, 'minResidualNeed'] = resNeed.where(resNeed >= 0, other=0)
        elif type == 'max':
            raise NameError('Battery level trip calculation called for type max but previous park activity not given')
        elif type == 'min':
            raise NameError('Battery level trip calculation called for type min but previous park activity not given')
        else:
            raise NameError(f'Battery level trip calculation called but parameter type was specified as {type}. Only '
                            f'"min" or "max" are allowed.')
        return tripActsIdx.reset_index()

    def calcBatLevPark(self, actID: int, parkActs: pd.DataFrame, type: str, prevTripActs: pd.DataFrame = None,
                       nextTripActs: pd.DataFrame = None):
        # Setting next park activity battery start level to battery end level of current trip
        activeHHPersonIDs = parkActs.loc[:, 'hhPersonID']
        multiIdxPark = [(id, None, actID) for id in activeHHPersonIDs]
        parkActsIdx = parkActs.set_index(['hhPersonID', 'tripID', 'parkID'])

        if type == 'max' and prevTripActs is not None:
            # Preliminary defs
            prevTripIDs = parkActs.loc[:, 'prevActID']
            multiIdxTrip = [(id, act, None) for id, act in zip(activeHHPersonIDs, prevTripIDs)]
            prevTripActsIdx = prevTripActs.set_index(['hhPersonID', 'tripID', 'parkID'])

            parkActsIdx.loc[multiIdxPark, 'maxBatteryLevelStart'] = prevTripActsIdx.loc[multiIdxTrip,
                                                                                        'maxBatteryLevelEnd'].values

            parkActsIdx['maxBatteryLevelEnd_unlimited'] = parkActsIdx.loc[
                multiIdxPark, 'maxBatteryLevelStart'] + parkActsIdx.loc[multiIdxPark, 'maxChargeVolume']
            parkActsIdx.loc[multiIdxPark, 'maxBatteryLevelEnd'] = parkActsIdx['maxBatteryLevelEnd_unlimited'].where(
                parkActsIdx['maxBatteryLevelEnd_unlimited'] <= self.nettoBatCap, other=self.nettoBatCap)
            parkActsIdx['maxOvershoot'] = parkActsIdx['maxBatteryLevelEnd_unlimited'] - parkActsIdx[
                'maxBatteryLevelEnd']
        elif type == 'min' and nextTripActs is not None:
            # Preliminary defs
            nextTripIDs = parkActs.loc[:, 'nextActID']
            multiIdxTrip = [(id, act, None) for id, act in zip(activeHHPersonIDs, nextTripIDs)]
            nextTripActsIdx = nextTripActs.set_index(['hhPersonID', 'tripID', 'parkID'])

            parkActsIdx.loc[multiIdxPark, 'minBatteryLevelEnd'] = nextTripActsIdx.loc[multiIdxTrip,
                                                                                      'minBatteryLevelStart'].values

            parkActsIdx['minBatteryLevelStart_unlimited'] = parkActsIdx.loc[
                multiIdxPark, 'minBatteryLevelEnd'] + parkActsIdx.loc[multiIdxPark, 'maxChargeVolume']
            parkActsIdx.loc[multiIdxPark, 'minBatteryLevelEnd'] = parkActsIdx['minBatteryLevelEnd_unlimited'].where(
                parkActsIdx['minBatteryLevelEnd_unlimited'] <= self.nettoBatCap, other=self.nettoBatCap)
            parkActsIdx['minOvershoot'] = parkActsIdx['minBatteryLevelEnd_unlimited'] - parkActsIdx[
                'minBatteryLevelEnd']
        elif type == 'max':
            raise NameError('Battery level trip calculation called for type max but previous park activity not given')
        elif type == 'min':
            raise NameError('Battery level trip calculation called for type min but previous park activity not given')
        else:
            raise NameError(f'Battery level trip calculation called but parameter type was specified as {type}. Only'
                            f'"min" or "max" are allowed.')
        return parkActsIdx.reset_index()

    # DEPRECATED
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
        self.activities.loc[~self.activities['tripID'].isna(), 'uncontrolledCharge'] = self.activities[
            'maxBatteryLevelEnd'] - self.activities['maxBatteryLevelStart']

    def auxFuelNeed(self):
        self.activities['auxiliaryFuelNeed'] = self.activities['residualNeed'] * self.flexConfig[
            'Fuel_consumption'] / self.flexConfig['Electric_consumption']

    def estimateTechnicalFlexibility(self):
        self.drain()
        self.maxChargeVolumePerParkingAct()
        self.batteryLevelMax()
        self.uncontrolledCharging()
        self.batteryLevelMin()


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
