__version__ = '0.4.X'
__maintainer__ = 'Niklas Wulff'
__contributors__ = 'Fabia Miorelli'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '21.04.2019'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'

if __package__ is None or __package__ == '':
    import sys
    from os import path
    sys.path.append(path.dirname(path.dirname(path.dirname(__file__))))

import time
from pathlib import Path

import numpy as np
import pandas as pd
from vencopy.core.dataParsers import ParseKiD, ParseMiD, ParseVF
from vencopy.core.flexEstimators import FlexEstimator
from vencopy.core.gridModelers import GridModeler
from vencopy.utils.globalFunctions import loadConfigDict, writeOut


class DiaryBuilder:
    def __init__(self, configDict: dict, datasetID: str, activities: pd.DataFrame):
        self.diaryConfig = configDict['diaryConfig']
        self.globalConfig = configDict['globalConfig']
        self.localPathConfig = configDict['localPathConfig']
        self.datasetID = datasetID
        self.activities = activities
        self.deltaTime = configDict['diaryConfig']['TimeDelta']
        self.distributedActivities = TimeDiscretiser(
            datasetID=self.datasetID, globalConfig=self.globalConfig,
            localPathConfig=self.localPathConfig, activities=self.activities, dt=self.deltaTime, method="distribute")
        self.selectedActivities = TimeDiscretiser(
            datasetID=self.datasetID, globalConfig=self.globalConfig,
            localPathConfig=self.localPathConfig, activities=self.activities, dt=self.deltaTime, method="select")

    def createDiaries(self):
        self.drain = self.distributedActivities.discretise(column="drain")
        self.uncontrolledCharge = self.distributedActivities.discretise(column="uncontrolledCharge")
        self.chargingPower = self.selectedActivities.discretise(column="chargingPower")
        self.maxBatteryLevel = self.selectedActivities.discretise(column="maxBatteryLevelStart")
        self.minBatteryLevel = self.selectedActivities.discretise(column="minBatteryLevelStart")
        # # self.residualNeed = self.distributedActivities.discretise(column="residualNeed") # in elec terms kWh elec
        # # self.maxBatteryLevelEnd = self.selectedActivities.discretise(column="maxBatteryLevelEnd")
        # # self.minBatteryLevelEnd = self.selectedActivities.discretise(column="minBatteryLevelEnd")

class TimeDiscretiser:
    def __init__(self, activities, dt, datasetID, method: str, globalConfig, localPathConfig):
        """
        Class for discretisation of activities to fixed temporal resolution. Act is
        a pandas Series with a unique ID in the index, ts is a pandas dataframe with two
        columns: timestampStart and timestampEnd, dt is a pandas TimeDelta object
        specifying the fixed resolution that the discretisation should output. Method
        specifies how the discretisation should be carried out. 'Distribute' assumes
        act provides a divisible variable (energy, distance etc.) and distributes this
        depending on the time share of the activity within the respective time interval.
        'Select' assumes an undivisible variable such as power is given and selects
        the values for the given timestamps. For now: If start or end timestamp of an
        activity exactly hits the middle of a time interval (dt/2), the value is allocated
        if its ending but not if its starting (value set to 0). For dt=30 min, a parking
        activity ending at 9:15 with a charging availability of 11 kW, 11 kW will be assigned
        to the last slot (9:00-9:30) whereas if it had started at 7:45, the slot (7:30-8:00)
        is set to 0 kW.
        The quantum is the shortest possible time interval for the discretiser, hard
        coded in the init and given as a pandas.TimeDelta. Thus if 1 minute is selected
        discretisation down to resolutions of seconds are not possible.

        Args:
            act (pd.dataFrame): _description_
            column (str): String specifying the column of the activities data set that should be discretized
            dt (pd.TimeDelta): _description_
            method (str): The discretisation method. Must be one of 'distribute' or 'select'.
        """
        self.activities = activities
        self.datasetID = datasetID
        self.method = method
        self.oneActivity = None
        self.localPathConfig = localPathConfig
        self.globalConfig = globalConfig
        self.quantum = pd.Timedelta(value=1, unit='min')
        self.dt = dt  # e.g. 15 min
        self.nTimeSlots = self._nSlotsPerInterval(interval=pd.Timedelta(value=self.dt, unit='min'))
        self.timeDelta = (pd.timedelta_range(start='00:00:00', end='24:00:00', freq=f'{self.dt}T'))
        self.timeIndex = list(self.timeDelta)

    def _nSlotsPerInterval(self, interval: pd.Timedelta):
        # Check if interval is an integer multiple of quantum
        # the minimum resolution is 1 min, case for resolution below 1 min
        if interval.seconds/60 < self.quantum.seconds/60:
            raise(ValueError(f'The specified resolution is not a multiple of {self.quantum} minute, '
                             f'which is the minmum possible resolution'))
        # Check if an integer number of intervals fits into one day (15 min equals 96 intervals)
        quot = (interval.seconds/3600/24)
        quotDay = pd.Timedelta(value=24, unit='h') / interval
        if ((1/quot) % int(1/quot) == 0):  # or (quot % int(1) == 0):
            return quotDay
        else:
            raise(ValueError(f'The specified resolution does not fit into a day.'
                             f'There cannot be {quotDay} finite intervals in a day'))

    def _datasetCleanup(self):
        # timestamp start and end, unique ID, column to discretise - get read of additional columns
        necessaryColumns = ['tripID', 'timestampStart', 'timestampEnd', 'genericID',
                            'parkID', 'isFirstActivity', 'isLastActivity', 'timedelta',
                            'actID', 'nextActID', 'prevActID'] + [self.columnToDiscretise]
        # FIXME: rename self.oneActivity to self.oneProfile
        self.oneActivity = self.activities[necessaryColumns].copy()
        self._correctDataset()

    def _correctDataset(self):
        self._correctValues()
        self._correctTimestamp()
        # FIXME: need of a recheck that timstampStart(t) == timestampEnd(t-1)?

    def _correctValues(self):
        if self.columnToDiscretise == 'drain':
            self.oneActivity['drain'] = self.oneActivity['drain'].fillna(0)
        elif self.columnToDiscretise == 'uncontrolledCharge':
            # remove all rows with tripID
            # self.oneActivity = self.oneActivity[self.oneActivity['uncontrolledCharge'].notna()]
            # instead of removing rows with tripID, assign 0 to rows with tripID
            self.oneActivity['uncontrolledCharge'] = self.oneActivity['uncontrolledCharge'].fillna(0)
        elif self.columnToDiscretise == 'residualNeed':
            # pad NaN with 0
            self.oneActivity['residualNeed'] = self.oneActivity['residualNeed'].fillna(0)
        return self.oneActivity

    def _correctTimestamp(self):
        self.oneActivity['timestampStartCorrected'] = self.oneActivity['timestampStart'].dt.round(f'{self.dt}min')
        self.oneActivity['timestampEndCorrected'] = self.oneActivity['timestampEnd'].dt.round(f'{self.dt}min')

    def _createDiscretisedStructure(self):
        self.discreteData = pd.DataFrame(
            index=self.oneActivity.genericID.unique(), columns=range(len(list(self.timeIndex))-1))

    def _identifyBinShares(self):  # calculate value share
        self._calculateValueBinsAndQuanta()
        self._identifyBins()
        # wrapper for method:
        if self.method == 'distribute':
            self._valueDistribute()
        elif self.method == 'select':
            self._valueSelect()
        else:
            raise(ValueError(
                f'Specified method {self.method} is not implemented please specify "distribute" or "select".'))

    def _calculateValueBinsAndQuanta(self):
        self.oneActivity['activityDuration'] = (
            self.oneActivity['timestampEndCorrected']-self.oneActivity['timestampStartCorrected'])
        # remove rows with activitsDuration = 0 which cause division by zero in nBins calculation
        # self.oneActivity = self.oneActivity[self.oneActivity['activityDuration'] != pd.Timedelta(value=0, unit='min')]
        self.oneActivity = self.oneActivity.drop(
            self.oneActivity[self.oneActivity.timestampStartCorrected == self.oneActivity.timestampEndCorrected].index)
        self.oneActivity['nBins'] = self.oneActivity['activityDuration'] / (pd.Timedelta(value=self.dt, unit='min'))
        # self.activities['nSlots'] = self.activities['delta'] / (pd.Timedelta(value=self.dt, unit='min'))
        # self.activities['nFullSlots'] = np.floor(self.activities['nSlots'])
        # self.activities['nPartialSlots'] = np.ceil((self.activities['nSlots'])-self.activities['nFullSlots'])
        # self.activities['nQuantaPerActivity'] = (
        #       self.activities['delta'] / np.timedelta64(1, 'm')) / (self.quantum.seconds/60)

    def _valueDistribute(self):
        # FIXME: add double check for edge case treatment for nBins == 0 (happens in uncontrolled Charge)
        self.oneActivity['valPerBin'] = self.oneActivity[self.columnToDiscretise] / self.oneActivity['nBins']
        # self.activities['valQuantum'] = (
        #           self.activities[self.columnToDiscretise] / self.activities['nQuantaPerActivity'])
        # self.activities['valFullSlot'] = (self.activities['valQuantum'] * ((
        #           pd.Timedelta(value=self.dt, unit='min')).seconds/60)).round(6)

    def _valueSelect(self):
        self.oneActivity['valPerBin'] = self.oneActivity[self.columnToDiscretise]
        # self.activities['valFullSlot'] = self.activities[self.columnToDiscretise]
        # self.activities['valLastSLot'] = self.activities[self.columnToDiscretise]

    def _identifyBins(self):
        self._identifyFirstBin()
        self._identifyLastBin()

    def _identifyFirstBin(self):
        self.oneActivity['timestampStartCorrected'] = self.oneActivity['timestampStartCorrected'].apply(
            lambda x: pd.to_datetime(str(x)))
        dayStart = self.oneActivity['timestampStartCorrected'].apply(
            lambda x: pd.Timestamp(year=x.year, month=x.month, day=x.day))
        self.oneActivity['dailyTimeDeltaStart'] = self.oneActivity['timestampStartCorrected'] - dayStart
        self.oneActivity['startTimeFromMidnightSeconds'] = self.oneActivity['dailyTimeDeltaStart'].apply(
            lambda x: x.seconds)
        bins = pd.DataFrame({'index': self.timeDelta})
        bins.drop(bins.tail(1).index, inplace=True)  # remove last element, which is zero
        self.binFromMidnightSeconds = bins['index'].apply(lambda x: x.seconds)
        # self.activities['firstBin'] = self.activities['startTimeFromMidnightSeconds'].apply(
        # lambda x: np.where(x >= self.binFromMidnightSeconds)[0][-1])
        # more efficient below (edge case of value bigger than any bin, index will be -1)
        self.oneActivity['firstBin'] = self.oneActivity['startTimeFromMidnightSeconds'].apply(
            lambda x: np.argmax(x < self.binFromMidnightSeconds)-1)

    def _identifyLastBin(self):
        dayEnd = self.oneActivity['timestampEndCorrected'].apply(
            lambda x: pd.Timestamp(year=x.year, month=x.month, day=x.day))
        self.oneActivity['dailyTimeDeltaEnd'] = self.oneActivity['timestampEndCorrected'] - dayEnd
        # Option 1
        # activitiesLength = self.activities['timedelta'].apply(lambda x: x.seconds)
        # self.activities['endTimeFromMidnightSeconds'] =
        #   self.activities['startTimeFromMidnightSeconds'] + activitiesLength
        # Option 2
        # self.activities['endTimeFromMidnightSeconds'] = self.activities['dailyTimeDeltaEnd'].apply(
        #               lambda x: x.seconds)
        # self.activities['lastBin'] = self.activities['endTimeFromMidnightSeconds'].apply(
        #                 lambda x: np.argmax(x < self.binFromMidnightSeconds)-1)
        # Option 3
        self.oneActivity['lastBin'] = self.oneActivity['firstBin'] + self.oneActivity['nBins'] - 1
        # FIXME: more elegant way for lastBin +1 if lastActivity = True -> +1 ?
        self.oneActivity.loc[self.oneActivity['isLastActivity'] == True, 'lastBin'] = (
            self.oneActivity.loc[self.oneActivity['isLastActivity'] == True, 'lastBin'] + 1)

    def _allocateBinShares(self):
        self._overlappingActivities()  # identify shared events in bin and handle them
        self._allocate()
        self._checkBinValues()

    def _checkBinValues(self):
        # verify all bins get a value assigned
        # pad with 0
        pass

    # def _dropNoLengthEvents(self):
    # moved before nBins calculation
    #     self.oneActivity.drop(
    #         self.oneActivity[self.oneActivity.timestampStartCorrected
    #         == self.oneActivity.timestampEndCorrected].index)

    def _overlappingActivities(self):
        pass
        # stategy if time resolution high enough so that event becomes negligible (now in calculateValueBinsAndQuanta)
        # self._dropNoLengthEvents()
        # define other strategies to treat overlapping events here

    def _allocate(self):
        for id in self.oneActivity.genericID.unique():
            vehicleSubset = self.oneActivity[self.oneActivity.genericID == id].reset_index(drop=True)
            for irow in range(len(vehicleSubset)):
                self.discreteData.loc[id, (vehicleSubset.loc[irow, 'firstBin']):(
                    vehicleSubset.loc[irow, 'lastBin'])] = vehicleSubset.loc[irow, 'valPerBin']
        # return self.discreteData

    def _writeOutput(self):
        writeOut(dataset=self.discreteData, outputFolder='diaryOutput', datasetID=self.datasetID,
                 fileKey=('outputDiaryBuilder'), manualLabel=str(self.columnToDiscretise),
                 localPathConfig=self.localPathConfig, globalConfig=self.globalConfig)

    def discretise(self, column: str):
        self.columnToDiscretise = column
        print(f"Starting to discretise {self.columnToDiscretise}.")
        self._datasetCleanup()
        self._createDiscretisedStructure()
        self._identifyBinShares()
        self._allocateBinShares()
        # self._writeOutput()
        print(f"Discretisation finished for {self.columnToDiscretise}.")
        self.columnToDiscretise = None
        return self.discreteData


if __name__ == '__main__':

    startTime = time.time()
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

    elapsedTime = time.time() - startTime
    print('Elapsed time:', elapsedTime)
