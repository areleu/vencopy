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

import math

import pandas as pd
from pathlib import Path
from vencopy.utils.globalFunctions import loadConfigDict
from vencopy.core.dataParsers import ParseMiD, ParseKiD, ParseVF
from vencopy.core.gridModelers import GridModeler
from vencopy.core.flexEstimators import FlexEstimator


class DiaryBuilder:
    def __init__(self, configDict: dict, activities: pd.DataFrame, debug: bool = False):
        self.diaryConfig = configDict['diaryConfig']
        self.globalConfig = configDict['globalConfig']
        self.localPathConfig = configDict['localPathConfig']
        self.datasetID = datasetID
        self.deltaTime = configDict['diaryConfig']['TimeDelta']
        if debug:
            self.activities = activities.loc[0:2000, :]
        else:
            self.activities = activities.copy()
        distributedActivities = TimeDiscretiser(
            activities=self.activities, dt=self.deltaTime, method="distribute")
        self.drain = distributedActivities.discretise(column="drain")
        # self.uncontrolledCharge = distributedActivities.discretise(distributedActivities, column="uncontrolledCharge")
        # self.auxiliaryFuel = distributedActivities.discretise(distributedActivities, column="auxFuel")
        selectedActivities = TimeDiscretiser(
            activities=self.activities, dt=self.deltaTime, method="select")
        self.chargingPower = selectedActivities.discretise(distributedActivities, column="chargingPower")
        # self.minBatteryLevel = selectedActivities.discretise(distributedActivities, column="minBatLev")
        # self.maxBatteryLevel = selectedActivities.discretise(distributedActivities, column="maxBatLev")



class TimeDiscretiser:
    def __init__(self, activities, dt, method: str):
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
        self.method = method
        self.quantum = pd.Timedelta(value=1, unit='min')
        self.dt = pd.Timedelta(value=dt, unit='min')  # e.g. 15 min
        # list of all time intervals
        # idx = pd.DatetimeIndex(start='00:00:00', end='23:59:00', freq=f'{self.nTimeSlots}T')
        # FIXME: right way to create list? do we want timedelta or datetime?
        self.timeList = list(pd.timedelta_range(start='00:00:00', end='23:59:00', freq=f'{self.nTimeSlots}T'))  
        self.weights = None
        # Column definitions
        # FIXME: drop days count in delta? only keep HH:MM:SS?
        self.activities['delta'] = self.activities['timestampEnd'] - self.activities['timestampStart']
        self.activities['nQuants'] = None
        self.activities['nQFirst'] = None
        self.activities['nQLast'] = None
        self.activities['nFullSlots'] = None
        self.activities['nPartialSlots'] = None
        self.activities['nQFull'] = None
        self.activities['wFirstTS'] = None
        self.activities['wLastTS'] = None
        self.activities['valFullTS'] = None
        self.activities['valFirstTS'] = None
        self.activities['valLastTS'] = None
        self.identifySlots()
        self.allocate()

    
    def identifySlots(self):
        self.nTimeSlots = self.nSlotsPerInterval(interval=self.dt)
        self.calcTimeQuantumShares()


    def nSlotsPerInterval(self, interval: pd.Timedelta):
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

    
    def calcTimeQuantumShares(self):
        self.activities['nSlots'] = (self.activities['delta'] / self.dt)
        self.activities['nFullSlots'] = self.calcNFullSlots()
        # self.activities['nQFirst'] = self.calcFirstSlotQuants()
        # self.activities['nQLast'] = self.calcLastSlotQuants()
        # self.activities = self.calcShare()

    def calcFirstSlotQuants():  # needed?
        # number of quants for slot
        pass

    def calcLastSlotQuants():  # needed?
        pass

    def calcNFullSlots(self):
        self.activities['nFullSlots'] = math.floor(self.activities['delta'] / self.dt)
        # FIXME: Correct after first and last slot shares are calculated
        # for i in range(len(self.activities)):
        #     if ((self.activities['delta'][i] / self.dt).is_integer()):
        #         self.activities['nFullSlots'][i] = (self.activities['delta'][i] / self.dt)
        #     else:
        #         self.activities['nPartialSlots'][i] = round((self.activities['delta'][i] / self.dt),2)
        # return self.activities

    def discretise(self, activities, column: str):
        self.discreteStructure()
        # add all other funtions that are not called in the init
        self.allocate()

    def discreteStructure(self, dt):
        self.discrete = pd.DataFrame(index=self.activities.index, columns=self.timeList)

    def getFirstSlot(self):
        tsStart = self.activities['timestampStart'].apply(lambda x: pd.Timestamp(year=x.year, month=x.month, day=x.day))
        self.activities['dailyTimeDeltaStart'] = self.activities['timestampStart'] - tsStart
        diff = self.activities['dailyTimeDeltaStart'].apply(lambda x: x - self.timeList.to_series())

    def getLastSlot(self):
        tsEnd = self.activities['timestampEnd'].apply(lambda x: pd.Timestamp(year=x.year, month=x.month, day=x.day))
        self.activities['dailyTimeDeltaEnd'] = self.activities['timestampEnd'] - tsEnd

    def calcWeights(self):
        self.activities['wFirstSlot'] = self.calculateFirstSlotWeight()
        self.activities['wLastSlot'] = self.calculateLastSlotWeight()
        self.weights = self.activities.loc[:, ['wFullSlots', 'wFirstSlot', 'wLastSlot']]

    def calcFirstSlotsWeight(self):
        pass

    def calcLastSlotWeight(self):
        pass

    def allocate(self):
        # wrapper for method:
        if self.method == 'distribute':
            self.valueDistribute()
        elif self.method == 'select':
            self.valueSelect()
        else:
            raise(ValueError(
                f'Specified method {self.method} is not implemented please specify "distribute" or "select".'))

    def valueDistribute(self):
        wSum = self.activities['wFullSlots'] * self.activities['nFullSlots'] + self.activities['wFirstSlot'] + self.activities['wLastSlot']
        val = self.activities['value']
        self.activities['valfullSlots'] = self.activities['wFullSlot'] / wSum * val
        self.activities['valfirstSlot'] = self.activities['wFirstSlot'] * val
        self.activities['vallastSlot'] = self.activities['wLastSlot'] * val

    def valueSelect(self):
        self.activities['valFullSlots'] = self.activities['value']
        if self.firstSlotNotFull():  # FIXME: make firstSlotNotFull part of another checking function
            self.activities['valFirstSlot'] = self.activities['value']
        else:
            self.activities['valFirstSlot'] = 0
        if self.lastSlotNotFull():
            self.activities['valLastSlot'] = self.activities['value']
        else:
            self.activities['valLastSlot'] = 0

    def isFirstSlotNotFull(self):
        pass

    def isLastSlotNotFull(self):
        pass

    def isSlotRelevant(self, timedelta):
        return timedelta / self.quantum >= self.nQPerDT / 2

    

    def mergeTrips(self):
        """
        Merge multiple individual trips into one diary consisting of multiple trips

        :param activities: Input trip data with specified time resolution
        :return: Merged trips diaries
        """
        print("Merging single dataframe entries into vehicle trip diaries.")
        # dataDay = self.activities.groupby(['genericID']).sum()
        # dataDay = self.activities.drop('tripID', axis=1)
        # return dataDay

    def createDiaries(self):
        self.mergeTrips()
        print(f'Diary creation completed. There are {len(self.activities)} diaries')


class FillHourValues: # re use in distribute()
    def __init__(self, data, rangeFunction):
        self.startHour = data['tripStartHour']
        self.distanceStartHour = data['shareStartHour'] * data['tripDistance']
        self.endHour = data['tripEndHour']
        self.distanceEndHour = data['shareEndHour'] * data['tripDistance']
        self.fullHourCols = data.apply(rangeFunction, axis=1)
        # self.fullHourRange = data['fullHourTripLength'] / data['noOfFullHours']
        self.fullHourRange = data['fullHourTripLength']

    def __call__(self, row):
        idx = row.name
        # if np.isnan(row[self.startHour[idx]]):
        row[self.startHour[idx]] = self.distanceStartHour[idx]
        # else:
        #    row[self.startHour[idx]] = row[self.startHour[idx]] + self.distanceStartHour[idx]

        if self.endHour[idx] != self.startHour[idx]:
            row[self.endHour[idx]] = self.distanceEndHour[idx]
        if isinstance(self.fullHourCols[idx], (range, list)):
            row[self.fullHourCols[idx]] = self.fullHourRange[idx] / len(self.fullHourCols[idx])
        return row


if __name__ == '__main__':

    from vencopy.utils.globalFunctions import loadConfigDict

    datasetID = "MiD17"
    basePath = Path(__file__).parent.parent
    configNames = (
        "globalConfig",
        "localPathConfig",
        "parseConfig",
        "diaryConfig",
        "gridConfig",
        "flexConfig",
        "evaluatorConfig",
    )
    configDict = loadConfigDict(configNames, basePath=basePath)

    if datasetID == "MiD17":
        vpData = ParseMiD(configDict=configDict, datasetID=datasetID)
    elif datasetID == "KiD":
        vpData = ParseKiD(configDict=configDict, datasetID=datasetID)
    elif datasetID == "VF":
        vpData = ParseVF(configDict=configDict, datasetID=datasetID)
    vpData.process()

    vpGrid = GridModeler(configDict=configDict, datasetID=datasetID, activities=vpData.activities, gridModel='simple')
    vpGrid.assignGrid()

    vpFlex = FlexEstimator(configDict=configDict, activities=vpGrid.activities)
    vpFlex.estimateTechnicalFlexibility()

    vpDiary = DiaryBuilder(configDict=configDict, activities=vpFlex.activities, debug=False)
    vpDiary.createDiaries()
