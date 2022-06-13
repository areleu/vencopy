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
        # self.distributedActivtiesSeries = self.activities.loc[:, 'drain']
        # self.selectedActivtiesSeries = self.activities.loc[:, 'chargingPower']
        distributedActivities = TimeDiscretizer(
            activities=self.activities, dt=self.deltaTime, method="distribute")
        self.drain = distributedActivities.discretise(distributedActivities, column="drain")
        selectedActivities = TimeDiscretizer(
            activities=self.activities, dt=self.deltaTime, method="distribute")
        # FIXME: check column names
        self.chargingPower = selectedActivities.discretise(distributedActivities, column="chargingPower")
        # self.minBatteryLevel = selectedActivities.discretise(distributedActivities, column="minBatLev")
        # self.maxBatteryLevel = selectedActivities.discretise(distributedActivities, column="maxBatLev")


class TimeDiscretizer:
    def __init__(self, activities: pd.Series, dt, method: str):
        """
        Class for discretization of activities to fixed temporal resolution. Act is
        a pandas Series with a unique ID in the index, ts is a pandas dataframe with two
        columns: timestampStart and timestampEnd, dt is a pandas TimeDelta object
        specifying the fixed resolution that the discretization should output. Method
        specifies how the discretization should be carried out. 'Distribute' assumes
        act provides a divisible variable (energy, distance etc.) and distributes this
        depending on the time share of the activity within the respective time interval.
        'Select' assumes an undivisible variable such as power is given and selects
        the values for the given timestamps. For now: If start or end timestamp of an
        activity exactly hits the middle of a time interval (dt/2), the value is allocated
        if its ending but not if its starting (value set to 0). For dt=30 min, a parking
        activity ending at 9:15 with a charging availability of 11 kW, 11 kW will be assigned
        to the last slot (9:00-9:30) whereas if it had started at 7:45, the slot (7:30-8:00)
        is set to 0 kW.
        The quantum is the shortest possible time interval for the discretizer, hard
        coded in the init and given as a pandas.TimeDelta. Thus if 1 minute is selected
        discretization down to resolutions of seconds are not possible.

        Args:
            act (pd.Series): _description_
            column (str): String specifying the column of the activities data set that should be discretized
            dt (pd.TimeDelta): _description_
            method (str): The discretization method. Must be one of 'distribute' or 'select'.
        """
        self.act = activities
        self.method = method

        self.quantum = pd.TimeDelta(value=1, unit='min')
        self.dt = dt  # e.g. 15 min
        self.nQPerDT = self.nQPerInterval(self.dt, self.quantum)

        self.timeList = self.createTimeList()  # list of all time intervals
        self.weights = None

        # Column definitions
        self.act['delta'] = self.act['timestampEnd'] - self.act['timestampStart']

        self.act['nQFirst'] = None
        self.act['nQLast'] = None
        self.act['nFullSlots'] = None
        self.act['nQFull'] = None
        self.__calcTimeQuantumShares()

        self.act['wFirstTS'] = None
        self.act['wLastTS'] = None
        self.act['valFullTS'] = None
        self.act['valFirstTS'] = None
        self.act['valLastTS'] = None

    def nQPerInterval(self, interval: pd.Timedelta, quantum=pd.Timedelta):
        # Check if interval is an even multiple of quantum
        quot = interval / quantum

        # Check if a full number of intervals fits into one day
        quotDay = pd.Timedelta(freq='D') / interval
        if isinstance(quot, int) and isinstance(quotDay, int):
            return quot
        elif isinstance(quot, int):
            raise(ValueError(f'Specified resolution does not fit into a day, There are {quotDay} intervals in a day'))
        else:
            raise(ValueError(f'Specified resolution is not a multiple of the pre specified quantum {self.quantum}.'
                             f'You specified {interval}'))

    def createTimeList(self):
        return list(pd.timedelta_range(start='00:00', end='23:59', freq=f'{self.nQPerDT}T'))

    def discreteStructure(self, dt):
        self.discrete = pd.DataFrame(index=self.act.index, columns=self.timeList)

    def getFirstSlot(self):
        tsStart = self.act['timestampStart'].apply(lambda x: pd.Timestamp(year=x.year, month=x.month, day=x.day))
        self.act['dailyTimeDeltaStart'] = self.act['timestampStart'] - tsStart
        diff = self.act['dailyTimeDeltaStart'].apply(lambda x: x - self.timeList.to_series())

    def getLastSlot(self):
        tsEnd = self.act['timestampEnd'].apply(lambda x: pd.Timestamp(year=x.year, month=x.month, day=x.day))
        self.act['dailyTimeDeltaEnd'] = self.act['timestampEnd'] - tsEnd

    def __calcTimeQuantumShares(self, quantumLength: pd.TimeDelta):
        self.act['nQFirst'] = self.calcFirstSlotQuants()
        self.act['nQLast'] = self.calcLastSlotQuants()
        self.act['nQFirst'] = self.calcNFullSlots()

    def calcFirstSlotQuants():
        pass

    def calcNFullTS(self):
        # FIXME: Correct after first and last slot shares are calculated
        self.act['nFullSlots'] = int(self.act['delta'] / self.act['dt'])  # FIXME: round

    def calcWeights(self):
        self.act['wFirstSlot'] = self.calculateFirstSlotWeight()
        self.act['wLastSlot'] = self.calculateLastSlotWeight()
        self.weights = self.act.loc[:, ['wFullSlots', 'wFirstSlot', 'wLastSlot']]

    def calcFirstSlotsWeight(self):
        pass

    def calcLastSlotWeight(self):
        pass

    def calcAbsoluteTSValue(self):
        if self.method == 'distribute':
            self.valueDistribute()
        elif self.method == 'select':
            self.valueSelect()
        else:
            raise(ValueError(
                f'Specified method {self.method} is not implemented please specify "distribute" or "select".'))

    def valueDistribute(self):
        wSum = self.act['wFullSlots'] * self.act['nFullSlots'] + self.act['wFirstSlot'] + self.act['wLastSlot']
        val = self.act['value']
        self.act['valfullSlots'] = self.act['wFullSlot'] / wSum * val
        self.act['valfirstSlot'] = self.act['wFirstSlot'] * val
        self.act['vallastSlot'] = self.act['wLastSlot'] * val

    def valueSelect(self):
        self.act['valFullSlots'] = self.act['value']
        if self.firstSlotNotFull():  # FIXME: make firstSlotNotFull part of another checking function
            self.act['valFirstSlot'] = self.act['value']
        else:
            self.act['valFirstSlot'] = 0
        if self.lastSlotNotFull():
            self.act['valLastSlot'] = self.act['value']
        else:
            self.act['valLastSlot'] = 0

    def isFirstSlotNotFull(self):
        pass

    def isLastSlotNotFull(self):
        pass

    def isSlotRelevant(self, timedelta):
        return timedelta / self.quantum >= self.nQPerDT / 2

    def allocate(self):
        pass

    def discretise(col: str):
        pass

    def mergeTrips(self):
        """
        Merge multiple individual trips into one diary consisting of multiple trips

        :param activities: Input trip data with specified time resolution
        :return: Merged trips diaries
        """
        print("Merging trips")
        # dataDay = self.activities.groupby(['genericID']).sum()
        # dataDay = self.activities.drop('tripID', axis=1)
        # return dataDay

    def createDiaries(self):
        self.mergeTrips()
        print(f'Diary creation completed. There are {len(self.activities)} diries')


class FillHourValues:
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
