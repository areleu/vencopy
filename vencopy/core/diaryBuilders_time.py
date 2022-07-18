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

import datetime
import numpy as np
import pandas as pd
from pathlib import Path
from vencopy.utils.globalFunctions import loadConfigDict
from vencopy.core.dataParsers import ParseMiD, ParseKiD, ParseVF
from vencopy.core.gridModelers import GridModeler
from vencopy.core.flexEstimators import FlexEstimator


class DiaryBuilder:
    def __init__(self, configDict: dict, activities: pd.DataFrame, debug: bool = True):
        self.diaryConfig = configDict['diaryConfig']
        self.globalConfig = configDict['globalConfig']
        self.localPathConfig = configDict['localPathConfig']
        self.datasetID = datasetID
        self.deltaTime = configDict['diaryConfig']['TimeDelta']
        if debug:
            self.activities = activities.loc[0:20, :]
        else:
            self.activities = activities.copy()
        distributedActivities = TimeDiscretiser(
            activities=self.activities, dt=self.deltaTime, method="distribute")
        self.drain = distributedActivities.discretise(column="drain")
        self.uncontrolledCharge = distributedActivities.discretise(column="uncontrolledCharge")
        self.residualNeed = distributedActivities.discretise(column="residualNeed") # in elec terms kWh elec
        selectedActivities = TimeDiscretiser(
            activities=self.activities, dt=self.deltaTime, method="select")
        self.chargingPower = selectedActivities.discretise(column="chargingPower")
        self.minBatteryLevel = selectedActivities.discretise(column="minBatLev")
        self.maxBatteryLevel = selectedActivities.discretise(column="maxBatLev")


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
        self.dt = dt  # e.g. 15 min
        self.nTimeSlots = self.nSlotsPerInterval(interval=pd.Timedelta(value=self.dt, unit='min'))
        self.timeDelta = (pd.timedelta_range(start='00:00:00', end='24:00:00', freq=f'{self.dt}T'))
        self.timeIndex = list(self.timeDelta)
        # self.createTimeIndex()

    # def createTimeIndex(self):
    #     pass

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

    def datasetCleanup(self):
        # timestamp start and end, unique ID, column to discretise - get read of additional columns
        # FIXME: add function to save other columns in a separate df or on disk
        necessaryColumns = ['tripID', 'timestampStart', 'timestampEnd', 'genericID',
                            'parkID', 'isFirstActivity', 'isLastActivity', 'timedelta',
                            'actID', 'nextActID', 'prevActID'] + [self.columnToDiscretise]
        self.activities = self.activities[necessaryColumns]
        self.correctValues()

    def correctValues(self):
        if self.columnToDiscretise == 'drain':
            pass
        elif self.columnToDiscretise == 'uncontrolledCharge':
            # remove all rows with tripID
            self.activities = self.activities[self.activities['uncontrolledCharge'].notna()]
        elif self.columnToDiscretise == 'residualNeed':
            # pad NaN with 0
            self.activities['residualNeed'] = self.activities['residualNeed'].fillna(0)
        else:
            return self.activities

    def createDiscretisedStructure(self):
        self.discreteData = pd.DataFrame(index=self.activities.index, columns=range(len(list(self.timeIndex))))

    def identifyBinShares(self):  # calculate value share
        self.calculateValueBinsAndQuanta()
        # wrapper for method:
        if self.method == 'distribute':
            self.valueDistribute()
        elif self.method == 'select':
            self.valueSelect()
        else:
            raise(ValueError(
                f'Specified method {self.method} is not implemented please specify "distribute" or "select".'))

    def calculateValueBinsAndQuanta(self):
        self.activities['delta'] = self.activities['timestampEnd'] - self.activities['timestampStart']
        self.activities['nSlots'] = self.activities['delta'] / (pd.Timedelta(value=self.dt, unit='min'))
        self.activities['nFullSlots'] = np.floor(self.activities['nSlots'])
        self.activities['nPartialSlots'] = np.ceil((self.activities['nSlots'])-self.activities['nFullSlots'])
        self.activities['nQuantaPerActivity'] = (self.activities['delta'] / np.timedelta64(1, 'm')) / (self.quantum.seconds/60)

    def valueDistribute(self):
        self.activities['valQuantum'] = self.activities[self.columnToDiscretise] / self.activities['nQuantaPerActivity']
        self.activities['valFullSlot'] = (self.activities['valQuantum'] * ((pd.Timedelta(value=self.dt, unit='min')).seconds/60)).round(6)
        self.activities['valLastSlot'] = (self.activities[self.columnToDiscretise] - (self.activities['valFullSlot'] * self.activities['nFullSlots'])).round(6)

    def valueSelect(self):
        self.activities['valFullSlot'] = self.activities[self.columnToDiscretise]
        self.activities['valLastSLot'] = self.activities[self.columnToDiscretise]

    def identifySharedEventInBin(self):
        # binsTouched (startTimes info + lenght of activity) based on event timestamp
        self.identifyBins()
        self.isNeighbouringEvent()

    def identifyBins(self):
        # self.nTimeSlots = self.nSlotsPerInterval(interval=self.dt)
        self.identifyFirstBin()
        self.identifyLastBin()

    def identifyFirstBin(self):
        self.activities['timestampStart'] = self.activities['timestampStart'].apply(lambda x : pd.to_datetime(str(x)))
        dayStart = self.activities['timestampStart'].apply(lambda x: pd.Timestamp(year=x.year, month=x.month, day=x.day))
        self.activities['dailyTimeDeltaStart'] = self.activities['timestampStart'] - dayStart
        self.activities['startTimeFromMidnightSeconds'] = self.activities['dailyTimeDeltaStart'].apply(lambda x: x.seconds)
        bins = pd.DataFrame({'index': self.timeDelta})
        bins.drop(bins.tail(1).index, inplace=True) # remove last element, which is zero        
        self.binFromMidnightSeconds = bins['index'].apply(lambda x: x.seconds)
        # self.activities['firstBin'] = self.activities['startTimeFromMidnightSeconds'].apply(lambda x: np.where(x >= self.binFromMidnightSeconds)[0][-1])
        # more efficient below (edge case of value bigger than any bin, index will be -1)
        self.activities['firstBin'] = self.activities['startTimeFromMidnightSeconds'].apply(lambda x: np.argmax(x < self.binFromMidnightSeconds)-1)

    def identifyLastBin(self):
        dayEnd = self.activities['timestampEnd'].apply(lambda x: pd.Timestamp(year=x.year, month=x.month, day=x.day))
        self.activities['dailyTimeDeltaEnd'] = self.activities['timestampEnd'] - dayEnd
        # Option 1
        # activitiesLength = self.activities['timedelta'].apply(lambda x: x.seconds)
        # self.activities['endTimeFromMidnightSeconds'] = self.activities['startTimeFromMidnightSeconds'] + activitiesLength
        # Option 2
        # self.activities['endTimeFromMidnightSeconds'] = self.activities['dailyTimeDeltaEnd'].apply(lambda x: x.seconds)
        # self.activities['lastBin'] = self.activities['endTimeFromMidnightSeconds'].apply(lambda x: np.argmax(x < self.binFromMidnightSeconds)-1)
        # Option 3
        self.activities['lastBin'] = self.activities['firstBin'] + self.activities['nFullSlots']

    def isNeighbouringEvent(self):
        # compare end bin event i with start bin event i+1
        # check by grouping through genericID
        for i in range(len(self.activities)):
            self.activities['isNeighbouringEvent'] = np.where(self.activities['lastBin'][i] < self.activities['firstBin'][i+1])

    def overlappingEvents(self):
        # function to handle overlapping bin i.e. where self.activities['isNeighbouringEvent'] == False
        # function of currentEvent, nextEvent, depending on how many bins are overlapping, collect bin to be filled in dictionary
        # differentiate between partial and complete bin, algo should be able to treat from 1 to n values per bin
        # list of tuples with current value of bin and current share
        pass
 
    def allocate(self):
        # strategies on how to treat overlapping bins
        # start allocation always with full bin
        pass

    def allocateBinShares(self):
        self.identifySharedEventInBin()
        self.overlappingEvents()  # identify shared events in bin and handle them
        self.allocate()

    def writeOut():
        pass


    def discretise(self, column: str):
        self.columnToDiscretise = column
        self.datasetCleanup()
        self.createDiscretisedStructure()
        self.identifyBinShares()
        self.allocateBinShares()
        self.writeOut()
        self.columnToDiscretise = None


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
