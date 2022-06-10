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
from vencopy.scripts.globalFunctions import loadConfigDict
from vencopy.classes.dataParsers import ParseMiD, ParseKiD, ParseVF


class DiaryBuilder:
    def __init__(self, configDict: dict, activities: pd.DataFrame, debug: bool = False):
        self.diaryConfig = configDict['diaryConfig']
        self.globalConfig = configDict['globalConfig']
        self.localPathConfig = configDict['localPathConfig']
        self.datasetID = datasetID
        self.deltaTime = self.diaryConfig['TimeDelta']
        if debug:
            self.activties = self.activties.loc[0:2000, :]
        else:
            self.activties = activities.data
        distributedActivities = TimeDiscretiser(self.deltaTime, self.activties, method="distribute")
        # FIXME: check column names
        drain = distributedActivities(distributedActivities, column="drain")
        selectedActivities = TimeDiscretiser(self.deltaTime, self.activties, method="distribute")
        # FIXME: check column names
        chargingPower = selectedActivities(distributedActivities, column="chargingPower")
        minBatteryLevel = selectedActivities(distributedActivities, column="minBatLev")
        maxBatteryLevel = selectedActivities(distributedActivities, column="maxBatLev")
        self.activties = self.mergeTrips()

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

class timeDiscretiser:
    def __init__(self, activities: pd.Series, ts: pd.DataFrame, column: str, dt: pd.TimeDelta, method: str):
        """Class for discretization of activities to fixed temporal resolution. Act is
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
        self.act = act

        # TBD if we wanna do it this way
        self.start = act['timestampStart']
        self.end = act['timestampEnd']
        self.method = method

        self.quantum = pd.TimeDelta(value=1, unit='min')
        self.dt = dt  # e.g. 15 min
        self.nQPerDT = dt / self.quantum

        self.timeList = self.createTimeList()  # list of all time intervals
        self.weights = None

        # Column definitions
        self.act['delta'] = None
        self.act['nFullTS'] = None  # FIXME: switch TS to time intervals
        self.act['nQFirst'] = None
        self.act['nQLast'] = None
        self.act['wFullTS'] = None  # FIXME: Maybe not needed
        self.act['wFirstTS'] = None
        self.act['wLastTS'] = None
        self.act['valFullTS'] = None
        self.act['valFirstTS'] = None
        self.act['valLastTS'] = None

        # FIXME: Change TS to slot

    def createTimeList(self):
        pass

    def __calcTimeQuantumShare(self, quantumLength: pd.TimeDelta):
        pass

    def discreteStructure(self, dt):
        self.discrete = pd.DataFrame(index=self.act.index,
                                     columns=self.timeList)

    def calcNFullTS(self):
        self.act['delta'] = self.act['timestampEnd'] - self.act['timestampStart']
        self.act['nFullTS'] = int(self.act['delta'] / self.act['dt'])  # FIXME: round

    def calcWeights(self):
        self.act['wFullTS'] = self.calculateFullTSWeight()  # FIXME: Probably not needed
        self.act['wFirstTS'] = self.calculateFirstTSWeight()
        self.act['wLastTS'] = self.calculateLastTSWeight()
        self.weights = self.act.loc[:, ['wFullTS', 'wFirstTS', 'wLastTS']]

    def calcFullTSWeight(self):
        pass

    def calcFirstTSWeight(self):
        pass

    def calcLastTSWeight(self):
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
        wSum = self.act['wFullTS'] * self.act['nFullTS'] + self.act['wFirstTS'] + self.act['wLastTS']
        val = self.act['value']
        self.act['valfullTS'] = self.act['wFullTS'] / wSum * val
        self.act['valfirstTS'] = self.act['wFirstTS'] * val
        self.act['vallastTS'] = self.act['wLastTS'] * val

    def valueSelect(self):
        self.act['valFullTS'] = self.act['value']
        if self.firstTSNotFull():  # FIXME: make firstTSNotFull part of another checking function
            self.act['valFirstTS'] = self.act['value']
        else:
            self.act['valFirstTS'] = 0
        if self.lastTSNotFull():
            self.act['valLastTS'] = self.act['value']
        else:
            self.act['valLastTS'] = 0

    def isFirstTSNotFull(self):
        pass

    def isLastTSNotFull(self):
        pass

    def isTSRelevant(self, timedelta: pd.TimeDelta):
        return timedelta / self.quantum >= self.nQPerDT / 2

    def allocate(self):
        pass

    def discretize(col: str):
        pass


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

    from vencopy.scripts.globalFunctions import loadConfigDict

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

    vpDiary = DiaryBuilder(configDict=configDict, activities=vpData, debug=False)
