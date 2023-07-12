__version__ = '0.4.X'
__maintainer__ = 'Niklas Wulff, Fabia Miorelli'
__birthdate__ = '12.07.2023'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'

if __package__ is None or __package__ == '':
    import sys
    from os import path
    sys.path.append(path.dirname(path.dirname(path.dirname(__file__))))

from pathlib import Path
import pandas as pd

from vencopy.core.profileAggregators import ProfileAggregator
from vencopy.utils.globalFunctions import (createFileName, writeOut)


class OutputFormatter():
    def __init__(self, configDict: dict, profiles: ProfileAggregator):
        self.outputConfig = configDict['outputConfig']
        self.globalConfig = configDict['globalConfig']
        self.localPathConfig = configDict['localPathConfig']
        self.datasetID = configDict["globalConfig"]["dataset"]
        self.deltaTime = configDict['diaryConfig']['TimeDelta']
        self.timeIndex = list(pd.timedelta_range(
            start='00:00:00', end='24:00:00', freq=f'{self.deltaTime}T'))
        self.drain = profiles.drainWeekly
        self.chargingPower = profiles.chargingPowerWeekly
        self.uncontrolledCharge = profiles.uncontrolledChargeWeekly
        self.maxBatteryLevel = profiles.maxBatteryLevelWeekly
        self.minBatteryLevel = profiles.minBatteryLevelWeekly

    def __createAnnualProfiles(self):
        startWeekday = self.outputConfig['startWeekday']  # (1: Monday, 7: Sunday)
        # shift input profiles to the right weekday and start with first bin of chosen weekday
        self.annualProfile = self.profile.iloc[(
            (startWeekday - 1) * ((len(list(self.timeIndex))) - 1)):]
        self.annualProfile = self.annualProfile.append(
            [self.profile] * 52, ignore_index=True)
        self.annualProfile.drop(
            self.annualProfile.tail(
                len(self.annualProfile) - ((len(list(
                    self.timeIndex))) - 1) * 365).index, inplace=True)

    def _writeOutput(self):
        root = Path(self.localPathConfig['pathAbsolute']['vencoPyRoot'])
        folder = self.globalConfig['pathRelative']['formatterOutput']

        fileName = createFileName(globalConfig=self.globalConfig, manualLabel=(
            '_' + self.profileName + ''),
            fileNameID='outputOutputFormatter', datasetID=self.datasetID)
        writeOut(data=self.profile, path=root / folder / fileName)

    def createTimeseries(self):
        profiles = (self.drain, self.uncontrolledCharge, self.chargingPower,
                    self.maxBatteryLevel, self.minBatteryLevel)
        profilesNames = ('drain', 'uncontrolledCharge', 'chargingPower',
                         'maxBatteryLevel', 'minBatteryLevel')
        for profile, profileName in zip(profiles, profilesNames):
            self.profileName = profileName
            self.profile = profile
            self.__createAnnualProfiles()
            self._writeOutput()
        print('Run finished.')
