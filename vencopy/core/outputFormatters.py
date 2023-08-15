__version__ = '0.4.X'
__maintainer__ = 'Niklas Wulff, Fabia Miorelli'
__birthdate__ = '12.07.2023'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'


from pathlib import Path
import pandas as pd

from vencopy.core.profileAggregators import ProfileAggregator
from vencopy.utils.globalFunctions import createFileName, writeOut


class OutputFormatter():
    def __init__(self, configDict: dict, profiles: ProfileAggregator):
        self.appConfig = configDict['appConfig']
        self.devConfig = configDict['devConfig']
        self.datasetID = self.appConfig["global"]["dataset"]
        self.deltaTime = self.appConfig['dataParsers']['TimeDelta']
        self.timeIndex = list(pd.timedelta_range(
            start='00:00:00', end='24:00:00', freq=f'{self.deltaTime}T'))
        self.drain = profiles.drainWeekly
        self.chargingPower = profiles.chargingPowerWeekly
        self.uncontrolledCharge = profiles.uncontrolledChargeWeekly
        self.maxBatteryLevel = profiles.maxBatteryLevelWeekly
        self.minBatteryLevel = profiles.minBatteryLevelWeekly

    def __createAnnualProfiles(self):
        startWeekday = self.appConfig["outputFormatters"]['startWeekday']  # (1: Monday, 7: Sunday)
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
        if self.appConfig["global"]["writeOutputToDisk"]["formatterOutput"]:
            root = Path(self.appConfig["global"]['pathAbsolute']['vencopyRoot'])
            folder = self.devConfig["global"]['pathRelative']['formatterOutput']
            fileName = createFileName(devConfig=self.devConfig, appConfig=self.appConfig, manualLabel='', fileNameID='outputOutputFormatter',
                                      datasetID=self.datasetID)
            writeOut(data=self.activities, path=root / folder / fileName)

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
