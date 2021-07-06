__version__ = '0.0.9'
__maintainer__ = 'Niklas Wulff'
__contributors__ = 'Fabia Miorelli, Parth Butte, Ronald Stegen'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '30.09.2020'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'


#----- imports & packages ------
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import yaml
import os
from scripts.globalFunctions import createFileString


class GridModeler:
    def __init__(self, gridConfig: dict, globalConfig:dict, datasetID : str ='MiD17'):
        self.inputFileName = createFileString(globalConfig=globalConfig, fileKey='purposesProcessed',
                                              datasetID=datasetID)
        self.inputFilePath = Path(globalConfig['pathRelative']['input']) / self.inputFileName
        self.inputDriveProfilesName = createFileString(globalConfig=globalConfig, fileKey='inputDataDriveProfiles',
                                                       datasetID=datasetID)
        self.inputDriveProfilesPath = Path(globalConfig['pathRelative']['input']) / self.inputDriveProfilesName
        self.scalarsPath = (Path(globalConfig['pathRelative']['input']) / globalConfig['files']['inputDataScalars'])
        self.gridMappings = gridConfig['chargingInfrastructureMappings']
        self.gridProbability = gridConfig['gridAvailabilityProbability']
        self.gridDistribution = gridConfig['gridAvailabilityDistribution']
        self.gridFastCharging = gridConfig['gridAvailabilityFastCharging']
        self.outputFileName = createFileString(globalConfig=globalConfig, fileKey='inputDataPlugProfiles',
                                               datasetID=datasetID)
        self.outputFilePath = Path(globalConfig['pathRelative']['input']) / self.outputFileName
        self.purposeData = pd.read_csv(self.inputFilePath, keep_default_na=False)
        self.driveData = pd.read_csv(self.inputDriveProfilesPath, keep_default_na=False)
        self.chargeAvailability = None

    def trips(self):
        self.scalars = pd.read_excel(self.scalarsPath, header=5, usecols='A:C', skiprows=0)
        scalarsOut = self.scalars.set_index('parameter')
        driveProfiles = self.driveData.set_index(['hhPersonID'])
        driveProfiles = driveProfiles.loc[:].sum(axis=1)
        driveProfiles = driveProfiles * (scalarsOut.loc['Electric_consumption_NEFZ', 'value'] / 100)
        driveProfiles = np.where(driveProfiles > (0.80 * (scalarsOut.loc['Battery_capacity', 'value'])), driveProfiles, 0)
        driveProfiles = pd.DataFrame(driveProfiles)
        driveProfiles.set_index(self.driveData['hhPersonID'], inplace=True)
        driveProfiles = driveProfiles.replace(0, np.nan)
        driveProfiles = driveProfiles.dropna(how='all', axis=0)
        driveProfiles.reset_index(inplace=True)
        drive = pd.Series(driveProfiles['hhPersonID'])
        driveList = []
        for i, item in drive.items():
            driveList.append(item)
        return driveList

    def assignSimpleGridViaPurposes(self):
        print(f'Starting with charge connection replacement of location purposes')
        self.chargeAvailability = self.purposeData.replace(self.gridMappings)
        self.chargeAvailability.set_index(['hhPersonID'], inplace=True)
        self.chargeAvailability = (~(self.chargeAvailability != True))
        print('Grid connection assignment complete')

    def assignGridViaProbabilities(self, model: str, driveList):
        self.chargeAvailability = self.purposeData.copy()
        self.chargeAvailability.set_index(['hhPersonID'], inplace=True)
        print('Starting with charge connection replacement ')
        np.random.seed(42)

        for hhPersonID, row in self.chargeAvailability.iterrows():
            activity = row.copy(deep=True)
            if hhPersonID in driveList:
                for iHour in range(0, len(row)):
                    if iHour == 0:
                        if model == 'probability':
                            row[iHour] = self.getRandomNumberForModel1(activity[iHour])
                        elif model == 'distribution':
                            row[iHour] = self.getRandomNumberForModel3(activity[iHour])
                    elif iHour > 0:
                        if activity[iHour] == activity[iHour - 1]:
                            # print(row[j-1])
                            row[iHour] = row[iHour - 1]
                        else:
                            if model == 'probability':
                                row[iHour] = self.getRandomNumberForModel1(activity[iHour])
                            elif model == 'distribution':
                                row[iHour] = self.getRandomNumberForModel3(activity[iHour])
            else:
                for iHour in range(0, len(row)):
                    if iHour == 0:
                        if model == 'probability':
                            row[iHour] = self.getRandomNumberForModel1(activity[iHour])
                        elif model == 'distribution':
                            row[iHour] = self.getRandomNumberForModel2(activity[iHour])
                    elif iHour > 0:
                        if activity[iHour] == activity[iHour - 1]:
                            # print(row[j-1])
                            row[iHour] = row[iHour - 1]
                        else:
                            if model == 'probability':
                                row[iHour] = self.getRandomNumberForModel1(activity[iHour])
                            elif model == 'distribution':
                                row[iHour] = self.getRandomNumberForModel2(activity[iHour])
            self.chargeAvailability.loc[hhPersonID] = row
        print('Grid connection assignment complete')

    def getRandomNumberForModel1(self, purpose):
        if purpose == "HOME":
            rnd = np.random.random_sample()
            if rnd <= 1:
                rnd = self.gridProbability['HOME'][1]
            else:
                rnd = 0.0
        elif purpose == "WORK":
            rnd = np.random.random_sample()
            if rnd <= 1:
                rnd = self.gridProbability['WORK'][1]
            else:
                rnd = 0.0
        elif purpose == "DRIVING":
            rnd = 0
            if rnd == 0:
                rnd = self.gridProbability['DRIVING'][1]
        elif purpose == "LEISURE":
            rnd = np.random.random_sample()
            if rnd <= 1:
                rnd = self.gridProbability['LEISURE'][1]
            else:
                rnd = 0.0
        elif purpose == "SHOPPING":
            rnd = np.random.random_sample()
            if rnd <= 1:
                rnd = self.gridProbability['SHOPPING'][1]
            else:
                rnd = 0.0
        elif purpose == "SCHOOL":
            rnd = np.random.random_sample()
            if rnd <= 1:
                rnd = self.gridProbability['SCHOOL'][1]
            else:
                rnd = 0.0
        elif purpose == "OTHER":
            rnd = np.random.random_sample()
            if rnd <= 1:
                rnd = self.gridProbability['OTHER'][1]
            else:
                rnd = 0.0
        else:
            rnd = 0
            if rnd == 0:
                rnd = self.gridProbability['NA'][1]
        return rnd

    def getRandomNumberForModel2(self, purpose):
        if purpose == 'DRIVING':
            rnd = 0
        else:
            rnd = np.random.random_sample()

        keys = list(self.gridDistribution[purpose].keys())
        values = list(self.gridDistribution[purpose].values())
        range_dict = {}
        length = len(keys)
        count = 0
        prob_min = 0
        n2 = keys[0]

        for index, (key, value) in enumerate(self.gridDistribution[purpose].items()):
            total = prob_min + key
            range_dict.update({index: {'min_range': prob_min, 'max_range': total}})
            prob_min = total
            # print('first total: ', total)
            # print('range: ', range_dict)
            # print('probmin: ', prob_min)

        # for i in range(0, length):
        #     total = prob_min + n2
        #     range_dict.update({count: {'min_range': prob_min, "max_range": total}})
        #     # 0: {'min_range': 0, "max_range": 0.5}
        #     # 1: {'min_range': 0.5, "max_range": 0.75}
        #     # 2: {'min_range': 0.75, "max_range": 80}
        #     # 3: {'min_range': 0.80, "max_range": 1}
        #     prob_min = total
        #     count += 1
        #     if count >= length:
        #         continue
        #     n2 = keys[length - (length - count)]

        for keyValue, rangeValue in range_dict.items():
            if rangeValue['min_range'] <= rnd <= rangeValue['max_range']:
                # print(keyValue, rangeValue, rnd, purpose)
                # print(rangeValue['min_range'], rangeValue['max_range'])
                power = values[keyValue]
                # print(power, purpose, rnd, keyValue)
                break
        return power

    def getRandomNumberForModel3(self, purpose):
        rnd = np.random.random_sample()

        keys = list(self.gridFastCharging[purpose].keys())
        values = list(self.gridFastCharging[purpose].values())
        range_dict_fast = {}
        prob_min = 0
        length = len(keys)
        count = 0
        n2 = keys[0]

        # for i in range(0, length):
        #     total = prob_min + n2
        #     range_dict_fast.update({count: {'min_range': prob_min, "max_range": total}})
        #     # 0: {'min_range': 0, "max_range": 0.5}
        #     # 1: {'min_range': 0.5, "max_range": 0.75}
        #     # 2: {'min_range': 0.75, "max_range": 80}
        #     # 3: {'min_range': 0.80, "max_range": 1}
        #     prob_min = total
        #     count += 1
        #     if count >= length:
        #         continue
        #     n2 = keys[length - (length - count)]

        for index, (key, value) in enumerate(self.gridFastCharging[purpose].items()):
            total = prob_min + key
            range_dict_fast.update({index: {'min_range': prob_min, 'max_range': total}})
            prob_min = total

        for keyValue, rangeValue in range_dict_fast.items():
            if rangeValue['min_range'] <= rnd <= rangeValue['max_range']:
                power = values[keyValue]
                # print(rnd, power, purpose)
                break
        return power

    def writeOutGridAvailability(self):
        self.chargeAvailability.to_csv(self.outputFilePath)

    def stackPlot(self):
        capacity = self.chargeAvailability.transpose()
        total3kW = capacity.where(capacity.loc[:] == 3.6).count(axis=1)
        total11kW = capacity.where(capacity.loc[:, :] == 11).count(axis=1)
        total22kW = capacity.where(capacity.loc[:, :] == 22).count(axis=1)
        total50kW = capacity.where(capacity.loc[:, :] == 50).count(axis=1)
        total75kW = capacity.where(capacity.loc[:, :] == 75).count(axis=1)
        total0kW = capacity.where(capacity.loc[:, :] == 0).count(axis=1)
        totalChargingStation = pd.concat([total3kW, total11kW, total22kW, total50kW, total75kW, total0kW], axis=1)
        totalChargingStation.rename(columns={0: '3.6 kW', 1: '11 kW', 2: '22 kW', 3: '50 kW', 4: '75 kW', 5: '0 kW'}, inplace=True)
        totalChargingStation = totalChargingStation
        totalChargingStation.plot(kind='area', title='Vehicles connected to different charging station over 24 hours', figsize=(10, 8))
        plt.show()

        purposes = self.purposeData.copy()
        purposes = purposes.replace('0.0', 0)
        purposes = purposes.set_index(['hhPersonID']).transpose()
        totalHome = purposes.where(purposes.loc[:] == 'HOME').count(axis=1)
        totalWork = purposes.where(purposes.loc[:] == 'WORK').count(axis=1)
        totalDriving = purposes.where(purposes.loc[:] == 'DRIVING').count(axis=1)
        totalShopping = purposes.where(purposes.loc[:] == 'SHOPPING').count(axis=1)
        totalLeisure = purposes.where(purposes.loc[:] == 'LEISURE').count(axis=1)
        totalSchool = purposes.where(purposes.loc[:] == 'SCHOOL').count(axis=1)
        totalOther = purposes.where(purposes.loc[:] == 'OTHER').count(axis=1)
        totalNA = purposes.where(purposes.loc[:] == 0).count(axis=1)
        totalTripPurpose = pd.concat([totalHome, totalWork, totalDriving, totalShopping, totalLeisure, totalSchool, totalOther, totalNA], axis=1)
        totalTripPurpose.rename(columns={0: 'Home', 1: 'Work', 2: 'Driving', 3: 'Shopping', 4: 'Leisure', 5: 'School', 6: 'Other', 7: 'NA'}, inplace=True)
        totalTripPurpose = totalTripPurpose/len(purposes.columns)
        totalTripPurpose.plot(kind='area', title='Trip purposes during 24 hours', figsize=(10, 8))
        plt.show()

if __name__ == '__main__':
    pathGlobalConfig = Path.cwd().parent / 'config' / 'globalConfig.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    with open(pathGlobalConfig) as ipf:
        globalConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    pathGridConfig = Path.cwd().parent / 'config' / 'gridConfig.yaml'
    with open(pathGridConfig) as ipf:
        gridConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    pathLocalPathConfig = Path.cwd().parent / 'config' / 'localPathConfig.yaml'
    with open(pathLocalPathConfig) as ipf:
        localPathConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    os.chdir(localPathConfig['pathAbsolute']['vencoPyRoot'])
    vpg = GridModeler(gridConfig=gridConfig, globalConfig=globalConfig)
    driveList = vpg.trips()
    # vpg.assignSimpleGridViaPurposes()
    vpg.assignGridViaProbabilities(model='distribution', driveList= driveList)
    vpg.writeOutGridAvailability()
    vpg.stackPlot()