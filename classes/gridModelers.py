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
from random import seed
import yaml
import os
from scripts.globalFunctions import createFileString


class GridModeler:
    def __init__(self, gridConfig: dict, globalConfig:dict, datasetID : str ='MiD17'):
        self.inputFileName = createFileString(globalConfig=globalConfig, fileKey='purposesProcessed',
                                              datasetID=datasetID)
        self.inputFilePath = Path(globalConfig['pathRelative']['input']) / self.inputFileName
        self.gridMappings = gridConfig['chargingInfrastructureMappings']
        self.gridProbability = gridConfig['gridAvailabilityProbability']
        self.gridDistribution = gridConfig['gridAvailabilityDistribution']
        self.outputFileName = createFileString(globalConfig=globalConfig, fileKey='inputDataPlugProfiles',
                                               datasetID=datasetID)
        self.outputFilePath = Path(globalConfig['pathRelative']['input']) / self.outputFileName
        self.purposeData = pd.read_csv(self.inputFilePath, keep_default_na=False)
        self.chargeAvailability = None

    def assignSimpleGridViaPurposes(self):
        print(f'Starting with charge connection replacement of location purposes')
        self.chargeAvailability = self.purposeData.replace(self.gridMappings)
        self.chargeAvailability.set_index(['hhPersonID'], inplace=True)
        # self.chargeAvailability = (~(self.chargeAvailability != True))
        print('Grid connection assignment complete')

    def assignGridViaProbabilities(self, model: str):
        self.chargeAvailability = self.purposeData.copy()
        self.chargeAvailability.set_index(['hhPersonID'], inplace=True)
        print('Starting with charge connection replacement ')
        np.random.seed(42)
        for hhPersonID, row in self.chargeAvailability.iterrows():
            activity = row.copy(deep=True)
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
        # print(purpose)
        range_dict = {}
        length = len(keys)
        count = 0
        prob_min = 0
        n2 = keys[0]
        for i in range(0, length):
            total = prob_min + n2
            range_dict.update({count: {'min_range': prob_min, "max_range": total}})
            # 0: {'min_range': 0, "max_range": 0.5}
            # 1: {'min_range': 0.5, "max_range": 0.8}
            # 2: {'min_range': 0.8, "max_range": 1}
            prob_min = total
            count += 1
            if count >= length:
                continue
            n2 = keys[length - (length - count)]

        for keyValue, rangeValue in range_dict.items():
            if rangeValue['min_range'] <= rnd <= rangeValue['max_range']:
                # print(keyValue, rangeValue, rnd, purpose)
                # print(rangeValue['min_range'], rangeValue['max_range'])
                power = values[keyValue]
                break
        return power
        # if purpose == "HOME":
        #     homeKey = list(self.gridDistribution['HOME'].keys())
        #     homeValue = list(self.gridDistribution['HOME'].values())
        #     rnd = np.random.random_sample()
        #     if rnd <= homeKey[0]:
        #         rnd = homeValue[0]
        #     elif rnd <= (homeKey[0] + homeKey[1]):
        #         rnd = homeValue[1]
        #     elif rnd <= (homeKey[0] + homeKey[1] + homeKey[2]):
        #         rnd = homeValue[2]
        #     elif rnd <= homeKey[0] + homeKey[1] + homeKey[2] + homeKey[3]:
        #         rnd = homeValue[3]
        #     elif purpose == "WORK":
        #         rnd = np.random.random_sample()
        #
        #     workValue= list(self.gridDistribution['WORK'].values())
        #     if rnd <= 0.5:
        #         rnd = workValue[0]
        #     elif rnd <= 0.7:
        #         rnd = workValue[1]
        #     elif rnd <= 1:
        #         rnd = workValue[2]
        # elif purpose == "DRIVING":
        #     rnd = 0
        #     if rnd == 0:
        #         rnd = self.gridDistribution['DRIVING'][1]
        # elif purpose == "LEISURE":
        #     rnd = np.random.random_sample()
        #     leisureValue= list(self.gridDistribution['LEISURE'].values())
        #     if rnd <= 0.35:
        #         rnd = leisureValue[0]
        #     elif rnd <= 0.55:
        #         rnd = leisureValue[1]
        #     elif rnd <= 1:
        #         rnd = leisureValue[2]
        # elif purpose == "SHOPPING":
        #     rnd = np.random.random_sample()
        #     shoppingValue= list(self.gridDistribution['SHOPPING'].values())
        #     if rnd <= 0.35:
        #         rnd = shoppingValue[0]
        #     elif rnd <= 0.55:
        #         rnd = shoppingValue[1]
        #     elif rnd <= 1:
        #         rnd = shoppingValue[2]
        # elif purpose == "SCHOOL":
        #     rnd = np.random.random_sample()
        #     schoolValue= list(self.gridDistribution['SCHOOL'].values())
        #     if rnd <= 0.35:
        #         rnd = schoolValue[0]
        #     elif rnd <= 0.55:
        #         rnd = schoolValue[1]
        #     elif rnd <= 1:
        #         rnd = schoolValue[2]
        # elif purpose == "OTHER":
        #     rnd = np.random.random_sample()
        #     otherValue= list(self.gridDistribution['OTHER'].values())
        #     if rnd <= 0.2:
        #         rnd = otherValue[0]
        #     elif rnd <= 0.3:
        #         rnd = otherValue[1]
        #     elif rnd <= 1:
        #         rnd = otherValue[2]
        # else:
        #     rnd = 0
        #     if rnd == 0:
        #         rnd = self.gridDistribution['NA'][1]


    def writeOutGridAvailability(self):
        self.chargeAvailability.to_csv(self.outputFilePath)

    def stackPlot(self):
        capacity = self.chargeAvailability.transpose()
        total3kW= capacity.where(capacity.loc[:] == 3.6).count(axis=1)
        total11kW= capacity.where(capacity.loc[:, :] == 11).count(axis=1)
        total22kW= capacity.where(capacity.loc[:, :] == 22).count(axis=1)
        total0kW= capacity.where(capacity.loc[:, :] == 0).count(axis=1)
        totalChargingStation= pd.concat([total3kW, total11kW, total22kW, total0kW], axis=1)
        totalChargingStation.rename(columns={0: 'Total 3.6 kW', 1: 'Total 11 kW', 2: 'Total 22 kW', 3: 'Total 0 kW'}, inplace=True)
        totalChargingStation= totalChargingStation/len(capacity.columns)
        totalChargingStation.plot(kind='area', title='Vehicles connected to different charging station over 24 hours', figsize=(10, 8))
        plt.show()

        purposes = self.purposeData.copy()
        purposes = purposes.set_index(['hhPersonID']).transpose()
        totalHome = purposes.where(purposes.loc[:] == 'HOME').count(axis=1)
        totalWork = purposes.where(purposes.loc[:] == 'WORK').count(axis=1)
        totalDriving= purposes.where(purposes.loc[:] == 'DRIVING').count(axis=1)
        totalShopping = purposes.where(purposes.loc[:] == 'SHOPPING').count(axis=1)
        totalLeisure= purposes.where(purposes.loc[:] == 'LEISURE').count(axis=1)
        totalSchool = purposes.where(purposes.loc[:] == 'SCHOOL').count(axis=1)
        totalTripPurpose= pd.concat([totalHome, totalWork, totalDriving, totalShopping, totalLeisure, totalSchool], axis=1)
        totalTripPurpose.rename(columns={0: 'Home', 1: 'Work', 2: 'Driving', 3: 'Shopping', 4: 'Leisure', 5: 'School'}, inplace=True)
        totalTripPurpose= totalTripPurpose/len(purposes.columns)
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
    # vpg.assignSimpleGridViaPurposes()
    vpg.assignGridViaProbabilities(model='distribution')
    vpg.writeOutGridAvailability()
    vpg.stackPlot()