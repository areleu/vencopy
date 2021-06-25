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
        self.gridDistributionsModel1 = gridConfig['chargingInfrastructureDistributionsModel1']
        self.gridDistributionsModel2 = gridConfig['chargingInfrastructureDistributionsModel2']
        self.outputFileName = createFileString(globalConfig=globalConfig, fileKey='inputDataPlugProfiles',
                                               datasetID=datasetID)
        self.outputFilePath = Path(globalConfig['pathRelative']['input']) / self.outputFileName
        self.purposeData = pd.read_csv(self.inputFilePath, keep_default_na=False)
        self.chargeAvailability = None
        self.chargingStationModel1 = None
        self.chargingStationModel2 = None

    def assignSimpleGridViaPurposes(self):
        print(f'Starting with charge connection replacement of location purposes')
        self.chargeAvailability = self.purposeData.replace(self.gridMappings)
        self.chargeAvailability.set_index(['hhPersonID'], inplace=True)
        # self.chargeAvailability = (~(self.chargeAvailability != True))
        print('Grid connection assignment complete')

    def assignGridViaProbabilities(self):
        print('Starting with charge connection replacement ')
        # self.chargingStationModel1 = self.purposeData
        # self.chargingStationModel1.set_index(['hhPersonID'], inplace=True)

        self.chargingStationModel2 = self.purposeData
        self.chargingStationModel2.set_index(['hhPersonID'], inplace=True)

        np.random.seed(42)
        # for iHour, row in self.chargingStationModel1.iterrows():
        for iHour, row in self.chargingStationModel2.iterrows():
            activity = row.copy(deep=True)
            for j in range(0, len(row)):
                if j == 0:
                    # row[j] = self.getRandomNumberForModel1(activity[j])
                    row[j] = self.getRandomNumberForModel2(activity[j])
                elif j > 0:
                    if activity[j] == activity[j - 1]:
                        row[j] = row[j - 1]
                    else:
                        # row[j] = self.getRandomNumberForModel1(activity[j])
                        row[j] = self.getRandomNumberForModel2(activity[j])
            # self.chargingStationModel1.loc[iHour] = row
            self.chargingStationModel2.loc[iHour] = row

        print('Grid connection assignment complete')

    def getRandomNumberForModel1(self, purpose, setSeed=1):
        seed(setSeed)
        if purpose == "HOME":
            rnd = np.random.random_sample()
            if 0.1 <= rnd <= 0.7:
                rnd = self.gridDistributionsModel1['HOME'][0.7]
            else:
                rnd = 0.0
        elif purpose == "WORK":
            rnd = np.random.random_sample()
            if 0.1 <= rnd <= 0.5:
                rnd = self.gridDistributionsModel1['WORK'][0.5]
            else:
                rnd = 0.0
        elif purpose == "DRIVING":
            rnd = 0
            if rnd == 0:
                rnd = self.gridDistributionsModel1['DRIVING'][1]
        elif purpose == "LEISURE":
            rnd = np.random.random_sample()
            if 0.1 <= rnd <= 0.5:
                rnd = self.gridDistributionsModel1['LEISURE'][0.5]
            else:
                rnd = 0.0
        elif purpose == "SHOPPING":
            rnd = np.random.random_sample()
            if 0.1 <= rnd <= 0.5:
                rnd = self.gridDistributionsModel1['SHOPPING'][0.5]
            else:
                rnd = 0.0
        elif purpose == "SCHOOL":
            rnd = np.random.random_sample()
            if 0.1 <= rnd <= 0.5:
                rnd = self.gridDistributionsModel1['SCHOOL'][0.5]
            else:
                rnd = 0.0
        elif purpose == "OTHER":
            rnd = np.random.random_sample()
            if 0.1 <= rnd <= 0.3:
                rnd = self.gridDistributionsModel1['OTHER'][0.5]
            else:
                rnd = 0.0
        else:
            rnd = 0
            if rnd == 0:
                rnd = self.gridDistributionsModel1['NA'][1]
        return rnd

    def getRandomNumberForModel2(self, purpose, setSeed=1):
        seed(setSeed)
        if purpose == "HOME":
            rnd = np.random.random_sample()
            if 0 < rnd <= 0.5:
                rnd = self.gridDistributionsModel2['HOME'][0.5]
            elif 0.5 < rnd <= 0.75:
                rnd = self.gridDistributionsModel2['HOME'][0.25]
            elif 0.75 < rnd <= 0.80:
                rnd = self.gridDistributionsModel2['HOME'][0.05]
            elif 0.80 < rnd <= 1:
                rnd = self.gridDistributionsModel2['HOME'][0.2]
        elif purpose == "WORK":
            rnd = np.random.random_sample()
            if 0 < rnd <= 0.5:
                rnd = self.gridDistributionsModel2['WORK'][0.5]
            elif 0.5 < rnd <= 0.7:
                rnd = self.gridDistributionsModel2['WORK'][0.2]
            elif 0.7 < rnd <= 1:
                rnd = self.gridDistributionsModel2['WORK'][0.3]
        elif purpose == "DRIVING":
            rnd = 0
            if rnd == 0:
                rnd = self.gridDistributionsModel2['DRIVING'][1]
        elif purpose == "LEISURE":
            rnd = np.random.random_sample()
            if 0 < rnd <= 0.35:
                rnd = self.gridDistributionsModel2['LEISURE'][0.35]
            elif 0.35 < rnd <= 0.55:
                rnd = self.gridDistributionsModel2['LEISURE'][0.2]
            elif 0.55 < rnd <= 1:
                rnd = self.gridDistributionsModel2['LEISURE'][0.45]
        elif purpose == "SHOPPING":
            rnd = np.random.random_sample()
            if 0 < rnd <= 0.35:
                rnd = self.gridDistributionsModel2['SHOPPING'][0.35]
            elif 0.35 < rnd <= 0.55:
                rnd = self.gridDistributionsModel2['SHOPPING'][0.2]
            elif 0.55 < rnd <= 1:
                rnd = self.gridDistributionsModel2['SHOPPING'][0.45]
        elif purpose == "SCHOOL":
            rnd = np.random.random_sample()
            if 0 < rnd <= 0.35:
                rnd = self.gridDistributionsModel2['SCHOOL'][0.35]
            elif 0.35 < rnd <= 0.55:
                rnd = self.gridDistributionsModel2['SCHOOL'][0.2]
            elif 0.55 < rnd <= 1:
                rnd = self.gridDistributionsModel2['SCHOOL'][0.45]
        elif purpose == "OTHER":
            rnd = np.random.random_sample()
            if 0 < rnd <= 0.2:
                rnd = self.gridDistributionsModel2['OTHER'][0.2]
            elif 0.2 < rnd <= 0.3:
                rnd = self.gridDistributionsModel2['OTHER'][0.1]
            elif 0.3 < rnd <= 1:
                rnd = self.gridDistributionsModel2['OTHER'][0.70]
        else:
            rnd = 0
            if rnd == 0:
                rnd = self.gridDistributionsModel2['NA'][1]
        return rnd

    def assignConnectionType(self):
        connectionType= self.chargingStationModel2.copy()
        for iHour, row in connectionType.iterrows():
            capacity = row.copy()
            for j in range(0, len(row)):
                if j >= 0:
                    if capacity[j] == capacity[j - 1]:
                        row[j] = 'Idle'
                    elif capacity[j] == 0:
                        row[j] = 'Driving'
                    elif capacity[j] > capacity[j - 1]:
                        row[j] = 'Charging'
            connectionType.loc[iHour] = row
            print(connectionType)
        return connectionType

    def writeOutGridAvailability(self):
        self.chargeAvailability.to_csv(self.outputFilePath)

    def writeOutGridAvailablitiyViaProbabilityModel1(self):
        self.chargingStationModel1.to_csv(self.outputFilePath)

    def writeOutGridAvailablitiyViaProbabilityModel2(self):
        self.chargingStationModel2.to_csv(self.outputFilePath)

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
    # vpg.writeOutGridAvailability()
    vpg.assignGridViaProbabilities()
    vpg.assignConnectionType()
    # vpg.writeOutGridAvailablitiyViaProbabilityModel1()
    vpg.writeOutGridAvailablitiyViaProbabilityModel2()