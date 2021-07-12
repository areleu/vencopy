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
        self.gridFastChargingThreshold = gridConfig['fastChargingThreshold']
        self.outputFileName = createFileString(globalConfig=globalConfig, fileKey='inputDataPlugProfiles',
                                               datasetID=datasetID)
        self.outputFilePath = Path(globalConfig['pathRelative']['input']) / self.outputFileName
        self.purposeData = pd.read_csv(self.inputFilePath, keep_default_na=False)
        self.driveData = pd.read_csv(self.inputDriveProfilesPath, keep_default_na=False)
        self.chargeAvailability = None

    def fastChargingList(self):
        '''
        Returns a list of household trips having consumption greater than 80% (40 kWh) of battery capacity (50 kWh)
        '''
        self.scalars = pd.read_excel(self.scalarsPath, header=5, usecols='A:C', skiprows=0, engine='openpyxl')
        scalarsOut = self.scalars.set_index('parameter')
        driveProfiles = self.driveData.set_index(['hhPersonID'])
        driveProfiles = driveProfiles.loc[:].sum(axis=1)
        driveProfiles = driveProfiles * (scalarsOut.loc['Electric_consumption_NEFZ', 'value'] / 100)
        driveProfiles = np.where(driveProfiles > (self.gridFastChargingThreshold * (scalarsOut.loc['Battery_capacity', 'value'])), driveProfiles, 0)
        driveProfiles = pd.DataFrame(driveProfiles)
        driveProfiles.set_index(self.driveData['hhPersonID'], inplace=True)
        driveProfiles = driveProfiles.replace(0, np.nan)
        driveProfiles = driveProfiles.dropna(how='all', axis=0)
        driveProfiles.reset_index(inplace=True)
        drive = pd.Series(driveProfiles['hhPersonID'])
        fastChargingHHID = []
        for i, item in drive.items():
            fastChargingHHID.append(item)
        return fastChargingHHID

    def assignSimpleGridViaPurposes(self):
        print(f'Starting with charge connection replacement of location purposes')
        self.chargeAvailability = self.purposeData.replace(self.gridMappings)
        self.chargeAvailability.set_index(['hhPersonID'], inplace=True)
        self.chargeAvailability = (~(self.chargeAvailability != True))
        print('Grid connection assignment complete')

    def assignGridViaProbabilities(self, model: str, fastChargingHHID):
        '''
        :param model: Input for assigning probability according to models presented in gridConfig
        :param fastChargingHHID: List of household trips for fast charging
        :return: Returns a dataFrame holding charging capacity for each trip assigned with probability distribution
        '''
        self.chargeAvailability = self.purposeData.copy()
        self.chargeAvailability.set_index(['hhPersonID'], inplace=True)
        print('Starting with charge connection replacement ')
        print('There are ' + str(len(fastChargingHHID)) + ' trips having consumption greater than ' + str(self.gridFastChargingThreshold) + '% of battery capacity')
        np.random.seed(42)
        for hhPersonID, row in self.chargeAvailability.iterrows():
            activity = row.copy(deep=True)
            if hhPersonID in fastChargingHHID:
                for iHour in range(0, len(row)):
                    if iHour == 0:
                        if model == 'probability':
                            row[iHour] = self.getRandomNumberForModel1(activity[iHour])
                        elif model == 'distribution':
                            row[iHour] = self.getRandomNumberForModel3(activity[iHour])
                            # print(row[iHour], activity[iHour], hhPersonID)
                    elif iHour > 0:
                        if activity[iHour] == activity[iHour - 1]:
                            # print(row[j-1])
                            row[iHour] = row[iHour - 1]
                        elif model == 'probability':
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
                            # print(f'Power: {row[iHour]}, Activity: {activity[iHour]},householdPersonID: {hhPersonID}')
                    elif iHour > 0:
                        if activity[iHour] == activity[iHour - 1]:
                            row[iHour] = row[iHour - 1]
                        elif model == 'probability':
                            row[iHour] = self.getRandomNumberForModel1(activity[iHour])
                        elif model == 'distribution':
                            row[iHour] = self.getRandomNumberForModel2(activity[iHour])
                            # print(f'Power: {row[iHour]}, Activity: {activity[iHour]}, householdPersonID: {hhPersonID}')
            self.chargeAvailability.loc[hhPersonID] = row
        print('Grid connection assignment complete')

    def getRandomNumberForModel1(self, purpose):
        '''
        Assigns a random number between 0 and 1 for all the purposes, and allots a charging station according to the probability distribution
        :param purpose: Purpose of each hour of a trip
        :return: Returns a charging capacity for a purpose based on probability distribution 1
        '''
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
        '''
        Assigns a random number between 0 and 1 for all the purposes, and allots a charging station according to the probability distribution
        :param purpose: Purpose of each hour of a trip
        :return: Returns a charging capacity for a purpose based on probability distribution model 2
        '''
        if purpose == 'DRIVING':
            rnd = 0
        elif purpose == 0:
            rnd = 0
        else:
            rnd = np.random.random_sample()

        values = list(self.gridDistribution[purpose].values())
        range_dict = {}
        prob_min = 0

        for index, (key, value) in enumerate(self.gridDistribution[purpose].items()):
            prob_max = prob_min + key
            range_dict.update({index: {'min_range': prob_min, 'max_range': prob_max}})
            prob_min = prob_max

        for dictIndex, rangeValue in range_dict.items():
            if rangeValue['min_range'] <= rnd <= rangeValue['max_range']:
                power = values[dictIndex]
                break
        return power

    def getRandomNumberForModel3(self, purpose):
        '''
        Assigns a random number between 0 and 1 for all the purposes, and allots a charging station according to the probability distribution
        :param purpose: Purpose of each hour of a trip
        :return: Returns a charging capacity for a purpose based on probability distribution model 3 (fast charging)
        '''
        if purpose == 'DRIVING':
            rnd = 0
        elif purpose == 0:
            rnd = 0
        else:
            rnd = np.random.random_sample()

        values = list(self.gridFastCharging[purpose].values())
        range_dict_fast = {}
        prob_min = 0

        for index, (key, value) in enumerate(self.gridFastCharging[purpose].items()):
            prob_max = prob_min + key
            range_dict_fast.update({index: {'min_range': prob_min, 'max_range': prob_max}})
            prob_min = prob_max

        for dictIndex, rangeValue in range_dict_fast.items():
            if rangeValue['min_range'] <= rnd <= rangeValue['max_range']:
                power = values[dictIndex]
                break
        return power

    def writeOutGridAvailability(self):
        self.chargeAvailability.to_csv(self.outputFilePath)

    def stackPlot(self):
        '''
        :return: Plots charging station assigned to each trip and EV's parking area/trip purposes during a time span of 24 hours
        '''
        capacity = self.chargeAvailability.transpose()
        total3kW = capacity.where(capacity.loc[:] == 3.6).count(axis=1)
        total11kW = capacity.where(capacity.loc[:, :] == 11).count(axis=1)
        total22kW = capacity.where(capacity.loc[:, :] == 22).count(axis=1)
        total50kW = capacity.where(capacity.loc[:, :] == 50).count(axis=1)
        total75kW = capacity.where(capacity.loc[:, :] == 75).count(axis=1)
        total0kW = capacity.where(capacity.loc[:, :] == 0).count(axis=1)
        totalChargingStation = pd.concat([total3kW, total11kW, total22kW, total50kW, total75kW, total0kW], axis=1)
        totalChargingStation.rename(columns={0: '3.6 kW', 1: '11 kW', 2: '22 kW', 3: '50 kW', 4: '75 kW', 5: '0 kW'},
                                    inplace=True)
        totalChargingStation = totalChargingStation
        totalChargingStation.plot(kind='area', title='Vehicles connected to different charging station over 24 hours',
                                  figsize=(10, 8))
        plt.show()

        # capacityNormalCharging = list(self.gridDistribution.values())
        # uniqueNormalChargingList = list(set(val for dic in capacityNormalCharging for val in dic.values()))
        # capacityFastCharging = list(self.gridFastCharging.values())
        # uniquefastChargingList= list(set(val for dic in capacityFastCharging for val in dic.values()))
        # capacityList = list(set(uniqueNormalChargingList + uniquefastChargingList))
        # capacityList.sort()
        # totalChargingStation = pd.DataFrame()
        #
        # for i in range(0, len(capacityList)):
        #     total = capacity.where(capacity.loc[:] == capacityList[i]).count(axis=1)
        #     print(total)
        #     # total3kW = capacity.where(capacity.loc[:, :] == capacityList[i]).count(axis=1)
        #     # total11kW = capacity.where(capacity.loc[:, :] == capacityList[i]).count(axis=1)
        #     # total22kW = capacity.where(capacity.loc[:, :] == capacityList[i]).count(axis=1)
        #     # total50kW = capacity.where(capacity.loc[:, :] == capacityList[i]).count(axis=1)
        #     # total75kW = capacity.where(capacity.loc[:, :] == capacityList[i]).count(axis=1)
        #
        #     totalChargingStation= totalChargingStation.append(total, ignore_index=True)
        # # totalChargingStation = totalChargingStation.reindex(sorted(totalChargingStation.columns), axis=1)
        # totalChargingStation = totalChargingStation.transpose()
        # totalChargingStation.sort_index(ascending=True, inplace=True)
        # totalChargingStation
        # # totalChargingStation = pd.concat([total3kW, total11kW, total22kW, total50kW, total75kW, total0kW], axis=1)
        # totalChargingStation.rename(columns={0: '0 kW', 1: '3.6 kW', 2: '11 kW', 3: '22 kW', 4: '50 kW', 5: '75 kW'}, inplace=True)
        # totalChargingStation = totalChargingStation.sort_index(ascending=True)
        # plt.show()


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
        # totalTripPurpose = totalTripPurpose/len(purposes.columns)
        # totalTripPurpose.plot(kind='area', title='Trip purposes during 24 hours', figsize=(10, 8))
        totalTripPurpose.index = np.arange(1, len(totalTripPurpose)+1)


        fig, ax = plt.subplots(1, figsize=(20, 8))
        x = np.arange(0, len(totalTripPurpose.index))
        plt.bar(x-0.4375, totalTripPurpose['Home'], width=0.125, color='#1D2F6F')
        plt.bar(x-0.3125, totalTripPurpose['Work'], width=0.125, color='#D35400')
        plt.bar(x-0.1875, totalTripPurpose['Driving'], width=0.125, color='#928aed')
        plt.bar(x-0.0625, totalTripPurpose['Shopping'], width=0.125, color='#6EAF46')
        plt.bar(x+0.0625, totalTripPurpose['Leisure'], width=0.125, color='#FAC748')
        plt.bar(x+0.1875, totalTripPurpose['School'], width=0.125, color='#FA8390')
        plt.bar(x+0.3125, totalTripPurpose['Other'], width=0.125, color='#FF0000')
        plt.bar(x+0.4375, totalTripPurpose['NA'], width=0.125, color='#1ABC9C')
        plt.ylabel('Trip purposes during 24 hours')
        plt.xlabel('Time (hours)')
        plt.xticks(x, totalTripPurpose.index)
        plt.xlim(-1, 24)
        ax.yaxis.grid(color='black', linestyle='dashed', alpha=0.3)
        # plt.title('EV parking during 24 hours', loc='left')
        plt.legend(['HOME', 'WORK', 'DRIVING', 'SHOPPING', 'LEISURE', 'SCHOOL', 'OTHER', 'NA'], loc='upper center', ncol= 8)
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
    fastChargingHHID = vpg.fastChargingList()
    # vpg.assignSimpleGridViaPurposes()
    vpg.assignGridViaProbabilities(model='distribution', fastChargingHHID=fastChargingHHID)
    vpg.writeOutGridAvailability()
    vpg.stackPlot()