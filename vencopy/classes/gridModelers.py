__version__ = '0.1.0'
__maintainer__ = 'Niklas Wulff'
__contributors__ = 'Fabia Miorelli, Parth Butte, Ronald Stegen'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '30.09.2020'
__status__ = 'prod'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'


#----- imports & packages ------
if __package__ is None or __package__ == '':
    import sys
    from os import path
    sys.path.append(path.dirname(path.dirname(path.dirname(__file__))))

from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import yaml
from vencopy.scripts.globalFunctions import createFileString


class GridModeler:
    def __init__(self, configDict: dict, datasetID: str):
        """
        Class for modeling individual vehicle connection options dependent on parking purposes. Configurations on
        charging station availabilities can be parametrized in gridConfig. globalConfig and datasetID are needed for
        reading the input files.

        :param gridConfig: Dictionary containing a key chargingInfrastructureMapping with a sub-dictionary mapping the
        relevant parking purposes to grid availability (true/false). The gridConfig will contain dictionaries with
        probabilistic grid availabilities per parking purpose and rated charging power.
        :param globalConfig: Dictionary with relative paths and filenames. Used for referencing the purpose input file
        :param datasetID: String, used for referencing the purpose input file
        """

        self.globalConfig = configDict['globalConfig']
        self.gridConfig = configDict['gridConfig']
        self.flexConfig = configDict['flexConfig']
        self.inputFileName = createFileString(globalConfig=self.globalConfig, fileKey='purposesProcessed',
                                              datasetID=datasetID)
        self.inputFilePath = Path(__file__).parent / self.globalConfig['pathRelative']['diaryOutput'] / self.inputFileName
        self.inputDriveProfilesName = createFileString(globalConfig=self.globalConfig, fileKey='inputDataDriveProfiles',
                                                      datasetID=datasetID)
        self.inputDriveProfilesPath = Path(__file__).parent / self.globalConfig['pathRelative']['diaryOutput'] / self.inputDriveProfilesName
        self.scalarsPath = self.flexConfig['inputDataScalars'][datasetID]
        self.gridMappings = self.gridConfig['chargingInfrastructureMappings']
        self.gridProbability = self.gridConfig['gridAvailabilityProbability']
        self.gridDistribution = self.gridConfig['gridAvailabilityDistribution']
        self.gridFastCharging = self.gridConfig['gridAvailabilityFastCharging']
        self.gridFastChargingThreshold = self.gridConfig['fastChargingThreshold']
        self.outputFileName = createFileString(globalConfig=self.globalConfig, fileKey='inputDataPlugProfiles',
                                               datasetID=datasetID)
        self.outputFilePath = Path(__file__).parent / self.globalConfig['pathRelative']['gridOutput'] / self.outputFileName
        self.purposeData = pd.read_csv(self.inputFilePath, keep_default_na=False)
        self.driveData = pd.read_csv(self.inputDriveProfilesPath, keep_default_na=False)
        self.chargeAvailability = None


    def assignSimpleGridViaPurposes(self):
        """
        Method to translate hourly purpose profiles into hourly profiles of true/false giving the charging station
        availability in each hour for each individual vehicle.

        :return: None
        """
        print(f'Starting with charge connection replacement of location purposes')
        self.chargeAvailability = self.purposeData.replace(self.gridMappings)
        self.chargeAvailability.set_index(['genericID'], inplace=True)
        self.chargeAvailability = (~(self.chargeAvailability != True))
        print('Grid connection assignment complete')

    # def fastChargingList(self):
    #     '''
    #     Returns a list of household trips having consumption greater than 80% (40 kWh) of battery capacity (50 kWh)
    #     '''
    #     self.scalars = pd.read_excel(self.scalarsPath, header=5, usecols='A:C', skiprows=0, engine='openpyxl')
    #     scalarsOut = self.scalars.set_index('parameter')
    #     driveProfiles = self.driveData.set_index(['genericID'])
    #     driveProfiles = driveProfiles.loc[:].sum(axis=1)
    #     driveProfiles = driveProfiles * (scalarsOut.loc['Electric_consumption', 'value'] / 100)
    #     driveProfiles = np.where(driveProfiles > (self.gridFastChargingThreshold * (scalarsOut.loc['Battery_capacity', 'value'])), driveProfiles, 0)
    #     driveProfiles = pd.DataFrame(driveProfiles)
    #     driveProfiles.set_index(self.driveData['genericID'], inplace=True)
    #     driveProfiles = driveProfiles.replace(0, np.nan)
    #     driveProfiles = driveProfiles.dropna(how='all', axis=0)
    #     driveProfiles.reset_index(inplace=True)
    #     drive = pd.Series(driveProfiles['genericID'])
    #     fastChargingHHID = []
    #     for i, item in drive.items():
    #         fastChargingHHID.append(item)
    #     return fastChargingHHID

    def assignGridViaProbabilities(self, model: str):
        '''
        :param model: Input for assigning probability according to models presented in gridConfig
        :param fastChargingHHID: List of household trips for fast charging
        :return: Returns a dataFrame holding charging capacity for each trip assigned with probability distribution
        '''
        # keys = list(self.gridDistribution['HOME'])
        # keys[0]+0.1
        # keys[2]-0.1
        # d1 = self.gridDistribution['HOME'][0.9]-0.1
        # self.gridDistribution['HOME'].update(d1)
        self.chargeAvailability = self.purposeData.copy()
        self.chargeAvailability.set_index(['genericID'], inplace=True)
        print('Starting with charge connection replacement ')
        # print('There are ' + str(len(fastChargingHHID)) + ' trips having consumption greater than ' + str(self.gridFastChargingThreshold) + '% of battery capacity')
        np.random.seed(42)
        for hhPersonID, row in self.chargeAvailability.iterrows():
            activity = row.copy(deep=True)
            activity
            # if None:
            # # if hhPersonID in fastChargingHHID:
            #     for iHour in range(0, len(row)):
            #         if iHour == 0:
            #             if model == 'probability':
            #                 row[iHour] = self.getRandomNumberForModel1(activity[iHour])
            #             elif model == 'distribution':
            #                 row[iHour] = self.getRandomNumberForModel3(activity[iHour])
            #                 # print(row[iHour], activity[iHour], hhPersonID)
            #         elif iHour > 0:
            #             if activity[iHour] == activity[iHour - 1]:
            #                 # print(row[j-1])
            #                 row[iHour] = row[iHour - 1]
            #             elif activity[iHour] == 'HOME' and (activity[iHour] in activity[range(0, iHour)].values):
            #                 selector = activity[activity == 'HOME']
            #                 selectorindex = selector.index[0]
            #                 row[iHour] = row[selectorindex]
            #             elif model == 'probability':
            #                 row[iHour] = self.getRandomNumberForModel1(activity[iHour])
            #             elif model == 'distribution':
            #                 row[iHour] = self.getRandomNumberForModel3(activity[iHour])
            # else:
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
                    elif activity[iHour] == 'HOME' and (activity[iHour] in activity[range(0, iHour)].values):
                        selector = activity[activity == 'HOME']
                        selectorindex = selector.index[0]
                        row[iHour] = row[selectorindex]
                    elif model == 'probability':
                        row[iHour] = self.getRandomNumberForModel1(activity[iHour])
                    elif model == 'distribution':
                        row[iHour] = self.getRandomNumberForModel2(activity[iHour])
                    # print(f'Power: {row[iHour]}, Activity: {activity[iHour]}, householdPersonID: {hhPersonID}')
            self.chargeAvailability.loc[hhPersonID] = row
        print('Grid connection assignment complete')

    def getRandomNumberForModel1(self, purpose):
        '''
        Assigns a random number between 0 and 1 for all the purposes, and allots a charging station according to the
        probability distribution
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
        Assigns a random number between 0 and 1 for all the purposes, and allots a charging station according to the
        probability distribution
        :param purpose: Purpose of each hour of a trip
        :return: Returns a charging capacity for a purpose based on probability distribution model 2
        '''
        if purpose == 'DRIVING':
            rnd = 0
        else:
            rnd = np.random.random_sample()

        keys = list(self.gridDistribution[purpose].keys())
        range_dict = {}
        prob_min = 0

        for index, (key, value) in enumerate(self.gridDistribution[purpose].items()):
            prob_max = prob_min + value
            range_dict.update({index: {'min_range': prob_min, 'max_range': prob_max}})
            prob_min = prob_max

        for dictIndex, rangeValue in range_dict.items():
            if rangeValue['min_range'] <= rnd <= rangeValue['max_range']:
                global power
                power = keys[dictIndex]
                break
        return power

    def getRandomNumberForModel3(self, purpose):
        '''
        Assigns a random number between 0 and 1 for all the purposes, and allots a charging station according to the
        probability distribution
        :param purpose: Purpose of each hour of a trip
        :return: Returns a charging capacity for a purpose based on probability distribution model 3 (fast charging)
        '''
        if purpose == 'DRIVING':
            rnd = 0
        else:
            rnd = np.random.random_sample()

        keys = list(self.gridFastCharging[purpose].keys())
        range_dict_fast = {}
        prob_min = 0

        for index, (key, value) in enumerate(self.gridFastCharging[purpose].items()):
            prob_max = prob_min + value
            range_dict_fast.update({index: {'min_range': prob_min, 'max_range': prob_max}})
            prob_min = prob_max

        for dictIndex, rangeValue in range_dict_fast.items():
            if rangeValue['min_range'] <= rnd <= rangeValue['max_range']:
                power = keys[dictIndex]
                break
        return power



    def writeOutGridAvailability(self):
        self.chargeAvailability.to_csv(self.outputFilePath)

    def stackPlot(self):
        '''
        :return: Plots charging station assigned to each trip and EV's parking area/trip purposes during a time span of
                 24 hours
        '''
        keys = []
        for key, value in self.gridDistribution.items():
            for nestedKey, nestedValue in value.items():
                keys.append(nestedKey)
        capacityList = keys
        # capacityList.remove(1)
        capacityList = list(set(capacityList))
        # capacityNormalCharging = list(self.gridDistribution(self.gridDistribution.values()))
        # uniqueNormalChargingList = list(set(val for dic in capacityNormalCharging for val in dic.values()))
        # capacityFastCharging = list(self.gridFastCharging.values())
        # uniquefastChargingList = list(set(val for dic in capacityFastCharging for val in dic.values()))
        # capacityList = list(set(uniqueNormalChargingList + uniquefastChargingList))
        # capacityList = list(set(capacityNormalCharging + capacityFastCharging))
        capacityList.sort()
        capacity = self.chargeAvailability.transpose()

        totalChargingStation = pd.DataFrame()
        for i in range(0, len(capacityList)):
            total = capacity.where(capacity.loc[:] == capacityList[i]).count(axis=1)
            totalChargingStation = pd.concat([totalChargingStation, total], ignore_index=True, axis=1)
        totalChargingStation.columns = totalChargingStation.columns[:-len(capacityList)].tolist() + capacityList
        totalChargingStation.index = np.arange(1, len(totalChargingStation)+1)
        totalChargingStation.plot(kind='area', title='Vehicles connected to different charging station over 24 hours',
                                  figsize=(10, 8), colormap='Paired')
        capacityListStr = [str(x) for x in capacityList]
        appendStr = ' kW'
        capacityListStr = [sub + appendStr for sub in capacityListStr]
        plt.xlim(1, 24)
        plt.xlabel('Time (hours)')
        plt.ylabel('Number of vehicles')
        plt.legend(capacityListStr, loc='upper center', ncol=len(capacityList))
        plt.show()

        purposeList = list(self.gridDistribution)
        purposes = self.purposeData.copy()
        purposes = purposes.set_index(['genericID']).transpose()
        totalTripPurpose = pd.DataFrame()

        for i in range(0, len(purposeList)):
            total = purposes.where(purposes.loc[:] == purposeList[i]).count(axis=1)
            totalTripPurpose = pd.concat([totalTripPurpose, total], ignore_index=True, axis=1)
        totalTripPurpose.columns = totalTripPurpose.columns[:-len(purposeList)].tolist() + purposeList
        totalTripPurpose.index = np.arange(1, len(totalTripPurpose) + 1)

        fig, ax = plt.subplots(1, figsize=(20, 8))
        x = np.arange(0, len(totalTripPurpose.index))
        plt.bar(x-0.4375, totalTripPurpose['DRIVING'], width=0.125, color='#D35400')
        plt.bar(x-0.3125, totalTripPurpose['HOME'], width=0.125, color='#1D2F6F')
        plt.bar(x-0.1875, totalTripPurpose['WORK'], width=0.125, color='#928aed')
        plt.bar(x-0.0625, totalTripPurpose['SCHOOL'], width=0.125, color='#6EAF46')
        plt.bar(x+0.0625, totalTripPurpose['SHOPPING'], width=0.125, color='#FAC748')
        plt.bar(x+0.1875, totalTripPurpose['LEISURE'], width=0.125, color='#FA8390')
        plt.bar(x+0.3125, totalTripPurpose['OTHER'], width=0.125, color='#FF0000')
        plt.bar(x+0.4375, totalTripPurpose['0.0'], width=0.125, color='#1ABC9C')
        plt.ylabel('Trip purposes during 24 hours')
        plt.xlabel('Time (hours)')
        plt.xticks(x, totalTripPurpose.index)
        plt.xlim(-1, 24)
        ax.yaxis.grid(color='black', linestyle='dashed', alpha=0.3)
        plt.legend(purposeList, loc='upper center', ncol=len(purposeList))
        plt.show()


    def writeOutGridAvailability(self):
        """
        Function to write out the boolean charging station availability for each vehicle in each hour to the output
        file path.

        :return: None
        """

        self.chargeAvailability.to_csv(self.outputFilePath)


if __name__ == '__main__':
    from vencopy.scripts.globalFunctions import loadConfigDict
    datasetID = 'MiD17'
    # datasetID = 'KiD'

    configNames = ('globalConfig', 'localPathConfig', 'parseConfig', 'tripConfig', 'gridConfig', 'flexConfig', 'evaluatorConfig')
    configDict = loadConfigDict(configNames)
    vpg = GridModeler(configDict=configDict, datasetID=datasetID)
    # fastChargingHHID = vpg.fastChargingList()
    vpg.assignSimpleGridViaPurposes()
    # vpg.assignGridViaProbabilities(model='distribution')
    vpg.writeOutGridAvailability()
    # vpg.stackPlot()

    # print('Data Analysis Started')
    # objs = [GridModeler(gridConfig=gridConfig, globalConfig=globalConfig) for i in range(8)]
    # for obj in objs:
    #     # keys = list(obj.gridDistribution.keys())
    #     # print(keys)
    #     # for i in np.arange(0.1, 1.0, 0.1):
    #     obj.gridDistribution['HOME'][11] = obj.gridDistribution['HOME'][11]+0.1
    #     # print(obj.gridDistribution)
    #     obj.gridDistribution['HOME'][0] = obj.gridDistribution['HOME'][0]-0.1
    #     print(obj.gridDistribution)
    #     fastChargingHHID = obj.fastChargingList()
    #     obj.assignGridViaProbabilities(model='distribution', fastChargingHHID=fastChargingHHID)
    #     obj.writeOutGridAvailability()
    #     obj.stackPlot()

