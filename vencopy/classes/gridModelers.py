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
from scipy.stats import beta, gamma
import matplotlib.pyplot as plt
from vencopy.scripts.globalFunctions import createFileString, loadConfigDict
from vencopy.classes.evaluators import Evaluator
from vencopy.classes.dataParsers import DataParser
# from profilehooks import profile


class GridModeler:
    def __init__(self, configDict: dict, datasetID: str):
        """
        Class for modeling individual vehicle connection options dependent on parking purposes. Configurations on
        charging station availabilities can be parametrized in gridConfig. globalConfig and datasetID are needed for
        reading the input files.

        :param configDict: A dictionary containing multiple yaml config files
        :param datasetID: String, used for referencing the purpose input file
        """

        self.globalConfig = configDict['globalConfig']
        self.gridConfig = configDict['gridConfig']
        self.flexConfig = configDict['flexConfig']
        self.gridAvailabilitySimple = self.gridConfig['chargingInfrastructureMappings']
        self.gridAvailabilityProb = self.gridConfig['gridAvailabilityDistribution']
        self.inputFileName = createFileString(globalConfig=self.globalConfig, fileKey='purposesProcessed',
                                              datasetID=datasetID)
        self.inputFilePath = Path(__file__).parent / self.globalConfig['pathRelative']['diaryOutput'] / self.inputFileName
        # FAST CHARGING - not to be merged
        # self.inputDriveProfilesName = createFileString(globalConfig=self.globalConfig, fileKey='inputDataDriveProfiles',
        #                                               datasetID=datasetID)
        # self.inputDriveProfilesPath = Path(__file__).parent / self.globalConfig['pathRelative']['diaryOutput'] / self.inputDriveProfilesName
        # self.scalarsPath = self.flexConfig['inputDataScalars'][datasetID]
        #self.gridFastChargingThreshold = self.gridConfig['fastChargingThreshold']

        self.outputFileName = createFileString(globalConfig=self.globalConfig, fileKey='inputDataPlugProfiles',
                                               datasetID=datasetID)

        # Needed for plugging choice modeling in flexEstimator
        self.outputFileNameTransactionStartHour = createFileString(globalConfig=self.globalConfig,
                                                                   fileKey='transactionStartHour', datasetID=datasetID)
        self.outputFilePath = Path(__file__).parent / self.globalConfig['pathRelative']['gridOutput'] / self.outputFileName
        self.outputFilePathTransactionStartHour = Path(__file__).parent / self.globalConfig['pathRelative']['gridOutput']\
                                                  / self.outputFileNameTransactionStartHour
        self.purposeData = pd.read_csv(self.inputFilePath, keep_default_na=False)

        # Needed only for FAST CHARGING
        # self.driveData = pd.read_csv(self.inputDriveProfilesPath, keep_default_na=False)
        # self.fastChargingHHID = None

        self.transactionStartHour = None
        self.chargeAvailability = None

    def assignGridViaPurposes(self):
        """
        Method to translate hourly purpose profiles into hourly profiles of true/false giving the charging station
        availability in each hour for each individual vehicle.

        :return: None
        """
        print(f'Starting with charge connection replacement of location purposes')
        self.chargeAvailability = self.purposeData.replace(self.gridAvailabilitySimple)
        self.chargeAvailability.set_index(['genericID'], inplace=True)
        self.chargeAvailability = (~(self.chargeAvailability != True))
        print('Grid connection assignment complete')

    # FAST CHARGING
    # def getFastChargingIDs(self):
    #     '''
    #     Returns a list of household IDs having consumption greater than 80% (40 kWh) of battery capacity (50 kWh) per
    #     daily trip diary
    #     '''
    #     driveProfiles = self.driveData.set_index(['genericID'])
    #     driveProfiles = driveProfiles * self.scalarsPath['Electric_consumption_corr'] / 100
    #     driveProfiles = driveProfiles.loc[:].sum(axis=1)
    #     driveProfiles = driveProfiles >= (self.gridFastChargingThreshold * (self.scalarsPath['Battery_capacity']))
    #     fastChargingHHID = driveProfiles.loc[driveProfiles].index.tolist()
    #     return fastChargingHHID

    def assignGridViaProbabilities(self, setSeed: int):
        """
        :param gridAvailability: Dictionary specifying the probability of different charging powers at different parking
            purposes
        :param fastChargingHHID: List of household trips for fast charging
        :param nIter: Pre-defined number for iteration
        :param setSeed: Seed for reproducing random number
        :return: Returns a dataFrame holding charging capacity for each trip assigned with probability distribution
        """
        self.chargeAvailability = self.purposeData.copy()
        self.chargeAvailability.set_index(['genericID'], inplace=True)
        self.chargeAvailability.columns= self.chargeAvailability.columns.astype(int)
        self.purposeData.set_index('genericID', inplace=True)
        self.purposeData.columns = self.purposeData.columns.astype(int)
        homePower = pd.Series()
        nHours = len(self.chargeAvailability.columns)
        print('Starting with charge connection replacement ')
        np.random.seed(setSeed)

        # NIGHT CHARGING
        # if nightCharging:
        #     for idxIt in range(nIter):
        #         for iHour in range(nHours):
        #             if iHour == 0:
        #                 self.chargeAvailability[iHour] = self.allocatePowerViaProbabilities(purpose=self.chargeAvailability
        #                 [iHour], gridAvailability=gridAvailability)
        #                 homePower = self.chargeAvailability[0].where(self.purposeData[0] == 'HOME', np.nan).dropna()
        #             elif 7 <= iHour <= 19:
        #                 print('I am' + str(iHour))
        #                 self.chargeAvailability[iHour] = 0
        #
        #             elif 1 <= iHour <= 6 or 20 <= iHour <= 23:
        #                 if 1 <= iHour <= 6:
        #                     if iHour > 1:
        #                         homePower = homePower.append(self.chargeAvailability[iHour - 1].where(
        #                             self.purposeData[iHour - 1] == 'HOME', np.nan).dropna())
        #                         homePower = homePower.iloc[
        #                             np.unique(homePower.index.values, return_index=True)[1]]
        #
        #                     oldPurpose = self.purposeData[iHour-1]
        #                     equalPurpose = oldPurpose == self.chargeAvailability[iHour]
        #                     unequalPurpose = self.chargeAvailability[iHour].where(~equalPurpose, np.nan).dropna()
        #                     unequalPurposeHome = unequalPurpose[unequalPurpose == 'HOME']
        #                     unequalPurposeHomePower = homePower.filter(unequalPurposeHome.index)
        #                     homeNew = pd.Series(unequalPurposeHome.index.isin(homePower.index), index=unequalPurposeHome.index)
        #                     unequalPurposeHomeNew = unequalPurposeHome.filter(homeNew.drop(homeNew[homeNew].index).index)
        #                     unequalPurpose.drop(unequalPurpose[unequalPurpose == 'HOME'].index, inplace=True)
        #                     unequalPurpose = unequalPurpose.append(unequalPurposeHomeNew).sort_index()
        #                     equalPurposePower = self.chargeAvailability[iHour - 1].where(equalPurpose, np.nan).dropna()
        #                     equalPurposePower = equalPurposePower.append(unequalPurposeHomePower).sort_index()
        #
        #                     if unequalPurpose.empty:
        #                         self.chargeAvailability[iHour] = equalPurposePower
        #                     else:
        #                         unequalPurposePower = self.allocatePowerViaProbabilities(purpose=unequalPurpose,
        #                                                                              gridAvailability=gridAvailability)
        #                         self.chargeAvailability[iHour] = unequalPurposePower.append(equalPurposePower).sort_index()
        #
        #                 elif 20 <= iHour <= 23:
        #                     if iHour == 20:
        #                         homePower = homePower.append(self.chargeAvailability[6].where(
        #                             self.purposeData[6] == 'HOME', np.nan).dropna())
        #                         homePower = homePower.iloc[
        #                             np.unique(homePower.index.values, return_index=True)[1]]
        #                         oldPurpose = self.purposeData[6]
        #                         equalPurpose = oldPurpose == self.chargeAvailability[iHour]
        #                         unequalPurpose = self.chargeAvailability[iHour].where(~equalPurpose, np.nan).dropna()
        #                         unequalPurposeHome = unequalPurpose[unequalPurpose == 'HOME']
        #                         unequalPurposeHomePower = homePower.filter(unequalPurposeHome.index)
        #                         homeNew = pd.Series(unequalPurposeHome.index.isin(homePower.index),
        #                                             index=unequalPurposeHome.index)
        #                         unequalPurposeHomeNew = unequalPurposeHome.filter(homeNew.drop(homeNew[homeNew].index).index)
        #                         unequalPurpose.drop(unequalPurpose[unequalPurpose == 'HOME'].index, inplace=True)
        #                         unequalPurpose = unequalPurpose.append(unequalPurposeHomeNew).sort_index()
        #                         equalPurposePower = self.chargeAvailability[6].where(equalPurpose, np.nan).dropna()
        #                         equalPurposePower = equalPurposePower.append(unequalPurposeHomePower).sort_index()
        #
        #                         if unequalPurpose.empty:
        #                             self.chargeAvailability[iHour] = equalPurposePower
        #                         else:
        #                             unequalPurposePower = self.allocatePowerViaProbabilities(purpose=unequalPurpose,
        #                                                                                      gridAvailability=gridAvailability)
        #                             self.chargeAvailability[iHour] = unequalPurposePower.append(equalPurposePower).sort_index()
        #                     else:
        #                         homePower = homePower.append(self.chargeAvailability[iHour - 1].where(
        #                             self.purposeData[iHour - 1] == 'HOME', np.nan).dropna())
        #                         homePower = homePower.iloc[
        #                             np.unique(homePower.index.values, return_index=True)[1]]
        #
        #                         oldPurpose = self.purposeData[iHour - 1]
        #                         equalPurpose = oldPurpose == self.chargeAvailability[iHour]
        #                         unequalPurpose = self.chargeAvailability[iHour].where(~equalPurpose, np.nan).dropna()
        #                         unequalPurposeHome = unequalPurpose[unequalPurpose == 'HOME']
        #                         unequalPurposeHomePower = homePower.filter(unequalPurposeHome.index)
        #                         homeNew = pd.Series(unequalPurposeHome.index.isin(homePower.index),
        #                                             index=unequalPurposeHome.index)
        #                         unequalPurposeHomeNew = unequalPurposeHome.filter(homeNew.drop(homeNew[homeNew].index).index)
        #                         unequalPurpose.drop(unequalPurpose[unequalPurpose == 'HOME'].index, inplace=True)
        #                         unequalPurpose = unequalPurpose.append(unequalPurposeHomeNew).sort_index()
        #                         equalPurposePower = self.chargeAvailability[iHour - 1].where(equalPurpose, np.nan).dropna()
        #                         equalPurposePower = equalPurposePower.append(unequalPurposeHomePower).sort_index()
        #
        #                         if unequalPurpose.empty:
        #                             self.chargeAvailability[iHour] = equalPurposePower
        #                         else:
        #                             unequalPurposePower = self.allocatePowerViaProbabilities(purpose=unequalPurpose,
        #                                                                                      gridAvailability=gridAvailability)
        #                             self.chargeAvailability[iHour] = unequalPurposePower.append(equalPurposePower).sort_index()
        #else:

        for iHour in range(nHours):
            if iHour == 0:
                self.chargeAvailability[iHour] = \
                    self.allocatePowerViaProbabilities(purpose=self.chargeAvailability[iHour],
                                                       gridAvailability=self.gridAvailabilityProb)
            else:
                homePower = homePower.append(self.chargeAvailability[iHour - 1].where(
                    self.purposeData[iHour - 1] == 'HOME', np.nan).dropna())
                homePower = homePower.iloc[np.unique(homePower.index.values, return_index=True)[1]]
                oldPurpose = self.purposeData[iHour - 1]
                equalPurpose = oldPurpose == self.chargeAvailability[iHour]
                unequalPurpose = self.chargeAvailability[iHour].where(~equalPurpose, np.nan).dropna()
                unequalPurposeHome = unequalPurpose[unequalPurpose == 'HOME']
                unequalPurposeHomePower = homePower.filter(unequalPurposeHome.index)
                newPurposeHome = pd.Series(unequalPurposeHome.index.isin(homePower.index),
                                    index=unequalPurposeHome.index)
                unequalPurposeHomeNew = \
                    unequalPurposeHome.filter(newPurposeHome.drop(newPurposeHome[newPurposeHome].index).index)
                unequalPurpose.drop(unequalPurpose[unequalPurpose == 'HOME'].index, inplace=True)
                unequalPurpose = unequalPurpose.append(unequalPurposeHomeNew).sort_index()
                equalPurposePower = self.chargeAvailability[iHour - 1].where(equalPurpose, np.nan).dropna()
                equalPurposePower = equalPurposePower.append(unequalPurposeHomePower).sort_index()

                if unequalPurpose.empty:
                    self.chargeAvailability[iHour] = equalPurposePower
                else:
                    unequalPurposePower = self.allocatePowerViaProbabilities(purpose=unequalPurpose,
                                                                             gridAvailability=self.gridAvailabilityProb)
                    self.chargeAvailability[iHour] = unequalPurposePower.append(equalPurposePower).sort_index()

        # for genericID, row in self.chargeAvailability.iterrows():
        #     activity = row.copy(deep=True)
        #     for iHour in range(0, len(row)):  # FIXME implement vectorized
        #         if iHour == 0:
        #             row[iHour] = self.allocatePowerViaProbabilities(purpose=activity[iHour],
        #                                                             gridAvailability=gridAvailability)
        #         elif iHour > 0:
        #             if activity[iHour] == activity[iHour - 1]:
        #                 row[iHour] = row[iHour - 1]
        #             elif activity[iHour] == 'HOME' and (activity[iHour] in activity[range(0, iHour)].values):
        #                 row[iHour] = row[activity[activity == 'HOME'].index[0]]
        #             else:
        #                 row[iHour] = self.allocatePowerViaProbabilities(purpose=activity[iHour],
        #                                                             gridAvailability=gridAvailability)
        #     self.chargeAvailability.loc[genericID] = row

        print('Grid connection assignment complete')
        return self.chargeAvailability

    def allocatePowerViaProbabilities(self, purpose: pd.Series, gridAvailability: dict):
        """
        Assigns a random number between 0 and 1 for all the purposes, and allots a charging station according to the
        probability distribution

        :param purpose: Purpose of each hour of a trip
        :param gridAvailability: Dictionary specifying the probability of different charging powers at different parking
            purposes
        :return: Returns a charging capacity for a purpose
        """
        for genericID, tripPurpose in purpose.items():
            rnd = np.random.random_sample()
            prob_min = 0
            for index, (iPow, iProb) in enumerate(gridAvailability[tripPurpose].items()):
                prob_max = prob_min + iProb
                if prob_min <= rnd <= prob_max:
                    power = iPow
                    purpose.loc[genericID] = power
                    break
                prob_min = prob_max
        return purpose

    def writeOutGridAvailability(self):
        """
           Function to write out the boolean charging station availability for each vehicle in each hour to the output
           file path.

           :return: None
           """
        self.chargeAvailability.to_csv(self.outputFilePath)

    # Plots outside evaluators is deprecated. DELETE FROM HERE WHEN EVALUATOR IS NICER
    # def stackPlot(self, gridAvailability: dict): #chargeAvail, purposes
    #     """
    #     :param gridAvailability:  Dictionary specifying the probability of different charging powers at different parking
    #         purposes
    #     :return: Plots charging station of each trip and EV's parking area/trip purposes during a time span of
    #         24 hours
    #     """
    #
    #     #Plot for charging station
    #     # capacity = pd.read_csv(self.outputFilePath, keep_default_na=False, index_col='genericID')
    #     capacity = self.chargeAvailability.transpose()
    #     capacityList = list(np.unique(capacity.loc[:, :].values))
    #
    #     totalChargingStation = pd.DataFrame()
    #     for i in range(0, len(capacityList)):
    #         total = capacity.where(capacity.loc[:] == capacityList[i]).count(axis=1)
    #         totalChargingStation = pd.concat([totalChargingStation, total], ignore_index=True, axis=1)
    #     totalChargingStation.columns = totalChargingStation.columns[:-len(capacityList)].tolist() + capacityList
    #     totalChargingStation.index = np.arange(1, len(totalChargingStation)+1)
    #     totalChargingStation.plot(kind='area', title='Vehicles connected to different charging station over 24 hours',
    #                               figsize=(10, 8), colormap='Paired')
    #     capacityListStr = [str(x) for x in capacityList]
    #     appendStr = ' kW'
    #     capacityListStr = [sub + appendStr for sub in capacityListStr]
    #     plt.xlim(1, 24)
    #     plt.xlabel('Time (hours)')
    #     plt.ylabel('Number of vehicles')
    #     plt.legend(capacityListStr, loc='upper center', ncol=len(capacityList))
    #     plt.show()
    #
    #     #Plot for purposes in purposeDiary
    #     purposeList = list(gridAvailability)
    #     purposes = self.purposeData.copy()
    #     # purposes = purposes.set_index(['genericID']).transpose()
    #     purposes = purposes.transpose()
    #     totalTripPurpose = pd.DataFrame()
    #
    #     for i in range(0, len(purposeList)):
    #         total = purposes.where(purposes.loc[:] == purposeList[i]).count(axis=1)
    #         totalTripPurpose = pd.concat([totalTripPurpose, total], ignore_index=True, axis=1)
    #     totalTripPurpose.columns = totalTripPurpose.columns[:-len(purposeList)].tolist() + purposeList
    #     totalTripPurpose.index = np.arange(1, len(totalTripPurpose) + 1)
    #
    #     fig, ax = plt.subplots(1, figsize=(20, 8))
    #     x = np.arange(0, len(totalTripPurpose.index))
    #     plt.bar(x-0.4375, totalTripPurpose['DRIVING'], width=0.125, color='#D35400')
    #     plt.bar(x-0.3125, totalTripPurpose['HOME'], width=0.125, color='#1D2F6F')
    #     plt.bar(x-0.1875, totalTripPurpose['WORK'], width=0.125, color='#928aed')
    #     plt.bar(x-0.0625, totalTripPurpose['SCHOOL'], width=0.125, color='#6EAF46')
    #     plt.bar(x+0.0625, totalTripPurpose['SHOPPING'], width=0.125, color='#FAC748')
    #     plt.bar(x+0.1875, totalTripPurpose['LEISURE'], width=0.125, color='#FA8390')
    #     plt.bar(x+0.3125, totalTripPurpose['OTHER'], width=0.125, color='#FF0000')
    #     plt.bar(x+0.4375, totalTripPurpose['0.0'], width=0.125, color='#1ABC9C')
    #     plt.ylabel('Trip purposes during 24 hours')
    #     plt.xlabel('Time (hours)')
    #     plt.xticks(x, totalTripPurpose.index)
    #     plt.xlim(-1, 24)
    #     ax.yaxis.grid(color='black', linestyle='dashed', alpha=0.3)
    #     plt.legend(purposeList, loc='upper center', ncol=len(purposeList))
    #     plt.show()

    # IMPLEMENTATION FOR PLUGGING CHOICES
    def getTransactionHourStart(self):
        """
        :param nIter: Pre-defined number for iteration
        return: Dataframe of transaction start hour based on the plug profiles
        """
        print('Caculating number of transactions')
        self.plugProfile = self.chargeAvailability.copy()
        self.transactionStartHour = pd.DataFrame(columns=self.plugProfile.columns)
        self.plugProfile.columns = self.plugProfile.columns.astype(int)
        nHours = len(self.transactionStartHour.columns)
        for iHour in range(nHours):
            if iHour == 0:
                self.transactionStartHour[iHour] = self.plugProfile[iHour] > self.plugProfile[nHours-1]
            else:
                self.transactionStartHour[iHour] = self.plugProfile[iHour] > self.plugProfile[iHour - 1]
        self.transactionStartHour.dropna(axis=1, inplace=True)
        print('There are ' + str(self.transactionStartHour.sum().sum()) + ' transactions')
        self.transactionStartHour.columns = self.transactionStartHour.columns.astype(int)
        return self.transactionStartHour

    def writeOutTransactionStartHour(self, transactionHours):
        """
           Function to write out the transaction start hour for each vehicle in each hour to the output
           file path.

           :return: None
           """
        transactionHours.to_csv(self.outputFilePathTransactionStartHour)

    def calcGrid(self, grid: str):
        """
        Wrapper function for grid assignment. The number of iterations for assignGridViaProbabilities() and
        transactionStartHour() and seed for reproduction of random numbers can be specified here.
        """
        if grid == 'simple':
            self.assignGridViaPurposes()
        elif grid == 'probability':  #
            self.assignGridViaProbabilities(setSeed=42)
        else:
            raise(ValueError(f'Specified grid modeling option {grid} is not implemented. Please choose'
                             f'"simple" or "probability"'))
        self.writeOutGridAvailability()
        self.transactionStartHour = self.getTransactionHourStart()
        self.writeOutTransactionStartHour(transactionHours=self.transactionStartHour)

    def betaMixtureModel(self):
        w = self.flexConfig['BMMParams']['weekdayPlugin']['mode1']['w1']
        a = self.flexConfig['BMMParams']['weekdayPlugin']['mode1']['a1']
        b = self.flexConfig['BMMParams']['weekdayPlugin']['mode1']['b1']
        # beta = lambda x: gamma(a + b) / (gamma(a) * gamma(b)) * x ^ (a - 1) * (1 - x) ^ (b - 1)
        fig, ax = plt.subplots(1, 1)
        x = np.linspace(beta.ppf(0.01, a),
                        beta.ppf(0.99, a), 100)
        ax.plot(x, beta.pdf(x, a),
                'r-', lw=5, alpha=0.6, label='gamma pdf')

if __name__ == '__main__':
    datasetID = 'MiD17'
    configNames = ('globalConfig', 'localPathConfig', 'parseConfig', 'tripConfig', 'gridConfig', 'flexConfig', 'evaluatorConfig')
    configDict = loadConfigDict(configNames)
    nightCharging = False
    vpData = DataParser(configDict=configDict, datasetID=datasetID, loadEncrypted=False)
    vpData.process()
    vpg = GridModeler(configDict=configDict, datasetID=datasetID)
    vpg.calcGrid(grid='simple')
    vpEval = Evaluator(configDict=configDict, parseData=pd.Series(data=vpData, index=[datasetID]))
    vpEval.plotParkingAndPowers(vpGrid=vpg)

    # FixMe: implement plotting in evaluator
    # vpEval = Evaluator()

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



### SANDBOX ###

    # def allocatePowerViaProbability(self, purpose):
    #     '''
    #     Assigns a random number between 0 and 1 for all the purposes, and allots a charging station according to the
    #     probability distribution
    #     :param purpose: Purpose of each hour of a trip
    #     :return: Returns a charging capacity for a purpose based on probability distribution 1
    #     '''
    #     if purpose == "HOME":
    #         rnd = np.random.random_sample()
    #         if rnd <= 1:
    #             rnd = self.gridProbability['HOME'][1]
    #         else:
    #             rnd = 0.0
    #     elif purpose == "WORK":
    #         rnd = np.random.random_sample()
    #         if rnd <= 1:
    #             rnd = self.gridProbability['WORK'][1]
    #         else:
    #             rnd = 0.0
    #     elif purpose == "DRIVING":
    #         rnd = 0
    #         if rnd == 0:
    #             rnd = self.gridProbability['DRIVING'][1]
    #     elif purpose == "LEISURE":
    #         rnd = np.random.random_sample()
    #         if rnd <= 1:
    #             rnd = self.gridProbability['LEISURE'][1]
    #         else:
    #             rnd = 0.0
    #     elif purpose == "SHOPPING":
    #         rnd = np.random.random_sample()
    #         if rnd <= 1:
    #             rnd = self.gridProbability['SHOPPING'][1]
    #         else:
    #             rnd = 0.0
    #     elif purpose == "SCHOOL":
    #         rnd = np.random.random_sample()
    #         if rnd <= 1:
    #             rnd = self.gridProbability['SCHOOL'][1]
    #         else:
    #             rnd = 0.0
    #     elif purpose == "OTHER":
    #         rnd = np.random.random_sample()
    #         if rnd <= 1:
    #             rnd = self.gridProbability['OTHER'][1]
    #         else:
    #             rnd = 0.0
    #     else:
    #         rnd = 0
    #         if rnd == 0:
    #             rnd = self.gridProbability['NA'][1]
    #     return rnd

    # def getRandomNumberForModel3(self, purpose):
    #     '''
    #     Assigns a random number between 0 and 1 for all the purposes, and allots a charging station according to the
    #     probability distribution
    #     :param purpose: Purpose of each hour of a trip
    #     :return: Returns a charging capacity for a purpose based on probability distribution model 3 (fast charging)
    #     '''
    #     if purpose == 'DRIVING':
    #         rnd = 0
    #     else:
    #         rnd = np.random.random_sample()
    #
    #     keys = list(self.gridFastCharging[purpose].keys())
    #     range_dict_fast = {}
    #     prob_min = 0
    #
    #     for index, (key, value) in enumerate(self.gridFastCharging[purpose].items()):
    #         prob_max = prob_min + value
    #         range_dict_fast.update({index: {'min_range': prob_min, 'max_range': prob_max}})
    #         prob_min = prob_max
    #
    #     for dictIndex, rangeValue in range_dict_fast.items():
    #         if rangeValue['min_range'] <= rnd <= rangeValue['max_range']:
    #             power = keys[dictIndex]
    #             break
    #     return power
    #
    # def getRandomNumberForModel4(self, purpose):
    #     '''
    #     Assigns a random number between 0 and 1 for all the purposes, and allots a charging station according to the
    #     probability distribution
    #     :param purpose: Purpose of each hour of a trip
    #     :return: Returns a charging capacity for a purpose based on probability distribution model 2
    #     '''
    #     if purpose == 'DRIVING':
    #         rnd = 0
    #     else:
    #         rnd = np.random.random_sample()
    #
    #     keys = list(self.gridAveragePower[purpose].keys())
    #     range_dict = {}
    #     prob_min = 0
    #
    #     for index, (key, value) in enumerate(self.gridAveragePower[purpose].items()):
    #         prob_max = prob_min + value
    #         range_dict.update({index: {'min_range': prob_min, 'max_range': prob_max}})
    #         prob_min = prob_max
    #
    #     for dictIndex, rangeValue in range_dict.items():
    #         if rangeValue['min_range'] <= rnd <= rangeValue['max_range']:
    #             power = keys[dictIndex]
    #             break
    #     return power