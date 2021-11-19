__version__ = '0.1.X'
__maintainer__ = 'Niklas Wulff'
__contributors__ = 'Fabia Miorelli, Parth Butte'
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
from vencopy.classes.dataParsers import ParseMiD


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
        self.localPathConfig = configDict['localPathConfig']
        self.gridAvailabilitySimple = self.gridConfig['chargingInfrastructureMappings']
        self.gridAvailabilityProb = self.gridConfig['gridAvailabilityDistribution']
        self.inputFileName = createFileString(globalConfig=self.globalConfig, fileKey='purposesProcessed',
                                              datasetID=datasetID)
        self.inputFilePath = Path(self.localPathConfig['pathAbsolute']['vencoPyRoot']) / \
                             self.globalConfig['pathRelative']['diaryOutput'] / self.inputFileName

        self.outputFileName = createFileString(globalConfig=self.globalConfig, fileKey='inputDataPlugProfiles',
                                               datasetID=datasetID)

        # Needed for plugging choice modeling in flexEstimator
        self.outputFileNameTransactionStartHour = createFileString(globalConfig=self.globalConfig,
                                                                   fileKey='transactionStartHour', datasetID=datasetID)
        self.outputFilePath = Path(self.localPathConfig['pathAbsolute']['vencoPyRoot']) / \
                                   self.globalConfig['pathRelative']['gridOutput'] / self.outputFileName
        self.outputFilePathTransactionStartHour = Path(self.localPathConfig['pathAbsolute']['vencoPyRoot']) / \
                                                       self.globalConfig['pathRelative']['gridOutput'] / \
                                                       self.outputFileNameTransactionStartHour
        self.purposeData = pd.read_csv(self.inputFilePath, keep_default_na=False, index_col='genericID')

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
        self.chargeAvailability = (~(self.chargeAvailability != True))
        print('Grid connection assignment complete')

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
        self.chargeAvailability.columns = self.chargeAvailability.columns.astype(int)
        self.purposeData.columns = self.purposeData.columns.astype(int)
        homePower = pd.Series()
        nHours = len(self.chargeAvailability.columns)
        print('Starting with charge connection replacement ')
        np.random.seed(setSeed)

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

        print('Grid connection assignment complete')
        return self.chargeAvailability

    def allocatePowerViaProbabilities(self, purpose: pd.Series, gridAvailability: dict):
        """
        Not tested, preleiminary version

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
        elif grid == 'probability':
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
    basePath = Path(__file__).parent.parent
    configNames = ('globalConfig', 'localPathConfig', 'parseConfig', 'tripConfig', 'gridConfig', 'flexConfig', 'evaluatorConfig')
    configDict = loadConfigDict(configNames, basePath=basePath)

    vpData = ParseMiD(configDict=configDict, datasetID=datasetID, loadEncrypted=False)
    vpData.process()

    vpg = GridModeler(configDict=configDict, datasetID=datasetID)
    vpg.calcGrid(grid='simple')

    vpEval = Evaluator(configDict=configDict, parseData=pd.Series(data=vpData, index=[datasetID]))
    vpEval.plotParkingAndPowers(vpGrid=vpg)
