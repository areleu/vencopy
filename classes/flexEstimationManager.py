__version__ = '0.0.9'
__maintainer__ = 'Niklas Wulff'
__contributors__ = 'Fabia Miorelli, Parth Butte'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '03.11.2019'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'


#----- imports & packages ------
import pathlib
import functools
import warnings
import os
import yaml
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from random import seed, random
from classes.parseManager import DataParser
from classes.evaluationManager import Evaluator
from scripts.globalFunctions import createFileString, mergeVariables


class FlexEstimator:
    def __init__(self, config: dict, globalConfig: dict, evaluatorConfig: dict, ParseData: DataParser, datasetID: str='MiD17'):
        # review: could be not pass one config dict into this class instead of three?
        #  we could encapsulate all three into one dict with the three keys and then
        #  extract the information inside. This would make the interface leaner and
        #  all classes could use the same convention of one config parameter?
        self.config = config
        self.globalConfig = globalConfig
        self.evaluatorConfig = evaluatorConfig
        self.hourVec = range(globalConfig['numberOfHours'])
        self.datasetID = datasetID
        self.linkDict, self.scalars, \
            self.driveProfilesIn, self.plugProfilesIn = self.readVencoInput(config=config, globalConfig=globalConfig, datasetID=datasetID)
        self.mergeDataToWeightsAndDays(ParseData)

        self.weights = self.indexWeights(self.driveProfilesIn.loc[:, ['hhPersonID', 'tripStartWeekday', 'tripWeight']])
        self.outputConfig = yaml.load(open(self.linkDict['linkOutputConfig']), Loader=yaml.SafeLoader)
        self.driveProfiles, self.plugProfiles = self.indexDriveAndPlugData(self.driveProfilesIn, self.plugProfilesIn,
                                                                      dropIdxLevel='tripWeight',
                                                                      nHours=globalConfig['numberOfHours'])
        self.scalarsProc = self.procScalars(self.driveProfilesIn, self.plugProfilesIn,
                                       self.driveProfiles, self.plugProfiles)

        # review: with so many values to be initialized, it might be a good idea to take a step back and
        #  think together about a composition approach with a dataclass based encapsulation.
        #  this would also make it easy to communicate the data to the outside of the class
        #  as the fields are then no longer directly in the main processing class.

        # Base profile attributes
        self.drainProfiles = None
        self.chargeProfiles = None
        self.chargeMaxProfiles = None
        self.chargeProfilesUncontrolled = None
        self.auxFuelDemandProfiles = None
        self.chargeMinProfiles = None

        # Filtering attributes
        self.randNoPerProfile = None
        self.profileSelectors = None
        self.electricPowerProfiles = None
        self.plugProfilesCons = None
        self.electricPowerProfilesCons = None
        self.chargeProfilesUncontrolledCons = None
        self.auxFuelDemandProfilesCons = None
        self.profilesSOCMinCons = None
        self.profilesSOCMaxCons = None

        # Aggregation attributes
        self.plugProfilesAgg = None
        self.electricPowerProfilesAgg = None
        self.chargeProfilesUncontrolledAgg = None
        self.auxFuelDemandProfilesAgg = None
        self.plugProfilesWAgg = None
        self.electricPowerProfilesWAgg = None
        self.chargeProfilesUncontrolledWAgg = None
        self.auxFuelDemandProfilesWAgg = None
        self.plugProfilesWAggVar = None
        self.electricPowerProfilesWAggVar = None
        self.chargeProfilesUncontrolledWAggVar = None
        self.auxFuelDemandProfilesWAggVar = None

        self.SOCMin = None
        self.SOCMax = None
        self.SOCMinVar = None
        self.SOCMaxVar = None

        # Correction attributes
        self.chargeProfilesUncontrolledCorr = None
        self.electricPowerProfilesCorr = None
        self.auxFuelDemandProfilesCorr = None

        # Normalization attributes
        self.socMinNorm = None
        self.socMaxNorm = None

        # Attributes for writeout and plotting
        self.profileDictOut = {}

        print('Flex Estimator initialization complete')

    def initializeLinkMgr(self, config, globalConfig: dict, datasetID: str) -> dict:
        """
        Setup link manager based on a VencoPy config file.

        :param config: Config file initiated by a yaml-loader

        :return: Returns link dictionary with relative links to input data and output folders.
        """
        # review: The methodname surprises me. It suggest that we return a manager
        #  which we clearly don't. we also don't really initialize anything in
        #  the sense of the word. would a name along the lines of buildLinkMapping
        #  be more true to the concept?
        linkDict = {'linkScalars': pathlib.Path(globalConfig['linksRelative']['input']) /
                                   pathlib.Path(globalConfig['files']['inputDataScalars']),
                    'linkDriveProfiles': pathlib.Path(globalConfig['linksRelative']['input']) /
                                         pathlib.Path(createFileString(globalConfig=globalConfig, fileKey='inputDataDriveProfiles',
                                                                       datasetID=datasetID)),
                    'linkPlugProfiles': pathlib.Path(globalConfig['linksRelative']['input']) /
                                        pathlib.Path(createFileString(globalConfig=globalConfig, fileKey='inputDataPlugProfiles',
                                                                      datasetID=datasetID)),
                    'linkOutputConfig': pathlib.Path(globalConfig['linksRelative']['outputConfig']),
                    # 'linkOutputAnnual': pathlib.Path(globalConfig['linksRelative']['resultsAnnual']),
                    'linkPlots': pathlib.Path(globalConfig['linksRelative']['plots']),
                    'linkOutput': pathlib.Path(globalConfig['linksRelative']['dataOutput'])}

        return linkDict

    def readInputScalar(self, filePath) -> pd.DataFrame:
        """
        Method that gets the path to a venco scalar input file specifying technical assumptions such as battery capacity
        specific energy consumption, usable battery capacity share for load shifting and charge power.

        :param filePath: The relative file path to the input file
        :return: Returns a dataframe with an index column and two value columns. The first value column holds numbers the
            second one holds units.
        """
        # review: As far as I can recon, this returns a list of scalars. If yes I would opt for renaming
        #  the method to readInputScalars (plural) as it returns more than one.

        #scalarInput = Assumptions
        inputRaw = pd.read_excel(filePath,
                                 header=5,
                                 usecols='A:C',
                                 skiprows=0)
        scalarsOut = inputRaw.set_index('parameter')
        return scalarsOut

    def readInputCSV(self, filePath) -> pd.DataFrame:
        """
        Reads input and cuts out value columns from a given CSV file.

        :param filePath: Relative file path to CSV file
        :return: Pandas dataframe with raw input from CSV file
        """
        inputRaw = pd.read_csv(filePath, header=0)
        inputData = inputRaw.loc[:, ~inputRaw.columns.str.match('Unnamed')]
        inputData = inputData.convert_dtypes()
        return inputData

    def stringToBoolean(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Replaces given strings with python values for true or false.
        FixMe: Foreseen to be more flexible in next release.

        :param df: Dataframe holding strings defining true or false values
        :return: Dataframe holding true and false
        """

        # review: Just one remark on the wording. In SE this
        #  data structure is called a mapping. A dict is the python
        #  incarnation of a mapping. As we here clearly refer to the concept of a
        #  mapping it would be more natural to call it booleanMapping (bol is also a
        #  funny abbreviation for boolean). This needs not to change but should be cleaned
        #  up over time especially for an external release. Also new variables should
        #  strictly avoid type declaration in the names.
        dictBol = {'WAHR': True,
                   'FALSCH': False}
        outBool = df.replace(to_replace=dictBol, value=None)
        return outBool

    def readInputBoolean(self, filePath) -> pd.DataFrame:
        """
        Wrapper function for reading boolean data from CSV.

        :param filePath: Relative path to CSV file
        :return: Returns a dataframe with boolean values
        """

        inputRaw = self.readInputCSV(filePath)
        inputData = self.stringToBoolean(inputRaw)
        return inputData

    def readVencoInput(self, config: dict, globalConfig: dict, datasetID: str) ->pd.DataFrame:
        """
        Initializing action for VencoPy-specific config-file, link dictionary and data read-in. The config file has
        to be a dictionary in a .yaml file containing three categories: linksRelative, linksAbsolute and files. Each
        category must contain itself a dictionary with the linksRelative to data, functions, plots, scripts, config and
        tsConfig. Absolute links should contain the path to the output folder. Files should contain a link to scalar input
        data, and the two timeseries files inputDataDriveProfiles and inputDataPlugProfiles.

        :param config: A yaml config file holding a dictionary with the keys 'linksRelative' and 'linksAbsolute'
        :return: Returns four dataframes: A link dictionary, scalars, drive profile data and plug profile
        data, the latter three ones in a raw data format.
        """

        linkDict = self.initializeLinkMgr(config, globalConfig, datasetID)

        # review: have you considered using the logging module for these kind of outputs?
        print('Reading Venco input scalars, drive profiles and boolean plug profiles')

        scalars = self.readInputScalar(linkDict['linkScalars'])
        driveProfiles_raw = self.readInputCSV(linkDict['linkDriveProfiles'])
        plugProfiles_raw = self.readInputBoolean(linkDict['linkPlugProfiles'])

        print('There are ' + str(len(driveProfiles_raw)) + ' drive profiles and ' +
              str(len(driveProfiles_raw)) + ' plug profiles.')

        return linkDict, scalars, driveProfiles_raw, plugProfiles_raw

    def procScalars(self, driveProfiles_raw, plugProfiles_raw, driveProfiles: pd.DataFrame,
                    plugProfiles: pd.DataFrame):
        """
        Calculates some scalars from the input data such as the number of hours of drive and plug profiles, the number of
        profiles etc.

        :param driveProfiles: Input drive profile input data frame with timestep specific driving distance in km
        :param plugProfiles: Input plug profile input data frame with timestep specific boolean grid connection values
        :return: Returns a dataframe of processed scalars including number of profiles and number of hours per profile
        """

        noHoursDrive = len(driveProfiles.columns)
        noHoursPlug = len(plugProfiles.columns)
        noDriveProfilesIn = len(driveProfiles)
        noPlugProfilesIn = len(plugProfiles)
        scalarsProc = {'noHoursDrive': noHoursDrive,
                       'noHoursPlug': noHoursPlug,
                       'noDriveProfilesIn': noDriveProfilesIn,
                       'noPlugProfilesIn': noPlugProfilesIn}
        if noHoursDrive == noHoursPlug:
            scalarsProc['noHours'] = noHoursDrive
        else:
            warnings.warn('Length of drive and plug input data differ! This will at the latest crash in calculating '
                          'profiles for SoC max')
        return scalarsProc

    def indexWeights(self, weights: pd.DataFrame) -> pd.DataFrame:
        weights = weights.convert_dtypes()
        return weights.set_index(['hhPersonID', 'tripStartWeekday'], drop=True)

    def findIndexCols(self, data: pd.DataFrame, nHours: int) -> list:
        dataCols = [str(i) for i in range(0, nHours + 1)]
        return data.columns[~data.columns.isin(dataCols)]

    def indexProfile(self, data: pd.DataFrame, nHours: int) -> pd.DataFrame:
        """
        Takes raw data as input and indices different profiles with the specified index columns und an unstacked form.

        :param driveProfiles_raw: Dataframe of raw drive profiles in km with as many index columns as elements
            of the list in given in indices. One column represents one timestep, e.g. hour.
        :param plugProfiles_raw: Dataframe of raw plug profiles as boolean values with as many index columns
            as elements of the list in given in indices. One column represents one timestep e.g. hour.
        :param indices: List of column names given as strings.
        :return: Two indexed dataframes with index columns as given in argument indices separated from data columns
        """

        indexCols = self.findIndexCols(data, nHours)
        data = data.convert_dtypes()  # Reduce column data types if possible (specifically hhPersonID column to int)
        dataIndexed = data.set_index(list(indexCols))

        # Typecast column indices to int for later looping over a range
        dataIndexed.columns = dataIndexed.columns.astype(int)
        return dataIndexed

    def indexDriveAndPlugData(self, driveData: pd.DataFrame, plugData: pd.DataFrame, dropIdxLevel: str,
                              nHours: int) -> pd.DataFrame:
        driveProfiles = self.indexProfile(driveData, nHours)
        plugProfiles = self.indexProfile(plugData, nHours)
        return driveProfiles.droplevel(dropIdxLevel), plugProfiles.droplevel(dropIdxLevel)

    def readInData(self, globalConfig, fileKey: str, datasetID: str) -> pd.DataFrame:
        """
        Generic read-in function for mobility datasets. This serves as interface between the daily trip distance
        and purpose calculation and the class Evaluator.

        :param fileKey: List of VencoPy-internal names for the filekeys to read in
        :return: a named pd.Series of all datasets with the given filekey_datasets as identifiers
        """

        return pd.read_csv(pathlib.Path(globalConfig['linksRelative']['input']) / createFileString(globalConfig=globalConfig, fileKey=fileKey,
                                                                                     datasetID=datasetID),
                           dtype={'hhPersonID': int}, index_col=['hhPersonID', 'tripStartWeekday'])

    def mergeDataToWeightsAndDays(self, ParseData):
        self.driveProfilesIn = mergeVariables(data=self.driveProfilesIn, variableData=ParseData.data,
                                              variables=['tripStartWeekday', 'tripWeight'])
        self.plugProfilesIn = mergeVariables(data=self.plugProfilesIn, variableData=ParseData.data,
                                             variables=['tripStartWeekday', 'tripWeight'])

    def calcDrainProfiles(self, driveProfiles: pd.DataFrame, scalars: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates electrical consumption profiles from drive profiles assuming specific consumption (in kWh/100 km)
        given in scalar input data file.

        :param driveProfiles: indexed profile file
        :param Scalars: dataframe holding technical assumptions
        :return: Returns a dataframe with consumption profiles in kWh/h in same format and length as driveProfiles but
        scaled with the specific consumption assumption.
        """

        return driveProfiles * scalars.loc['Electric consumption NEFZ', 'value'] / float(100)

    def calcChargeProfiles(self, plugProfiles: pd.DataFrame, scalars: pd.DataFrame) -> pd.DataFrame:
        '''
        Calculates the maximum possible charge power based on the plug profile assuming the charge column power
        given in the scalar input data file (so far under Panschluss).

        :param plugProfiles: indexed boolean profiles for vehicle connection to grid
        :param scalars: VencoPy scalar dataframe
        :return: Returns scaled plugProfile in the same format as plugProfiles.
        '''

        return plugProfiles * scalars.loc['Rated power of charging column', 'value'].astype(float)

    def calcChargeMaxProfiles(self, chargeProfiles: pd.DataFrame, consumptionProfiles: pd.DataFrame,
                              scalars: pd.DataFrame, scalarsProc: pd.DataFrame, nIter: int) -> pd.DataFrame:
        """
        Calculates all maximum SoC profiles under the assumption that batteries are always charged as soon as they
        are plugged to the grid. Values are assured to not fall below SoC_min * battery capacity or surpass
        SoC_max * battery capacity. Relevant profiles are chargeProfile and consumptionProfile. An iteration assures
        the boundary condition of chargeMaxProfile(0) = chargeMaxProfile(len(profiles)). The number of iterations
        is given as parameter.

        :param chargeProfiles: Indexed dataframe of charge profiles.
        :param consumptionProfiles: Indexed dataframe of consumptionProfiles.
        :param scalars: DataFrame holding techno-economic assumptions.
        :param scalarsProc: DataFrame holding information about profile length and number of hours.
        :param nIter: Number of iterations to assure that the minimum and maximum value are approximately the same
        :return: Returns an indexed DataFrame with the same length and form as chargProfiles and consumptionProfiles,
        containing single-profile SOC max values for each hour in each profile.
        """

        chargeMaxProfiles = chargeProfiles.copy()
        batCapMin = scalars.loc['Battery capacity', 'value'] * scalars.loc['Minimum SOC', 'value']
        batCapMax = scalars.loc['Battery capacity', 'value'] * scalars.loc['Maximum SOC', 'value']
        nHours = scalarsProc['noHours']
        for idxIt in range(nIter):
            print(f'Starting with iteration {idxIt}')
            for iHour in range(nHours):
                # print(f'Starting with hour {iHour}')
                if iHour == 0:
                    chargeMaxProfiles[iHour] = chargeMaxProfiles[nHours - 1].where(
                        chargeMaxProfiles[iHour] <= batCapMax,
                        batCapMax)
                else:
                    # Calculate and append column with new SoC Max value for comparison and cleaner code
                    chargeMaxProfiles['newCharge'] = chargeMaxProfiles[iHour - 1] + \
                                                     chargeProfiles[iHour] - \
                                                     consumptionProfiles[iHour]

                    # Ensure that chargeMaxProfiles values are between batCapMin and batCapMax
                    chargeMaxProfiles[iHour] \
                        = chargeMaxProfiles['newCharge'].where(chargeMaxProfiles['newCharge'] <= batCapMax,
                                                               other=batCapMax)
                    chargeMaxProfiles[iHour] \
                        = chargeMaxProfiles[iHour].where(chargeMaxProfiles[iHour] >= batCapMin, other=batCapMin)

            devCrit = chargeMaxProfiles[nHours - 1].sum() - chargeMaxProfiles[0].sum()
            print(devCrit)
        chargeMaxProfiles.drop(labels='newCharge', axis='columns', inplace=True)
        return chargeMaxProfiles


    def calcChargeProfilesUncontrolled(self, chargeMaxProfiles: pd.DataFrame,
                                       scalarsProc: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates uncontrolled electric charging based on SoC Max profiles for each hour for each profile.

        :param chargeMaxProfiles: Dataframe holding timestep dependent SOC max values for each profile.
        :param scalarsProc: VencoPy Dataframe holding meta-information about read-in profiles.
        :return: Returns profiles for uncontrolled charging under the assumption that charging occurs as soon as a
        vehicle is connected to the grid up to the point that the maximum battery SOC is reached or the connection
        is interrupted. DataFrame has the same format as chargeMaxProfiles.
        """

        chargeMaxProfiles = chargeMaxProfiles.copy()
        chargeProfilesUncontrolled = chargeMaxProfiles.copy()
        nHours = scalarsProc['noHours']

        for iHour in range(nHours):
            if iHour != 0:
                chargeProfilesUncontrolled[iHour] = (chargeMaxProfiles[iHour] - chargeMaxProfiles[iHour - 1]).where(
                    chargeMaxProfiles[iHour] >= chargeMaxProfiles[iHour - 1], other=0)

        # set value of uncontrolled charging for first hour to average between hour 1 and hour 23
        # because in calcChargeMax iteration the difference is minimized.
        chargeProfilesUncontrolled[0] = \
            (chargeProfilesUncontrolled[1] + chargeProfilesUncontrolled[nHours - 1]) / 2
        return chargeProfilesUncontrolled

    def calcDriveProfilesFuelAux(self, chargeMaxProfiles: pd.DataFrame, chargeProfilesUncontrolled: pd.DataFrame,
                                 driveProfiles: pd.DataFrame, scalars: pd.DataFrame,
                                 scalarsProc: pd.DataFrame) -> pd.DataFrame:
        # ToDo: alternative vectorized format for looping over columns? numpy, pandas: broadcasting-rules
        """
         Calculates necessary fuel consumption profile of a potential auxilliary unit (e.g. a gasoline motor) based
        on gasoline consumption given in scalar input data (in l/100 km). Auxilliary fuel is needed if an hourly
        mileage is higher than the available SoC Max in that hour.

        :param chargeMaxProfiles: Dataframe holding hourly maximum SOC profiles in kWh for all profiles
        :param chargeProfilesUncontrolled: Dataframe holding hourly uncontrolled charging values in kWh/h for all profiles
        :param driveProfiles: Dataframe holding hourly electric driving demand in kWh/h for all profiles.
        :param scalars: Dataframe holding technical assumptions
        :param scalarsProc: Dataframe holding meta-infos about the input
        :return: Returns a DataFrame with single-profile values for back-up fuel demand in the case a profile cannot
        completely be fulfilled with electric driving under the given consumption and battery size assumptions.
        """

        # review: the hardcoding of the column names can cause a lot of problems for people later on if we do not ship the date with the tool.
        # I would recommend to move these column names to a config file similar to i18n strategies
        consumptionPower = scalars.loc['Electric consumption NEFZ', 'value']
        consumptionFuel = scalars.loc['Fuel consumption NEFZ', 'value']

        # initialize data set for filling up later on
        driveProfilesFuelAux = chargeMaxProfiles.copy()
        nHours = scalarsProc['noHours']

        for iHour in range(nHours):
            if iHour != 0:
                driveProfilesFuelAux[iHour] = (consumptionFuel / consumptionPower) * \
                                              (driveProfiles[iHour] * consumptionPower / 100 -
                                               chargeProfilesUncontrolled[iHour] -
                                               (chargeMaxProfiles[iHour - 1] - chargeMaxProfiles[iHour]))

        # Setting value of hour=0 equal to the average of hour=1 and last hour
        driveProfilesFuelAux[0] = (driveProfilesFuelAux[nHours - 1] + driveProfilesFuelAux[1]) / 2
        driveProfilesFuelAux = driveProfilesFuelAux.round(4)
        return driveProfilesFuelAux

    def calcChargeMinProfiles(self, chargeProfiles: pd.DataFrame, consumptionProfiles: pd.DataFrame,
                              driveProfilesFuelAux: pd.DataFrame, scalars: pd.DataFrame, scalarsProc: pd.DataFrame,
                              nIter: int) -> pd.DataFrame:
        # ToDo param minSecurityFactor
        """
        Calculates minimum SoC profiles assuming that the hourly mileage has to exactly be fulfilled but no battery charge
        is kept inspite of fulfilling the mobility demand. It represents the minimum charge that a vehicle battery has to
        contain in order to fulfill all trips.
        An iteration is performed in order to assure equality of the SoCs at beginning and end of the profile.

        :param chargeProfiles: Charging profiles with techno-economic assumptions on connection power.
        :param consumptionProfiles: Profiles giving consumed electricity for each trip in each hour assuming specified
            consumption.
        :param driveProfilesFuelAux: Auxilliary fuel demand for fulfilling trips if purely electric driving doesn't suffice.
        :param scalars: Techno-economic input assumptions such as consumption, battery capacity etc.
        :param scalarsProc: Number of profiles and number of hours of each profile.
        :param nIter: Gives the number of iterations to fulfill the boundary condition of the SoC equalling in the first
            and in the last hour of the profile.
        :return: Returns an indexed DataFrame containing minimum SOC values for each profile in each hour in the same
            format as chargeProfiles, consumptionProfiles and other input parameters.
        """

        # review general remark: white spaces in column names give me the creeps as it is easy to mistype
        # and create all kind of wired errors.
        # Especially if there are columns with similar names only differing in whitespaces.
        # This is clearly not the case here, but did you consider naming columns with underscores for easier reference?
        chargeMinProfiles = chargeProfiles.copy()
        batCapMin = scalars.loc['Battery capacity', 'value'] * scalars.loc['Minimum SOC', 'value']
        batCapMax = scalars.loc['Battery capacity', 'value'] * scalars.loc['Maximum SOC', 'value']
        consElectric = scalars.loc['Electric consumption NEFZ', 'value']
        consGasoline = scalars.loc['Fuel consumption NEFZ', 'value']
        nHours = scalarsProc['noHours']
        for idxIt in range(nIter):
            for iHour in range(nHours):
                if iHour == nHours - 1:
                    chargeMinProfiles[iHour] = chargeMinProfiles[0].where(batCapMin <= chargeMinProfiles[iHour],
                                                                          other=batCapMin)
                else:
                    # Calculate and append column with new SOC Max value for comparison and nicer code
                    chargeMinProfiles['newCharge'] = chargeMinProfiles[iHour + 1] + \
                                                     consumptionProfiles[iHour + 1] - \
                                                     chargeProfiles[iHour + 1] - \
                                                     (driveProfilesFuelAux[iHour + 1] * consElectric / consGasoline)

                    # Ensure that chargeMinProfiles values are between batCapMin and batCapMax
                    chargeMinProfiles[iHour] \
                        = chargeMinProfiles['newCharge'].where(chargeMinProfiles['newCharge'] >= batCapMin,
                                                               other=batCapMin)
                    chargeMinProfiles[iHour] \
                        = chargeMinProfiles[iHour].where(chargeMinProfiles[iHour] <= batCapMax, other=batCapMax)

            devCrit = chargeMinProfiles[nHours - 1].sum() - chargeMinProfiles[0].sum()
            print(devCrit)
        chargeMinProfiles.drop('newCharge', axis='columns', inplace=True)
        return chargeMinProfiles

    def baseProfileCalculation(self):
        self.drainProfiles = self.calcDrainProfiles(self.driveProfiles, self.scalars)
        self.chargeProfiles = self.calcChargeProfiles(self.plugProfiles, self.scalars)
        self.chargeMaxProfiles = self.calcChargeMaxProfiles(self.chargeProfiles, self.drainProfiles, self.scalars,
                                                       self.scalarsProc, nIter=3)
        # self.splitChargeMaxCalc(chargeProfiles=self.chargeProfiles, drainProfiles=self.drainProfiles)
        self.chargeProfilesUncontrolled = self.calcChargeProfilesUncontrolled(self.chargeMaxProfiles, self.scalarsProc)
        self.auxFuelDemandProfiles = self.calcDriveProfilesFuelAux(self.chargeMaxProfiles,
                                                                   self.chargeProfilesUncontrolled, self.driveProfiles,
                                                                   self.scalars, self.scalarsProc)
        self.chargeMinProfiles = self.calcChargeMinProfiles(self.chargeProfiles, self.drainProfiles,
                                                       self.auxFuelDemandProfiles, self.scalars, self.scalarsProc,
                                                       nIter=3)
        print(f'Base profile calculation complete for dataset {self.datasetID}')

    def createRandNo(self, driveProfiles: pd.DataFrame, setSeed=1):
        """
        Creates a random number between 0 and 1 for each profile based on driving profiles.

        :param driveProfiles: Dataframe holding hourly electricity consumption values in kWh/h for all profiles
        :param setSeed: Seed for reproducing stochasticity. Scalar number.
        :return: Returns an indexed series with the same indices as dirveProfiles with a random number between 0 and 1 for
        each index.
        """

        idxData = driveProfiles.copy()
        seed(setSeed)  # seed random number generator for reproducibility
        idxData['randNo'] = np.random.random(len(idxData))
        idxData['randNo'] = [random() for _ in
                             range(len(idxData))]  # generate one random number for each profile / index
        randNo = idxData.loc[:, 'randNo']
        return randNo

    def calcProfileSelectors(self, chargeProfiles: pd.DataFrame,
                             consumptionProfiles: pd.DataFrame,
                             driveProfiles: pd.DataFrame,
                             driveProfilesFuelAux: pd.DataFrame,
                             randNos: pd.DataFrame,
                             scalars: pd.DataFrame,
                             fuelDriveTolerance,
                             isBEV: bool) -> pd.DataFrame:
        """
        This function calculates two filters. The first filter, filterCons, excludes profiles that depend on auxiliary
        fuel with an option of a tolerance (bolFuelDriveTolerance) and those that don't reach a minimum daily average for
        mileage (bolMinDailyMileage).
        A second filter filterDSM excludes profiles where charging throughout the day supplies less energy than necessary
        for the respective trips (bolConsumption) and those where the battery doesn't suffice the mileage (bolSuffBat).

        :param chargeProfiles: Indexed DataFrame giving hourly charging profiles
        :param consumptionProfiles: Indexed DataFrame giving hourly consumption profiles
        :param driveProfiles:  Indexed DataFrame giving hourly electricity demand profiles for driving.
        :param driveProfilesFuelAux: Indexed DataFrame giving auxiliary fuel demand.
        :param randNos: Indexed Series giving a random number between 0 and 1 for each profiles.
        :param scalars: Techno-economic assumptions
        :param fuelDriveTolerance: Give a threshold value how many liters may be needed throughout the course of a day
        in order to still consider the profile.
        :param isBEV: Boolean value. If true, more 2030 profiles are taken into account (in general).
        :return: The bool indices are written to one DataFrame in the DataManager with the columns randNo, indexCons and
        indexDSM and the same indices as the other profiles.
        """

        boolBEV = scalars.loc['Is BEV?', 'value']
        minDailyMileage = scalars.loc['Minimum daily mileage', 'value']
        batSize = scalars.loc['Battery capacity', 'value']
        socMax = scalars.loc['Maximum SOC', 'value']
        socMin = scalars.loc['Minimum SOC', 'value']
        filterCons = driveProfiles.copy()
        filterCons['randNo'] = randNos
        filterCons['bolFuelDriveTolerance'] = driveProfilesFuelAux.sum(axis='columns') * boolBEV < fuelDriveTolerance
        filterCons['bolMinDailyMileage'] = driveProfiles.sum(axis='columns') > \
                                           (2 * randNos * minDailyMileage + (1 - randNos) * minDailyMileage * isBEV)
        filterCons['indexCons'] = filterCons.loc[:, 'bolFuelDriveTolerance'] & filterCons.loc[:, 'bolMinDailyMileage']
        filterCons['bolConsumption'] = consumptionProfiles.sum(axis=1) < chargeProfiles.sum(axis=1)
        filterCons['bolSuffBat'] = consumptionProfiles.sum(axis=1) < batSize * (socMax - socMin)
        filterCons['indexDSM'] = filterCons['indexCons'] & filterCons['bolConsumption'] & filterCons['bolSuffBat']

        print('There are ' + str(sum(filterCons['indexCons'])) + ' considered profiles and ' + \
              str(sum(filterCons['indexDSM'])) + ' DSM eligible profiles.')
        filterCons_out = filterCons.loc[:, ['randNo', 'indexCons', 'indexDSM']]
        return filterCons_out

    def calcElectricPowerProfiles(self, consumptionProfiles: pd.DataFrame, driveProfilesFuelAux: pd.DataFrame,
                                  scalars: pd.DataFrame, filterCons: pd.DataFrame, scalarsProc: pd.DataFrame,
                                  filterIndex) -> pd.DataFrame:
        """
        Calculates electric power profiles that serve as outflow of the fleet batteries.

        :param consumptionProfiles: Indexed DataFrame containing electric vehicle consumption profiles.
        :param driveProfilesFuelAux: Indexed DataFrame containing
        :param scalars: VencoPy Dataframe containing technical assumptions
        :param filterCons: Dataframe containing one boolean filter value for each profile
        :param scalarsProc: Dataframe containing meta information of input profiles
        :param filterIndex: Can be either 'indexCons' or 'indexDSM' so far. 'indexDSM' applies stronger filters and results
        are thus less representative.
        :return: Returns electric demand from driving filtered and aggregated to one fleet.
        """

        consumptionPower = scalars.loc['Electric consumption NEFZ', 'value']
        consumptionFuel = scalars.loc['Fuel consumption NEFZ', 'value']
        indexCons = filterCons.loc[:, 'indexCons']
        indexDSM = filterCons.loc[:, 'indexDSM']
        nHours = scalarsProc['noHours']
        electricPowerProfiles = consumptionProfiles.copy()
        for iHour in range(nHours):
            electricPowerProfiles[iHour] = (consumptionProfiles[iHour] - driveProfilesFuelAux[iHour] *
                                            (consumptionPower / consumptionFuel))
            if filterIndex == 'indexCons':
                electricPowerProfiles[iHour] = electricPowerProfiles[iHour] * indexCons
            elif filterIndex == 'indexDSM':
                electricPowerProfiles[iHour] = electricPowerProfiles[iHour] * indexDSM
        return electricPowerProfiles

    def setUnconsideredBatProfiles(self, chargeMaxProfiles: pd.DataFrame, chargeMinProfiles: pd.DataFrame,
                                   filterCons: pd.DataFrame, minValue, maxValue):
        """
        Sets all profile values with filterCons = False to extreme values. For SoC max profiles, this means a value
        that is way higher than SoC max capacity. For SoC min this means usually 0. This setting is important for the
        next step of filtering out extreme values.

        :param chargeMaxProfiles: Dataframe containing hourly maximum SOC profiles for all profiles
        :param chargeMinProfiles: Dataframe containing hourly minimum SOC profiles for all profiles
        :param filterCons: Dataframe containing one boolean value for each profile
        :param minValue: Value that non-reasonable values of SoC min profiles should be set to.
        :param maxValue: Value that non-reasonable values of SoC max profiles should be set to.
        :return: Writes the two profiles files 'chargeMaxProfilesDSM' and 'chargeMinProfilesDSM' to the DataManager.
        """

        chargeMinProfilesDSM = chargeMinProfiles.copy()
        chargeMaxProfilesDSM = chargeMaxProfiles.copy()
        # if len(chargeMaxProfilesCons) == len(filterCons): #len(chargeMaxProfilesCons) = len(chargeMinProfilesCons) by design
        # How can I catch pandas.core.indexing.IndexingError ?
        try:
            chargeMinProfilesDSM.loc[~filterCons['indexDSM'].astype('bool'), :] = minValue
            chargeMaxProfilesDSM.loc[~filterCons['indexDSM'].astype('bool'), :] = maxValue
        except Exception as E:
            print("Declaration doesn't work. "
                  "Maybe the length of filterCons differs from the length of chargeMaxProfiles")
            raise E
            # raise user defined

        return chargeMaxProfilesDSM, chargeMinProfilesDSM

    def filterConsProfiles(self, profile: pd.DataFrame, filterCons: pd.DataFrame, critCol) -> pd.DataFrame:
        """
        Filter out all profiles from given profile types whose boolean indices (so far DSM or cons) are FALSE.

        :param profile: Dataframe of hourly values for all filtered profiles
        :param filterCons: Identifiers given as list of string to store filtered profiles back into the DataManager
        :param critCol: Criterium column for filtering
        :return: Stores filtered profiles in the DataManager under keys given in dmgrNames
        """

        outputProfile = profile.loc[filterCons[critCol], :]
        return outputProfile

    def socProfileSelection(self, profilesMin: pd.DataFrame, profilesMax: pd.DataFrame, filter, alpha) -> pd.Series:
        """
        Selects the nth highest value for each hour for min (max profiles based on the percentage given in parameter
        'alpha'. If alpha = 10, the 10%-biggest (10%-smallest) value is selected, all other values are disregarded.
        Currently, in the Venco reproduction phase, the hourly values are selected independently of each other. min and max
        profiles have to have the same number of columns.

        :param profilesMin: Profiles giving minimum hypothetic SOC values to supply the driving demand at each hour
        :param profilesMax: Profiles giving maximum hypothetic SOC values if vehicle is charged as soon as possible
        :param filter: Filter method. Currently implemented: 'singleValue'
        :param alpha: Percentage, giving the amount of profiles whose mobility demand can not be fulfilled after selection.
        :return: Returns the two profiles 'SOCMax' and 'SOCMin' in the same time resolution as input profiles.
        """

        profilesMin = profilesMin.convert_dtypes()
        profilesMax = profilesMax.convert_dtypes()
        noProfiles = len(profilesMin)
        noProfilesFilter = int(alpha / 100 * noProfiles)
        if filter == 'singleValue':
            profileMinOut = profilesMin.iloc[0, :].copy()
            for col in profilesMin:
                profileMinOut[col] = min(profilesMin[col].nlargest(noProfilesFilter))

            profileMaxOut = profilesMax.iloc[0, :].copy()
            for col in profilesMax:
                profileMaxOut[col] = max(profilesMax[col].nsmallest(noProfilesFilter))

        else:
            # review have you considered implementing your own error like class FilterError(Exception):
            # pass which would give the user an additional hint on what went wrong?
            raise ValueError('You selected a filter method that is not implemented.')
        return profileMinOut, profileMaxOut

    def normalizeProfiles(self, scalars: pd.DataFrame, socMin: pd.Series, socMax: pd.Series, normReferenceParam) -> \
            pd.Series:
        # ToDo: Implement a normalization to the maximum of a given profile

        """
        Normalizes given profiles with a given scalar reference.

        :param scalars: Dataframe containing technical assumptions e.g. battery capacity
        :param socMin: Minimum SOC profile subject to normalization
        :param socMax: Minimum SOC profile subject to normalization
        :param normReferenceParam: Reference parameter that is taken for normalization.
        This has to be given in scalar input data and is most likely the battery capacity.
        :return: Writes the normalized profiles to the DataManager under the specified keys
        """

        normReference = scalars.loc[normReferenceParam, 'value']
        try:
            socMinNorm = socMin.div(float(normReference))
            socMaxNorm = socMax.div(float(normReference))

        except ValueError as E:
            # review general so is this not a problem at all if this happens?
            # s I understand this code, socMin and socMax would be unchanged by this function call
            raise (f"There was a value error. {E} I don't know what to tell you.")
        return socMinNorm, socMaxNorm

    def filter(self):
        self.randNoPerProfile = self.createRandNo(driveProfiles=self.driveProfiles)

        self.profileSelectors = self.calcProfileSelectors(chargeProfiles=self.chargeProfiles,
                                                     consumptionProfiles=self.drainProfiles,
                                                     driveProfiles=self.driveProfiles,
                                                     driveProfilesFuelAux=self.auxFuelDemandProfiles,
                                                     randNos=self.randNoPerProfile, scalars=self.scalars,
                                                     fuelDriveTolerance=1, isBEV=True)

        # Additional fuel consumption is subtracted from the consumption
        self.electricPowerProfiles = self.calcElectricPowerProfiles(self.drainProfiles, self.auxFuelDemandProfiles,
                                                               self.scalars, self.profileSelectors, self.scalarsProc,
                                                               filterIndex='indexDSM')

        # Profile filtering for flow profiles
        self.plugProfilesCons = self.filterConsProfiles(self.plugProfiles, self.profileSelectors, critCol='indexCons')
        self.electricPowerProfilesCons = self.filterConsProfiles(self.electricPowerProfiles, self.profileSelectors,
                                                            critCol='indexCons')
        self.chargeProfilesUncontrolledCons = self.filterConsProfiles(self.chargeProfilesUncontrolled, self.profileSelectors,
                                                                 critCol='indexCons')
        self.auxFuelDemandProfilesCons = self.filterConsProfiles(self.auxFuelDemandProfiles, self.profileSelectors,
                                                            critCol='indexCons')

        # Profile filtering for state profiles
        self.profilesSOCMinCons = self.filterConsProfiles(self.chargeMinProfiles, self.profileSelectors, critCol='indexDSM')
        self.profilesSOCMaxCons = self.filterConsProfiles(self.chargeMaxProfiles, self.profileSelectors, critCol='indexDSM')

    def aggregateProfilesMean(self, profilesIn: pd.DataFrame) -> pd.Series:
        """
        This method aggregates all single-vehicle profiles that are considered to one fleet profile.

        :param profilesIn: Dataframe of hourly values of all filtered profiles
        :return: Returns a Dataframe with hourly values for one aggregated profile
        """

        # Typecasting is necessary for aggregation of boolean profiles
        profilesIn = profilesIn.loc[~profilesIn.apply(lambda x: x.isna(), axis=0).any(axis=1), :]
        lenProfiles = len(profilesIn)
        profilesOut = profilesIn.apply(sum, axis=0) / lenProfiles
        return profilesOut

    def aggregateProfilesWeight(self, profilesIn: pd.DataFrame, weights: pd.DataFrame) -> pd.Series:
        """
        :param profilesIn: Dataframe of hourly values of all filtered profiles
        :param weights: Returns a Dataframe with hourly values considering the weight of individual trip
        :return:
        """
        profilesIn = profilesIn.loc[~profilesIn.apply(lambda x: x.isna(), axis=0).any(axis=1), :]
        weights = weights.loc[profilesIn.index, :]  # Filtering weight data to equate lengths
        profilesOut = profilesIn.apply(Evaluator.calculateWeightedAverage, weightCol=weights['tripWeight'])
        return profilesOut

    def aggregateDiffVariable(self, data: pd.DataFrame, by: str, weights: pd.Series, hourVec: list) -> pd.Series:
        """
        :param data: list of strings declaring the datasets to be read in
        :param by:
        :param weights:
        :param hourVec:
        :return:
        """
        vars = set(data.index.get_level_values(by))
        ret = pd.DataFrame(index=hourVec, columns=vars)
        data = data.reset_index(level=by)
        weights = weights.loc[data.index, :].reset_index(level=by)
        for iVar in vars:
            dataSlice = data.loc[data.loc[:, by] == iVar, hourVec]
            weightSlice = weights.loc[weights.loc[:, by] == iVar, 'tripWeight']
            ret.loc[:, iVar] = dataSlice.apply(Evaluator.calculateWeightedAverage, weightCol=weightSlice)
        ret = ret.stack()
        ret.index.names = ['time', by]
        # ret.index = ret.index.swaplevel(0, 1)
        return ret

    def aggregate(self):
        # Profile aggregation for flow profiles by averaging
        self.plugProfilesAgg = self.aggregateProfilesMean(self.plugProfilesCons)
        self.electricPowerProfilesAgg = self.aggregateProfilesMean(self.electricPowerProfilesCons)
        self.chargeProfilesUncontrolledAgg = self.aggregateProfilesMean(self.chargeProfilesUncontrolledCons)
        self.auxFuelDemandProfilesAgg = self.aggregateProfilesMean(self.auxFuelDemandProfilesCons)

        # Profile aggregation for flow profiles by averaging
        self.plugProfilesWAgg = self.aggregateProfilesWeight(self.plugProfilesCons, self.weights)
        self.electricPowerProfilesWAgg = self.aggregateProfilesWeight(self.electricPowerProfilesCons, self.weights)
        self.chargeProfilesUncontrolledWAgg = self.aggregateProfilesWeight(self.chargeProfilesUncontrolledCons, self.weights)
        self.auxFuelDemandProfilesWAgg = self.aggregateProfilesWeight(self.auxFuelDemandProfilesCons, self.weights)

        # Define a partial method for variable specific weight-considering aggregation
        aggDiffVar = functools.partial(self.aggregateDiffVariable, by='tripStartWeekday', weights=self.weights,
                                       hourVec=self.hourVec)

        # Profile aggregation for flow profiles by averaging
        self.plugProfilesWAggVar = aggDiffVar(data=self.plugProfilesCons)
        self.electricPowerProfilesWAggVar = aggDiffVar(data=self.electricPowerProfilesCons)
        self.chargeProfilesUncontrolledWAggVar = aggDiffVar(data=self.chargeProfilesUncontrolledCons)
        self.auxFuelDemandProfilesWAggVar = aggDiffVar(data=self.auxFuelDemandProfilesCons)

        # Profile aggregation for state profiles by selecting one profiles value for each hour
        self.SOCMin, self.SOCMax = self.socProfileSelection(self.profilesSOCMinCons, self.profilesSOCMaxCons,
                                                       filter='singleValue', alpha=10)

        self.SOCMinVar, self.SOCMaxVar = self.socSelectionVar(dataMin=self.profilesSOCMinCons,
                                                              dataMax=self.profilesSOCMaxCons,
                                                              by='tripStartWeekday', filter='singleValue', alpha=10)

    def socSelectionVar(self, dataMin: pd.DataFrame, dataMax: pd.DataFrame, by, filter, alpha) -> pd.Series:
        """
        :param dataMin:
        :param dataMax:
        :param by:
        :param filter:
        :param alpha:
        :return:
        """
        socSelectionPartial = functools.partial(self.socProfileSelection, filter=filter, alpha=alpha)
        vars = set(dataMin.index.get_level_values(by))
        retMin = pd.DataFrame(index=self.hourVec, columns=vars)
        retMax = pd.DataFrame(index=self.hourVec, columns=vars)
        dataMin = dataMin.reset_index(level=by)
        dataMax = dataMax.reset_index(level=by)
        for iVar in vars:
            dataSliceMin = dataMin.loc[dataMin.loc[:, by] == iVar, self.hourVec]
            dataSliceMax = dataMax.loc[dataMax.loc[:, by] == iVar, self.hourVec]
            retMin.loc[:, iVar], retMax.loc[:, iVar] = socSelectionPartial(profilesMin=dataSliceMin,
                                                                           profilesMax=dataSliceMax)

        retMin = retMin.stack()
        retMax = retMax.stack()
        retMin.index.names = ['time', by]
        retMax.index.names = ['time', by]
        # ret.index = ret.index.swaplevel(0, 1)
        return retMin, retMax

    def correctProfiles(self, scalars: pd.DataFrame, profile: pd.Series, profType) -> pd.Series:
        """
        This method scales given profiles by a correction factor. It was written for VencoPy scaling consumption data
        with the more realistic ARTEMIS driving cycle.

        :param scalars: Dataframe of technical assumptions
        :param profile: Dataframe of profile that should be corrected
        :param profType: A list of strings specifying if the given profile type is an electric or a fuel profile.
        profType has to have the same length as profiles.
        :return:
        """

        if profType == 'electric':
            consumptionElectricNEFZ = scalars.loc['Electric consumption NEFZ', 'value']
            consumptionElectricArtemis = scalars.loc['Electric consumption Artemis', 'value']
            corrFactor = consumptionElectricArtemis / consumptionElectricNEFZ
        elif profType == 'fuel':
            consumptionFuelNEFZ = scalars.loc['Fuel consumption NEFZ', 'value']
            consumptionFuelArtemis = scalars.loc['Fuel consumption Artemis', 'value']
            corrFactor = consumptionFuelArtemis / consumptionFuelNEFZ
        else:
            # review I expect raising an exception here. Would it not be a problem if the processing continues silently?
            print(f'Either parameter "{profType}" is not given or not assigned to either "electric" or "fuel".')
        profileOut = corrFactor * profile
        return profileOut

    def correct(self):
        self.chargeProfilesUncontrolledCorr = self.correctProfiles(self.scalars, self.chargeProfilesUncontrolledAgg,
                                                              'electric')
        self.electricPowerProfilesCorr = self.correctProfiles(self.scalars, self.electricPowerProfilesAgg, 'electric')
        self.auxFuelDemandProfilesCorr = self.correctProfiles(self.scalars, self.auxFuelDemandProfilesAgg, 'fuel')

    def normalize(self):
        # Profile normalization for state profiles with the basis battery capacity
        self.socMinNorm, self.socMaxNorm = self.normalizeProfiles(self.scalars, self.SOCMin, self.SOCMax,
                                                             normReferenceParam='Battery capacity')

    def writeProfilesToCSV(self, globalConfig: dict, profileDictOut, singleFile=True, datasetID='MiD17'):
        """
        Function to write VencoPy profiles to either one or five .csv files in the output folder specified in outputFolder.

        :param outputFolder: Link to output folder
        :param profileDictOut: Dictionary with profile names in keys and profiles as pd.Series containing a VencoPy
        profile each to be written in value
        :param singleFile: If True, all profiles will be appended and written to one .csv file. If False, five files are
        written
        :param strAdd: String addition for filenames
        :return: None
        """

        if singleFile:
            dataOut = pd.DataFrame(profileDictOut)
            dataOut.to_csv(pathlib.Path(globalConfig['linksRelative']['dataOutput']) /
                           createFileString(globalConfig=globalConfig, fileKey='vencoPyOutput', datasetID=datasetID), header=True)
        else:
            for iName, iProf in profileDictOut.items():
                iProf.to_csv(pathlib.Path(globalConfig['linksRelative']['dataOutput']) /
                             pathlib.Path(r'vencoPyOutput_' + iName + datasetID + '.csv'), header=True)

    def writeOut(self):
        self.profileDictOut = dict(uncontrolledCharging=self.chargeProfilesUncontrolledCorr,
                                   electricityDemandDriving=self.electricPowerProfilesCorr,
                                   SOCMax=self.socMaxNorm, SOCMin=self.socMinNorm,
                                   gridConnectionShare=self.plugProfilesAgg,
                                   auxFuelDriveProfile=self.auxFuelDemandProfilesCorr)

        self.writeProfilesToCSV(profileDictOut=self.profileDictOut, globalConfig=self.globalConfig, singleFile=True,
                                datasetID=self.datasetID)

    def sortData(data):
        data.index = data.index.swaplevel(0, 1)
        return data.sort_index()

    def linePlot(self, profileDict, linkOutput, config, show=True, write=True, ylabel='Normalized profiles', ylim=None,
                 filename=''):
        plt.rcParams.update(self.evaluatorConfig['plotConfig']['plotRCParameters'])  # set plot layout
        fig, ax = plt.subplots()
        plt.tick_params(labelsize=self.evaluatorConfig['plotConfig']['plotRCParameters']['font.size'])
        for iKey, iVal in profileDict.items():
            if isinstance(iVal.index, pd.MultiIndex):
                iVal = self.sortData(iVal)
                sns.lineplot(range(iVal.index.size), iVal, label=iKey, sort=False)
            else:
                sns.lineplot(iVal.index, iVal, label=iKey, sort=False)
        xRange = range(0, len(profileDict[list(profileDict)[0]]) + 1, self.evaluatorConfig['plotConfig']['xAxis']['xTickSteps'])
        # xLabels = [f'{iDay}\n{str(iTime)}:00' for iDay in config['plotConfig']['xAxis']['weekdays'] for iTime in config['plotConfig']['xAxis']['hours']]
        xLabels = [f'{str(iTime)}:00' for iTime in self.evaluatorConfig['plotConfig']['xAxis']['hours']]
        ax.set_xticks(xRange)
        ax.set_xticklabels(xLabels, fontsize=self.evaluatorConfig['plotConfig']['xAxis']['ticklabelsize'])
        ax.set_ylim(bottom=0, top=ylim)
        ax.set_xlabel('Hour', fontsize=self.evaluatorConfig['plotConfig']['plotRCParameters']['axes.labelsize'])
        ax.set_ylabel(ylabel, fontsize=self.evaluatorConfig['plotConfig']['plotRCParameters']['axes.labelsize'])
        plt.legend(loc='upper center')  # , bbox_to_anchor=(0.5, 1.3) ncol=2,
        plt.tight_layout()
        filePlot = linkOutput / pathlib.Path(
            createFileString(globalConfig=self.globalConfig, datasetID=self.datasetID, fileKey='flexPlotName', manualLabel=filename,
                             filetypeStr='svg'))
        if show:
            plt.show()
        if write:
            fig.savefig(filePlot)

    def separateLinePlots(self, profileDictList, config, globalConfig, datasetID='MiD17', show=True, write=True,
                          ylabel=[], ylim=[], filenames=[]):
        for iDict, iYLabel, iYLim, iName in zip(profileDictList, ylabel, ylim, filenames):
            self.writeProfilesToCSV(profileDictOut=iDict, globalConfig=globalConfig, singleFile=False, datasetID=datasetID)
            self.linePlot(iDict, linkOutput=self.globalConfig['linksRelative']['plots'], config=config, show=show, write=write,
                     ylabel=iYLabel, ylim=iYLim, filename=iName)


    def plotProfiles(self):
        self.linePlot(self.profileDictOut, linkOutput=self.linkDict['linkPlots'], config=self.config,
                      show=True, write=True, filename='allPlots' + self.datasetID)

        # Separately plot flow and state profiles
        profileDictConnectionShare = dict(gridConnectionShare=self.plugProfilesAgg)

        profileDictFlowsNorm = dict(uncontrolledCharging=self.chargeProfilesUncontrolledCorr,
                                    electricityDemandDriving=self.electricPowerProfilesCorr,
                                    gridConnectionShare=self.plugProfilesAgg)
        profileDictFlowsAbs = dict(uncontrolledCharging=self.chargeProfilesUncontrolledAgg,
                                   electricityDemandDriving=self.electricPowerProfilesAgg)

        profileDictStateNorm = dict(SOCMax=self.socMaxNorm, SOCMin=self.socMinNorm)
        profileDictStateAbs = dict(SOCMax=self.SOCMax, SOCMin=self.SOCMin)

        profileDictList = [profileDictConnectionShare, profileDictFlowsAbs, profileDictStateAbs]

        self.separateLinePlots(profileDictList, self.config, self.globalConfig,
                               show=True, write=True,
                               ylabel=['Average EV connection share', 'Average EV flow in kW', 'Average EV SOC in kWh'],
                               filenames=[self.datasetID + '_connection', self.datasetID + '_flows', self.datasetID + '_state'],
                               ylim=[1, 0.9, 50])

    def compareProfiles(self, compareTo):
        if not isinstance(compareTo, FlexEstimator):
            raise('Argument to compare to is not a class instance of FlexEstimator')

        profileList = [
                       # 'plugProfilesAgg', 'plugProfilesWAgg', 'chargeProfilesUncontrolledAgg',
                       # 'chargeProfilesUncontrolledWAgg', 'electricPowerProfilesAgg', 'electricPowerProfilesWAgg',
                       # 'plugProfilesWAggVar', 'electricPowerProfilesWAggVar', 'chargeProfilesUncontrolledWAggVar'
                       # 'auxFuelDemandProfilesWAggVar',
                       ]

        profileDictList = self.compileDictList(compareTo=compareTo, profileNameList=profileList)
        SOCDataWeek = { 'MiD08_SOCmin': self.SOCMinVar,
                        'MiD08_SOCmax': self.SOCMaxVar,
                        'MiD17_SOCmin': compareTo.SOCMinVar,
                        'MiD17_SOCmax': compareTo.SOCMaxVar }

        profileDictList.append(SOCDataWeek)

        self.separateLinePlots(profileDictList, self.config,
                          show=self.evaluatorConfig['plotConfig']['show'], write=self.evaluatorConfig['plotConfig']['save'],
                          ylabel=[
                                  # 'Average EV connection share', 'Weighted Average EV connection share',
                                  # 'Uncontrolled charging in kW', 'Weighted Uncontrolled charging in kW',
                                  # 'Electricity consumption for driving in kWh',
                                  # 'Weighted Electricity consumption for driving in kWh',
                                  # 'Weighted average EV fleet connection share',
                                  # 'Electricity consumption for driving in kWh',
                                  # 'Weighted average uncontrolled charging in kW'
                                  # 'auxFuelDemandProfilesWAggVar'
                                  'State of charge in kWh'
                                  ],
                          filenames=[
                                     # '_connection', '_connectionWeighted',
                                     # '_uncCharge', '_uncChargeWeighted',
                                     # '_drain', '_drainWeighted',
                                     # '_plugDiffDay', '_drainDiffDay',
                                     # '_uncChargeDiffDay'
                                     #  '_auxFuelDiffDay',
                                     '_socWeek'
                                     ],
                          ylim=[
                              # 1, 1, 1,
                              # 1, 1, 1
                              # 1, 1, 1
                              # 1
                              50
                                ])

    def compileDictList(self, compareTo, profileNameList):
        ret = []
        keys = [self.datasetID, compareTo.datasetID]
        for iProf in profileNameList:
            iDict = self.compileProfileComparisonDict(keys=keys,
                                                      values=[getattr(self, iProf), getattr(compareTo, iProf)])
            ret.append(iDict)
        return ret

    @staticmethod
    def compileProfileComparisonDict(keys: list, values: list):
        return {iKey: iVal for iKey, iVal in zip(keys, values)}

# review: This should be a method of FlexEstimation with name run().
#  this is a common approach to define a workflow for a class based structure.
#  I suggest to not return Flexstimator.
def runFlexEstimation(config, globalConfig, evaluatorConfig, ParseData: DataParser, datasetID: str='MiD17'):
    Flexstimator = FlexEstimator(config=config, globalConfig=globalConfig, evaluatorConfig=evaluatorConfig,
                                 datasetID=datasetID, ParseData=ParseData)
    # review: Flexstimator should be lower case as it is an instance not a class.
    Flexstimator.baseProfileCalculation()
    Flexstimator.filter()
    Flexstimator.aggregate()
    Flexstimator.correct()
    Flexstimator.normalize()
    Flexstimator.writeOut()
    # Flexstimator.plotProfiles()
    return Flexstimator
#
# def runFlexstimation(config, datasetID, variable):
#     indexedDriveData = mergeVariables(data=driveDataDays, variableData=tripDataClean, variables=['tripStartWeekday',
#                                                                                                 'tripWeight'])
#     indexedPurposeData = mergeVariables(data=purposeDataDays, variableData=tripDataClean, variables=['tripStartWeekday',
#                                                                                                     'tripWeight'])
#     Flexstimator = FlexEstimator(config=config, datasetID=datasetID)
#     Flexstimator.baseProfileCalculation()
#     Flexstimator.filter()
#     Flexstimator.aggregate()
#     Flexstimator.correct()
#     Flexstimator.normalize()
#     Flexstimator.writeOut()

if __name__ == '__main__':
    linkGlobalConfig = pathlib.Path.cwd().parent / 'config' / 'globalConfig.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    globalConfig = yaml.load(open(linkGlobalConfig), Loader=yaml.SafeLoader)
    linkFlexConfig = pathlib.Path.cwd().parent / 'config' / 'flexConfig.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    flexConfig = yaml.load(open(linkFlexConfig), Loader=yaml.SafeLoader)
    linkParseConfig = pathlib.Path.cwd().parent / 'config' / 'parseConfig.yaml'
    parseConfig = yaml.load(open(linkParseConfig), Loader=yaml.SafeLoader)
    linkEvaluatorConfig = pathlib.Path.cwd().parent / 'config' / 'evaluatorConfig.yaml'
    evaluatorConfig = yaml.load(open(linkEvaluatorConfig), Loader=yaml.SafeLoader)
    os.chdir(globalConfig['linksAbsolute']['vencoPyRoot'])

    vpData = DataParser(config=parseConfig, globalConfig=globalConfig, loadEncrypted=False)
    vpFlexEst17 = runFlexEstimation(config=flexConfig, globalConfig=globalConfig, evaluatorConfig=evaluatorConfig,
                                    ParseData=vpData, datasetID='MiD17')

    print(f'Total absolute electricity charged in uncontrolled charging based on MiD08: '
          f'{vpFlexEst17.chargeProfilesUncontrolled.sum().sum()} based on MiD17')

    print('This is the end')
