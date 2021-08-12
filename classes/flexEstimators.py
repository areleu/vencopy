__version__ = '0.0.9'
__maintainer__ = 'Niklas Wulff'
__contributors__ = 'Fabia Miorelli, Parth Butte'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '03.11.2019'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'


#----- imports & packages ------
from pathlib import Path
import functools
import warnings
import os
import yaml
import pandas as pd
import numpy as np
from random import seed, random

from scripts.globalFunctions import createFileString, mergeVariables, calculateWeightedAverage, writeProfilesToCSV


class FlexEstimator:
    def __init__(self, globalConfig: dict, flexConfig : dict, evaluatorConfig: dict, ParseData,
                 datasetID: str):
        """
        Class to estimate uncontrolled charging, electricity drain, grid connection, auxiliary fuel, SOC min and
        SOC max profiles based on hourly driving and boolean grid connection profiles. Requires a .xlsx file specifying
        a global value for specific electric consumption (in kWh/ 100 km) and a global rated capacity of considered
        charging stations. Automatically compiles input file names from the filekeys "inputDataDriveProfiles" /
        "inputDataPlugProfiles", runlabel (as specified in globalConfig) and datasetID (as given on instantiation). The
        number of iterations for SOC max and min calculations is defined in the function self.baseProfielCalculation as
        parameter of calcChargeMinProfiles() and calcChargeMaxProfiles(). A complete estimation consists of the
        following calls also specified in self.run():
        self.baseProfileCalculation()
        self.filter()
        self.aggregate()
        self.correct()
        self.normalize()
        self.writeOut()

        :param globalConfig: Dictionary used for relative paths, filenames and runlabels
        :param flexConfig: So far not used
        :param evaluatorConfig: DEPRECATED, may be cleared soon
        :param ParseData: Class instance of type DataParser
        :param datasetID: String used for file name composition on input files
        """

        #self.config = config
        self.globalConfig = globalConfig
        self.flexConfig = flexConfig
        self.evaluatorConfig = evaluatorConfig
        self.hourVec = range(self.globalConfig['numberOfHours'])
        self.datasetID = datasetID
        self.driveProfilesIn, self.plugProfilesIn = self.readVencoInput(datasetID=datasetID)
        self.mergeDataToWeightsAndDays(ParseData)
        self.weights = self.indexWeights(self.driveProfilesIn.loc[:, ['genericID', 'tripStartWeekday', 'tripWeight']])
        # self.outputConfig = yaml.load(open(Path(self.globalConfig['pathRelative']['config']) /
        #                                    self.globalConfig['files']['outputConfig']), Loader=yaml.SafeLoader)
        self.driveProfiles, self.plugProfiles = self.indexDriveAndPlugData(self.driveProfilesIn, self.plugProfilesIn,
                                                                      dropIdxLevel='tripWeight',
                                                                      nHours=self.globalConfig['numberOfHours'])
        self.scalarsProc = self.procScalars(self.driveProfilesIn, self.plugProfilesIn,
                                       self.driveProfiles, self.plugProfiles)

        #  Future release: with so many values to be initialized, a composition approach with a dataclass based
        #  encapsulation might be a good idea. This would also make it easy to communicate the data to the outside of
        #  the class as the fields are then no longer directly in the main processing class.

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

        self.socMin = None
        self.socMax = None
        self.socMinVar = None
        self.socMaxVar = None

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

    def booleanMapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Replaces given strings with python values for true or false.

        :param df: Dataframe holding strings defining true or false values
        :return: Dataframe holding true and false
        """
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
        #inputData = self.booleanMapping(inputRaw)
        return inputRaw

    def readVencoInput(self, datasetID: str) -> pd.DataFrame:
        """
        Initializing action for VencoPy-specific config-file, path dictionary and data read-in. The config file has
        to be a dictionary in a .yaml file containing three categories: pathRelative, pathAbsolute and files. Each
        category must contain itself a dictionary with the pathRelative to data, functions, plots, scripts, config and
        tsConfig. Absolute paths should contain the path to the output folder. Files should contain a path to scalar input
        data, and the two timeseries files inputDataDriveProfiles and inputDataPlugProfiles.

        :param config: A yaml config file holding a dictionary with the keys 'pathRelative' and 'pathAbsolute'
        :return: Returns four dataframes: A path dictionary, scalars, drive profile data and plug profile
        data, the latter three ones in a raw data format.
        """
        print('Reading Venco input scalars, drive profiles and boolean plug profiles')

        driveProfiles_raw = self.readInputCSV(Path(self.globalConfig['pathRelative']['input']) /
                                              createFileString(globalConfig=self.globalConfig,
                                                               fileKey='inputDataDriveProfiles',
                                                               datasetID=datasetID))
        plugProfiles_raw = self.readInputBoolean(Path(self.globalConfig['pathRelative']['input']) /
                                                 createFileString(globalConfig=self.globalConfig,
                                                                  fileKey='inputDataPlugProfiles',
                                                                  datasetID=datasetID))

        print('There are ' + str(len(driveProfiles_raw)) + ' drive profiles and ' +
              str(len(driveProfiles_raw)) + ' plug profiles.')

        return driveProfiles_raw, plugProfiles_raw

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
        """
        Reduces the dtype from string to float if possible and sets the index of the weights to align with the indices
        of drive and plug profiles (genericID and tripStartWeekday).

        :param weights: dataframe containing the MiD trip weights of the original trips
        :return: An indexed pandas DataFrame of the MiD trip weights
        """
        weights = weights.convert_dtypes()
        return weights.set_index(['genericID', 'tripStartWeekday'], drop=True)

    def findIndexCols(self, data: pd.DataFrame, nHours: int) -> list:
        """
        Identifies columns that contain index strings rather than numeric data. It does so by checking for column
        names equal to the numbers from 0 to the number of hours.

        :param data: Pandas DataFrame where index columns should be found
        :param nHours: Integer giving the analyzed number of hours
        :return: List of index columns
        """

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
        data = data.convert_dtypes()  # Reduce column data types if possible (specifically genericID column to int)
        dataIndexed = data.set_index(list(indexCols))

        # Typecast column indices to int for later looping over a range
        dataIndexed.columns = dataIndexed.columns.astype(int)
        return dataIndexed

    def indexDriveAndPlugData(self, driveData: pd.DataFrame, plugData: pd.DataFrame, dropIdxLevel: str,
                              nHours: int) -> tuple:
        """
        Wrapper function for indexing drive and plug profiles so that value columns are all made up of hourly data.

        :param driveData: Hourly drive data for individual vehicles as floats
        :param plugData: Hourly plug data for individual vehicles as boolean
        :param dropIdxLevel: Column to be dropped
        :param nHours: Integer specifying the number of values columns
        :return: Tuple of indexed drive and plug profiles as pandas DataFrames
        """
        driveProfiles = self.indexProfile(driveData, nHours)
        plugProfiles = self.indexProfile(plugData, nHours)
        return driveProfiles.droplevel(dropIdxLevel), plugProfiles.droplevel(dropIdxLevel)

    def mergeDataToWeightsAndDays(self, ParseData):
        """
        Function to merging weekday and trip weight data to driving and plugging data of respective personHHIDs. It is
        assumed that trips occur on one daz and trip weights are equal for all trips of one genericID

        :param ParseData: Class instance of type DataParser
        :return: None
        """

        self.driveProfilesIn = mergeVariables(data=self.driveProfilesIn, variableData=ParseData.data,
                                              variables=['tripStartWeekday', 'tripWeight'])
        self.plugProfilesIn = mergeVariables(data=self.plugProfilesIn, variableData=ParseData.data,
                                             variables=['tripStartWeekday', 'tripWeight'])

    def calcDrainProfiles(self, driveProfiles: pd.DataFrame, flexConfig) -> pd.DataFrame:
        """
        Calculates electrical consumption profiles from drive profiles assuming specific consumption (in kWh/100 km)
        given in scalar input data file.

        :param driveProfiles: indexed profile file
        :param flexConfig: YAML config which holds all relative paths and filenames for flexEstimators.py
        :return: Returns a dataframe with consumption profiles in kWh/h in same format and length as driveProfiles but
        scaled with the specific consumption assumption.
        """

        return driveProfiles * flexConfig['inputDataScalars'][self.datasetID]['Electric_consumption_NEFZ'] / float(100)

    def calcChargeProfiles(self, plugProfiles: pd.DataFrame, flexConfig) -> pd.DataFrame:
        '''
        Calculates the maximum possible charge power based on the plug profile assuming the charge column power
        given in the scalar input data file (so far under Panschluss).

        :param plugProfiles: indexed boolean profiles for vehicle connection to grid
        :param flexConfig: YAML config which holds all relative paths and filenames for flexEstimators.py
        :return: Returns scaled plugProfile in the same format as plugProfiles.
        '''

        return plugProfiles * flexConfig['inputDataScalars'][self.datasetID]['Rated_power_of_charging_column']

    def calcChargeMaxProfiles(self, chargeProfiles: pd.DataFrame, consumptionProfiles: pd.DataFrame,
                              nIter: int) -> pd.DataFrame:
        """
        Calculates all maximum SoC profiles under the assumption that batteries are always charged as soon as they
        are plugged to the grid. Values are assured to not fall below SoC_min * battery capacity or surpass
        SoC_max * battery capacity. Relevant profiles are chargeProfile and consumptionProfile. An iteration assures
        the boundary condition of chargeMaxProfile(0) = chargeMaxProfile(len(profiles)). The number of iterations
        is given as parameter.

        :param chargeProfiles: Indexed dataframe of charge profiles.
        :param consumptionProfiles: Indexed dataframe of consumptionProfiles.
        :param flexConfig: YAML config holds all relative paths and filenames for flexEstimators.py
        :param scalarsProc: DataFrame holding information about profile length and number of hours.
        :param nIter: Number of iterations to assure that the minimum and maximum value are approximately the same
        :return: Returns an indexed DataFrame with the same length and form as chargProfiles and consumptionProfiles,
        containing single-profile SOC max values for each hour in each profile.
        """

        chargeMaxProfiles = chargeProfiles.copy()
        batCapMin = self.flexConfig['inputDataScalars'][self.datasetID]['Battery_capacity'] * self.flexConfig['inputDataScalars'][self.datasetID]['Minimum_SOC']
        batCapMax = self.flexConfig['inputDataScalars'][self.datasetID]['Battery_capacity'] * self.flexConfig['inputDataScalars'][self.datasetID]['Maximum_SOC']
        nHours = self.scalarsProc['noHours']
        for idxIt in range(nIter):
            print(f'Starting with iteration {idxIt}')
            for iHour in range(nHours):
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
                                 driveProfiles: pd.DataFrame, flexConfig,
                                 scalarsProc: pd.DataFrame) -> pd.DataFrame:
        # FixMe: alternative vectorized format for looping over columns? numpy, pandas: broadcasting-rules
        """
         Calculates necessary fuel consumption profile of a potential auxilliary unit (e.g. a gasoline motor) based
        on gasoline consumption given in scalar input data (in l/100 km). Auxilliary fuel is needed if an hourly
        mileage is higher than the available SoC Max in that hour.

        :param chargeMaxProfiles: Dataframe holding hourly maximum SOC profiles in kWh for all profiles
        :param chargeProfilesUncontrolled: Dataframe holding hourly uncontrolled charging values in kWh/h for all profiles
        :param driveProfiles: Dataframe holding hourly electric driving demand in kWh/h for all profiles.
        :param flexConfig: YAML config which holds all relative paths and filenames for flexEstimators.py
        :param scalarsProc: Dataframe holding meta-infos about the input
        :return: Returns a DataFrame with single-profile values for back-up fuel demand in the case a profile cannot
        completely be fulfilled with electric driving under the given consumption and battery size assumptions.
        """

        # Future release:
        # the hardcoding of the column names can cause a lot of problems for people later on if we do not ship
        # the date with the tool. I would recommend to move these column names to a config file similar to i18n
        # strategies
        consumptionPower = flexConfig['inputDataScalars'][self.datasetID]['Electric_consumption_NEFZ']
        consumptionFuel = flexConfig['inputDataScalars'][self.datasetID]['Fuel_consumption_NEFZ']

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
        driveProfilesFuelAux = driveProfilesFuelAux.astype(float).round(4)
        return driveProfilesFuelAux

    def calcChargeMinProfiles(self, chargeProfiles: pd.DataFrame, consumptionProfiles: pd.DataFrame,
                              driveProfilesFuelAux: pd.DataFrame, flexConfig,
                              nIter: int = 3) -> pd.DataFrame:
        """
        Calculates minimum SoC profiles assuming that the hourly mileage has to exactly be fulfilled but no battery charge
        is kept inspite of fulfilling the mobility demand. It represents the minimum charge that a vehicle battery has to
        contain in order to fulfill all trips.
        An iteration is performed in order to assure equality of the SoCs at beginning and end of the profile.

        :param chargeProfiles: Charging profiles with techno-economic assumptions on connection power.
        :param consumptionProfiles: Profiles giving consumed electricity for each trip in each hour assuming specified
            consumption.
        :param driveProfilesFuelAux: Auxilliary fuel demand for fulfilling trips if purely electric driving doesn't suffice.
        :param flexConfig: YAML config which holds all relative paths and filenames for flexEstimators.py
        :param scalarsProc: Number of profiles and number of hours of each profile.
        :param nIter: Gives the number of iterations to fulfill the boundary condition of the SoC equalling in the first
            and in the last hour of the profile.
        :return: Returns an indexed DataFrame containing minimum SOC values for each profile in each hour in the same
            format as chargeProfiles, consumptionProfiles and other input parameters.
        """
        chargeMinProfiles = chargeProfiles.copy()
        batCapMin = self.flexConfig['inputDataScalars'][self.datasetID]['Battery_capacity'] * self.flexConfig['inputDataScalars'][self.datasetID]['Minimum_SOC']
        batCapMax = self.flexConfig['inputDataScalars'][self.datasetID]['Battery_capacity'] * self.flexConfig['inputDataScalars'][self.datasetID]['Maximum_SOC']
        consElectric = self.flexConfig['inputDataScalars'][self.datasetID]['Electric_consumption_NEFZ']
        consGasoline = self.flexConfig['inputDataScalars'][self.datasetID]['Fuel_consumption_NEFZ']
        nHours = self.scalarsProc['noHours']
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
        """
        Wrapper function for first part of flexibility estimation calculating the six resulting profiles for all
        individual vehicles. The number of iterations for calcChargeMaxProfiles() and calcChargeMinProfiles() can be
        specified here.

        :return: None
        """

        self.drainProfiles = self.calcDrainProfiles(self.driveProfiles, self.flexConfig)
        self.chargeProfiles = self.calcChargeProfiles(self.plugProfiles, self.flexConfig)
        self.chargeMaxProfiles = self.calcChargeMaxProfiles(self.chargeProfiles, self.drainProfiles, nIter=3)
        # self.connectionType = self.assignConnectionType()
        self.chargeProfilesUncontrolled = self.calcChargeProfilesUncontrolled(self.chargeMaxProfiles, self.scalarsProc)
        self.auxFuelDemandProfiles = self.calcDriveProfilesFuelAux(self.chargeMaxProfiles,
                                                                   self.chargeProfilesUncontrolled, self.driveProfiles,
                                                                   self.flexConfig, self.scalarsProc)
        self.chargeMinProfiles = self.calcChargeMinProfiles(self.chargeProfiles, self.drainProfiles,
                                                       self.auxFuelDemandProfiles, self.scalarsProc)
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
        :param fuelDriveTolerance: Give a threshold value how many liters may be needed throughout the course of a day
        in order to still consider the profile.
        :param isBEV: Boolean value. If true, more 2030 profiles are taken into account (in general).
        :return: The bool indices are written to one DataFrame in the DataManager with the columns randNo, indexCons and
        indexDSM and the same indices as the other profiles.
        """

        boolBEV = self.flexConfig['inputDataScalars'][self.datasetID]['Is_BEV?']
        minDailyMileage = self.flexConfig['inputDataScalars'][self.datasetID]['Minimum_daily_mileage']
        batSize = self.flexConfig['inputDataScalars'][self.datasetID]['Battery_capacity']
        socMax = self.flexConfig['inputDataScalars'][self.datasetID]['Maximum_SOC']
        socMin = self.flexConfig['inputDataScalars'][self.datasetID]['Minimum_SOC']
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
                                  filterCons: pd.DataFrame, filterIndex) -> pd.DataFrame:
        """
        Calculates electric power profiles that serve as outflow of the fleet batteries.

        :param consumptionProfiles: Indexed DataFrame containing electric vehicle consumption profiles.
        :param driveProfilesFuelAux: Indexed DataFrame containing
        :param flexConfig: YAML config which holds all relative paths and filenames for flexEstimators.py
        :param filterCons: Dataframe containing one boolean filter value for each profile
        :param scalarsProc: Dataframe containing meta information of input profiles
        :param filterIndex: Can be either 'indexCons' or 'indexDSM' so far. 'indexDSM' applies stronger filters and results
        are thus less representative.
        :return: Returns electric demand from driving filtered and aggregated to one fleet.
        """

        consumptionPower = self.flexConfig['inputDataScalars'][self.datasetID]['Electric_consumption_NEFZ']
        consumptionFuel = self.flexConfig['inputDataScalars'][self.datasetID]['Fuel_consumption_NEFZ']
        indexCons = filterCons.loc[:, 'indexCons']
        indexDSM = filterCons.loc[:, 'indexDSM']
        nHours = self.scalarsProc['noHours']
        electricPowerProfiles = consumptionProfiles.copy()
        for iHour in range(nHours):
            electricPowerProfiles[iHour] = (consumptionProfiles[iHour] - driveProfilesFuelAux[iHour] *
                                            (consumptionPower / consumptionFuel))
            if filterIndex == 'indexCons':
                electricPowerProfiles[iHour] = electricPowerProfiles[iHour] * indexCons
            elif filterIndex == 'indexDSM':
                electricPowerProfiles[iHour] = electricPowerProfiles[iHour] * indexDSM.astype(int)
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
        try:
            chargeMinProfilesDSM.loc[~filterCons['indexDSM'].astype('bool'), :] = minValue
            chargeMaxProfilesDSM.loc[~filterCons['indexDSM'].astype('bool'), :] = maxValue
        except Exception as E:
            print("Declaration doesn't work. "
                  "Maybe the length of filterCons differs from the length of chargeMaxProfiles")
            raise E
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

    def socProfileSelection(self, profilesMin: pd.DataFrame, profilesMax: pd.DataFrame, filter, alpha) -> tuple:
        """
        Selects the nth highest value for each hour for min (max profiles based on the percentage given in parameter
        'alpha'. If alpha = 10, the 10%-biggest (10%-smallest) value is selected, all other values are disregarded.
        Currently, in the Venco reproduction phase, the hourly values are selected independently of each other. min and max
        profiles have to have the same number of columns.

        :param profilesMin: Profiles giving minimum hypothetic SOC values to supply the driving demand at each hour
        :param profilesMax: Profiles giving maximum hypothetic SOC values if vehicle is charged as soon as possible
        :param filter: Filter method. Currently implemented: 'singleValue'
        :param alpha: Percentage, giving the amount of profiles whose mobility demand can not be fulfilled after selection.
        :return: Returns the two profiles 'socMax' and 'socMin' in the same time resolution as input profiles.
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
            raise ValueError('You selected a filter method that is not implemented.')
        return profileMinOut, profileMaxOut

    def normalizeProfiles(self, socMin: pd.Series, socMax: pd.Series) -> \
            tuple:
        """
        Normalizes given profiles with a given scalar reference.

        :param scalars: Dataframe containing technical assumptions e.g. battery capacity
        :param socMin: Minimum SOC profile subject to normalization
        :param socMax: Minimum SOC profile subject to normalization
        :param normReferenceParam: Reference parameter that is taken for normalization.
        This has to be given in scalar input data and is most likely the 'Battery_capacity'.
        :return: Writes the normalized profiles to the DataManager under the specified keys
        """

        normReference = self.flexConfig['inputDataScalars'][self.datasetID]['Battery_capacity']
        socMinNorm = socMin.div(float(normReference))
        socMaxNorm = socMax.div(float(normReference))
        return socMinNorm, socMaxNorm

    def filter(self):
        """
        Wrapper function to carry out filtering and selection procedures. A tolerance for needing additional fuel to
        carry out trips can be specified to keep profiles in the analyzed data basis.

        :return: None
        """

        self.randNoPerProfile = self.createRandNo(driveProfiles=self.driveProfiles)

        self.profileSelectors = self.calcProfileSelectors(chargeProfiles=self.chargeProfiles,
                                                     consumptionProfiles=self.drainProfiles,
                                                     driveProfiles=self.driveProfiles,
                                                     driveProfilesFuelAux=self.auxFuelDemandProfiles,
                                                     randNos=self.randNoPerProfile,
                                                     fuelDriveTolerance=1, isBEV=True)




        # Additional fuel consumption is subtracted from the consumption
        self.electricPowerProfiles = self.calcElectricPowerProfiles(self.drainProfiles, self.auxFuelDemandProfiles,
                                                                    self.profileSelectors, filterIndex='indexDSM')




        # Profile filtering for flow profiles
        self.plugProfilesCons = self.filterConsProfiles(self.plugProfiles, self.profileSelectors, critCol='indexCons')
        self.electricPowerProfilesCons = self.filterConsProfiles(self.electricPowerProfiles, self.profileSelectors,
                                                                 critCol='indexCons')
        self.chargeProfilesUncontrolledCons = self.filterConsProfiles(self.chargeProfilesUncontrolled,
                                                                      self.profileSelectors, critCol='indexCons')
        self.auxFuelDemandProfilesCons = self.filterConsProfiles(self.auxFuelDemandProfiles, self.profileSelectors,
                                                                 critCol='indexCons')

        # Profile filtering for state profiles
        self.profilesSOCMinCons = self.filterConsProfiles(self.chargeMinProfiles, self.profileSelectors,
                                                          critCol='indexDSM')
        self.profilesSOCMaxCons = self.filterConsProfiles(self.chargeMaxProfiles, self.profileSelectors,
                                                          critCol='indexDSM')

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
        Aggregation of profiles considering the specific weights given for the household person IDs. No rescaling of
        the weights is carried out so far. The function calculateWeightedAverage() is specified in globalFunctions.py

        :param profilesIn: Dataframe of hourly values of all filtered profiles
        :param weights: Returns a Dataframe with hourly values considering the weight of individual trip
        :return:
        """
        profilesIn = profilesIn.loc[~profilesIn.apply(lambda x: x.isna(), axis=0).any(axis=1), :]
        weights = weights.loc[profilesIn.index, :]  # Filtering weight data to equate lengths
        return profilesIn.apply(calculateWeightedAverage, weightCol=weights['tripWeight'])

    def aggregateDiffVariable(self, data: pd.DataFrame, by: str, weights: pd.Series, hourVec: list) -> pd.Series:
        """
        A separate weighted aggregation function differentiating by the variable defined as a string in str. Weights as
        given in MiD.

        :param data: list of strings declaring the datasetIDs to be read in
        :param by: String specifyzing a variable.
        :param weights: Weight vector as given in the MiD
        :param hourVec: hour vector specifying the hours used for VencoPy analysis
        :return:
        """
        vars = set(data.index.get_level_values(by))
        ret = pd.DataFrame(index=hourVec, columns=vars)
        data = data.reset_index(level=by)
        weights = weights.loc[data.index, :].reset_index(level=by)
        for iVar in vars:
            dataSlice = data.loc[data.loc[:, by] == iVar, hourVec]
            weightSlice = weights.loc[weights.loc[:, by] == iVar, 'tripWeight']
            ret.loc[:, iVar] = dataSlice.apply(calculateWeightedAverage, weightCol=weightSlice)
        ret = ret.stack()
        ret.index.names = ['time', by]
        # ret.index = ret.index.swaplevel(0, 1)
        return ret

    def aggregate(self):
        """
        Wrapper function to aggregate profiles from individual vehicle level to fleet level. This is done in two
        categories: Aggregating to one representative weekday with 24 hours and aggregating for a representative week.
        Within these categories mean and weighted mean values are calculated for comparison. Simple means are only
        calculated for 24 hour profiles. For state of charge profiles (soc max and soc min), simple estimations of
        minimum and maximum state-of-charge profiles are carried out. This is done based on
        https://elib.dlr.de/92151/1/Dissertation_Diego_Luca_de_Tena.pdf by selecting the nth maximum or minimum value.
        See https://doi.org/10.3390/en14144349 for further explanations.
        """

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

        # Define a partial method for variable specific weight-considering aggregation to make following lines shorter
        aggDiffWeekday = functools.partial(self.aggregateDiffVariable, by='tripStartWeekday', weights=self.weights,
                                           hourVec=self.hourVec)

        # Profile aggregation for flow profiles by averaging
        self.plugProfilesWAggVar = aggDiffWeekday(data=self.plugProfilesCons)
        self.electricPowerProfilesWAggVar = aggDiffWeekday(data=self.electricPowerProfilesCons)
        self.chargeProfilesUncontrolledWAggVar = aggDiffWeekday(data=self.chargeProfilesUncontrolledCons)
        self.auxFuelDemandProfilesWAggVar = aggDiffWeekday(data=self.auxFuelDemandProfilesCons)

        # Profile aggregation for state profiles by selecting one profiles value for each hour
        self.socMin, self.socMax = self.socProfileSelection(self.profilesSOCMinCons, self.profilesSOCMaxCons,
                                                            filter='singleValue', alpha=10)

        self.socMinVar, self.socMaxVar = self.socSelectionVar(dataMin=self.profilesSOCMinCons,
                                                              dataMax=self.profilesSOCMaxCons,
                                                              by='tripStartWeekday', filter='singleValue', alpha=10)

    def socSelectionVar(self, dataMin: pd.DataFrame, dataMax: pd.DataFrame, by: str, filter: str, alpha: int) -> tuple:
        """
        SOC selection function to aggregate state profiles from individual vehicle to fleet level.

        :param dataMin: Pandas Dataframe of hourly SOC min profiles for each household person ID
        :param dataMax: Pandas Dataframe of hourly SOC max profiles for each household person ID
        :param by: index level to differentiate selections by. Given as a string.
        :param filter: Filter method given as a string. Currently only 'singleValue' is implemented
        :param alpha: Percentile value to filter out extreme minimum and maximum soc values. E.g. 10 selects the 90th
        percentile for SOC max and the 10th percentile for SOC min values in each hour. These 24 values most likely do
        not belong to the same profile.
        :return: Returns a touple of estimated fleet socMin and socMax profiles for nHour x len(set(dataMin.loc[:, by])
        values. E.g. if running for 24 hour profiles additionally differentiating weekdays, this yields 168 values per
        resulting profile.
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

    def correctProfiles(self, profile: pd.Series, profType) -> pd.Series:
        """
        This method scales given profiles by a correction factor. It was written for VencoPy scaling consumption data
        with the more realistic ARTEMIS driving cycle.

        :param flexConfig: YAML config which holds all relative paths and filenames for flexEstimators.py
        :param profile: Dataframe of profile that should be corrected
        :param profType: A list of strings specifying if the given profile type is an electric or a fuel profile.
        profType has to have the same length as profiles.
        :return:
        """

        if profType == 'electric':
            consumptionElectricNEFZ = self.flexConfig['inputDataScalars'][self.datasetID]['Electric_consumption_NEFZ']
            consumptionElectricArtemis = self.flexConfig['inputDataScalars'][self.datasetID]['Electric_consumption_Artemis']
            corrFactor = consumptionElectricArtemis / consumptionElectricNEFZ
        elif profType == 'fuel':
            consumptionFuelNEFZ = self.flexConfig['inputDataScalars'][self.datasetID]['Fuel_consumption_NEFZ']
            consumptionFuelArtemis = self.flexConfig['inputDataScalars'][self.datasetID]['Fuel_consumption_Artemis']
            corrFactor = consumptionFuelArtemis / consumptionFuelNEFZ
        else:
            raise Exception(f'Either parameter "{profType}" is not given or not assigned to either "electric" or '
                            f'"fuel".')
        return corrFactor * profile

    def correct(self):
        """
        Wrapper function to correct all electric and fuel demand profiles with more realistic specific consumption
        values.

        :return: None
        """

        self.chargeProfilesUncontrolledCorr = self.correctProfiles(self.chargeProfilesUncontrolledAgg,
                                                                   'electric')
        self.electricPowerProfilesCorr = self.correctProfiles(self.electricPowerProfilesAgg, 'electric')
        self.auxFuelDemandProfilesCorr = self.correctProfiles(self.auxFuelDemandProfilesAgg, 'fuel')

    def normalize(self):
        """
        Normalization of soc profiles with regard to the battery capacity.

        :return: None
        """

        self.socMinNorm, self.socMaxNorm = self.normalizeProfiles(self.socMin, self.socMax)


    def writeOut(self):
        """
        Generic write-out function for estimated flexibility profiles. Profiles are all written to output/data as
        specified in the globalConfig and ammended by "vencoPyOutput", the runlabel as specified in the globalConfig
        as well as the datasetID. Output profiles are all written to one single file.

        :return: None
        """
        self.profileDictOut = dict(uncontrolledCharging=self.chargeProfilesUncontrolledWAggVar,
                                   electricityDemandDriving=self.electricPowerProfilesWAggVar,
                                   socMax=self.socMaxVar, socMin=self.socMinVar,
                                   gridConnectionShare=self.plugProfilesWAggVar,
                                   auxFuelDriveProfile=self.auxFuelDemandProfilesWAggVar)

        writeProfilesToCSV(profileDictOut=self.profileDictOut, globalConfig=self.globalConfig, singleFile=True,
                           datasetID=self.datasetID)

    def run(self):
        """
        Wrapper function for the whole flexibility estimation workflow containing the six above described wrapper
        functions.

        :return: None
        """
        self.baseProfileCalculation()
        self.filter()
        self.aggregate()
        self.correct()
        self.normalize()
        self.writeOut()


if __name__ == '__main__':
    from classes.dataParsers import DataParser
    from classes.evaluators import Evaluator
    datasetID = 'KiD'
    pathGlobalConfig = Path.cwd().parent / 'config' / 'globalConfig.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    with open(pathGlobalConfig) as ipf:
        globalConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    pathFlexConfig = Path.cwd().parent / 'config' / 'flexConfig.yaml'
    with open(pathFlexConfig) as ipf:
        flexConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    pathParseConfig = Path.cwd().parent / 'config' / 'parseConfig.yaml'
    with open(pathParseConfig) as ipf:
        parseConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    pathEvaluatorConfig = Path.cwd().parent / 'config' / 'evaluatorConfig.yaml'
    with open(pathEvaluatorConfig) as ipf:
        evaluatorConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    pathLocalPathConfig = Path.cwd().parent / 'config' / 'localPathConfig.yaml'
    with open(pathLocalPathConfig) as ipf:
        localPathConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    os.chdir(localPathConfig['pathAbsolute']['vencoPyRoot'])

    vpData = DataParser(parseConfig=parseConfig, globalConfig=globalConfig, localPathConfig=localPathConfig,
                        datasetID=datasetID, loadEncrypted=False)
    vpFlexEst = FlexEstimator(flexConfig=flexConfig, globalConfig=globalConfig, evaluatorConfig=evaluatorConfig,
                                ParseData=vpData, datasetID=datasetID)
    vpFlexEst.run()
    vpEval = Evaluator(globalConfig=globalConfig, evaluatorConfig=evaluatorConfig,
                       parseData=pd.Series(data=vpData, index=[datasetID]))
    vpEval.plotProfiles(flexEstimator=vpFlexEst)


    print(f'Total absolute electricity charged in uncontrolled charging based on MiD08: '
          f'{vpFlexEst.chargeProfilesUncontrolled.sum().sum()} based on MiD17')