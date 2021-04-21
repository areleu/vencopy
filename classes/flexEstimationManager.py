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
import matplotlib.pyplot as plt
import seaborn as sns
from scripts.libProfileCalculation import *
from classes.parseManager import DataParser
from scripts.globalFunctions import createFileString, mergeVariables


class FlexEstimator:
    def __init__(self, config, ParseData: DataParser, datasetID: str= 'MiD17'):
        self.config = config
        self.hourVec = range(config['numberOfHours'])
        self.dataset = datasetID
        self.linkDict, self.scalars, \
            self.driveProfilesIn, self.plugProfilesIn = self.readVencoInput(config=config, dataset=datasetID)
        self.mergeDataToWeightsAndDays(ParseData)

        self.weights = self.indexWeights(self.driveProfilesIn.loc[:, ['hhPersonID', 'tripStartWeekday', 'tripWeight']])
        self.outputConfig = yaml.load(open(self.linkDict['linkOutputConfig']), Loader=yaml.SafeLoader)
        self.driveProfiles, self.plugProfiles = self.indexDriveAndPlugData(self.driveProfilesIn, self.plugProfilesIn,
                                                                      dropIdxLevel='tripWeight',
                                                                      nHours=config['numberOfHours'])
        self.scalarsProc = self.procScalars(self.driveProfilesIn, self.plugProfilesIn,
                                       self.driveProfiles, self.plugProfiles)
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

    def initializeLinkMgr(self, config, dataset):
        """
        Setup link manager based on a VencoPy config file.

        :param config: Config file initiated by a yaml-loader

        :return: Returns link dictionary with relative links to input data and output folders.
        """
        linkDict = {'linkScalars': pathlib.Path(config['linksRelative']['input']) /
                                   pathlib.Path(config['files']['inputDataScalars']),
                    'linkDriveProfiles': pathlib.Path(config['linksRelative']['input']) /
                                         pathlib.Path(createFileString(config=config, fileKey='inputDataDriveProfiles',
                                                                       dataset=dataset)),
                    'linkPlugProfiles': pathlib.Path(config['linksRelative']['input']) /
                                        pathlib.Path(createFileString(config=config, fileKey='inputDataPlugProfiles',
                                                                      dataset=dataset)),
                    'linkOutputConfig': pathlib.Path(config['linksRelative']['outputConfig']),
                    # 'linkOutputAnnual': pathlib.Path(config['linksRelative']['resultsAnnual']),
                    'linkPlots': pathlib.Path(config['linksRelative']['plots']),
                    'linkOutput': pathlib.Path(config['linksRelative']['dataOutput'])}
        return linkDict

    def readInputScalar(self, filePath):
        """
        Method that gets the path to a venco scalar input file specifying technical assumptions such as battery capacity
        specific energy consumption, usable battery capacity share for load shifting and charge power.

        :param filePath: The relative file path to the input file
        :return: Returns a dataframe with an index column and two value columns. The first value column holds numbers the
            second one holds units.
        """

        #scalarInput = Assumptions
        inputRaw = pd.read_excel(filePath,
                                 header=5,
                                 usecols='A:C',
                                 skiprows=0)
        scalarsOut = inputRaw.set_index('parameter')
        return scalarsOut

    def readInputCSV(self, filePath):
        """
        Reads input and cuts out value columns from a given CSV file.

        :param filePath: Relative file path to CSV file
        :return: Pandas dataframe with raw input from CSV file
        """
        inputRaw = pd.read_csv(filePath, header=0)
        inputData = inputRaw.loc[:, ~inputRaw.columns.str.match('Unnamed')]
        inputData = inputData.convert_dtypes()
        return inputData

    def stringToBoolean(self, df):
        """
        Replaces given strings with python values for true or false.
        FixMe: Foreseen to be more flexible in next release.

        :param df: Dataframe holding strings defining true or false values
        :return: Dataframe holding true and false
        """

        dictBol = {'WAHR': True,
                   'FALSCH': False}
        outBool = df.replace(to_replace=dictBol, value=None)
        return outBool

    def readInputBoolean(self, filePath):
        """
        Wrapper function for reading boolean data from CSV.

        :param filePath: Relative path to CSV file
        :return: Returns a dataframe with boolean values
        """

        inputRaw = self.readInputCSV(filePath)
        inputData = self.stringToBoolean(inputRaw)
        return inputData

    def readVencoInput(self, config, dataset):
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

        linkDict = self.initializeLinkMgr(config, dataset)

        # review: have you considered using the logging module for these kind of outputs?
        print('Reading Venco input scalars, drive profiles and boolean plug profiles')

        scalars = self.readInputScalar(linkDict['linkScalars'])
        driveProfiles_raw = self.readInputCSV(linkDict['linkDriveProfiles'])
        plugProfiles_raw = self.readInputBoolean(linkDict['linkPlugProfiles'])

        print('There are ' + str(len(driveProfiles_raw)) + ' drive profiles and ' +
              str(len(driveProfiles_raw)) + ' plug profiles.')

        return linkDict, scalars, driveProfiles_raw, plugProfiles_raw

    def procScalars(self, driveProfiles_raw, plugProfiles_raw, driveProfiles, plugProfiles):
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

    def findIndexCols(self, data, nHours):
        dataCols = [str(i) for i in range(0, nHours + 1)]
        return data.columns[~data.columns.isin(dataCols)]

    def indexProfile(self, data, nHours):
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

    def indexDriveAndPlugData(self, driveData: pd.DataFrame, plugData: pd.DataFrame, dropIdxLevel: str, nHours: int):
        driveProfiles = self.indexProfile(driveData, nHours)
        plugProfiles = self.indexProfile(plugData, nHours)
        return driveProfiles.droplevel(dropIdxLevel), plugProfiles.droplevel(dropIdxLevel)

    def readInData(self, fileKey: str, dataset: str) -> pd.DataFrame:
        """
        Generic read-in function for mobility datasets. This serves as interface between the daily trip distance
        and purpose calculation and the class Evaluator.

        :param fileKey: List of VencoPy-internal names for the filekeys to read in
        :return: a named pd.Series of all datasets with the given filekey_datasets as identifiers
        """

        return pd.read_csv(pathlib.Path(config['linksRelative']['input']) / createFileString(config=config, fileKey=fileKey,
                                                                                     dataset=dataset),
                           dtype={'hhPersonID': int}, index_col=['hhPersonID', 'tripStartWeekday'])

    def mergeDataToWeightsAndDays(self, ParseData):
        self.driveProfilesIn = mergeVariables(data=self.driveProfilesIn, variableData=ParseData.data,
                                              variables=['tripStartWeekday', 'tripWeight'])
        self.plugProfilesIn = mergeVariables(data=self.plugProfilesIn, variableData=ParseData.data,
                                             variables=['tripStartWeekday', 'tripWeight'])

    def baseProfileCalculation(self):
        self.drainProfiles = calcDrainProfiles(self.driveProfiles, self.scalars)
        self.chargeProfiles = calcChargeProfiles(self.plugProfiles, self.scalars)
        self.chargeMaxProfiles = calcChargeMaxProfiles(self.chargeProfiles, self.drainProfiles, self.scalars,
                                                       self.scalarsProc, nIter=3)
        # self.splitChargeMaxCalc(chargeProfiles=self.chargeProfiles, drainProfiles=self.drainProfiles)
        self.chargeProfilesUncontrolled = calcChargeProfilesUncontrolled(self.chargeMaxProfiles, self.scalarsProc)
        self.auxFuelDemandProfiles = calcDriveProfilesFuelAux(self.chargeMaxProfiles, self.chargeProfilesUncontrolled,
                                                              self.driveProfiles, self.scalars, self.scalarsProc)
        self.chargeMinProfiles = calcChargeMinProfiles(self.chargeProfiles, self.drainProfiles,
                                                       self.auxFuelDemandProfiles, self.scalars, self.scalarsProc,
                                                       nIter=3)
        print(f'Base profile calculation complete for dataset {self.dataset}')

    def filter(self):
        self.randNoPerProfile = createRandNo(driveProfiles=self.driveProfiles)

        self.profileSelectors = calcProfileSelectors(chargeProfiles=self.chargeProfiles,
                                                     consumptionProfiles=self.drainProfiles,
                                                     driveProfiles=self.driveProfiles,
                                                     driveProfilesFuelAux=self.auxFuelDemandProfiles,
                                                     randNos=self.randNoPerProfile, scalars=self.scalars,
                                                     fuelDriveTolerance=1, isBEV=True)

        # Additional fuel consumption is subtracted from the consumption
        self.electricPowerProfiles = calcElectricPowerProfiles(self.drainProfiles, self.auxFuelDemandProfiles,
                                                               self.scalars, self.profileSelectors, self.scalarsProc,
                                                               filterIndex='indexDSM')

        # Profile filtering for flow profiles
        self.plugProfilesCons = filterConsProfiles(self.plugProfiles, self.profileSelectors, critCol='indexCons')
        self.electricPowerProfilesCons = filterConsProfiles(self.electricPowerProfiles, self.profileSelectors,
                                                            critCol='indexCons')
        self.chargeProfilesUncontrolledCons = filterConsProfiles(self.chargeProfilesUncontrolled, self.profileSelectors,
                                                                 critCol='indexCons')
        self.auxFuelDemandProfilesCons = filterConsProfiles(self.auxFuelDemandProfiles, self.profileSelectors,
                                                            critCol='indexCons')

        # Profile filtering for state profiles
        self.profilesSOCMinCons = filterConsProfiles(self.chargeMinProfiles, self.profileSelectors, critCol='indexDSM')
        self.profilesSOCMaxCons = filterConsProfiles(self.chargeMaxProfiles, self.profileSelectors, critCol='indexDSM')

    def aggregate(self):
        # Profile aggregation for flow profiles by averaging
        self.plugProfilesAgg = aggregateProfilesMean(self.plugProfilesCons)
        self.electricPowerProfilesAgg = aggregateProfilesMean(self.electricPowerProfilesCons)
        self.chargeProfilesUncontrolledAgg = aggregateProfilesMean(self.chargeProfilesUncontrolledCons)
        self.auxFuelDemandProfilesAgg = aggregateProfilesMean(self.auxFuelDemandProfilesCons)

        # Profile aggregation for flow profiles by averaging
        self.plugProfilesWAgg = aggregateProfilesWeight(self.plugProfilesCons, self.weights)
        self.electricPowerProfilesWAgg = aggregateProfilesWeight(self.electricPowerProfilesCons, self.weights)
        self.chargeProfilesUncontrolledWAgg = aggregateProfilesWeight(self.chargeProfilesUncontrolledCons, self.weights)
        self.auxFuelDemandProfilesWAgg = aggregateProfilesWeight(self.auxFuelDemandProfilesCons, self.weights)

        # Define a partial method for variable specific weight-considering aggregation
        aggDiffVar = functools.partial(aggregateDiffVariable, by='tripStartWeekday', weights=self.weights,
                                       hourVec=self.hourVec)

        # Profile aggregation for flow profiles by averaging
        self.plugProfilesWAggVar = aggDiffVar(data=self.plugProfilesCons)
        self.electricPowerProfilesWAggVar = aggDiffVar(data=self.electricPowerProfilesCons)
        self.chargeProfilesUncontrolledWAggVar = aggDiffVar(data=self.chargeProfilesUncontrolledCons)
        self.auxFuelDemandProfilesWAggVar = aggDiffVar(data=self.auxFuelDemandProfilesCons)

        # Profile aggregation for state profiles by selecting one profiles value for each hour
        self.SOCMin, self.SOCMax = socProfileSelection(self.profilesSOCMinCons, self.profilesSOCMaxCons,
                                                       filter='singleValue', alpha=10)

        self.SOCMinVar, self.SOCMaxVar = self.socSelectionVar(dataMin=self.profilesSOCMinCons,
                                                              dataMax=self.profilesSOCMaxCons,
                                                              by='tripStartWeekday', filter='singleValue', alpha=10)

    def socSelectionVar(self, dataMin, dataMax, by, filter, alpha):
        socSelectionPartial = functools.partial(socProfileSelection, filter=filter, alpha=alpha)
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

    def correct(self):
        self.chargeProfilesUncontrolledCorr = correctProfiles(self.scalars, self.chargeProfilesUncontrolledAgg,
                                                              'electric')
        self.electricPowerProfilesCorr = correctProfiles(self.scalars, self.electricPowerProfilesAgg, 'electric')
        self.auxFuelDemandProfilesCorr = correctProfiles(self.scalars, self.auxFuelDemandProfilesAgg, 'fuel')

    def normalize(self):
        # Profile normalization for state profiles with the basis battery capacity
        self.socMinNorm, self.socMaxNorm = normalizeProfiles(self.scalars, self.SOCMin, self.SOCMax,
                                                             normReferenceParam='Battery capacity')

    def writeProfilesToCSV(self, profileDictOut, config, singleFile=True, dataset='MiD17'):
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
            dataOut.to_csv(pathlib.Path(config['linksRelative']['dataOutput']) /
                           createFileString(config=config, fileKey='vencoPyOutput', dataset=dataset), header=True)
        else:
            for iName, iProf in profileDictOut.items():
                iProf.to_csv(pathlib.Path(config['linksRelative']['dataOutput']) /
                             pathlib.Path(r'vencoPyOutput_' + iName + dataset + '.csv'), header=True)

    def writeOut(self):
        self.profileDictOut = dict(uncontrolledCharging=self.chargeProfilesUncontrolledCorr,
                                   electricityDemandDriving=self.electricPowerProfilesCorr,
                                   SOCMax=self.socMaxNorm, SOCMin=self.socMinNorm,
                                   gridConnectionShare=self.plugProfilesAgg,
                                   auxFuelDriveProfile=self.auxFuelDemandProfilesCorr)

        self.writeProfilesToCSV(profileDictOut=self.profileDictOut, config=self.config, singleFile=True,
                           dataset=self.dataset)

    def sortData(data):
        data.index = data.index.swaplevel(0, 1)
        return data.sort_index()

    def linePlot(self, profileDict, linkOutput, config, show=True, write=True, ylabel='Normalized profiles', ylim=None,
                 filename=''):
        plt.rcParams.update(config['plotConfig']['plotRCParameters'])  # set plot layout
        fig, ax = plt.subplots()
        plt.tick_params(labelsize=config['plotConfig']['plotRCParameters']['font.size'])
        for iKey, iVal in profileDict.items():
            if isinstance(iVal.index, pd.MultiIndex):
                iVal = self.sortData(iVal)
                sns.lineplot(range(iVal.index.size), iVal, label=iKey, sort=False)
            else:
                sns.lineplot(iVal.index, iVal, label=iKey, sort=False)
        xRange = range(0, len(profileDict[list(profileDict)[0]]) + 1, config['plotConfig']['xAxis']['xTickSteps'])
        # xLabels = [f'{iDay}\n{str(iTime)}:00' for iDay in config['plotConfig']['xAxis']['weekdays'] for iTime in config['plotConfig']['xAxis']['hours']]
        xLabels = [f'{str(iTime)}:00' for iTime in config['plotConfig']['xAxis']['hours']]
        ax.set_xticks(xRange)
        ax.set_xticklabels(xLabels, fontsize=config['plotConfig']['xAxis']['ticklabelsize'])
        ax.set_ylim(bottom=0, top=ylim)
        ax.set_xlabel('Hour', fontsize=config['plotConfig']['plotRCParameters']['axes.labelsize'])
        ax.set_ylabel(ylabel, fontsize=config['plotConfig']['plotRCParameters']['axes.labelsize'])
        plt.legend(loc='upper center')  # , bbox_to_anchor=(0.5, 1.3) ncol=2,
        plt.tight_layout()
        filePlot = linkOutput / pathlib.Path(
            createFileString(config=config, fileKey='flexPlotName', manualLabel=filename,
                             filetypeStr='svg'))
        if show:
            plt.show()
        if write:
            fig.savefig(filePlot)

    def separateLinePlots(self,profileDictList, config, dataset='MiD17', show=True, write=True, ylabel=[], ylim=[],
                          filenames=[]):
        for iDict, iYLabel, iYLim, iName in zip(profileDictList, ylabel, ylim, filenames):
            self.writeProfilesToCSV(profileDictOut=iDict, config=config, singleFile=False, dataset=dataset)
            self.linePlot(iDict, linkOutput=config['linksRelative']['plots'], config=config, show=show, write=write,
                     ylabel=iYLabel, ylim=iYLim, filename=iName)


    def plotProfiles(self):
        self.linePlot(self.profileDictOut, linkOutput=self.linkDict['linkPlots'], config=self.config,
                 show=True, write=True, filename='allPlots' + self.dataset)

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

        self.separateLinePlots(profileDictList, self.config,
                          show=True, write=True,
                          ylabel=['Average EV connection share', 'Average EV flow in kW', 'Average EV SOC in kWh'],
                          filenames=[self.dataset + '_connection', self.dataset + '_flows', self.dataset + '_state'],
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
                          show=self.config['plotConfig']['show'], write=self.config['plotConfig']['save'],
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
        keys = [self.dataset, compareTo.dataset]
        for iProf in profileNameList:
            iDict = self.compileProfileComparisonDict(keys=keys,
                                                      values=[getattr(self, iProf), getattr(compareTo, iProf)])
            ret.append(iDict)
        return ret

    @staticmethod
    def compileProfileComparisonDict(keys: list, values: list):
        return {iKey: iVal for iKey, iVal in zip(keys, values)}



def runFlexEstimation(config, ParseData : DataParser, dataset : str = 'MiD17'):
    Flexstimator = FlexEstimator(config=config, datasetID=dataset, ParseData=ParseData)
    Flexstimator.baseProfileCalculation()
    Flexstimator.filter()
    Flexstimator.aggregate()
    Flexstimator.correct()
    Flexstimator.normalize()
    Flexstimator.writeOut()
    # Flexstimator.plotProfiles()
    return Flexstimator

def runFlexstimation(config, dataset, variable):
    #indexedDriveData = mergeVariables(data=driveDataDays, variableData=tripDataClean, variables=['tripStartWeekday',
    #                                                                                             'tripWeight'])
    #indexedPurposeData = mergeVariables(data=purposeDataDays, variableData=tripDataClean, variables=['tripStartWeekday',
    #                                                                                                 'tripWeight'])
    Flexstimator = FlexEstimator(config=config, datasetID=dataset)
    Flexstimator.baseProfileCalculation()
    Flexstimator.filter()
    Flexstimator.aggregate()
    Flexstimator.correct()
    Flexstimator.normalize()
    Flexstimator.writeOut()

if __name__ == '__main__':
    linkConfig = pathlib.Path.cwd().parent / 'config' / 'config.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    config = yaml.load(open(linkConfig), Loader=yaml.SafeLoader)
    os.chdir(config['linksAbsolute']['vencoPyRoot'])
    vpData = DataParser(config=config, loadEncrypted=False)
    vpFlexEst17 = runFlexEstimation(config=config, ParseData=vpData)

    print(f'Total absolute electricity charged in uncontrolled charging based on MiD08: '
          f'{vpFlexEst17.chargeProfilesUncontrolled.sum().sum()} based on MiD17')

    print('This is the end')
