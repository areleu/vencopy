__version__ = '0.0.8'
__maintainer__ = 'Niklas Wulff'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '03.11.2019'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'


#----- imports & packages ------
import pathlib
import functools
from scripts.libInput import *
from scripts.libPreprocessing import *
from scripts.libProfileCalculation import *
from scripts.libOutput import *
# from scripts.libPlotting import *
from scripts.libLogging import logger
from profilehooks import profile
from classes.parseManager import DataParser

class FlexEstimator:
    def __init__(self, config, ParseData: DataParser, datasetID: str= 'MiD17'):
        self.config = config
        self.hourVec = range(config['numberOfHours'])
        self.dataset = datasetID
        self.linkDict, self.scalars, \
            self.driveProfilesIn, self.plugProfilesIn = readVencoInput(config=config, dataset=datasetID)
        self.mergeDataToWeightsAndDays(ParseData)

        self.weights = indexWeights(self.driveProfilesIn.loc[:, ['hhPersonID', 'tripStartWeekday', 'tripWeight']])
        self.outputConfig = yaml.load(open(self.linkDict['linkOutputConfig']), Loader=yaml.SafeLoader)
        self.driveProfiles, self.plugProfiles = indexDriveAndPlugData(self.driveProfilesIn, self.plugProfilesIn,
                                                                      dropIdxLevel='tripWeight',
                                                                      nHours=config['numberOfHours'])
        self.scalarsProc = procScalars(self.driveProfilesIn, self.plugProfilesIn,
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

    def readInData(self, fileKey: str, dataset: str) -> pd.DataFrame:
        """
        Generic read-in function for mobility datasets. This serves as interface between the daily trip distance
        and purpose calculation and the class Evaluator.

        :param fileKey: List of VencoPy-internal names for the filekeys to read in
        :return: a named pd.Series of all datasets with the given filekey_datasets as identifiers
        """

        return pd.read_csv(Path(config['linksRelative']['input']) / createFileString(config=config, fileKey=fileKey,
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

    def writeOut(self):
        self.profileDictOut = dict(uncontrolledCharging=self.chargeProfilesUncontrolledCorr,
                                   electricityDemandDriving=self.electricPowerProfilesCorr,
                                   SOCMax=self.socMaxNorm, SOCMin=self.socMinNorm,
                                   gridConnectionShare=self.plugProfilesAgg,
                                   auxFuelDriveProfile=self.auxFuelDemandProfilesCorr)

        writeProfilesToCSV(profileDictOut=self.profileDictOut, config=self.config, singleFile=True,
                           dataset=self.dataset)

    def plotProfiles(self):
        linePlot(self.profileDictOut, linkOutput=self.linkDict['linkPlots'], config=self.config,
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

        separateLinePlots(profileDictList, self.config,
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

        separateLinePlots(profileDictList, self.config,
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



# @profile(immediate=True)
# def vencoRun(config, dataset='MiD17'):
#     #----- data and config read-in -----
#     linkDict, scalars, driveProfilesRaw, plugProfilesRaw = readVencoInput(config, dataset)
#     outputConfig = yaml.load(open(linkDict['linkOutputConfig']), Loader=yaml.SafeLoader)
#     # indices = ['VEHICLE', 'Day', 'Weight']  # ['HP_ID_Reg', 'ST_WOTAG_str'] ['CASEID', 'PKWID']
#     driveProfiles, plugProfiles = indexDriveAndPlugData(driveProfilesRaw, plugProfilesRaw, config['numberOfHours'])
#     scalarsProc = procScalars(driveProfilesRaw, plugProfilesRaw, driveProfiles, plugProfiles)
#
#     # driveProfiles = driveProfiles.query("ST_WOTAG_str == 'SAT'")
#     # plugProfiles = plugProfiles.query("ST_WOTAG_str == 'SAT'")
#
#     consumptionProfiles = calcDrainProfiles(driveProfiles, scalars)
#
#     chargeProfiles = calcChargeProfiles(plugProfiles, scalars)
#
#     chargeMaxProfiles = calcChargeMaxProfiles(chargeProfiles,
#                                               consumptionProfiles,
#                                               scalars,
#                                               scalarsProc,
#                                               nIter=7)
#
#     chargeProfilesUncontrolled = calcChargeProfilesUncontrolled(chargeMaxProfiles,
#                                                                 scalarsProc)
#
#     driveProfilesFuelAux = calcDriveProfilesFuelAux(chargeMaxProfiles,
#                                                     chargeProfilesUncontrolled,
#                                                     driveProfiles,
#                                                     scalars,
#                                                     scalarsProc)
#
#     chargeMinProfiles = calcChargeMinProfiles(chargeProfiles,
#                                               consumptionProfiles,
#                                               driveProfilesFuelAux,
#                                               scalars,
#                                               scalarsProc,
#                                               nIter=3)
#
#     randNoPerProfile = createRandNo(driveProfiles)
#
#     profileSelectors = calcProfileSelectors(chargeProfiles,
#                                             consumptionProfiles,
#                                             driveProfiles,
#                                             driveProfilesFuelAux,
#                                             randNoPerProfile,
#                                             scalars,
#                                             fuelDriveTolerance=1,
#                                             isBEV=True)
#
#     # Additional fuel consumption is subtracted from the consumption
#     electricPowerProfiles = calcElectricPowerProfiles(consumptionProfiles,
#                                                       driveProfilesFuelAux,
#                                                       scalars,
#                                                       profileSelectors,
#                                                       scalarsProc,
#                                                       filterIndex='indexDSM')
#
#     # Profile filtering for flow profiles
#     plugProfilesCons = filterConsProfiles(plugProfiles, profileSelectors, critCol='indexCons')
#     electricPowerProfilesCons = filterConsProfiles(electricPowerProfiles, profileSelectors, critCol='indexCons')
#     chargeProfilesUncontrolledCons = filterConsProfiles(chargeProfilesUncontrolled, profileSelectors,
#                                                         critCol='indexCons')
#     driveProfilesFuelAuxCons = filterConsProfiles(driveProfilesFuelAux, profileSelectors, critCol='indexCons')
#
#     # Profile filtering for state profiles
#     profilesSOCMinCons = filterConsProfiles(chargeMinProfiles, profileSelectors, critCol='indexDSM')
#     profilesSOCMaxCons = filterConsProfiles(chargeMaxProfiles, profileSelectors, critCol='indexDSM')
#
#     # Profile aggregation for flow profiles by averaging
#     plugProfilesAgg = aggregateProfilesMean(plugProfilesCons)
#     electricPowerProfilesAgg = aggregateProfilesMean(electricPowerProfilesCons)
#     chargeProfilesUncontrolledAgg = aggregateProfilesMean(chargeProfilesUncontrolledCons)
#     driveProfilesFuelAuxAgg = aggregateProfilesMean(driveProfilesFuelAuxCons)
#
#     # Profile aggregation for state profiles by selecting one profiles value for each hour
#     SOCMin, SOCMax = socProfileSelection(profilesSOCMinCons, profilesSOCMaxCons,
#                                          filter='singleValue', alpha=1)
#
#     # Profile correction for flow profiles
#     chargeProfilesUncontrolledCorr = correctProfiles(scalars, chargeProfilesUncontrolledAgg, 'electric')
#     electricPowerProfilesCorr = correctProfiles(scalars, electricPowerProfilesAgg, 'electric')
#     driveProfilesFuelAuxCorr = correctProfiles(scalars, driveProfilesFuelAuxAgg, 'fuel')
#
#     # Profile normalization for state profiles with the basis battery capacity
#     socMinNorm, socMaxNorm = normalizeProfiles(scalars, SOCMin, SOCMax,
#                                                normReferenceParam='Battery capacity')
#
#     # Result output
#     profileDictOut = dict(uncontrolledCharging=chargeProfilesUncontrolledCorr,
#                           electricityDemandDriving=electricPowerProfilesCorr, SOCMax=socMaxNorm, SOCMin=socMinNorm,
#                           gridConnectionShare=plugProfilesAgg, auxFuelDriveProfile=driveProfilesFuelAuxCorr)
#
#     writeProfilesToCSV(profileDictOut=profileDictOut,
#                        config=config,
#                        singleFile=True,
#                        dataset=dataset)
#
#     # writeAnnualOutputForREMix(profileDictOut, outputConfig, linkDict['linkOutputAnnual'],
#     #                         config['postprocessing']['hoursClone'], config['labels']['technologyLabel'],
#     #                         strAdd='_MR1_alpha1_batCap40_cons15')
#
#     linePlot(profileDictOut, linkOutput=linkDict['linkPlots'], config=config,
#              show=True, write=True, filename='allPlots' + dataset)
#
#     # Separately plot flow and state profiles
#     profileDictConnectionShare = dict(gridConnectionShare=plugProfilesAgg)
#
#     profileDictFlowsNorm = dict(uncontrolledCharging=chargeProfilesUncontrolledCorr,
#                           electricityDemandDriving=electricPowerProfilesCorr, gridConnectionShare=plugProfilesAgg)
#     profileDictFlowsAbs = dict(uncontrolledCharging=chargeProfilesUncontrolledAgg,
#                                 electricityDemandDriving=electricPowerProfilesAgg)
#
#     profileDictStateNorm = dict(SOCMax=socMaxNorm, SOCMin=socMinNorm)
#     profileDictStateAbs = dict(SOCMax=SOCMax, SOCMin=SOCMin)
#
#     profileDictList = [profileDictConnectionShare, profileDictFlowsAbs, profileDictStateAbs]
#
#     separateLinePlots(profileDictList, config,
#                         show=True, write=True,
#                         ylabel=['Average EV connection share', 'Average EV flow in kW', 'Average EV SOC in kWh'],
#                         filenames=[dataset + '_connection', dataset + '_flows', dataset + '_state'],
#                         ylim=[1, 0.9, 50])

def runFlexEstimation(config, dataset):
    Flexstimator = FlexEstimator(config=config, datasetID=dataset)
    Flexstimator.baseProfileCalculation()
    Flexstimator.filter()
    Flexstimator.aggregate()
    Flexstimator.correct()
    Flexstimator.normalize()
    Flexstimator.writeOut()
    # Flexstimator.plotProfiles()
    return Flexstimator

def runFlexstimation(config, dataset, variable):
    indexedDriveData = mergeVariables(data=driveDataDays, variableData=tripDataClean, variables=['tripStartWeekday',
                                                                                                 'tripWeight'])
    indexedPurposeData = mergeVariables(data=purposeDataDays, variableData=tripDataClean, variables=['tripStartWeekday',
                                                                                                     'tripWeight'])
    Flexstimator = FlexEstimator(config=config, datasetID=dataset)
    Flexstimator.baseProfileCalculation()
    Flexstimator.filter()
    Flexstimator.aggregate()
    Flexstimator.correct()
    Flexstimator.normalize()
    Flexstimator.writeOut()

if __name__ == '__main__':
    linkConfig = pathlib.Path.cwd() / 'config' / 'config.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    config = yaml.load(open(linkConfig), Loader=yaml.SafeLoader)
    # vencoRun(config=config, dataset='MiD08')
    vpFlexEst08 = runFlexEstimation(config=config, dataset='MiD08')
    vpFlexEst17 = runFlexEstimation(config=config, dataset='MiD17')

    print(f'Total absolute electricity charged in uncontrolled charging based on MiD08: '
          f'{vpFlexEst08.chargeProfilesUncontrolled.sum().sum()} and'
          f'{vpFlexEst17.chargeProfilesUncontrolled.sum().sum()} based on MiD17')

    vpFlexEst08.compareProfiles(compareTo=vpFlexEst17)

    print('This is the end')
