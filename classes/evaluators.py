__version__ = '0.0.9'
__maintainer__ = 'Niklas Wulff'
__contributors__ = 'Fabia Miorelli, Parth Butte'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '21.09.2020'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'

import yaml
import os
import pathlib
import pandas as pd
import seaborn as sns
from pathlib import Path
import matplotlib.pyplot as plt
from scripts.globalFunctions import createFileString, calculateWeightedAverage, mergeDataToWeightsAndDays
from classes.flexEstimators import FlexEstimator
from classes.dataParsers import DataParser

class Evaluator:
    def __init__(self, globalConfig:dict, evaluatorConfig: dict, label, parseData: pd.Series, weightPlot=True):
        self.globalConfig = globalConfig
        self.evaluatorConfig = evaluatorConfig
        self.label = label
        self.weightPlot = weightPlot
        self.normPlotting = True
        self.dailyMileageGermany2008 = 3.080e9  # pkm/d
        self.dailyMileageGermany2017 = 3.214e9  # pkm/d
        self.hourVec = [str(i) for i in range(0, globalConfig['numberOfHours'])]
        self.datasetIDs = list(parseData.index)
        self.parseData = parseData
        self.inputDataRaw = self.readInData(['inputDataDriveProfiles'], self.datasetIDs)
        self.inputData = pd.Series(dtype=object)
        self.mergeDaysAndWeights()
        self.data = pd.Series(dtype=object)
        self.reindexData()
        self.dataStacked = None
        self.aggregateIDDict = self.setupAggDict()
        self.hourlyAggregates = self.aggregateAcrossTrips()
        # self.hourlyAggregates = self.sortData(self.hourlyAggregates)
        print('Evaluator initialization complete')

    def readInData(self, fileKeys: list, datasets: list) -> pd.Series:
        """
        Generic read-in function for mobility datasetIDs. This serves as interface between the daily trip distance
        and purpose calculation and the class Evaluator.

        :param fileKeys: List of VencoPy-internal names for the filekeys to read in
        :param datasets: list of strings declaring the datasetIDs to be read in
        :return: a named pd.Series of all datasetIDs with the given filekey_datasets as identifiers
        """

        ret = pd.Series(dtype=object)
        for iFileKey in fileKeys:
            for iDat in datasets:
                dataIn = pd.read_csv(pathlib.Path(self.globalConfig['pathRelative']['input']) /
                                     createFileString(globalConfig=self.globalConfig, fileKey=iFileKey,
                                                      datasetID=iDat), dtype={'hhPersonID': int},
                                     # index_col=['hhPersonID', 'tripStartWeekday'])
                                     index_col=['hhPersonID'])
                ret[iDat] = dataIn
        return ret

    def mergeDaysAndWeights(self):
        for iDat in self.datasetIDs:
            self.inputData[iDat] = mergeDataToWeightsAndDays(self.inputDataRaw[iDat], self.parseData[iDat])
            self.inputData[iDat].dropna()

    def assignWeight(self, datasetIDs: list):
        ret = pd.Series(dtype=object)
        for iDat in datasetIDs:
            weightData = mergeDataToWeightsAndDays(self.inputData[iDat], self.parseData[iDat])
            weights = weightData.loc[:, ['hhPersonID', 'tripStartWeekday', 'tripWeight']]
            weights = weights.convert_dtypes()
            ret[iDat] = weights.set_index(['hhPersonID'], drop=True)
        return ret

    def calculateMobilityQuota(self, dataset: str) -> None:
        """
        Calculates the number of survey days where mobiity occured.

        :param dataset: name of dataset
        :return: Scalar, the ratio of mobile days to total days
        """
        dataKey = f'inputDataDriveProfiles_{dataset}'
        if not dataKey in self.inputData.keys():
            assert 'Specified dataset was not read in during Evaluator initialization'
        dat = self.inputData[dataKey]
        isNoTrip = dat == 0
        isNoTrip.apply(any, axis=1)

    def reindexData(self):
        for iDat in self.datasetIDs:
            self.data[iDat] = self.inputData[iDat].set_index(['hhPersonID', 'tripStartWeekday'], drop=True)
            self.data[iDat].dropna(inplace=True)

    # Maybe not needed at all?
    def stackData(self):
        ret = {}
        for idx, iDat in self.inputData.items():
            iDatRaw = iDat.drop(columns=['hhPersonID']).set_index('tripStartWeekday',
                                                                              append=True).stack()  # 'tripWeight', 'tripScaleFactor'
            iDat = iDatRaw.reset_index([1, 2])
            iDat.columns = ['Day', 'Hour', 'Value']
            ret[idx] = iDat
        return ret

    def setupAggDict(self):
        IDDict = {'sum': None, 'mean': None, 'wMean': None}
        IDDict['sum'] = [f'{iDatID}_sum' for iDatID in self.datasetIDs]
        IDDict['mean'] = [f'{iDatID}_mean' for iDatID in self.datasetIDs]
        IDDict['wMean'] = [f'{iDatID}_wMean' for iDatID in self.datasetIDs]
        return IDDict

    def aggregateAcrossTrips(self):
        ret = pd.DataFrame()
        for iDatID, iDat in self.inputData.items():
            ret.loc[:, f'{iDatID}_sum'] = iDat.loc[:, self.hourVec].sum(axis=0)
            ret.loc[:, f'{iDatID}_mean'] = iDat.loc[:, self.hourVec].mean(axis=0)
            if self.weightPlot:
                ret.loc[:, f'{iDatID}_wMean'] = iDat.loc[:, self.hourVec].apply(calculateWeightedAverage,
                                                                                args=[iDat.loc[:, 'tripWeight']])
        return ret

    def calcVariableSpecAggregates(self, by):
        ret = pd.DataFrame()
        for iDatID, iDat in self.data.items():
            if not all([iBy in iDat.index.names for iBy in by]):
                raise Exception('At least one required variable name is not in index names. Aborting')
            ret.loc[:, f'{iDatID}_sum'] = iDat.loc[:, self.hourVec].groupby(level=by).sum().stack()  # needed .stack() here?
            ret.loc[:, f'{iDatID}_mean'] = iDat.loc[:, self.hourVec].groupby(level=by).mean().stack()
            if self.weightPlot:
                # ret.loc[:, f'{iDatID}_wMean'] = iDat.loc[:,
                #                                 self.hourVec].groupby(level=by).apply(calculateWeightedAverage,
                #                                                     weightCol=[iDat.loc[:, 'tripWeight']]).stack()
                ret.loc[:, f'{iDatID}_wMean'] = self.calcWeightedTripValues(dataIn=iDat, idxLvl=by[0])
        return ret

    def calcWeightedTripValues(self, dataIn, idxLvl):
        # Option 3: Looping :(
        vars = set(dataIn.index.get_level_values(idxLvl))
        ret = pd.DataFrame(index=self.hourVec, columns=vars)
        data = dataIn.loc[:, self.hourVec].reset_index(level=idxLvl)
        weights = dataIn.loc[:, 'tripWeight'].reset_index(level=idxLvl)
        for iVar in vars:
            dataSlice = data.loc[data.loc[:, idxLvl] == iVar, self.hourVec]
            weightSlice = weights.loc[data.loc[:, idxLvl] == iVar, 'tripWeight']
            ret.loc[:, iVar] = dataSlice.apply(calculateWeightedAverage, weightCol=weightSlice)
        ret = ret.stack()
        ret.index = ret.index.swaplevel(0, 1)
        return ret

    def sortData(self, data):
        """
        Method used for plotting to order index values

        :param data: Pandas Dataframe with two indices
        """
        data.index = data.index.swaplevel(0, 1)
        return data.sort_index()

    def plotAggregates(self):
        self.hourlyAggregates = self.hourlyAggregates.swaplevel(0, 1)
        # Plotting aggregates
        fig, ax = plt.subplots(2, 1)
        plt.tick_params(labelsize=self.evaluatorConfig['plotConfig']['plotRCParameters']['font.size'])
        meanCols = self.aggregateIDDict['mean']
        meanCols.extend(self.aggregateIDDict['wMean'])
        self.hourlyAggregates.loc[:, self.aggregateIDDict['sum']].plot.line(ax=ax[0])
        self.hourlyAggregates.loc[:, meanCols].plot.line(ax=ax[1])
        xRange = range(0, len(self.hourlyAggregates) + 1, self.evaluatorConfig['plotConfig']['xAxis']['xTickSteps'])
        xLabels = [f'{iDay}\n{str(iTime)}:00' for iDay in self.evaluatorConfig['plotConfig']['xAxis']['weekdays']
                   for iTime in self.evaluatorConfig['plotConfig']['xAxis']['hours']]
        # xLabels = [f'{str(iTime)}:00' for iTime in self.evaluatorConfig['plotConfig']['xAxis']['hours']]
        ax[1].set_xticks(xRange)
        ax[1].set_xticklabels(xLabels, fontsize=self.evaluatorConfig['plotConfig']['xAxis']['ticklabelsize'])
        ax[0].ticklabel_format(axis='y', style='sci', scilimits=(0, 0), useMathText=True)
        ax[0].set_xticks(xRange)
        ax[0].set_xticklabels('')
        ax[0].set_xlabel('')
        ax[0].set_ylabel('Sum of all trips \n in sample in km',
                         fontsize=self.evaluatorConfig['plotConfig']['yAxis']['ticklabelsize'])
        ax[1].set_xlabel("Hour", fontsize=self.evaluatorConfig['plotConfig']['plotRCParameters']['axes.labelsize'])
        ax[1].set_ylabel("Average of hourly \n trips in km",
                         fontsize=self.evaluatorConfig['plotConfig']['plotRCParameters']['axes.labelsize'])
        ax[0].tick_params(axis='y', labelsize=self.evaluatorConfig['plotConfig']['yAxis']['ticklabelsize'])
        ax[0].legend()
        ax[1].legend()

        if self.evaluatorConfig['plotConfig']['show']:
            plt.show()
        if self.evaluatorConfig['plotConfig']['save']:
            fileName = createFileString(globalConfig=self.globalConfig, fileKey='aggPlotName', manualLabel=self.label,
                                        filetypeStr='svg')
            fig.savefig(Path(self.globalConfig['pathRelative']['plots']) / fileName, bbox_inches='tight')

    def linePlot(self, profileDict, pathOutput, show=True, write=True, ylabel='Normalized profiles', ylim=None,
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
        filePlot = pathOutput / Path(
            createFileString(globalConfig=self.globalConfig, datasetID=self.datasetID, fileKey='flexPlotName', manualLabel=filename,
                             filetypeStr='svg'))
        if show:
            plt.show()
        if write:
            fig.savefig(filePlot)

    def separateLinePlots(self, profileDictList, datasetID='MiD17', show=True, write=True,
                          ylabel=[], ylim=[], filenames=[]):
        for iDict, iYLabel, iYLim, iName in zip(profileDictList, ylabel, ylim, filenames):
            self.writeProfilesToCSV(profileDictOut=iDict, singleFile=False, datasetID=datasetID)
            self.linePlot(iDict, pathOutput=Path(self.globalConfig['pathRelative']['plots']), show=show,
                          write=write, ylabel=iYLabel, ylim=iYLim, filename=iName)

    def plotProfiles(self):
        self.linePlot(self.profileDictOut, pathOutput=Path(self.globalConfig['pathRelative']['plots']),
                      show=True, write=True, filename='allPlots' + self.datasetID)

        # Separately plot flow and state profiles
        profileDictConnectionShare = dict(gridConnectionShare=self.plugProfilesAgg)

        profileDictFlowsNorm = dict(uncontrolledCharging=self.chargeProfilesUncontrolledCorr,
                                    electricityDemandDriving=self.electricPowerProfilesCorr,
                                    gridConnectionShare=self.plugProfilesAgg)
        profileDictFlowsAbs = dict(uncontrolledCharging=self.chargeProfilesUncontrolledAgg,
                                   electricityDemandDriving=self.electricPowerProfilesAgg)

        profileDictStateNorm = dict(socMax=self.socMaxNorm, socMin=self.socMinNorm)
        profileDictStateAbs = dict(socMax=self.socMax, socMin=self.socMin)

        profileDictList = [profileDictConnectionShare, profileDictFlowsAbs, profileDictStateAbs]

        self.separateLinePlots(profileDictList, show=True, write=True,
                               ylabel=['Average EV connection share', 'Average EV flow in kW', 'Average EV SOC in kWh'],
                               filenames=[self.datasetID + '_connection', self.datasetID + '_flows',
                                          self.datasetID + '_state'],
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

        self.separateLinePlots(profileDictList, show=self.evaluatorConfig['plotConfig']['show'],
                               write=self.evaluatorConfig['plotConfig']['save'],
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

    def compileProfileComparisonDict(keys: list, values: list):
        return {iKey: iVal for iKey, iVal in zip(keys, values)}

if __name__ == '__main__':
    pathGlobalConfig = Path.cwd().parent / 'config' / 'globalConfig.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    with open(pathGlobalConfig) as ipf:
        globalConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
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
    parseDataAll = pd.Series(dtype=object)
    parseDataAll['MiD08'] = DataParser(datasetID='MiD08', parseConfig=parseConfig, globalConfig=globalConfig,
                          localPathConfig=localPathConfig, loadEncrypted=False)
    parseDataAll['MiD17'] = DataParser(datasetID='MiD17', parseConfig=parseConfig, globalConfig=globalConfig,
                          localPathConfig=localPathConfig, loadEncrypted=False)

    vpEval = Evaluator(globalConfig=globalConfig, evaluatorConfig=evaluatorConfig, label='base', parseData=parseDataAll)
    vpEval.hourlyAggregates = vpEval.calcVariableSpecAggregates(by=['tripStartWeekday'])
    vpEval.plotAggregates()

    # vpEval.data = mergeVariables(data=vpEval.inputData['MiD17'].reset_index(),
    #                              variableData=p.data, variables=['tripWeight'])

    print('this is the end')