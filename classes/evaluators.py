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
from scripts.globalFunctions import createFileString, calculateWeightedAverage, mergeDataToWeightsAndDays, \
    writeProfilesToCSV
# from classes.flexEstimators import FlexEstimator
from classes.dataParsers import DataParser


class Evaluator:
    def __init__(self, globalConfig:dict, evaluatorConfig: dict, label: str, parseData: pd.Series=None,
                 weightPlot=True):
        """
        CURRENTLY IN SEMI-PROUDCTION MODE. Some interfaces may only apply to specific cases.
        Overall evaluation class for assessing vencopy mobility and charging profiles.

        :param globalConfig: global config instances for paths
        :param evaluatorConfig: evaluator config mainly for plot properties
        :param label: String for saving plots to hard disk
        :param parseData: Series with instances of VencoPy class ParseData and keys specifying the name of the
            respective class
        :param weightPlot: If True, profiles are weighted for plotting
        """

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
        """
        Method to merge mobility data to respective trip weights and days.

        :return: None
        """
        for iDat in self.datasetIDs:
            self.inputData[iDat] = mergeDataToWeightsAndDays(self.inputDataRaw[iDat], self.parseData[iDat])
            self.inputData[iDat].dropna()

    # DEPRECATED WILL BE DELETED ON NEXT RELEASE
    def assignWeight(self, datasetIDs: list):
        """
        Reformatting function

        :param datasetIDs:
        :return:
        """

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
        """
        Formatting function to set index of profiles with weekday and weight, in preparation for plotting

        :return: None
        """

        for iDat in self.datasetIDs:
            self.data[iDat] = self.inputData[iDat].set_index(['hhPersonID', 'tripStartWeekday'], drop=True)
            self.data[iDat].dropna(inplace=True)

    # Maybe not needed at all?
    def stackData(self):
        """
        Function to rearrange and rename data for plotting

        :return: None
        """

        ret = {}
        for idx, iDat in self.inputData.items():
            iDatRaw = iDat.drop(columns=['hhPersonID']).set_index('tripStartWeekday',
                                                                              append=True).stack()  # 'tripWeight', 'tripScaleFactor'
            iDat = iDatRaw.reset_index([1, 2])
            iDat.columns = ['Day', 'Hour', 'Value']
            ret[idx] = iDat
        return ret

    def setupAggDict(self):
        """
        Setup lookup-dictionaries for plotting.

        :return: Updated dictionary of data set IDs
        """

        IDDict = {'sum': None, 'mean': None, 'wMean': None}
        IDDict['sum'] = [f'{iDatID}_sum' for iDatID in self.datasetIDs]
        IDDict['mean'] = [f'{iDatID}_mean' for iDatID in self.datasetIDs]
        IDDict['wMean'] = [f'{iDatID}_wMean' for iDatID in self.datasetIDs]
        return IDDict

    def aggregateAcrossTrips(self):
        """
        Aggregate trip distances across all trops for specific hours. Aggregation is carried out threefold: Summation,
        simple average and weighted average if self.weightPlot is True.

        :return: DataFrame with a column for each aggregated time series and a row for each hour in the original data
            set
        """

        ret = pd.DataFrame()
        for iDatID, iDat in self.inputData.items():
            ret.loc[:, f'{iDatID}_sum'] = iDat.loc[:, self.hourVec].sum(axis=0)
            ret.loc[:, f'{iDatID}_mean'] = iDat.loc[:, self.hourVec].mean(axis=0)
            if self.weightPlot:
                ret.loc[:, f'{iDatID}_wMean'] = iDat.loc[:, self.hourVec].apply(calculateWeightedAverage,
                                                                                args=[iDat.loc[:, 'tripWeight']])
        return ret

    def calcVariableSpecAggregates(self, by: list):
        """
        Aggregation method that calculated aggregates such in aggregateAcrossTrips but differentiating one additional
        criterium given in by. The method is used to calculate weekday specific aggregates.

        :param by: List of column names to add differentiation. Currently only tested for tripStartWeekday
        :return: Pandas DataFrame with three columns each one for summed, average and weighted average and a multiindex
            differentiating between hours and one additional variable specificied in by.
        """

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
        """
        Function to calculate weighted trip values with an additional differentiating column given in idxLvl.

        :param dataIn: Input data for aggregation
        :param idxLvl: Index level specifying additional differentiation criterium
        :return:
        """
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
        """
        Plotting method for plotting aggregates that are differentiated with one additional variable. In the current
        implementation both summed and averaged values are plotted. Arguments show and write in the evaluator config
        determine if plots are shown to the user and/or written to hard disk. X axis ticks are currently manually
        configured to fit weekday and hour differentiation for one week.

        :return: None
        """

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
        ax[1].set_xticks(xRange[:-1])
        ax[1].set_xticklabels(xLabels, fontsize=self.evaluatorConfig['plotConfig']['xAxis']['ticklabelsize'])
        ax[0].ticklabel_format(axis='y', style='sci', scilimits=(0, 0), useMathText=True)
        ax[0].set_xticks(xRange[:-1])
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

    def linePlot(self, profileDict, pathOutput, flexEstimator, show=True, write=True, ylabel='Normalized profiles', ylim=None,
                 filename=''):
        """
        Basic line plot functionality

        :param profileDict: Dictionary specifiying which profiles should be plotted together in one plot
        :param pathOutput: Path to write Figure to
        :param flexEstimator: Instance of VencoPy class FlexEstimator for plotting of charging profiles
        :param show: If True, Figure is displayed during runtime
        :param write: If True, Figrue is written to hard disk
        :param ylabel: Label for y Axis
        :param ylim: Manually set y limit
        :param filename: Manually set name of file written to hard disk
        :return: None
        """

        plt.rcParams.update(self.evaluatorConfig['plotConfig']['plotRCParameters'])  # set plot layout
        fig, ax = plt.subplots()
        plt.tick_params(labelsize=self.evaluatorConfig['plotConfig']['plotRCParameters']['font.size'])
        for iKey, iVal in profileDict.items():
            if isinstance(iVal.index, pd.MultiIndex):
                iVal = self.sortData(iVal)
                sns.lineplot(x=range(iVal.index.size), y=iVal, label=iKey, sort=False)
            else:
                sns.lineplot(x=iVal.index, y=iVal, label=iKey, sort=False)
        xRange = range(0, len(profileDict[list(profileDict)[0]]) + 1,
                       self.evaluatorConfig['plotConfig']['xAxis']['xTickSteps'])
        xLabels = [f'{iDay}\n{str(iTime)}:00' for iDay in self.evaluatorConfig['plotConfig']['xAxis']['weekdays']
                   for iTime in self.evaluatorConfig['plotConfig']['xAxis']['hours']]

        # Labeling for 24 hour plots
        # xLabels = [f'{str(iTime)}:00' for iTime in self.evaluatorConfig['plotConfig']['xAxis']['hours']]
        ax.set_xticks(xRange[:-1])
        ax.set_xticklabels(xLabels, fontsize=self.evaluatorConfig['plotConfig']['xAxis']['ticklabelsize'])
        ax.set_ylim(bottom=0, top=ylim)
        ax.set_xlabel('Weekday and Hour',
                      fontsize=self.evaluatorConfig['plotConfig']['plotRCParameters']['axes.labelsize'])
        ax.set_ylabel(ylabel, fontsize=self.evaluatorConfig['plotConfig']['plotRCParameters']['axes.labelsize'])
        plt.legend(loc='upper center')
        plt.tight_layout()
        filePlot = pathOutput / Path(
            createFileString(globalConfig=self.globalConfig, datasetID=flexEstimator.datasetID, fileKey='flexPlotName',
                             manualLabel=filename, filetypeStr='svg'))
        if show:
            plt.show()
        if write:
            fig.savefig(filePlot)

    def separateLinePlots(self, profileDictList: list, flexEstimator, show=True,
                          write=True, ylabel=[], ylim=[], filenames=[]):
        """
        Wrapper function to draw and write multiple plots using linePlot().

        :param profileDictList: List of dictionaries. Each dictionary specifies one plot drawn.
        :param flexEstimator: Instance of VencoPy class FlexEstimator for data set ID
        :param show: If True, plots are shown to the user during runtime
        :param write: If True, plots are written to hard drive
        :param ylabel: List of ylabels. Has to be of same length as profileDictList
        :param ylim: List of ylimits. Has to be of same length as profileDictList
        :param filenames: Name of file to be written to hard drive
        :return: None
        """
        for iDict, iYLabel, iYLim, iName in zip(profileDictList, ylabel, ylim, filenames):
            writeProfilesToCSV(profileDictOut=iDict, globalConfig=self.globalConfig, singleFile=False,
                               datasetID=flexEstimator.datasetID)
            self.linePlot(iDict, pathOutput=Path(self.globalConfig['pathRelative']['plots']),
                          flexEstimator=flexEstimator, show=show, write=write, ylabel=iYLabel, ylim=iYLim,
                          filename=iName)

    def plotProfiles(self, flexEstimator, profileDictList: dict = None, yLabels: list = None, yLimits: list = None,
                     filenames: list = None):
        """
        Wrapper function to plot both one Figure with all resulting output profiles and separate Figures for flow,
        connection and state profiles after VencoPy flexibility estimation.

        :param flexEstimator: Instance of VencoPy class FlexEstimator
        :param profileDictList: List of diciontaries with keys specifying the profile name and value holding a pandas
        dataframe with a multiindex series
        :param yLabels: list of y axis labels. Has to be of same length as profileDictList
        :param yLimitsL List of y axis limits. Has to be of same length as profileDictList
        :param filenames: List of filenames to write the plots to. Has to be of same length as profileDictList
        :return: None
        """
        # self.linePlot(flexEstimator.profileDictOut, pathOutput=Path(flexEstimator.globalConfig['pathRelative']['plots']),
        #               flexEstimator=flexEstimator, show=True, write=True, filename='allPlots' + flexEstimator.datasetID)

        if profileDictList is None:
            # Separately plot flow and state profiles
            profileDictConnectionShare = dict(gridConnectionShare=flexEstimator.plugProfilesWAggVar)

            # profileDictFlowsNorm = dict(uncontrolledCharging=flexEstimator.chargeProfilesUncontrolledCorr,
            #                             electricityDemandDriving=flexEstimator.electricPowerProfilesCorr,
            #                             gridConnectionShare=flexEstimator.plugProfilesAgg)
            profileDictFlowsAbs = dict(uncontrolledCharging=flexEstimator.chargeProfilesUncontrolledWAggVar,
                                       electricityDemandDriving=flexEstimator.electricPowerProfilesWAggVar)

            # profileDictStateNorm = dict(socMax=flexEstimator.socMaxNorm, socMin=flexEstimator.socMinNorm)
            profileDictStateAbs = dict(socMax=flexEstimator.socMaxVar, socMin=flexEstimator.socMinVar)

            profileDictList = [profileDictConnectionShare, profileDictFlowsAbs, profileDictStateAbs]
            yLabels = ['Average EV connection share', 'Average EV flow in kW', 'Average EV SOC in kWh']
            yLimits = [1, 5, 50]
            filenames = [flexEstimator.datasetID + '_connection', flexEstimator.datasetID + '_flows',
                                          flexEstimator.datasetID + '_state']
        self.separateLinePlots(profileDictList, show=True, write=True, flexEstimator=flexEstimator,
                               ylabel=yLabels, filenames=filenames, ylim=yLimits)

    def compareProfiles(self, compareTo):
        """
        EXPERIMENTAL STATE. Deprecated method to compare profiles of MiD08 and MiD17. Currently not in production and
        not tested.
        """

        # if not isinstance(compareTo, FlexEstimator):
        #     raise('Argument to compare to is not a class instance of FlexEstimator')

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
        """
        Helper function for compareProfiles()

        :param compareTo: Dataset to compare to
        :param profileNameList: List of names of files to be written
        :return:
        """

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