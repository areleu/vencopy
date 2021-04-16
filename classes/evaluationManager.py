__version__ = '0.0.2'
__maintainer__ = 'Niklas Wulff'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '21.09.2020'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'

import pandas as pd
import collections as col
from pathlib import Path
from profilehooks import profile
import seaborn as sns
import matplotlib.pyplot as plt
from scripts.libPlotting import *
from scripts.utilsParsing import createFileString, mergeVariables
from classes.parseManager import DataParser



# Some functions
def wavg(data, avg_name, weight_name):
    """ http://stackoverflow.com/questions/10951341/pandas-dataframe-aggregate-function-using-multiple-columns
    In rare instance, we may not have weights, so just return the mean. Customize this if your business case
    should return otherwise.
    """
    d = data[avg_name]
    w = data[weight_name]
    try:
        return (d * w).sum() / w.sum()
    except ZeroDivisionError:
        return d.mean()


class Evaluator:
    def __init__(self, config, label, weightPlot=True):
        self.config = config
        self.label = label
        self.weightPlot = weightPlot
        self.normPlotting = True
        self.dailyMileageGermany2008 = 3.080e9  # pkm/d
        self.dailyMileageGermany2017 = 3.214e9  # pkm/d
        self.hourVec = [str(i) for i in range(0, config['numberOfHours'])]
        self.datasets = ['MiD08', 'MiD17']
        self.inputData = self.readInData(['inputDataDriveProfiles'], self.datasets)
        self.data = None
        self.dataStacked = None
        self.aggregateIDDict = self.setupAggDict()
        self.hourlyAggregates = self.aggregateAcrossTrips()
        print('Evaluator initialization complete')

    def readInData(self, fileKeys: list, datasets: list) -> pd.Series:
        """
        Generic read-in function for mobility datasets. This serves as interface between the dailz trip distance
        and purpose calculation and the class Evaluator.

        :param fileKeys: List of VencoPy-internal names for the filekeys to read in
        :param datasets: list of strings declaring the datasets to be read in
        :return: a named pd.Series of all datasets with the given filekey_datasets as identifiers
        """

        ret = pd.Series(dtype=object)
        for iFileKey in fileKeys:
            for iDat in datasets:
                dataIn = pd.read_csv(Path(self.config['linksRelative']['input']) /
                                     createFileString(config=self.config, fileKey=iFileKey,
                                                      dataset=iDat), dtype={'hhPersonID': int},
                                     # index_col=['hhPersonID', 'tripStartWeekday'])
                                     index_col=['hhPersonID'])
                ret[iDat] = dataIn
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
        IDDict['sum'] = [f'{iDatID}_sum' for iDatID in self.datasets]
        IDDict['mean'] = [f'{iDatID}_mean' for iDatID in self.datasets]
        IDDict['wMean'] = [f'{iDatID}_wMean' for iDatID in self.datasets]
        return IDDict

    def aggregateAcrossTrips(self):
        ret = pd.DataFrame()
        for iDatID, iDat in self.inputData.items():
            ret.loc[:, f'{iDatID}_sum'] = iDat.loc[:, self.hourVec].sum(axis=0)
            ret.loc[:, f'{iDatID}_mean'] = iDat.loc[:, self.hourVec].mean(axis=0)
            if self.weightPlot:
                ret.loc[:, f'{iDatID}_wMean'] = iDat.loc[:, self.hourVec].apply(self.calculateWeightedAverage, args=[iDat.loc[:, 'tripWeight']])
        return ret

    @staticmethod
    def calculateWeightedAverage(col, weightCol):
        return sum(col * weightCol) / sum(weightCol)

    def calcVariableSpecAggregates(self, by):
        ret = pd.DataFrame()
        for iDatID, iDat in self.inputData.items():
            if not all([iBy in iDat.index.names for iBy in by]):
                raise Exception('At least one required variable name is not in index names. Aborting')
            ret.loc[:, f'{iDatID}_sum'] = iDat.loc[:, self.hourVec].groupby(level=by).sum().stack()  # needed .stack() here?
            ret.loc[:, f'{iDatID}_mean'] = iDat.loc[:, self.hourVec].groupby(level=by).mean().stack()
            if self.weightPlot:
                # ret.loc[:, f'{iDatID}_wMean'] = iDat.loc[:,
                #                                 self.hourVec].groupby(level=by).apply(self.calculateWeightedAverage,
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
            ret.loc[:, iVar] = dataSlice.apply(self.calculateWeightedAverage, weightCol=weightSlice)
        ret = ret.stack()
        ret.index = ret.index.swaplevel(0, 1)
        return ret

    def plotAggregates(self):
        # Plotting aggregates
        fig, ax = plt.subplots(2, 1)

        plt.tick_params(labelsize=self.config['plotConfig']['plotRCParameters']['font.size'])
        meanCols = self.aggregateIDDict['mean']
        meanCols.extend(self.aggregateIDDict['wMean'])
        self.hourlyAggregates.loc[:, self.aggregateIDDict['sum']].plot.line(ax=ax[0])
        self.hourlyAggregates.loc[:, meanCols].plot.line(ax=ax[1])
        xRange = range(0, len(self.hourlyAggregates) + 1, self.config['plotConfig']['xAxis']['xTickSteps'])
        # xLabels = [f'{iDay}\n{str(iTime)}:00' for iDay in self.config['plotConfig']['xAxis']['weekdays'] for iTime in self.config['plotConfig']['xAxis']['hours']]
        xLabels = [f'{str(iTime)}:00' for iTime in config['plotConfig']['xAxis']['hours']]
        ax[1].set_xticks(xRange)
        ax[1].set_xticklabels(xLabels, fontsize=self.config['plotConfig']['xAxis']['ticklabelsize'])
        ax[0].ticklabel_format(axis='y', style='sci', scilimits=(0, 0), useMathText=True)
        ax[0].set_xticks(xRange)
        ax[0].set_xticklabels('')
        ax[0].set_xlabel('')
        ax[0].set_ylabel('Sum of all trips \n in sample in km',
                         fontsize=self.config['plotConfig']['yAxis']['ticklabelsize'])
        ax[1].set_xlabel("Hour", fontsize=self.config['plotConfig']['plotRCParameters']['axes.labelsize'])
        ax[1].set_ylabel("Average of hourly \n trips in km",
                         fontsize=self.config['plotConfig']['plotRCParameters']['axes.labelsize'])
        ax[0].tick_params(axis='y', labelsize=config['plotConfig']['yAxis']['ticklabelsize'])
        ax[0].legend()
        ax[1].legend()

        if config['plotConfig']['show']:
            plt.show()
        if config['plotConfig']['save']:
            fileName = createFileString(config=config, fileKey='aggPlotName', manualLabel=self.label, filetypeStr='svg')
            fig.savefig(Path(config['linksRelative']['sesPlots']) / fileName, bbox_inches='tight')

if __name__ == '__main__':
    linkConfig = Path.cwd() / 'config' / 'config.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    config = yaml.load(open(linkConfig), Loader=yaml.SafeLoader)
    # evaluateDriveProfiles(config=config)
    vpEval = Evaluator(config, label='base')
    vpEval.hourlyAggregates = vpEval.calcVariableSpecAggregates(by=['tripStartWeekday'])
    # p = ParseData(datasetID='MiD17', config=config, strColumns=True, loadEncrypted=False)

    vpEval.plotAggregates()

    # vpEval.data = mergeVariables(data=vpEval.inputData['MiD17'].reset_index(),
    #                              variableData=p.data, variables=['tripWeight'])

    print('this is the end')