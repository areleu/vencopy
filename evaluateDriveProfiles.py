__version__ = '0.0.2'
__maintainer__ = 'Niklas Wulff'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '21.09.2020'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'

import pandas as pd
from pathlib import Path
from profilehooks import profile
import seaborn as sns
import matplotlib.pyplot as plt
from scripts.libPlotting import *
from scripts.utilsParsing import createFileString, mergeVariables
from scripts.parseManager import ParseData



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
    def __init__(self, config, weightPlot=True):
        self.config = config
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
                dataIn = pd.read_csv(Path(config['linksRelative']['input']) /
                                     createFileString(config=config, fileKey=iFileKey,
                                                      dataset=iDat), dtype={'hhPersonID': int},
                                     index_col=['hhPersonID', 'tripStartWeekday'])
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

    def calculateWeightedAverage(self, col, weightCol):
        return sum(col * weightCol) / sum(weightCol)

    def calcVariableSpecAggregates(self, by):
        ret = pd.DataFrame()
        for iDatID, iDat in self.inputData.items():
            if not all([iBy in iDat.index.names for iBy in by]):
                raise Exception('At least one required variable name is not in index names. Aborting')
            ret.loc[:, f'{iDatID}_sum'] = iDat.loc[:, self.hourVec].groupby(level=by).sum().stack()  # needed .stack() here?
            ret.loc[:, f'{iDatID}_mean'] = iDat.loc[:, self.hourVec].groupby(level=by).mean().stack()
            if self.weightPlot:
                ret.loc[:, f'{iDatID}_wMean'] = iDat.loc[:,
                                                self.hourVec].groupby(level=by).apply(self.calculateWeightedAverage,
                                                                    weightCol=[iDat.loc[:, 'tripWeight']]).stack()
        return ret

        driveDataWeekday = pd.DataFrame({'mid08sum':
                                         driveData_mid2008.groupby(
                                             'tripStartWeekday').sum().stack()})  # .drop(labels=['Weight', 'tripScaleFactor'], axis=1)
        driveDataWeekday.loc[:, 'mid08simpleAvrg'] = driveData_mid2008.groupby(
        'tripStartWeekday').mean().stack()  # .drop(labels=['Weight', 'tripScaleFactor'], axis=1)
        if weightPlot:
            for iCol in hourVec:
                for iDay in driveData_mid2008.Day.unique():
                    driveDataWeekday.loc[(iDay, iCol), 'mid08wAvrg'] \
                        = sum(driveData_mid2008.loc[driveData_mid2008.loc[:, 'Day'] == iDay, iCol]
                          * driveData_mid2008.loc[driveData_mid2008.loc[:, 'Day'] == iDay, 'Weight']) \
                            / sum(driveData_mid2008.loc[driveData_mid2008.loc[:, 'Day'] == iDay, 'Weight'])

        driveDataWeekday.loc[:, 'mid17sum'] \
            = driveData_mid2017.groupby('tripStartWeekday').sum().stack()  # .drop(labels=['tripWeight', 'tripScaleFactor'], axis=1)
        driveDataWeekday.loc[:, 'mid17simpleAvrg'] \
            = driveData_mid2017.groupby(
            'tripStartWeekday').mean().stack()  # .drop(labels=['tripWeight', 'tripScaleFactor'], axis=1)

        if weightPlot:
            for iCol in hourVec:
                for iDay in driveData_mid2017.tripStartWeekday.unique():
                    driveDataWeekday.loc[(iDay, iCol), 'mid17wAvrg'] \
                        = sum(driveData_mid2017.loc[driveData_mid2017.loc[:, 'tripStartWeekday'] == iDay, iCol]
                              * driveData_mid2017.loc[driveData_mid2017.loc[:, 'tripStartWeekday'] == iDay, 'tripWeight']) \
                          / sum(driveData_mid2017.loc[driveData_mid2017.loc[:, 'tripStartWeekday'] == iDay, 'tripWeight'])

    def plotAggregates(self):
        # Plotting aggregates
        fig, ax = plt.subplots(2, 1)
        meanCols = self.aggregateIDDict['mean']
        meanCols.extend(self.aggregateIDDict['wMean'])
        self.hourlyAggregates.loc[:, self.aggregateIDDict['sum']].plot.line(ax=ax[0])
        self.hourlyAggregates.loc[:, meanCols].plot.line(ax=ax[1])
        plt.xticks(range(0, len(self.hourVec)+1, self.config['plotConfig']['xTickSteps']))
        ax[0].set(xlabel="Hour", ylabel="Sum of all trips")
        ax[1].set(xlabel="Hour", ylabel="Average of all hourly trips")
        ax[0].legend()
        ax[1].legend()

        if config['plotConfig']['show']:
            plt.show()
        if config['plotConfig']['save']:
            fileName = createFileString(config=config, fileKey='aggPlotName', filetypeStr='svg')
            fig.savefig(Path(config['linksRelative']['sesPlots']) / fileName)

@profile(immediate=True)
def evaluateDriveProfiles(config, weightPlot=False):

    normPlotting = True
    dailyMileageGermany2008 = 3.080e9  # pkm/d
    dailyMileageGermany2017 = 3.214e9  # pkm/d
    hourVec = [str(i) for i in range(0, 24)]
    driveData_mid2008 = pd.read_csv(Path(config['linksRelative']['input']) /
                                    createFileString(config=config, fileKey='inputDataDriveProfiles', dataset='MiD08'))
    driveData_mid2017 = pd.read_csv(Path(config['linksRelative']['input']) /
                                    createFileString(config=config, fileKey='inputDataDriveProfiles', dataset='MiD17'))




    data08_raw = driveData_mid2008.drop(columns=['hhPersonID']).set_index('tripStartWeekday', append=True).stack()  # 'tripWeight', 'tripScaleFactor'
    data08 = data08_raw.reset_index([1, 2])
    data08.columns = ['Day', 'Hour', 'Value']
    data17_raw = driveData_mid2017.drop(columns=['hhPersonID']).set_index('tripStartWeekday', append=True).stack()  # 'tripWeight', 'tripScaleFactor'
    data17 = data17_raw.reset_index([1, 2])
    data17.columns = ['Day', 'Hour', 'Value']

    # Data calculation and concatenation into one dataframe
    driveData = pd.DataFrame({'mid08sum': driveData_mid2008.loc[:, hourVec].sum(axis=0)})
    driveData.loc[:, 'mid08simpleAvrg'] = driveData_mid2008.loc[:, hourVec].mean(axis=0)

    # Weighted average calculation for MiD08
    if weightPlot:
        for iCol in hourVec:
            driveData.loc[iCol, 'mid08wAvrg'] = sum(driveData_mid2008.loc[:, iCol] * driveData_mid2008.loc[:, 'Weight']) / \
                                                sum(driveData_mid2008.loc[:, 'Weight'])
    driveData.loc[:, 'mid17sum'] = driveData_mid2017.loc[:, hourVec].sum(axis=0)
    driveData.loc[:, 'mid17simpleAvrg'] = driveData_mid2017.loc[:, hourVec].mean(axis=0)

    # Weighted average calculation for MiD17
    if weightPlot:
        for iCol in hourVec:
            driveData.loc[iCol, 'mid17wAvrg'] = sum(driveData_mid2017.loc[:, iCol] * driveData_mid2017.loc[:, 'tripWeight']) \
                                                / sum(driveData_mid2017.loc[:, 'tripWeight'])

    # Plotting aggregates
    fig, ax = plt.subplots(2, 1)
    driveData.loc[:, ['mid08sum', 'mid17sum']].plot.line(ax=ax[0])
    driveData.loc[:, ['mid08simpleAvrg', 'mid17simpleAvrg']].plot.line(ax=ax[1])  # 'mid08wAvrg', 'mid17wAvrg'

    if config['plotConfig']['show']:
        plt.show()
    if config['plotConfig']['save']:
        fileName = 'allDays_08vs17_%s.png' % (config['labels']['runLabel'])
        fig.savefig(Path(config['linksRelative']['sesPlots']) / fileName)

    if normPlotting:

    # Plotting weekday specific
        driveDataWeekday = pd.DataFrame({'mid08sum':
                    driveData_mid2008.groupby('tripStartWeekday').sum().stack()})  # .drop(labels=['Weight', 'tripScaleFactor'], axis=1)
        driveDataWeekday.loc[:,
            'mid08simpleAvrg'] = driveData_mid2008.groupby('tripStartWeekday').mean().stack()  # .drop(labels=['Weight', 'tripScaleFactor'], axis=1)
        if weightPlot:
            for iCol in hourVec:
                for iDay in driveData_mid2008.Day.unique():
                    driveDataWeekday.loc[(iDay, iCol), 'mid08wAvrg'] \
                        = sum(driveData_mid2008.loc[driveData_mid2008.loc[:, 'Day'] == iDay, iCol]
                              * driveData_mid2008.loc[driveData_mid2008.loc[:, 'Day'] == iDay, 'Weight']) \
                          / sum(driveData_mid2008.loc[driveData_mid2008.loc[:, 'Day'] == iDay, 'Weight'])


        driveDataWeekday.loc[:,'mid17sum'] \
            = driveData_mid2017.groupby('tripStartWeekday').sum().stack()  # .drop(labels=['tripWeight', 'tripScaleFactor'], axis=1)
        driveDataWeekday.loc[:,'mid17simpleAvrg'] \
            = driveData_mid2017.groupby('tripStartWeekday').mean().stack()  # .drop(labels=['tripWeight', 'tripScaleFactor'], axis=1)

        if weightPlot:
            for iCol in hourVec:
                for iDay in driveData_mid2017.tripStartWeekday.unique():
                    driveDataWeekday.loc[(iDay, iCol), 'mid17wAvrg'] \
                        = sum(driveData_mid2017.loc[driveData_mid2017.loc[:, 'tripStartWeekday'] == iDay, iCol]
                              * driveData_mid2017.loc[driveData_mid2017.loc[:, 'tripStartWeekday'] == iDay, 'tripWeight']) \
                          / sum(driveData_mid2017.loc[driveData_mid2017.loc[:, 'tripStartWeekday'] == iDay, 'tripWeight'])

        driveDataWeekday = driveDataWeekday.reset_index(level=0)
        fig, ax = plt.subplots(3, 2)
        driveDataWeekday.loc[driveDataWeekday.loc[:,'tripStartWeekday'] == 'MON', ['mid08sum', 'mid17sum']].plot.line(ax=ax[0,0])
        driveDataWeekday.loc[driveDataWeekday.loc[:,'tripStartWeekday'] == 'MON', ['mid08simpleAvrg', 'mid17simpleAvrg']].plot.line(ax=ax[0,1])  # 'mid08wAvrg','mid17wAvrg'
        # driveDataWeekday.loc[driveDataWeekday.loc[:,'tripStartWeekday'] == 'TUE', ['mid08sum', 'mid17sum']].plot.line(ax=ax[1,0])
        # driveDataWeekday.loc[driveDataWeekday.loc[:,'tripStartWeekday'] == 'TUE', ['mid08simpleAvrg', 'mid08wAvrg', 'mid17simpleAvrg', 'mid17wAvrg']].plot.line(ax=ax[1,1])
        # driveDataWeekday.loc[driveDataWeekday.loc[:,'tripStartWeekday'] == 'WED', ['mid08sum', 'mid17sum']].plot.line(ax=ax[2,0])
        # driveDataWeekday.loc[driveDataWeekday.loc[:,'tripStartWeekday'] == 'WED', ['mid08simpleAvrg', 'mid08wAvrg', 'mid17simpleAvrg', 'mid17wAvrg']].plot.line(ax=ax[2,1])
        driveDataWeekday.loc[driveDataWeekday.loc[:,'tripStartWeekday'] == 'THU', ['mid08sum', 'mid17sum']].plot.line(ax=ax[1,0])
        driveDataWeekday.loc[driveDataWeekday.loc[:,'tripStartWeekday'] == 'THU', ['mid08simpleAvrg', 'mid17simpleAvrg']].plot.line(ax=ax[1,1])
        # driveDataWeekday.loc[driveDataWeekday.loc[:,'tripStartWeekday'] == 'FRI', ['mid08sum', 'mid17sum']].plot.line(ax=ax[4,0])
        # driveDataWeekday.loc[driveDataWeekday.loc[:,'tripStartWeekday'] == 'FRI', ['mid08simpleAvrg', 'mid08wAvrg', 'mid17simpleAvrg', 'mid17wAvrg']].plot.line(ax=ax[4,1])
        driveDataWeekday.loc[driveDataWeekday.loc[:,'tripStartWeekday'] == 'SAT', ['mid08sum', 'mid17sum']].plot.line(ax=ax[2,0])
        driveDataWeekday.loc[driveDataWeekday.loc[:,'tripStartWeekday'] == 'SAT', ['mid08simpleAvrg', 'mid17simpleAvrg']].plot.line(ax=ax[2,1])
        # driveDataWeekday.loc[driveDataWeekday.loc[:,'tripStartWeekday'] == 'SUN', ['mid08sum', 'mid17sum']].plot.line(ax=ax[6,0])
        # driveDataWeekday.loc[driveDataWeekday.loc[:,'tripStartWeekday'] == 'SUN', ['mid08simpleAvrg', 'mid08wAvrg', 'mid17simpleAvrg', 'mid17wAvrg']].plot.line(ax=ax[6,1])
        ax[1, 0].get_legend().set_visible(False)
        ax[1, 1].get_legend().set_visible(False)
        ax[2, 0].get_legend().set_visible(False)
        ax[2, 1].get_legend().set_visible(False)
        if config['plotConfig']['show']:
            plt.show()
        if config['plotConfig']['save']:
            fileName = 'absRel08v17_%s_MoThSa.png' % (config['labels']['runLabel'])
            fig.savefig(Path(config['linksRelative']['plotsDCMob']) / fileName)


    # dataTueSat =  driveDataWeekday.loc[driveDataWeekday.loc[:,'tripStartWeekday'].isin(['TUE', 'SAT']), ['mid08wAvrg', 'mid17wAvrg']]
    # # violinPlot(dataTueSat, hue='tripStartWeekday')
    #
    # dataTueSat = driveDataWeekday.loc[driveDataWeekday.loc[:,'tripStartWeekday'].isin(['TUE', 'SAT']), ['tripStartWeekday', 'mid08wAvrg', 'mid17wAvrg']].set_index('tripStartWeekday', append=True).stack()
    # dataTueSat = dataTueSat.reset_index([1,2])
    # dualViolinPlot(dataTueSat, x='tripStartWeekday', y=0, hue='level_2')

    data08_TuSa = data08.loc[(data08.loc[:, 'Day'].isin(['TUE', 'SAT'])) & (data08.loc[:, 'Value'] < 30), :]
    data17_TuSa = data17.loc[(data17.loc[:, 'Day'].isin(['TUE', 'SAT'])) & (data17.loc[:, 'Value'] < 30), :]

    dualViolinPlot(data08, x=data08.loc[:,'Hour'].astype('int32'), y='Value', hue=None,
                   write='MiD08_' + config['labels']['runLabel'] + '_drive_diffHour', config=config)
    dualViolinPlot(data08_TuSa, x=data08_TuSa.loc[:,'Hour'].astype('int32'), y='Value', hue='Day',
                   write='MiD08_' + config['labels']['runLabel'] + '_diffHourDay', config=config)
    dualViolinPlot(data17, x=data17.loc[:,'Hour'].astype('int32'), y='Value', hue=None,
                   write='MiD17_' + config['labels']['runLabel'] + 'diffHour', config=config)
    dualViolinPlot(data17_TuSa, x=data17_TuSa.loc[:,'Hour'].astype('int32'), y='Value', hue='Day',
                   write='MiD17_' + config['labels']['runLabel'] + 'diffHourDay', config=config)
    print('end')

if __name__ == '__main__':
    linkConfig = Path.cwd() / 'config' / 'config.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    config = yaml.load(open(linkConfig), Loader=yaml.SafeLoader)
    # evaluateDriveProfiles(config=config)
    vpEval = Evaluator(config)
    vpEval.hourlyAggregates = vpEval.calcVariableSpecAggregates(by=['tripStartWeekday'])
    # p = ParseData(datasetID='MiD17', config=config, strColumns=True, loadEncrypted=False)

    vpEval.plotAggregates()

    # vpEval.data = mergeVariables(data=vpEval.inputData['MiD17'].reset_index(),
    #                              variableData=p.data, variables=['tripWeight'])

    print('this is the end')