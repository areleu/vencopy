
from vencopy.core.diaryBuilders import WeekDiaryBuilder, DiaryBuilder
from vencopy.core.dataParsers.dataParsers import ParseMiD
from vencopy.core.gridModelers import GridModeler
from vencopy.core.flexEstimators import WeekFlexEstimator
from vencopy.utils.globalFunctions import load_configs, createOutputFolders
import sys
import pickle
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from profilehooks import profile

# Needed to run in VSCode properties currently
sys.path.append('.')

# from vencopy.core.evaluators import Evaluator

__version__ = '0.2.X'
__maintainer__ = 'Niklas Wulff'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '11.01.2022'
__status__ = 'test'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'

# Columns for debugging purposes
# ['genericID', 'parkID', 'tripID', 'actID', 'nextActID', 'prevActID', 'dayActID', 'timestampStart', 'timestampEnd']


def composeSampleSizeScanDict(
        nWeeks: list[int],
        weekDiaryBuilder: WeekDiaryBuilder, threshold: float, seed: int = None, replace: bool = True) -> dict:
    """ Compose multiple weekly samples of the size of the ints given in the list of ints in nWeeks.

    Args:
        nWeeks (list[int]): List of integers defining the sample size per sample base
        seed (int): Seed for reproducible sampling
        replace (bool): Should bootstrapping of the sample bases be allowed?
        threshold (float): SOC threshold over which charging does not occur

    Returns:
        dict: Dictionary of activities with sample size in the keys and pandas DataFrames describing activities in
        the values.
    """
    actDict = {}
    for w in nWeeks:
        wa = weekDiaryBuilder.composeWeekActivities(nWeeks=w, seed=seed, replace=replace)
        vpWeFlex = WeekFlexEstimator(configDict=configDict, datasetID=datasetID, activities=wa, threshold=threshold)
        vpWeFlex.estimateTechnicalFlexibility()
        actDict[w] = vpWeFlex.activities
    return actDict


def composeSameSampleSizeDict(
        nWeeks: int, nSample: int, configDict: dict, weekDiaryBuilder: WeekDiaryBuilder, threshold: float,
        seed: list[int] = None, replace: bool = True) -> dict:
    """ Compose nSample weekly samples of the size of nWeeks. The weekDiaryBuilder instance must be given alongside
    a charging threshold. A seed is optional for reproducibility. If replace is True more samples can be drawn even
    from a sample base than elements exist (bootstrapping).

    Args:
        nWeeks (int): Integer defining the sample size per sample base
        nSample (int): Number of runs the complete sample (with nWeeks samples) is drawn for comparison
        weekDiaryBuilder (WeekDiaryBuilder): Week diary builder instance from VencoPy
        threshold (float): SOC threshold over which charging does not occur
        seed (int): Optional seeds for reproducible sampling. Has to match
        replace (bool): Should bootstrapping of the sample bases be allowed?

    Returns:
        dict: Dictionary of activities with a range index in the keys and pandas DataFrames describing activities in
        the values.
    """
    actDict = {}
    # profileDict = {}
    for s in range(nSample):
        weekActs = weekDiaryBuilder.composeWeekActivities(nWeeks=nWeeks, seed=seed, replace=replace)
        vpWeFlex = WeekFlexEstimator(configDict=configDict, datasetID=datasetID, activities=weekActs,
                                     threshold=threshold)
        vpWeFlex.estimateTechnicalFlexibility()
        actDict[s] = vpWeFlex.activities

        # vpDiary = DiaryBuilder(configDict=configDict, datasetID=datasetID, activities=vpWeFlex.activities,
        #                        isWeekDiary=True)
        # vpDiary.createDiaries()
        # profileDict[s] = vpDiary.uncontrolledCharge
    return actDict  # , profileDict


def composeMultiThresholdDict(nWeeks: int, weekDiaryBuilder: WeekDiaryBuilder, threshold: list[float],
                              seed: int = None, replace: bool = True) -> dict:
    """ Compose multiple weekly samples of the size of the nWeeks and varying charging threshold given in the list
    of floats in threshold.

    Args:
        nWeeks (list[int]): List of integers defining the sample size per sample base
        seed (int): Seed for reproducible sampling
        replace (bool): Should bootstrapping of the sample bases be allowed?
        threshold (list[float]): List of SOC thresholds over which charging does not occur

    Returns:
        dict: Dictionary of activities with thresholds in the keys and pandas DataFrames describing activities in
        the values.
    """
    actDict = {}
    weekActs = weekDiaryBuilder.composeWeekActivities(nWeeks=nWeeks, seed=seed, replace=replace)
    for t in threshold:
        vpWeFlex = WeekFlexEstimator(configDict=configDict, datasetID=datasetID, activities=weekActs,
                                     threshold=t)
        vpWeFlex.estimateTechnicalFlexibility()
        actDict[t] = vpWeFlex.activities
    return actDict


def plotDistribution(vpActDict: dict, var: str, subset: str, lTitle: str = '', pTitle: str = ''):  # **kwargs
    """Plot multiple distributions for different sample sizes of a specific variable in a plot.

    Args:
        vpActDict (dict): A dictionary with the sample size as ints in the keys and a pandas DataFrame in the values
        representing the activities sampled from the sample bases (written for the application of sampling via
        WeekDiaryBuilder).
        var (str): Variable column to plot histogram of
        subset (str): Must be either 'park' or 'trip'
        lTitle (str): Legend title
        pTitle (str): Plot title
    """
    plt.figure()

    for p, acts in vpActDict.items():
        # plt.hist(data=acts[var], label=f'nWeeks={nWeeks}', kwargs)
        if subset == 'park':
            vec = acts.loc[~acts['parkID'].isna(), var]
            plt.hist(x=vec, label=f'paraVar={p}', bins=100, alpha=0.5, density=True)
            if var != 'weekdayStr':
                addMeanMedianText(vector=vec)
        elif subset == 'trip':
            vec = acts.loc[~acts['tripID'].isna(), var]
            plt.hist(x=vec, label=f'paraVar={p}', bins=100, alpha=0.5, density=True)
            if var != 'weekdayStr':
                addMeanMedianText(vector=vec)
        else:
            plt.hist(x=acts[var], label=f'nWeeks={nWeeks}', bins=100, alpha=0.5, density=True)
            if var != 'weekdayStr':
                addMeanMedianText(vector=acts[var])
    plt.legend(title=lTitle)
    plt.title(label=pTitle)
    plt.show()


def addMeanMedianText(vector: pd.Series):
    plt.text(x=0.1, y=0.9, s=f'Average={np.average(vector)}')
    plt.text(x=0.1, y=0.8, s=f'Median={np.median(vector)}')


def plotArrivalHourDistribution(vpActDict: dict, paraName: str, pTitle: str):
    plt.figure()
    for t, acts in vpActDict.items():
        plt.hist(x=acts.loc[~acts['parkID'].isna(), 'timestampStart'].dt.hour,
                 label=f'{paraName}={t}', bins=100, alpha=0.5, density=True)
    plt.legend()
    plt.title(label=pTitle)
    plt.show()


def plotParkDurationDistribution(vpActDict: dict, paraName: str, pTitle: str):
    plt.figure()
    for t, acts in vpActDict.items():
        plt.hist(x=acts.loc[~acts['parkID'].isna(), 'timedelta'].dt.total_seconds() / 60,
                 label=f'{paraName}={t}', bins=100, alpha=0.5, density=True)
    plt.legend()
    plt.title(label=pTitle)
    plt.show()


if __name__ == '__main__':
    # Set dataset and config to analyze, create output folders
    datasetID = 'MiD17'
    configNames = ('globalConfig', 'localPathConfig', 'parseConfig', 'gridConfig', 'flexConfig', 'diaryConfig',
                   'evaluatorConfig')
    basePath = Path(__file__).parent.parent.parent / 'vencopy'
    configDict = load_configs(configNames, basePath)
    createOutputFolders(configDict=configDict)

    CALC = True
    FN_ACT = 'acts_threshold_areaType_t05-10_n2000_b40'
    FN_PROF = 'profiles_sample_areaType_t10_n100_b40_samples10'

    if CALC:
        vpData = ParseMiD(configDict=configDict, datasetID=datasetID)
        vpData.process(splitOvernightTrips=False)

        # Grid model application
        vpGrid = GridModeler(configDict=configDict, datasetID=datasetID, activities=vpData.activities,
                             gridModel='probability')
        vpGrid.assignGrid()

        # Week diary building
        vpWDB = WeekDiaryBuilder(activities=vpGrid.activities, catCols=['areaType'])
        vpWDB.summarizeSamplingBases()

    # Sampling of multiple weeks varying sample size
    if CALC:
        # nWeeks = [10, 50, 100, 500, 1000]
        # sampleDict = composeSampleSizeScanDict(nWeeks=nWeeks, weekDiaryBuilder=vpWDB, replace=True, threshold=0.8)
        # , profileDict
        # actDict = composeSameSampleSizeDict(nWeeks=1000, nSample=10, configDict=configDict,
        #                                    weekDiaryBuilder=vpWDB, replace=True, threshold=1)
        tList = [0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
        thresholdDict = composeMultiThresholdDict(nWeeks=2000, weekDiaryBuilder=vpWDB, replace=True, threshold=tList)

        pickle.dump(thresholdDict, open(f'{FN_ACT}.p', 'wb'))
        # pickle.dump(profileDict, open(f'{FN_PROF}.p', 'wb'))
    else:  # load from pickle
        actDict = pickle.load(open(f'{FN_ACT}.p', 'rb'))
        profDict = pickle.load(open(f'{FN_PROF}.p', 'rb'))

    print('this is the end')

    # Sampling of multiple weeks varying plugin threshold
    # loadThresholdsFromPickle = True
    # if not loadThresholdsFromPickle:
    #     thresholds = [0.7, 0.8, 0.9, 1]
    #     thresholdDictN100 = composeMultiThresholdDict(nWeeks=100, weekDiaryBuilder=vpWDB, seed=42, replace=True,
    #                                                   threshold=thresholds)
    #     thresholdDictN200 = composeMultiThresholdDict(nWeeks=200, weekDiaryBuilder=vpWDB, seed=42, replace=True,
    #                                                   threshold=thresholds)
    #     thresholdDictN500 = composeMultiThresholdDict(nWeeks=500, weekDiaryBuilder=vpWDB, seed=42, replace=True,
    #                                                   threshold=thresholds)

    #     pickle.dump(thresholdDictN100, open('thresholdDictN100.p', 'wb'))
    #     pickle.dump(thresholdDictN200, open('thresholdDictN200.p', 'wb'))
    #     pickle.dump(thresholdDictN500, open('thresholdDictN500.p', 'wb'))
    # else:
    #     thresholdDictN100 = pickle.load(open('thresholdDictN100.p', 'rb'))
    #     thresholdDictN200 = pickle.load(open('thresholdDictN200.p', 'rb'))
    #     thresholdDictN500 = pickle.load(open('thresholdDictN500.p', 'rb'))

    # Plotting uncontrolled charging for multiple sample sizes and thresholds
    # plotDistribution(vpActDict=sampleDict, var='uncontrolledCharge', subset='park')
    # plotDistribution(vpActDict=thresholdDictN100, var='uncontrolledCharge', subset='park', lTitle='Threshold',
    #                  pTitle='Sample size n=100')
    # plotDistribution(vpActDict=thresholdDictN200, var='uncontrolledCharge', subset='park', lTitle='Threshold',
    #                  pTitle='Sample size n=200')
    # plotDistribution(vpActDict=thresholdDictN500, var='uncontrolledCharge', subset='park', lTitle='Threshold',
    #                  pTitle='Sample size n=500')

    # Plotting weekday distribution
    # plotDistribution(vpActDict=sampleDict, var='weekdayStr', subset='trip')
    # plotDistribution(vpActDict=thresholdDictN100, var='weekdayStr', subset='trip', lTitle='Threshold')

    # Plotting charging power distribution
    # plotDistribution(vpActDict=sampleDict, var='chargingPower', subset='park')
    # plotDistribution(vpActDict=thresholdDictN100, var='chargingPower', subset='park', lTitle='Threshold')

    # Plotting arrival hour
    # plotArrivalHourDistribution(vpActDict=sampleDict, paraName='Sample size', pTitle='Threshold t=0.8')
    # plotArrivalHourDistribution(vpActDict=thresholdDictN100, pTitle='Sample size n=100')
    # plotArrivalHourDistribution(vpActDict=thresholdDictN200, pTitle='Sample size n=200')
    # plotArrivalHourDistribution(vpActDict=thresholdDictN500, pTitle='Sample size n=500')

    # Plotting parking duration in minutes
    # plotParkDurationDistribution(vpActDict=sampleDict, paraName='Sample size', pTitle='Threshold t=0.8')
    # plotParkDurationDistribution(vpActDict=thresholdDictN100, pTitle='Sample size n=100')
    # plotParkDurationDistribution(vpActDict=thresholdDictN200, pTitle='Sample size n=200')
    # plotParkDurationDistribution(vpActDict=thresholdDictN500, pTitle='Sample size n=500')

    print('this is the end')


# FIXME: Continue with calculating averages, distributions and profiles for different categories
