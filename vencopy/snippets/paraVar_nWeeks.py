from vencopy.utils.globalFunctions import loadConfigDict, createOutputFolders
from vencopy.core.flexEstimators import WeekFlexEstimator
from vencopy.core.gridModelers import GridModeler
from vencopy.core.dataParsers import ParseMiD
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
# from vencopy.core.diaryBuilders import WeekDiaryBuilder

__version__ = '0.2.X'
__maintainer__ = 'Niklas Wulff'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '11.01.2022'
__status__ = 'test'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'

# Columns for debugging purposes
# ['genericID', 'parkID', 'tripID', 'actID', 'nextActID', 'prevActID', 'dayActID', 'timestampStart', 'timestampEnd']

if __name__ == '__main__':
    # Set dataset and config to analyze, create output folders
    datasetID = 'MiD17'
    configNames = ('globalConfig', 'localPathConfig', 'parseConfig', 'gridConfig', 'flexConfig', 'evaluatorConfig')
    basePath = Path(__file__).parent.parent
    configDict = loadConfigDict(configNames, basePath)
    createOutputFolders(configDict=configDict)

    vpData = ParseMiD(configDict=configDict, datasetID=datasetID)
    vpData.process(splitOvernightTrips=False)

    # Grid model application
    vpGrid = GridModeler(configDict=configDict, datasetID=datasetID, activities=vpData.activities,
                         gridModel='simple')
    vpGrid.assignGrid()

    class WeekDiaryBuilder:
        @profile(immediate=True)
        def __init__(self, activities: pd.DataFrame, catCols: list[str], seed: int = None):
            """
            Class that synthesizes weekly activity chains from daily activity chains. This is done for a specific set of
            categories that is determined by columns in the activities data set. The cross-product of the unique entries
            of each category column forms the number of categories for merging randomly drawn day activities within 
            each set. The user can then give the number of sampled weeks within each sample base that should be
            synthesized from the day chains. 

            :param activities: Activities dataset with daily activity chains. Has to contain all VencoPy internal column
            names.
            :param catCols: List of strings giving the column names that name the category dimensions as sample base.

            """
            self.activities = activities
            self.catCols = catCols
            self.weekdayIDs = list(range(1, 8))

            self.__days = None
            self.dayWeekMap = None
            self.weekActivities = None

            self.samplingCols = catCols.copy()
            self.samplingCols.append('tripStartWeekday')

            # Combinatorics of categories and weekdays
            self.categories = self.__retrieveUniqueCategories(acts=self.activities, catCols=self.catCols)
            self.categoryID = self.__createCategoryIndex(uCats=self.categories, catNames=self.catCols)
            self.sampleBaseID = self.__createSampleBases(cats=self.categories, samplingCols=self.samplingCols)

            # Merging uniqueIDs to activities - assigning day activity chains to sampling bases
            self.activities = self.__assignCategoryID(acts=self.activities, catIDs=self.categoryID,
                                                      catCols=self.catCols)
            self.activities = self.__assignSamplingBaseID(acts=self.activities, sampleBaseIDs=self.sampleBaseID)
            self.sampleBaseInAct = self.__subsetSampleBase()

        def __retrieveUniqueCategories(self, acts: pd.DataFrame, catCols: list[str]):
            return [set(acts.loc[:, c]) for c in catCols]

        def __createCategoryIndex(self, uCats: list, catNames: list[str]):
            mIdx = pd.MultiIndex.from_product(uCats, names=catNames)
            return self.__dfFromMultiIndex(multiIndex=mIdx, colname='categoryID')

        def __createSampleBases(self, cats: list[set], samplingCols: list[str]):
            levels = cats.copy()
            levels.append(self.weekdayIDs)
            mIdx = pd.MultiIndex.from_product(levels, names=samplingCols)
            return self.__dfFromMultiIndex(multiIndex=mIdx, colname='sampleBaseID')

        def __dfFromMultiIndex(self, multiIndex: pd.MultiIndex, colname: str = 'value'):
            df = pd.DataFrame(index=multiIndex)
            df = df.reset_index()
            df[colname] = df.index
            return df.set_index(multiIndex.names)

        def __assignCategoryID(self, acts: pd.DataFrame, catIDs: pd.Series, catCols: list[str]):
            acts.set_index(list(catIDs.index.names), inplace=True)
            acts = acts.merge(catIDs, left_index=True, right_index=True, how='left')
            return acts.reset_index(catCols)

        def __assignSamplingBaseID(self, acts: pd.DataFrame, sampleBaseIDs: pd.Series):
            idxCols = list(sampleBaseIDs.index.names)  # List of index column names
            acts.set_index(idxCols, inplace=True)
            acts = acts.merge(sampleBaseIDs, left_index=True, right_index=True, how='left')
            return acts.reset_index(idxCols)

        def __subsetSampleBase(self):
            """Filter the sample base to only the category combinations that really exist in the data set.

            Returns:
                pd.Series: A subset of self.sampleBaseID representing only the sample bases for the categories that
                exist and are thus not empty.
            """
            return self.sampleBaseID.loc[self.sampleBaseID['sampleBaseID'].isin(self.activities['sampleBaseID'])]

        def summarizeSamplingBases(self):
            print(f'There are {len(self.categoryID)} category combinations of the categories {self.catCols}.')
            print(f'There are {len(self.sampleBaseID)} sample bases (each category for every weekday).')

            nSBInAct = self.sampleBaseID['sampleBaseID'].isin(self.activities['sampleBaseID']).sum()
            print(f'Of those sample bases, {nSBInAct} category combinations exist in the activities data set')

            self.__days = self.activities.groupby(by=['genericID']).first()
            nSamplingBase = self.__days.groupby(by=self.samplingCols).count()
            sampleBaseLength = nSamplingBase.iloc[:, 0]

            smallestSampleBase = sampleBaseLength.loc[sampleBaseLength == min(sampleBaseLength)]
            largestSampleBase = sampleBaseLength.loc[sampleBaseLength == max(sampleBaseLength)]

            print(f'The number of samples in each sample base ranges from {smallestSampleBase}')
            print(f'to {largestSampleBase}.')
            print(f'The average sample size is approximately {sampleBaseLength.mean().round()}.')
            print(f'The median sample size is approximately {sampleBaseLength.median().round()}.')

        def __assignWeeks(self, nWeeks: int, how: str = 'random', seed: int = None, replace: bool = False):
            """Interface function to generate nWeeks weeks from the specified sample base. Here, the mapping of the 
            genericID to the respective weekID is generated

            Args:
                nWeeks (int): Number of weeks to sample from the sample bases defined by the elements of catCol
                how (str): Different sampling methods. Currently only random is implemented
                seed (int): Random seed for reproducibility
                replace (bool): In sampling, should it be possible to draw more samples than in the sampleBase? See
                docstring of randomSample for details.
            """

            if how == 'random':
                sample = self.__randomSample(nWeeks=nWeeks, seed=seed, replace=replace)
            elif how not in ['weighted', 'stratified']:
                raise NotImplementedError(f'The requested method {how} is not implemented. Please select "random", '
                                          f'"weighted" or "stratified".')
            return sample

        def __randomSample(self, nWeeks: int, seed: int = None, replace: bool = False) -> pd.DataFrame:
            """ Pulls nWeeks samples from each sample base. Each sample represents one day and is identified by one
            genericID. Per default only as many samples can be drawn from the sample base as there are days in the 
            original data set (replace=False). This can be overwritten by replace=True

            Args:
                nWeeks (int): Number of samples to be pulled out of each sampleBase
                seed (int, optional): Random seed for reproducibility. Defaults to None.
                replace (bool): If True, an infinite number of samples can be drawn from the sampleBase (German: Mit 
                zurücklegen), if False only as many as there are days in the sampleBase (no bootstrapping, German: Ohne
                zurücklegen). Defaults to False.

            Returns:
                pd.DataFrame: A pd.DataFrame with the three columns sampleBaseID, genericID and weekID where weekID is 
                a range object for each sampleBase, genericID are the samples. 
            """

            # Set seed for reproducibiilty for debugging
            if seed:
                np.random.seed(seed=seed)

            sample = pd.DataFrame()

            for sbID in self.sampleBaseInAct['sampleBaseID']:
                sampleBase = self.activities.loc[self.activities['sampleBaseID'] == sbID, 'genericID'].unique()
                subSample = np.random.choice(sampleBase, replace=replace, size=nWeeks)
                df = pd.DataFrame.from_dict({'sampleBaseID': sbID,
                                             'genericID': subSample,
                                             'weekID': list(range(nWeeks))})
                sample = pd.concat([sample, df])
            return sample

        @profile(immediate=True)
        def composeWeekActivities(self, nWeeks: int = 10, seed: int = None, replace: bool = False):
            """Wrapper function to call function for sampling each person (day mobility) to a specific week in a 
            specified category. activityID and genericID are adapted to cover the weekly pattern of the sampled mobility
            days within each week. 

            Args:
                nWeeks (int): Number of weeks to sample from the sample bases defined by the elements of catCol
                seed (int): Seed for random choice from the sampling bases for reproducibility
                replace (bool): In sampling, should it be possible to draw more samples than in the sampleBase? See
                docstring of randomSample for details.
            """

            print(f'Composing weeks for {nWeeks} choices from each sample base.')

            self.dayWeekMap = self.__assignWeeks(nWeeks=nWeeks, seed=seed, replace=replace)
            weekActs = self.__merge(dayWeekMap=self.dayWeekMap, dayActs=self.activities, index_col='genericID')
            weekActs = self.__adjustGenericID(acts=weekActs)
            weekActs = self.__orderViaWeekday(acts=weekActs)
            weekActs = self.__adjustActID(acts=weekActs)

            self.weekActivities = weekActs
            return weekActs

        def __merge(self, dayWeekMap: pd.DataFrame, dayActs: pd.DataFrame, index_col: str) -> pd.DataFrame:
            """Utility function to merge two dataframes on a column, via more performant index merging. Indices will 
            be reset before returning the merged DataFrame.

            Args:
                left (pd.DataFrame): Left DataFrame for merging
                right (pd.DataFrame): Right DataFrame for merging
                index_col (str): String that determines a column in both DataFrames

            Returns:
                pd.DataFrame: Merged DataFrame
            """

            dayWeekMapIdx = dayWeekMap.set_index(index_col)
            dayActsIdx = dayActs.set_index(index_col)
            merged = dayWeekMapIdx.merge(dayActsIdx, left_index=True, right_index=True)
            return merged.reset_index(index_col)

        def __adjustGenericID(self, acts: pd.DataFrame) -> pd.DataFrame:
            """Replaces the generic ID that characterized a specific person living in a specific household (MiD) or 
            a specific vehicle (KID) and its respective daily travel patterns with a combination of 7 genericIDs - 
            one for every day of the week determined by the categories given in catCols in the class instantiation.
            The genericID column does not have to be stored elsewhere since hhPersonID exists as a separate column.

            Args:
                acts (pd.DataFrame): Activity data set at least with the columns 'genericID' and 'weekID'
            """
            acts['dayGenericID'] = acts['genericID']
            acts['genericID'] = (acts['categoryID'].apply(str) + acts['weekID'].apply(str)).apply(int)
            return acts

        def __orderViaWeekday(self, acts) -> pd.DataFrame:
            return acts.sort_values(by=['genericID', 'tripStartWeekday'])

        def __adjustActID(self, acts: pd.DataFrame) -> pd.DataFrame:
            """Replaces the activityID with a weekly identifier so that activities count up all the way within a 
            sampled week instead of just in a day. Day activity IDs will be stored for debugging in a separate column.

            Args:
                acts (pd.DataFrame): Activity data set.
            """

            acts = self.__mergeParkActs(acts=acts)
            acts = self.__reassignParkIDs(acts=acts)
            acts = self.__reassignTripIDs(acts=acts)
            acts = self.__reassignActIDs(acts=acts)

            print('Finished weekly activity ID chaining')
            return acts

        def __mergeParkActs(self, acts: pd.DataFrame) -> pd.DataFrame:
            """ In a week activity data set, merge the last parking of a previous day with the first parking of the next
            day to one activity spanning two days.

            Args:
                acts (pd.DataFrame): Activity data set, where the week is identified by the column genericID and the 
                day via the column tripStartWeekday
            """

            # Calculate shifted columns for merging last and first day park acts and updating lastAct col
            acts, nextVars = self.__addNextActVars(acts=acts,
                                                   vars=['genericID', 'parkID', 'timestampEnd', 'tripPurpose'])
            acts = self.__adjustEndTimestamp(acts=acts)
            acts = self.__addONParkVariable(acts=acts)
            acts = self.__neglectFirstParkActs(acts=acts)  # only for weekdays TUE-SUN
            acts = self.__updateLastWeekActs(acts=acts)
            acts = self.__removeNextActVarCols(acts=acts, nextVars=nextVars)
            print('Finished last and first daily parking to one parking activity')
            return acts

        def __addNextActVars(self, acts: pd.DataFrame, vars: list[str]) -> (pd.DataFrame, list[str]):
            vars_next = [v + '_next' for v in vars]
            acts[vars_next] = acts[vars].shift(-1)
            return acts, vars_next

        def __adjustEndTimestamp(self, acts: pd.DataFrame) -> pd.DataFrame:
            acts.loc[self.__getLastParkActsWOSun(acts),
                     'timestampEnd'] = acts.loc[self.__getLastParkActsWOSun(acts),
                                                'timestampEnd_next']
            return acts

        def __addONParkVariable(self, acts: pd.DataFrame) -> pd.DataFrame:
            """Adds a column for merged park activities. This is important later for the maximum charged energy at the 
            ON parking activities because the timestamps do not have the same day. For the calculation of the length of
            the parking activity, and consequently the maximum electricity that can be charged, this has to be taken 
            into account. 

            Args:
                acts (pd.DataFrame): Activity data set 

            Returns:
                pd.DataFrame: Activity data set with the added column 'isSyntheticONPark'
            """

            acts['isSyntheticONPark'] = False
            acts.loc[self.__getLastParkActsWOSun(acts), 'isSyntheticONPark'] = True
            return acts

        def __neglectFirstParkActs(self, acts: pd.DataFrame) -> pd.DataFrame:
            """Removes all first parking activities with two exceptions: First parking on Mondays will be kept since 
            they form the first activities of the week (genericID). Also, first parking activities directly after trips
            that end exactly at 00:00 are kept. 

            Args:
                acts (pd.DataFrame): Activities with daily first activities for every day activity chain

            Returns:
                pd.DataFrame: Activities with weekly first activities for every week activity chain
            """

            # Exclude the second edge case of previous trip activities ending exactly at 00:00. In that case the
            # previous activity is a trip not a park, so the identification goes via type checking of shifted parkID.
            acts['parkID_prev'] = acts['parkID'].shift(1)
            idxToNeglect = (self.__getFirstParkActsWOMon(acts)) & ~(acts['parkID_prev'].isna())

            # Set isFirstActivity to False for the above mentioned edge case
            idxSetFirstActFalse = (acts['isFirstActivity']) & ~(acts['tripStartWeekday'] == 1)
            acts.loc[idxSetFirstActFalse, 'isFirstActivity'] = False

            return acts.loc[~idxToNeglect, :].drop(columns=['parkID_prev'])

        def __getFirstParkActsWOMon(self, acts: pd.DataFrame) -> pd.Series:
            """ Select all first park activities except the first activities of Mondays - those will be the first 
            activities of the week and thus remain unchanged. 

            Args:
                acts (pd.DataFrame): Activities data set with at least the columns 'parkID', 'isFirstActivity' and 
                'tripStartWeekday'

            Returns:
                pd.Series: A boolean Series which is True for all first activities except the ones on the first day of the 
            week, i.e. Mondays.
            """

            return (~acts['parkID'].isna()) & (acts['isFirstActivity']) & ~(acts['tripStartWeekday'] == 1)

        def __getLastParkActsWOSun(self, acts) -> pd.Series:
            """ Select all last park activities except the last activities of Sundays - those will be the last 
            activities of the week and thus remain unchanged. 

            Args:
                acts (pd.DataFrame): Activities data set with at least the columns 'parkID', 'isLastActivity' and 
                'tripStartWeekday'

            Returns:
                pd.Series: A boolean Series which is True for all last activities except the ones on the first day of 
                the week, i.e. Sundays.
            """

            return (~acts['parkID'].isna()) & (acts['isLastActivity']) & ~(acts['tripStartWeekday'] == 7)

        def __updateLastWeekActs(self, acts: pd.DataFrame) -> pd.DataFrame:
            """ Updates the column isLastActivity for a week diary after merging 7 day activity chains to one week 
            activity chain. 

            Args:
                acts (pd.DataFrame): Activity data set that has to at least have the columns 'isLastActivity', 
                'genericID' and 'nextGenericID'. The last one should be available from before by 
                acts['genericID'].shift(-1)

            Returns:
                pd.DataFrame: The activity data set with an ammended column 'isLastActivity' updated for the week chain
            """

            isLastWeekAct = (acts['isLastActivity']) & (acts['genericID'] != acts['genericID_next'])
            acts.loc[:, 'isLastActivity'] = isLastWeekAct
            return acts

        def __removeNextActVarCols(self, acts: pd.DataFrame, nextVars: list[str]) -> pd.DataFrame:
            return acts.drop(columns=nextVars)

        def __reassignParkIDs(self, acts: pd.DataFrame) -> pd.DataFrame:
            """ Resets the activitiy IDs of the day park chain (increasing acts per Day) with continuously increasing 
            actvity IDs per week identified by genericID. The day activity IDs will be stored in a separate column 
            'dayActID'

            Args:
                acts (pd.DataFrame): The park activities of the activity data set at least with the columns 
                'genericID' and 'actID'

            Returns:
                pd.DataFrame: Activities data set with week actIDs and a new column 'dayActID' with the day
                activity IDs. 
            """

            parkIdx = ~acts['parkID'].isna()
            acts.loc[parkIdx, 'dayActID'] = acts['parkID']

            acts.loc[parkIdx, 'parkID'] = acts.loc[parkIdx, ['genericID', 'parkID']].groupby(
                by='genericID').apply(lambda week: pd.Series(range(len(week)))).values
            return acts

        def __reassignTripIDs(self, acts: pd.DataFrame) -> pd.DataFrame:
            """ Resets the activitiy IDs of the day trip chain (increasing acts per day) with continuously increasing 
            actvity IDs per week identified by genericID. The day activity IDs will be stored in a separate column 
            'dayActID'

            Args:
                acts (pd.DataFrame): The trip activities of the activity data set at least with the columns 
                'genericID', 'tripID' and 'actID'

            Returns:
                pd.DataFrame: Activities data set with week actIDs and a new column 'dayActID' with the day
                activity IDs. 
            """

            tripIdx = ~acts['tripID'].isna()
            acts.loc[tripIdx, 'dayActID'] = acts['tripID']

            acts.loc[tripIdx, 'tripID'] = acts.loc[tripIdx, ['genericID', 'parkID']].groupby(
                by='genericID').apply(lambda week: pd.Series(range(len(week)))).values
            return acts

        def __reassignActIDs(self, acts: pd.DataFrame) -> pd.DataFrame:
            """Reassigns the column actID from tripID and parkID and updates next and prevActID accordingly

            Args:
                acts (pd.DataFrame): Activities data set with weekly tripIDs and parkIDs

            Returns:
                pd.DataFrame: Activities with updated column actID, prevActID and nextActID
            """

            # FIXME: Improve performance, currently only this func takes 43 seconds
            acts.loc[~acts['tripID'].isna(), 'actID'] = acts.loc[:, 'tripID']
            acts.loc[~acts['parkID'].isna(), 'actID'] = acts.loc[:, 'parkID']
            acts.loc[~acts['isLastActivity'], 'nextActID'] = acts.loc[:, 'actID'].shift(-1)
            acts.loc[~acts['isFirstActivity'], 'prevActID'] = acts.loc[:, 'actID'].shift(1)
            return acts

    # Week diary building
    vpWDB = WeekDiaryBuilder(activities=vpGrid.activities, catCols=['bundesland', 'areaType'])
    vpWDB.summarizeSamplingBases()

    def composeMultiSampleDict(
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

    # Sampling of multiple weeks varying sample size
    loadSamplesFromPickle = True
    if not loadSamplesFromPickle:
        nWeeks = [10, 50, 100, 500, 1000]
        sampleDict = composeMultiSampleDict(nWeeks=nWeeks, weekDiaryBuilder=vpWDB, seed=42, replace=True, threshold=0.8)
        pickle.dump(sampleDict, open('sampleDictT0.8.p', 'wb'))
    else:
        sampleDict = pickle.load(open('sampleDictT0.8.p', 'rb'))

    # Sampling of multiple weeks varying plugin threshold
    loadThresholdsFromPickle = True
    if not loadThresholdsFromPickle:
        thresholds = [0.7, 0.8, 0.9, 1]
        thresholdDictN100 = composeMultiThresholdDict(nWeeks=100, weekDiaryBuilder=vpWDB, seed=42, replace=True,
                                                      threshold=thresholds)
        thresholdDictN200 = composeMultiThresholdDict(nWeeks=200, weekDiaryBuilder=vpWDB, seed=42, replace=True,
                                                      threshold=thresholds)
        thresholdDictN500 = composeMultiThresholdDict(nWeeks=500, weekDiaryBuilder=vpWDB, seed=42, replace=True,
                                                      threshold=thresholds)

        pickle.dump(thresholdDictN100, open('thresholdDictN100.p', 'wb'))
        pickle.dump(thresholdDictN200, open('thresholdDictN200.p', 'wb'))
        pickle.dump(thresholdDictN500, open('thresholdDictN500.p', 'wb'))
    else:
        thresholdDictN100 = pickle.load(open('thresholdDictN100.p', 'rb'))
        thresholdDictN200 = pickle.load(open('thresholdDictN200.p', 'rb'))
        thresholdDictN500 = pickle.load(open('thresholdDictN500.p', 'rb'))

    # Plotting uncontrolled charging for multiple sample sizes and thresholds
    plotDistribution(vpActDict=sampleDict, var='uncontrolledCharge', subset='park')
    # plotDistribution(vpActDict=thresholdDictN100, var='uncontrolledCharge', subset='park', lTitle='Threshold',
    #                  pTitle='Sample size n=100')
    # plotDistribution(vpActDict=thresholdDictN200, var='uncontrolledCharge', subset='park', lTitle='Threshold',
    #                  pTitle='Sample size n=200')
    # plotDistribution(vpActDict=thresholdDictN500, var='uncontrolledCharge', subset='park', lTitle='Threshold',
    #                  pTitle='Sample size n=500')

    # Plotting weekday distribution
    plotDistribution(vpActDict=sampleDict, var='weekdayStr', subset='trip')
    # plotDistribution(vpActDict=thresholdDictN100, var='weekdayStr', subset='trip', lTitle='Threshold')

    # Plotting charging power distribution
    plotDistribution(vpActDict=sampleDict, var='chargingPower', subset='park')
    # plotDistribution(vpActDict=thresholdDictN100, var='chargingPower', subset='park', lTitle='Threshold')

    # Plotting arrival hour
    plotArrivalHourDistribution(vpActDict=sampleDict, paraName='Sample size', pTitle='Threshold t=0.8')
    # plotArrivalHourDistribution(vpActDict=thresholdDictN100, pTitle='Sample size n=100')
    # plotArrivalHourDistribution(vpActDict=thresholdDictN200, pTitle='Sample size n=200')
    # plotArrivalHourDistribution(vpActDict=thresholdDictN500, pTitle='Sample size n=500')

    # Plotting parking duration in minutes
    plotParkDurationDistribution(vpActDict=sampleDict, paraName='Sample size', pTitle='Threshold t=0.8')
    # plotParkDurationDistribution(vpActDict=thresholdDictN100, pTitle='Sample size n=100')
    # plotParkDurationDistribution(vpActDict=thresholdDictN200, pTitle='Sample size n=200')
    # plotParkDurationDistribution(vpActDict=thresholdDictN500, pTitle='Sample size n=500')

    print('this is the end')


# FIXME: Continue with calculating averages, distributions and profiles for different categories
