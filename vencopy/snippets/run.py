import sys
import numpy as np
import pandas as pd
import numpy as np
from pathlib import Path
from itertools import product
from profilehooks import profile

# Needed to run in VSCode properties currently
sys.path.append('.')

from vencopy.core.dataParsers import ParseMiD
from vencopy.core.gridModelers import GridModeler
from vencopy.core.flexEstimators import FlexEstimator
from vencopy.utils.globalFunctions import loadConfigDict, createOutputFolders



# from vencopy.core.diaryBuilders import WeekDiaryBuilder


__version__ = '0.2.X'
__maintainer__ = 'Niklas Wulff'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '11.01.2022'
__status__ = 'test'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'


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
        def __init__(self, activities: pd.DataFrame, catCols: list[str]):
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
            self.sampleBaseInAct = self.__subsetSampleBase(acts=self.activities, sampleBaseIDs=self.sampleBaseID)

            # Merging sampled householdPersonIDs to activity data set
            # FIXME: Overwrite self.activities later with correct columns
            self.weekActivities = self.__composeWeekActivities()

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

        def __subsetSampleBase(self, acts: pd.DataFrame, sampleBaseIDs: pd.Series):
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

        def __assignWeeks(self, nWeeks: int, how: str = 'random'):
            """Interface function to generate nWeeks weeks from the specified sample base. Here, the mapping of the 
            genericID to the respective weekID is generated

            Args:
                nWeeks (int): Number of weeks to sample from the sample bases defined by the elements of catCol
            """

            if how == 'random':
                sample = self.__randomSample(nWeeks=nWeeks)
            elif how not in ['weighted', 'stratified']:
                raise NotImplementedError(f'The requested method {how} is not implemented. Please select "random", '
                                          f'"weighted" or "stratified".')
            return sample

        def __randomSample(self, nWeeks: int):
            sample = pd.DataFrame()
            for sbID in self.sampleBaseInAct['sampleBaseID']:
                sampleBase = self.activities.loc[self.activities['sampleBaseID'] == sbID, 'genericID']
                subSample = np.random.choice(sampleBase, replace=False, size=nWeeks)
                df = pd.DataFrame.from_dict({'sampleBaseID': sbID,
                                             'genericID': subSample,
                                             'weekID': list(range(nWeeks))})
                sample = pd.concat([sample, df])
            return sample

        def __composeWeekActivities(self):
            """Wrapper function to call function for sampling each person (day mobility) to a specific week in a 
            specified category. activityID and genericID are adapted to cover the weekly pattern of the sampled mobility
            days within each week. 
            """

            dayWeekMap = self.__assignWeeks(nWeeks=10)
            weekActs = self.__merge(dayWeekMap=dayWeekMap, dayActs=self.activities, index_col='genericID')
            weekActs = self.__adjustGenericID(acts=weekActs)
            weekActs = self.__orderViaWeekday(acts=weekActs)
            weekActs = self.__adjustActID(acts=weekActs)

        def __merge(self, dayWeekMap: pd.DataFrame, dayActs: pd.DataFrame, index_col: str):
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

        def __adjustGenericID(self, acts: pd.DataFrame):
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

        def __orderViaWeekday(self, acts):
            return acts.sort_values(by=['genericID', 'tripStartWeekday'])

        def __adjustActID(self, acts: pd.DataFrame):
            """Ammends the activityID column with a week identifier so that activities count up all the way within a 
            sampled week instead of just in a day. Day activity IDs will be stored for debugging in a separate column.

            Args:
                acts (pd.DataFrame): Activity data set.
            """

            acts = self.__mergeParkActs(acts)
            acts = self.__reassignParkActIDs(acts)
            acts = self.__reassignTripActIDs(acts)

            return acts

        def __mergeParkActs(self, acts: pd.DataFrame):
            """ In a week activity data set, merge the last parking of a previous day with the first parking of the next
            day to one activity spanning two days.

            Args:
                acts (pd.DataFrame): Activity data set, where the week is identified by the column genericID and the 
                day via the column tripStartWeekday
            """
            # Exclude Mondays from merge
            boolIdxFirst = (~acts['parkID'].isna()) & (acts['isFirstActivity']) & ~(acts['tripStartWeekday'] == 1)
            boolIdxLast = (~acts['parkID'].isna()) & (acts['isLastActivity']) & ~(acts['tripStartWeekday'] == 7)
            firstParkActsToMerge = acts.loc[boolIdxFirst, :]

            # Adjust timestampStart
            for w in self.weekdayIDs[1:]:
                firstParkActsToMerge.loc[firstParkActsToMerge.loc[:, 'tripStartWeekday'] == w,
                                         'timestampStart'] = acts.loc[
                                             (boolIdxLast) & (acts.loc[:, 'tripStartWeekday'] == w-1), 'timestampEnd']  # .values?

            # Adjust purpose

            # Adjust isFirst and isLastActivity

            # FIXME: Cope with overnight trips

            # Get rid of lastPark activity

    # Week diary building
    vpWDB = WeekDiaryBuilder(activities=vpGrid.activities, catCols=['bundesland', 'areaType'])
    vpWDB.summarizeSamplingBases()

    # Estimate charging flexibility based on driving profiles and charge connection
    vpFlex = FlexEstimator(configDict=configDict, datasetID=datasetID, activities=vpGrid.activities)
    vpFlex.estimateTechnicalFlexibility()
