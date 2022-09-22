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
from vencopy.core.flexEstimators import WeekFlexEstimator
from vencopy.utils.globalFunctions import loadConfigDict, createOutputFolders



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
            self.sampleBaseInAct = self.__subsetSampleBase(acts=self.activities, sampleBaseIDs=self.sampleBaseID)

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

        def __assignWeeks(self, nWeeks: int, how: str = 'random', seed: int = None):
            """Interface function to generate nWeeks weeks from the specified sample base. Here, the mapping of the 
            genericID to the respective weekID is generated

            Args:
                nWeeks (int): Number of weeks to sample from the sample bases defined by the elements of catCol
                how (str): Different sampling methods. Currently only random is implemented
                seed (int): Random seed for reproducibility
            """

            if how == 'random':
                sample = self.__randomSample(nWeeks=nWeeks, seed=seed)
            elif how not in ['weighted', 'stratified']:
                raise NotImplementedError(f'The requested method {how} is not implemented. Please select "random", '
                                          f'"weighted" or "stratified".')
            return sample

        def __randomSample(self, nWeeks: int, seed: int = None):
            # Set seed for reproducibiilty for debugging
            if seed:
                np.random.seed(seed=seed)

            sample = pd.DataFrame()

            for sbID in self.sampleBaseInAct['sampleBaseID']:
                sampleBase = self.activities.loc[self.activities['sampleBaseID'] == sbID, 'genericID']
                subSample = np.random.choice(sampleBase, replace=False, size=nWeeks)
                df = pd.DataFrame.from_dict({'sampleBaseID': sbID,
                                             'genericID': subSample,
                                             'weekID': list(range(nWeeks))})
                sample = pd.concat([sample, df])
            return sample

        @profile(immediate=True)
        def composeWeekActivities(self, nWeeks: int, seed: int = None):
            """Wrapper function to call function for sampling each person (day mobility) to a specific week in a 
            specified category. activityID and genericID are adapted to cover the weekly pattern of the sampled mobility
            days within each week. 
            
            Args:
                nWeeks (int): Number of weeks to sample from the sample bases defined by the elements of catCol
                seed (int): Seed for random choice from the sampling bases for reproducibility
            """

            self.dayWeekMap = self.__assignWeeks(nWeeks=10, seed=seed)
            weekActs = self.__merge(dayWeekMap=self.dayWeekMap, dayActs=self.activities, index_col='genericID')
            weekActs = self.__adjustGenericID(acts=weekActs)
            weekActs = self.__orderViaWeekday(acts=weekActs)
            weekActs = self.__adjustActID(acts=weekActs)

            self.weekActivities = weekActs

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
    vpWDB.composeWeekActivities(seed=42, nWeeks=1000)

    # Estimate charging flexibility based on driving profiles and charge connection
    vpWeFlex = WeekFlexEstimator(configDict=configDict, datasetID=datasetID, activities=vpWDB.weekActivities)
    vpWeFlex.estimateTechnicalFlexibility()

    print('end break')