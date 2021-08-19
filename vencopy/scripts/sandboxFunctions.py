__version__ = '0.0.9'
__maintainer__ = 'Niklas Wulff 31.12.2019'
__contributors__ = 'Fabia Miorelli, Parth Butte'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '31.12.2019'
__status__ = 'dev'  # options are: dev, test, prod


import io
import getpass
import pandas as pd
from zipfile import ZipFile
import numpy as np
import pathlib


def assignMultiColToDType(dataFrame, cols, dType):
    dictDType = dict.fromkeys(cols, dType)
    dfOut = dataFrame.astype(dictDType)
    return(dfOut)


def harmonizeVariables(data, dataset, config):
    replacementDict = createReplacementDict(dataset, config['dataVariables'])
    dataRenamed = data.rename(columns=replacementDict)
    if dataset == 'MiD08':
        dataRenamed['hhPersonID'] = dataRenamed['hhid'].astype('string') + '__' + \
                                    dataRenamed['hhPersonID'].astype('string')
    return dataRenamed


def createReplacementDict(dataset, dictRaw):
    if dataset in dictRaw['dataset']:
        listIndex = dictRaw['dataset'].index(dataset)
        return {val[listIndex]: key for (key, val) in dictRaw.items()}
    else:
        raise ValueError(f'Dataset {dataset} not specified in MiD variable dictionary.')


def removeNA(variables:list):
    variables.remove('NA')
    if 'NA' in variables:
        removeNA(variables)


def createListOfVariables(dataset:str, config):
    listIndex = config['dataVariables']['dataset'].index(dataset)
    variables = [key if not val[listIndex] == 'NA' else 'NA' for key, val in config['dataVariables'].items()]
    variables.remove('dataset')
    if 'NA' in variables:
        removeNA(variables)
    return variables


def filterOutTripsBelongingToMultiModalDays(data):
    idsWFirstTrip = data.loc[data.loc[:, 'tripID'] == 1, 'hhPersonID']
    return data.loc[data.loc[:, 'hhPersonID'].isin(idsWFirstTrip), :]

def replaceTripsBeforeFirstTrips(driveData, replacement:str):
    firstTripIdx = driveData.loc[~driveData.isnull()].index[0]
    driveData.loc[range(0, firstTripIdx)] = replacement
    return driveData

def assignHome(driveData):
    pass

def assignPurpose(driveData, tripData):
    firstHour = tripData['tripStartHour']
    lastHour = tripData['tripEndHour']
    tripPurpose = tripData['tripPurpose']

def initiateColRange(self, row):
    if row['tripStartHour'] + 1 < row['tripEndHour']:
        return range(row['tripStartHour'] + 1, row[
                    'tripEndHour'])  # The hour of arrival (tripEndHour) will not be indexed further below but is part of the range() object
    else:
        return None

class FillTripPurposes:
    def __init__(self, tripData, mergedDayTrips, rangeFunction=initiateColRange):
        self.startHour = tripData['tripStartHour']
        self.endHour = tripData['tripEndHour']
        self.tripHourCols = tripData.apply(rangeFunction, axis=1)
        self.purpose = tripData['tripPurpose']
        self.tripDict = tripData.loc[:, ['hhPersonID', 'tripID']].groupby(['hhPersonID']).list()

    def __call__(self, row):
        idx = row.name
        row[self.startHour[idx]] = 'DRIVING'
        if self.endHour[idx] != self.startHour[idx]:
            row[self.endHour[idx]] = self.distanceEndHour[idx]
        if isinstance(self.fullHourCols[idx], range):
            row[self.fullHourCols[idx]] = self.fullHourRange[idx]
        return row

def hoursToDatetime(tripData):
    tripData.loc[:, 'W_SZ_datetime'] = pd.to_datetime(tripData.loc[:, 'tripStartClock'])
    tripData.loc[:, 'W_AZ_datetime'] = pd.to_datetime(tripData.loc[:, 'tripEndClock'])
    return tripData


# def writeOut(config, datasetID, dataDrive, dataPurpose):
#     dataDrive.to_csv(Path(config['pathRelative']['input']) /
#                      createFileString(config=config, fileKey='inputDataDriveProfiles', dataset=datasetID), na_rep=0)
#     dataPurpose.to_csv(Path(config['pathRelative']['input']) /
#                        createFileString(config=config, fileKey='purposesProcessed', dataset=datasetID))
#     print(f"Drive data and trip purposes written to files "
#           f"{createFileString(config=config, fileKey='inputDataDriveProfiles', dataset=datasetID)} and"
#           f"{createFileString(config=config, fileKey='purposesProcessed', dataset=datasetID)}")
#

def readZipData(filePath):
    """
    Opening the zip file in READ mode and transform scalars.csv to data frame
    :param filePath: path to zip-file
    :return: data frame with scalars.csv content
    """
    with ZipFile(filePath.as_posix(), 'r') as zip:
        scalars = None
        for i in zip.namelist():
            if i.endswith('Scalars.csv'):
                scalars = i
                break
        print('Reading', scalars)
        if scalars is None:
            print('No scalars file exists in zip file!')
            return pd.DataFrame()
        scalars = zip.read(scalars)
        # allow colon and semicolon as separators
        df = pd.read_csv(io.BytesIO(scalars), sep=',|;')
    return df


def readEncryptedFile(filePath, fileName):
    print('Starting extraction of encrypted zipfile. Password required')
    pw = getpass.getpass(stream=None)
    with ZipFile(filePath, 'r') as zip:
        for iFile in zip.namelist():
            if iFile.endswith(fileName):
                #trips = zip.read(iFile, pwd=bytes(pw, 'utf-8'))
                trips = zip.read(iFile, pwd=pw)
                if fileName.endswith('.csv'):
                    return pd.read_csv(io.BytesIO(trips))
                elif fileName.endswith('.dta'):
                    return pd.read_stata(io.BytesIO(trips))


def returnBottomKeys(self, baseDict: dict, lst: list = []):
    for iKey, iVal in baseDict.items():
        if isinstance(iVal, dict):
            self.returnValueList(iVal, lst)
        else:
            lst.append(iKey)
    return lst




def writeAnnualOutputForREMix(profileDict, outputConfig, outputPath, noOfHoursOutput, technologyLabel, strAdd):
    """
    Output wrapper function to call cloneAndWriteProfile once for each output profile.

    :param profileDict: Dictionary holding profile names and profiles in pd.Series to be cloned and written
    :param outputConfig: REMix specific configuration file holding model nodes
    :param outputPath: path to output folder
    :param noOfHoursOutput: Integer describing the number of hours that the profiles are cloned to
    :param technologyLabel: String holding a REMix eCarsDtl technology label
    :param strAdd: String addition for output writing
    :return: None
    """
    for iName, iProf in profileDict.items():
        filename = technologyLabel + '_' + iName + strAdd
        cloneAndWriteProfile(iProf, outputConfig, outputPath, noOfHoursOutput, technologyLabel, filename)



def cloneAndWriteProfile(profile, outputConfig, outputPath, noOfHoursOutput, technologyLabel, filename):
    """
    This action clones daily profiles to cover the specified time horizon given in noOfHoursOutput.

    :param profileDict: A dictionary holding five VencoPy profiles as Series including their names as keys.
    :param pathDict: A VencoPy path dictionary.
    :param noOfHoursOutput: Number of hours to clone the daily profile to (for 1 (non-gap-)year set to 8760)
    :param technologyLabel: Technology (e.g. vehicle segment "BEV-S") label for the filename that is written.
    :param filename: Name of the file to be written.
    :return: None.
    """

    df = createEmptyDataFrame(technologyLabel, noOfHoursOutput, outputConfig['Nodes'])
    # review is this correct? What happens when noOfHoursOutput/len(profile) is smaller then 0? Then noOfClones
    # would be negative and I am not sure if this would be coerced to 0 by the following int type cast later on.
    # Is this handled upstream in the call chain?
    noOfClones = noOfHoursOutput / len(profile) - 1

    # FixMe the int type cast could have a nasty side effect, as it is behaving like a floor operation
    # for the float division above. Is this intended?
    profileCloned = profile.append([profile] * int(noOfClones), ignore_index=True)

    if len(profileCloned) < noOfHoursOutput:
        subHours = noOfHoursOutput - len(profileCloned)
        profileCloned = profileCloned.append(profile[range(subHours)], ignore_index=True)

    # FixMe this .copy() seems to be redundant if createEmptyDataFrame above indeed creates a fresh new empty
    # dataframe. Am I missing something here?
    profilesOut = df.copy()
    for i in outputConfig['NonNullNodes']:
        profilesOut.loc[:, i] = np.round(profileCloned, 3)

    profilesOut.to_csv(outputPath / pathlib.Path(filename + '.csv'), index=False)


def createEmptyDataFrame(technologyLabel, numberOfHours, nodes):
    """
    Creation method for building a specifically formatted dataframe for output processing of VencoPy profiles.

    :param technologyLabel: String for an index column
    :param numberOfHours: Length of resulting dataframe
    :param nodes: Number of columns of resultung dataframe
    :return: Empty dataframe with the technologyLabel as values in the first column, number of rows as specified by
    numberOfHours. Nodes gives number of value columns.
    """

    df = pd.concat([pd.DataFrame([i], columns=['']) for i in range(1, numberOfHours + 1)], ignore_index=True)
    df[' '] = technologyLabel  # Add technology column
    df = df[[' ', '']]  # Re-arrange columns order

    # review if nodes is a list of column labels then one could also write it like this:
    # df[nodes] = 0 instead of the explicit loop.
    # I am not 100% sure of the syntax but there is a way to write this without a loop.
    # Should be detailed in pandas indexing docu
    for i in nodes:
        df[i] = 0

    s = df[''] < 10
    s1 = (df[''] >= 10) & (df[''] < 100)
    s2 = (df[''] >= 100) & (df[''] < 1000)
    s3 = df[''] >= 1000

    # review: there exists the python string formatting mini language which provides padding of strings (also leading).
    # see here: https://docs.python.org/3.4/library/string.html#format-specification-mini-language
    #  I think with a format string of the shape 't'+'{0:0<4.0d}'.format(x) would result for all four lines below in
    #  the correct output. Then also lines 894 to 897 would be superfluous.

    df.loc[s, ''] = df.loc[s, ''].apply(lambda x: "{}{}".format('t000', x))
    df.loc[s1, ''] = df.loc[s1, ''].apply(lambda x: "{}{}".format('t00', x))
    df.loc[s2, ''] = df.loc[s2, ''].apply(lambda x: "{}{}".format('t0', x))
    df.loc[s3, ''] = df.loc[s3, ''].apply(lambda x: "{}{}".format('t', x))
    return df

def appendREMixProfiles(pre, names, post, pathFiles, pathOutput, outputPre, outputPost):
    """
    REMix specific append functionality to integrate results of three different VencoPy-runs into one file per profile.

    :param pre: String part before profile name
    :param names: list of profile names
    :param post: String part after profile name
    :param pathFiles: path to folder of files
    :param pathOutput: path to appended file
    :param outputPre: String before profile name for output
    :param outputPost: String after profile name for output
    :return: None
    """

    strDict = composeStringDict(pre, names, post)
    dataDict = {}
    for key, strList in strDict.items():
        dfList = []
        for strIdx in strList:
            df = pd.read_csv(pathFiles / strIdx)
            df.ix[df.iloc[:, 0] == 'BEV', 0] = strIdx[0:5]
            df.rename(columns={'Unnamed: 1': ' '}, inplace=True)
            dfList.append(df)
        dataDict[key] = dfList

    resultDict = {}
    for key, value in dataDict.items():
        resultDict[key] = pd.concat(value)
        resultDict[key].to_csv(index=False,
                               path_or_buf=pathOutput / pathlib.Path(outputPre + key + outputPost + '.csv'),
                               float_format='%.3f')


def composeStringDict(pre, names, post):
    dict = {}
    for nIdx in names:
        listStr = []
        for preIdx, postIdx in zip(pre, post):
            str = preIdx + nIdx + postIdx + '.csv'
            listStr.append(str)
        dict[nIdx] = listStr
    return dict

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

def determinePurposeHourRange(self, departure, arrival):
    tripDuration = arrival - departure
    startHour = self.determinePurposeStartHour(departure, tripDuration)
    return range(startHour, self.endHour)

def readInputScalars(self, filePath) -> pd.DataFrame:
    """
    Method that gets the path to a venco scalar input file specifying technical assumptions such as battery capacity
    specific energy consumption, usable battery capacity share for load shifting and charge power.

    :param filePath: The relative file path to the input file
    :return: Returns a dataframe with an index column and two value columns. The first value column holds numbers the
        second one holds units.
    """
    inputRaw = pd.read_excel(filePath,
                             header=5,
                             usecols='A:C',
                             skiprows=0,
                             engine='openpyxl')
    scalarsOut = inputRaw.set_index('parameter')
    return scalarsOut


def calcNTripsPerDay(self):
   """
   :return: Returns number of trips trips per household person per day
   """
   return self.data['hhPersonID'].value_counts().mean()

def calcDailyTravelDistance(self):
    """
    :return: Returns daily travel distance per household
    """
    dailyDistances = self.data.loc[:, ['hhPersonID', 'tripDistance']].groupby(by=['hhPersonID']).sum()
    return dailyDistances.mean()

def calcDailyTravelTime(self):
    """
    :return: Returns daily travel time per household person
    """
    travelTime = self.data.loc[:, ['hhPersonID', 'travelTime']].groupby(by=['hhPersonID']).sum()
    return travelTime.mean()

def calcAverageTripDistance(self):
    """
    :return: Returns daily average trip distance
    """
    return self.data.loc[:, 'tripDistance'].mean()
### EXPERIMENTAL SANDBOX PART
# def fillDayPurposesPerformant(tripData, purposeDataDays):  #FixMe: Ask Ben for performance improvements
#     # This is an adaptation of fillDayPurposes()
#
# def merger(tripData, purposeDataDays):
#     hpID = str()
#     maxWID = int()
#     maxHour = len(purposeDataDays.columns)
#     for idx, iRow in tripData.iterrows():
#         isSameHPID = hpID == iRow['hhPersonID']
#         if not isSameHPID:
#             hpID = iRow['hhPersonID']
#             allWIDs = list(tripData.loc[tripData['hhPersonID'] == hpID, 'tripID'])
#             minWID = min(allWIDs)
#             maxWID = max(allWIDs)
#
#         idxOld = idx
#     return purposeDataDays
#
# def filler(tripData, purposeDataDays):
#     for idx, iRow in tripData.iterrows():
#         if iRow['tripID'] == 1:  # Differentiate if trip starts in first half hour or not
#             if iRow['timestampStart'].minute <= 30:
#                 purposeDataDays.loc[hpID, range(0, iRow['tripStartHour'])] = 'HOME'
#             else:
#                 purposeDataDays.loc[hpID, range(0, iRow['tripStartHour'] + 1)] = 'HOME'
#             if iRow['tripID'] == maxWID:
#                 if iRow['timestampEnd'].minute <= 30:
#                     purposeDataDays.loc[hpID, range(iRow['tripEndHour'], maxHour)] = 'HOME'
#                 else:
#                     purposeDataDays.loc[hpID, range(iRow['tripEndHour'] + 1, maxHour)] = 'HOME'
#         elif iRow['tripID'] == minWID:
#             if iRow['timestampStart'].minute <= 30:
#                 purposeDataDays.loc[hpID, range(0, iRow['tripStartHour'])] = 'HOME'
#             else:
#                 purposeDataDays.loc[hpID, range(0, iRow['tripStartHour'] + 1)] = 'HOME'
#             if iRow['tripID'] == maxWID:
#                 purposeDataDays.loc[hpID, range(iRow['tripEndHour'] + 1, maxHour)] = 'HOME'
#         else:
#             purposeHourStart = determinePurposeStartHour(tripData.loc[idxOld, 'timestampStart'],
#                                                          tripData.loc[idxOld, 'timestampEnd'])
#             if iRow['timestampStart'].minute <= 30:
#                 hoursBetween = range(purposeHourStart, iRow['tripStartHour'])  # FIXME: case differentiation on arrival hour
#             else:
#                 hoursBetween = range(purposeHourStart,
#                                      iRow['tripStartHour'] + 1)
#             purposeDataDays.loc[hpID, hoursBetween] = tripData.loc[idxOld, 'purposeStr']
#             if iRow['tripID'] == maxWID:
#                 if iRow['timestampEnd'].minute <= 30:
#                     purposeDataDays.loc[hpID, range(iRow['tripEndHour'], maxHour)] = 'HOME'
#                 else:
#                     purposeDataDays.loc[hpID, range(iRow['tripEndHour'] + 1, maxHour)] = 'HOME'
#
# Basic ideas for more performant code:
#     - first set all columns for each trip (independent of hhPersonID) (vectorized)
#     - then merge