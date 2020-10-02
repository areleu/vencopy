# -*- coding:utf-8 -*-

__version__ = '0.0.8'
__maintainer__ = 'Niklas Wulff 24.02.2020'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '04.09.2019'
__status__ = 'test'  # options are: dev, test, prod

# This script holds the function definitions for output writing and plotting of calaculated profiles from VencoPy.

import os
import numpy as np
import yaml
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import pathlib
from .libLogging import logit
from .libLogging import logger


@logit
def writeAnnualOutputForREMix(profileDict, outputConfig, outputLink, noOfHoursOutput, technologyLabel, strAdd):
    """
    Output wrapper function to call cloneAndWriteProfile once for each output profile.

    :param profileDict: Dictionary holding profile names and profiles in pd.Series to be cloned and written
    :param outputConfig: REMix specific configuration file holding model nodes
    :param outputLink: link to output folder
    :param noOfHoursOutput: Integer describing the number of hours that the profiles are cloned to
    :param technologyLabel: String holding a REMix eCarsDtl technology label
    :param strAdd: String addition for output writing
    :return: None
    """
    for iName, iProf in profileDict.items():
        filename = technologyLabel + '_' + iName + strAdd
        cloneAndWriteProfile(iProf, outputConfig, outputLink, noOfHoursOutput, technologyLabel, filename)


@logit
def cloneAndWriteProfile(profile, outputConfig, outputLink, noOfHoursOutput, technologyLabel, filename):
    """
    This action clones daily profiles to cover the specified time horizon given in noOfHoursOutput.

    :param profileDict: A dictionary holding five VencoPy profiles as Series including their names as keys.
    :param linkDict: A VencoPy link dictionary.
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

    profilesOut.to_csv(outputLink / pathlib.Path(filename + '.csv'), index=False)


@logit
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


@logit
def writeProfilesToCSV(outputFolder, profileDictOut, singleFile=True, strAdd=''):
    """
    Function to write VencoPy profiles to either one or five .csv files in the output folder specified in outputFolder.

    :param outputFolder: Link to output folder
    :param profileDictOut: Dictionary with profile names in keys and profiles as pd.Series containing a VencoPy
    profile each to be written in value
    :param singleFile: If True, all profiles will be appended and written to one .csv file. If False, five files are
    written
    :param strAdd: String addition for filenames
    :return: None
    """

    if singleFile:
        dataOut = pd.DataFrame(profileDictOut)
        dataOut.to_csv(outputFolder / pathlib.Path(r'vencoOutput' + strAdd + '.csv'), header=True)
    else:
        for iName, iProf in profileDictOut.items():
            iProf.to_csv(outputFolder / pathlib.Path(r'vencoOutput_' + iName + strAdd + '.csv'), header=True)


@logit
def appendREMixProfiles(pre, names, post, linkFiles, linkOutput, outputPre, outputPost):
    """
    REMix specific append functionality to integrate results of three different VencoPy-runs into one file per profile.

    :param pre: String part before profile name
    :param names: list of profile names
    :param post: String part after profile name
    :param linkFiles: Link to folder of files
    :param linkOutput: Link to appended file
    :param outputPre: String before profile name for output
    :param outputPost: String after profile name for output
    :return: None
    """

    strDict = composeStringDict(pre, names, post)
    dataDict = {}
    for key, strList in strDict.items():
        dfList = []
        for strIdx in strList:
            df = pd.read_csv(linkFiles / strIdx)
            df.ix[df.iloc[:, 0] == 'BEV', 0] = strIdx[0:5]
            df.rename(columns={'Unnamed: 1': ' '}, inplace=True)
            dfList.append(df)
        dataDict[key] = dfList

    resultDict = {}
    for key, value in dataDict.items():
        resultDict[key] = pd.concat(value)
        resultDict[key].to_csv(index=False,
                               path_or_buf=linkOutput / pathlib.Path(outputPre + key + outputPost + '.csv'),
                               float_format='%.3f')


@logit
def composeStringDict(pre, names, post):
    dict = {}
    for nIdx in names:
        listStr = []
        for preIdx, postIdx in zip(pre, post):
            str = preIdx + nIdx + postIdx + '.csv'
            listStr.append(str)
        dict[nIdx] = listStr
    return dict


@logit
def linePlot(profileDict, linkOutput, config, show=True, write=True, ylabel='Normalized profiles', filename=''):
    plt.rcParams.update(config['plotConfig'])  # set plot layout
    fig, ax = plt.subplots()
    for iKey, iVal in profileDict.items():
        sns.lineplot(iVal.index, iVal, label=iKey, sort=False)
    ax.set_ylim(bottom=0)
    ax.set_xlabel('Hour')
    ax.set_ylabel(ylabel)
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.3))  # ncol=2,
    plt.tight_layout()
    filePlot = linkOutput / pathlib.Path(filename + '_' + config['labels']['strAdd'] + '.png')
    if show:
        plt.show()
    if write:
        fig.savefig(filePlot)

@logit
def separateLinePlots(profileDictList, config, show=True, write=True, ylabel=[], filenames=[]):
    for iDict, iYLabel, iName in zip(profileDictList, ylabel, filenames):
        writeProfilesToCSV(outputFolder=config['linksRelative']['sesData'],
                       profileDictOut=iDict,
                       singleFile=False,
                       strAdd='_mid08')
        linePlot(iDict, linkOutput=config['linksRelative']['sesPlots'], config=config,
                    show=show, write=write, ylabel=iYLabel, filename=iName)
