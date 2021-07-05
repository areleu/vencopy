import pandas as pd
from pathlib import Path
import yaml
import numpy as np


def getData(globalConfig: dict, localPathConfig: dict, datasetID: str = 'MiD17'):
    '''
    :param globalConfig:
    :param localPathConfig:
    :param datasetID:
    :return:
    '''
    rawDataPath = Path(localPathConfig['pathAbsolute'][datasetID]) / globalConfig['files'][datasetID]['tripsDataRaw']
    rawData = pd.read_stata(rawDataPath, convert_categoricals=False, convert_dates=False, preserve_dtypes=False)
    dataSample = rawData.copy().loc[:, 'HP_ID_Reg'].sample(n=500, random_state=0).sort_values()
    # data= pd.DataFrame(rawData.copy().loc[:, 'HP_ID_Reg'].sample(n=500, random_state=0), columns= ['HP_ID_Reg']).sort_values(by=['HP_ID_Reg'])
    rawData= rawData.loc[:, :].where(rawData['HP_ID_Reg'].isin(dataSample)).dropna()
    return rawData


def selectColumns(rawData: pd.DataFrame):
    data = rawData.loc[:, compileVariableList(parseConfig=parseConfig)]
    return data


def compileVariableList(parseConfig, datasetID: str = 'MiD17') -> list:
    listIndex = parseConfig['dataVariables']['datasetID'].index(datasetID)
    variables = [val[listIndex] if not val[listIndex] == 'NA' else 'NA' for key, val in parseConfig['dataVariables'].items()]
    variables.remove(datasetID)
    removeNA(variables)
    return variables


def removeNA(variables: list):
    vars = [iVar.upper() for iVar in variables]
    counter = 0
    for idx, iVar in enumerate(vars):
        if iVar == 'NA':
            del variables[idx - counter]
            counter += 1


def harmonizeVariables(data, datasetID: str = 'MiD17'):
    replacementDict = createReplacementDict(datasetID, parseConfig['dataVariables'])
    dataHarmonized = data.rename(columns=replacementDict)
    return dataHarmonized


def createReplacementDict(datasetID : str, dictRaw : dict) -> None:

    if datasetID in dictRaw['datasetID']:
        listIndex = dictRaw['datasetID'].index(datasetID)
        return {val[listIndex]: key for (key, val) in dictRaw.items()}
    else:
        raise ValueError(f'Data set {datasetID} not specified in MiD variable dictionary.')


def convertTypes(dataHarmonized, parseConfig: dict):
    conversionDict = parseConfig['inputDTypes']
    keys = {iCol for iCol in conversionDict.keys() if iCol in dataHarmonized.columns}
    subDict = {key: conversionDict[key] for key in conversionDict.keys() & keys}
    dataConverted = dataHarmonized.astype(subDict)
    dataConverted.reset_index(drop=True, inplace=True)
    return dataConverted

def replaceHouseholdPersonID(dataConverted):
    dataConverted.to_csv('data.csv')
    dataSampled= dataConverted.drop(['hhID', 'personID'], axis=1)
    np.random.seed(1)
    nums_hhPersonID = np.random.choice(range(len(dataSampled)), size=dataSampled['hhPersonID'].nunique(), replace=False)
    dataSampled['hhPersonID'] = dataSampled['hhPersonID'].map(dict(zip(dataSampled['hhPersonID'].unique(), nums_hhPersonID)))
    dataSampled.to_csv('sampleData.csv')
    return dataSampled


if __name__ == '__main__':
    pathLocalPathConfig = Path.cwd().parent / 'config' / 'localPathConfig.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    with open(pathLocalPathConfig) as ipf:
        localPathConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    pathParseConfig = Path.cwd().parent / 'config' / 'parseConfig.yaml'
    with open(pathParseConfig) as ipf:
        parseConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    pathGlobalConfig = Path.cwd().parent / 'config' / 'globalConfig.yaml'
    with open(pathGlobalConfig) as ipf:
        globalConfig = yaml.load(ipf, Loader=yaml.SafeLoader)
    rawData = getData(globalConfig=globalConfig, localPathConfig=localPathConfig)
    data = selectColumns(rawData)
    dataHarmonized = harmonizeVariables(data)
    dataConverted = convertTypes(dataHarmonized, parseConfig=parseConfig)
    dataSampled = replaceHouseholdPersonID(dataConverted)
    print('END')