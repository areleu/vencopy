from pathlib import Path
from vencopy.core.dataParsers import ParseMiD, ParseKiD, ParseVF
from vencopy.core.flexEstimators import FlexEstimator
from vencopy.core.gridModelers import GridModeler
from vencopy.utils.globalFunctions import createOutputFolders, dumpReferenceData, loadConfigDict


def standard(returnValidation: bool = False):
    # Set dataset and config to analyze, create output folders
    # datasetID options: 'MiD08' - 'MiD17' - 'KiD' - 'VF'
    datasetID = "MiD17"
    basePath = Path(__file__).parent
    configNames = ("globalConfig", "localPathConfig", "parseConfig", "diaryConfig",
                   "gridConfig", "flexConfig", "aggregatorConfig", "evaluatorConfig")
    configDict = loadConfigDict(configNames, basePath=basePath)
    createOutputFolders(configDict=configDict)

    if datasetID == "MiD17":
        vpData = ParseMiD(configDict=configDict, datasetID=datasetID, debug=True)
    elif datasetID == "KiD":
        vpData = ParseKiD(configDict=configDict, datasetID=datasetID, debug=False)
    elif datasetID == "VF":
        vpData = ParseVF(configDict=configDict, datasetID=datasetID, debug=False)
    vpData.process()

    vpGrid = GridModeler(configDict=configDict, datasetID=datasetID, activities=vpData.activities, gridModel='simple')
    vpGrid.assignGrid()

    vpFlex = FlexEstimator(configDict=configDict, datasetID=datasetID, activities=vpGrid.activities)
    vpFlex.estimateTechnicalFlexibility()

    if 'postFlex' in configDict['globalConfig']['validation']['tags']:
        if returnValidation:
            return vpFlex.activities
        dumpReferenceData(data=vpFlex.activities,
                          tag='postFlex',
                          path=Path(configDict['globalConfig']['validation']['path']))
    