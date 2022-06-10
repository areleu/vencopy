__version__ = "0.1.X"
__maintainer__ = "Niklas Wulff"
__contributors__ = "Fabia Miorelli, Parth Butte"
__email__ = "Niklas.Wulff@dlr.de"
__birthdate__ = "23.10.2020"
__status__ = "dev"  # options are: dev, test, prod
__license__ = "BSD-3-Clause"


# ----- imports & packages ------
if __package__ is None or __package__ == "":
    import sys
    from os import path

    sys.path.append(path.dirname(path.dirname(__file__)))

import pandas as pd
from pathlib import Path
from vencopy.core.dataParsers import ParseMiD, ParseKiD, ParseVF
from vencopy.core.diaryBuilders import diaryBuilder
from vencopy.core.gridModelers import GridModeler
from vencopy.core.flexEstimators import FlexEstimator
from vencopy.core.evaluators import Evaluator
from vencopy.utils.globalFunctions import loadConfigDict, createOutputFolders

if __name__ == "__main__":
    # Set dataset and config to analyze, create output folders
    # datasetID options: 'MiD08' - 'MiD17' - 'KiD' - 'VF'
    datasetID = "MiD17"
    configNames = (
        "globalConfig",
        "localPathConfig",
        "parseConfig",
        "diaryConfig",
        "gridConfig",
        "flexConfig",
        "evaluatorConfig",
    )
    basePath = Path(__file__).parent
    configDict = loadConfigDict(configNames, basePath)
    createOutputFolders(configDict=configDict)

    # Parsing datasets
    if datasetID == "MiD17":
        vpData = ParseMiD(configDict=configDict, datasetID=datasetID)
    elif datasetID == "KiD":
        vpData = ParseKiD(configDict=configDict, datasetID=datasetID)
    elif datasetID == "VF":
        vpData = ParseVF(configDict=configDict, datasetID=datasetID)
    vpData.process()

    # # Trip distance and purpose diary compositions
    vpDiary = diaryBuilder(
        datasetID=datasetID,
        configDict=configDict,
        ParseData=vpData,
        debug=True,
    )

    # Grid model application regular
    vpGrid = GridModeler(configDict=configDict, datasetID=datasetID, gridModel='simple')
    vpGrid.calcGrid()

    # Evaluate drive and trip purpose profile
    vpEval = Evaluator(
        configDict=configDict,
        parseData=pd.Series(data=vpData, index=[datasetID]),
    )
    vpEval.plotParkingAndPowers(vpGrid=vpGrid)
    vpEval.hourlyAggregates = vpEval.calcVariableSpecAggregates(
        by=["tripStartWeekday"]
    )
    vpEval.plotAggregates()

    # Estimate charging flexibility based on driving profiles
    # # and charge connection
    vpFlex = FlexEstimator(
        configDict=configDict, datasetID=datasetID, ParseData=vpData
    )
    vpFlex.baseProfileCalculation(gridModel=vpGrid.gridModel)
    vpFlex.filter()
    vpFlex.aggregate()
    vpFlex.correct()
    vpFlex.normalize()
    vpFlex.writeOut()
    print(
        f"Total absolute electricity charged in uncontrolled charging: "
        f"{vpFlex.chargeProfilesUncontrolled.sum().sum()} based on "
        f"{datasetID}"
    )
    vpEval.plotProfiles(flexEstimator=vpFlex)
