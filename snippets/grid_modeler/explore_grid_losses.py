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

import os
from pathlib import Path
from vencopy.core.dataParsers import ParseMiD
from vencopy.core.gridModelers import GridModeler
from vencopy.core.flexEstimators import FlexEstimator
from vencopy.utils.globalFunctions import loadConfigDict, createOutputFolders

if __name__ == "__main__":
    # Set dataset and config to analyze, create output folders
    # datasetID options: 'MiD08' - 'MiD17' - 'KiD' - 'VF'
    datasetID = "MiD17"
    # basePath = Path(__file__).parent.parent.parent
    configNames = ("globalConfig", "localPathConfig", "parseConfig", "gridConfig", "flexConfig")
    configDict = loadConfigDict(configNames, basePath=Path(os.getcwd()) / 'vencopy')
    createOutputFolders(configDict=configDict)

    vpData = ParseMiD(configDict=configDict, datasetID=datasetID, debug=True)
    vpData.process()

    vpGrid = GridModeler(configDict=configDict, datasetID=datasetID, activities=vpData.activities, gridModel='simple')
    vpGrid.assignGrid(losses=True)

    vpFlex = FlexEstimator(configDict=configDict, datasetID=datasetID, activities=vpGrid.activities)
    vpFlex.estimateTechnicalFlexibility()
