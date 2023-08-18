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
from vencopy.core.dataParsers.dataParsers import ParseMiD
from vencopy.core.gridModelers import GridModeler
from vencopy.core.flexEstimators import FlexEstimator
from vencopy.utils.globalFunctions import load_configs, createOutputFolders

if __name__ == "__main__":
    # Set dataset and config to analyze, create output folders
    # datasetID options: 'MiD08' - 'MiD17' - 'KiD' - 'VF'
    datasetID = "MiD17"
    # basePath = Path(__file__).parent.parent.parent
    configNames = ("globalConfig", "localPathConfig", "parseConfig", "gridConfig", "flexConfig")
    configDict = load_configs(configNames, basePath=Path(os.getcwd()) / 'vencopy')
    createOutputFolders(configDict=configDict)

    vpData = ParseMiD(configDict=configDict, datasetID=datasetID, debug=True)
    vpData.process()

    vpGrid = GridModeler(configDict=configDict, datasetID=datasetID, activities=vpData.activities, gridModel='simple')
    actWLosses = vpGrid.assignGrid(losses=True).copy()
    actWOLosses = vpGrid.assignGrid(losses=False).copy()

    vpFlexWLosses = FlexEstimator(configDict=configDict, datasetID=datasetID, activities=actWLosses)
    actWLosses = vpFlexWLosses.estimateTechnicalFlexibility()

    vpFlexWOLosses = FlexEstimator(configDict=configDict, datasetID=datasetID, activities=actWOLosses)
    actWOLosses = vpFlexWOLosses.estimateTechnicalFlexibility()

    print('Sum of uncontrolled charging of avtivities')
    print(f"With losses: {actWLosses['uncontrolledCharge'].sum()}")
    print(f"Without losses: {actWOLosses['uncontrolledCharge'].sum()}")

    # Slight deviations are acceptable here because there are park activities that are short and for which the amount
    # of energy is limited by the available power.

    print('END breakpoint')
