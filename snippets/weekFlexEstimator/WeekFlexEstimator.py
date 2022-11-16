import sys
import pandas as pd
from os import path
sys.path.append(path.dirname(path.dirname(__file__)))

from pathlib import Path

from vencopy.core.flexEstimators import WeekFlexEstimator
from vencopy.utils.globalFunctions import loadConfigDict, createOutputFolders


datasetID = "MiD17"
basePath = Path(__file__).parent.parent
configNames = ("globalConfig", "localPathConfig", "parseConfig", "diaryConfig",
               "gridConfig", "flexConfig", "aggregatorConfig", "evaluatorConfig")
configDict = loadConfigDict(configNames, basePath=Path('./vencopy/'))

# Test data set
df_test = pd.DataFrame(columns=['tripID', 'parkID', 'actID', 'isFirstActivity', 'isLastActivity', 'timestampStart',
                                'timestampEnd', 'timedelta', 'maxChargeVolume', 'drain'])


# Aim of the test: Test if this instantiation is possible in general
vpWeFlex = WeekFlexEstimator(configDict=configDict, datasetID=datasetID, activities=df_test,
                             threshold=0.8)

# Data set specification:
# - type: pd.DataFrame
# - isHierarchicalIndex=False (for column and row index)
# - columns that are needed in activities for WeekFlexEstimator instantiation: ['tripID', 'parkID', 'actID',
#       'isFirstActivity', 'isLastActivity', 'timestampStart', 'timestampEnd', 'timedelta', 'maxChargeVolume', 'drain']
