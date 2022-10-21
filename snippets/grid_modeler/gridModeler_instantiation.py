from vencopy.core.gridModelers import GridModeler
from vencopy.utils.globalFunctions import loadConfigDict
import pandas as pd
from pathlib import Path

basePath = Path(__file__).parent.parent.parent/'vencopy'
datasetID = "MiD17"
configNames = ("globalConfig", "localPathConfig", "parseConfig", "diaryConfig",
                "gridConfig", "flexConfig", "aggregatorConfig", "evaluatorConfig")
configDict = loadConfigDict(configNames, basePath=basePath)
vpGrid = GridModeler(configDict=configDict, datasetID=datasetID, activities=pd.DataFrame(), gridModel='probability')
print(vpGrid.datasetID)
print(vpGrid.activities)
print(vpGrid.gridModel)
