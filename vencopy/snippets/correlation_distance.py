import sys
import numpy as np
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
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
__birthdate__ = '31.08.2022'
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

    # Correlation 
    trips = vpData.activities.loc[~vpData.activities['tripID'].isna(), :]
    corr = trips.apply(lambda x: x.corr(trips['tripDistance'], method='pearson'))
    corr = corr.concat([corr, trips.apply(lambda x: x.corr(trips['tripDistance'], method='kendall'))])
    corr = corr.concat([corr, trips.apply(lambda x: x.corr(trips['tripDistance'], method='spearman'))])
    
    print('debug break')