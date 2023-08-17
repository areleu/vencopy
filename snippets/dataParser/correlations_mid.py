from vencopy.utils.globalFunctions import loadConfigDict, createOutputFolders
from vencopy.core.flexEstimators import WeekFlexEstimator
from vencopy.core.gridModelers import GridModeler
from vencopy.core.dataParsers.dataParsers import ParseMiD
import pandas as pd
import seaborn as sns
from pathlib import Path
import sys

# Needed to run in VSCode properties currently
sys.path.append('.')


basePath = Path(__file__).parent
configDict = loadConfigDict(basePath=basePath)

vpData = parseData(configDict=configDict)
vpData.process()

act_raw = vpData.activities.copy()
act_raw['startHour'] = act_raw['timestampStart'].dt.hour
act_raw['endHour'] = act_raw['timestampEnd'].dt.hour

act = act_raw[['tripID', 'tripPurpose', 'tripDistance', 'travelTime',
               'tripStartWeekday', 'startHour', 'endHour']]

actm = act.melt(var_name='columns')

sns.displot(data=actm, x='value', col='columns', col_wrap=3, bins=40,
            common_norm=False, kde=True, stat='density', common_bins=False,
            facet_kws={'sharey': False, 'sharex': False})
