
from vencopy.utils.globalFunctions import load_configs, createOutputFolders
from vencopy.core.flexEstimators import WeekFlexEstimator
from vencopy.core.diaryBuilders import WeekDiaryBuilder, DiaryBuilder
from vencopy.core.gridModelers import GridModeler
from vencopy.core.dataParsers.dataParsers import ParseMiD
import sys
import pickle
import numpy as np
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from itertools import product
from profilehooks import profile


__version__ = '0.2.X'
__maintainer__ = 'Niklas Wulff'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '11.01.2022'
__status__ = 'test'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'


# Columns for debugging purposes
# ['genericID', 'parkID', 'tripID', 'actID', 'nextActID', 'prevActID', 'dayActID', 'timestampStart', 'timestampEnd']


if __name__ == '__main__':
    # Set dataset and config to analyze, create output folders
    datasetID = 'MiD17'
    configNames = ('globalConfig', 'localPathConfig', 'parseConfig', 'gridConfig', 'flexConfig', 'diaryConfig',
                   'evaluatorConfig')
    basePath = Path(__file__).parent.parent.parent / 'vencopy'
    configDict = load_configs(configNames, basePath)
    createOutputFolders(configDict=configDict)

    CALC = True
    FN = 'vpWeFlex_acts_areaType_t08n500b30'

    if CALC:
        vpData = ParseMiD(configDict=configDict, datasetID=datasetID)
        vpData.process(splitOvernightTrips=False)

        # Grid model application
        vpGrid = GridModeler(configDict=configDict, datasetID=datasetID, activities=vpData.activities,
                             gridModel='simple')
        vpGrid.assignGrid()

        # Week diary building
        vpWDB = WeekDiaryBuilder(activities=vpGrid.activities, catCols=['areaType'])
        vpWDB.summarizeSamplingBases()
        vpWDB.composeWeekActivities(seed=42, nWeeks=500, replace=True)

        # Estimate charging flexibility based on driving profiles and charge connection
        vpWeFlex = WeekFlexEstimator(configDict=configDict,
                                     datasetID=datasetID,
                                     activities=vpWDB.weekActivities,
                                     threshold=0.8)
        vpWeFlex.estimateTechnicalFlexibility()

        pickle.dump(vpWeFlex.activities, open(f'{FN}.p', 'wb'))
        print(f'Weekly activities dumped to {FN}.p')
        vpWeFlex.activities.to_csv(f'C:/repos/vencopy_paper/2022_EMPSIS/results/VencoPy_acts/{FN}.csv')

    else:
        print(f'Reading weekly activities from dump at {FN}.p')
        acts = pickle.load(open(f'{FN}.p', 'rb'))

    # Diary building
    if CALC:
        vpDiary = DiaryBuilder(configDict=configDict, datasetID=datasetID, activities=vpWeFlex.activities,
                               isWeekDiary=True)
    else:
        vpDiary = DiaryBuilder(configDict=configDict, datasetID=datasetID, activities=acts, isWeekDiary=True)
    vpDiary.createDiaries()
    vpDiary.uncontrolledCharge.sum(axis=0).plot()
    plt.show()

    # pickle.dump(vpDiary, open('classDiary_T0.8_N100_smpGrid.p', 'wb'))
    vpDiary.uncontrolledCharge.to_csv(
        Path(f'C:/repos/vencopy_paper/2022_EMPSIS/results/VencoPy_profiles/{FN}_uncCharge_probGrid.csv'))
    # vpDiary.drain.to_csv(
    #     Path('C:/repos/vencopy_paper/2022_EMPSIS/results/VencoPy_profiles/vencopy_week_drain_N1000_smpGrid.csv'))
    # vpDiary.chargingPower.to_csv(
    #     Path('C:/repos/vencopy_paper/2022_EMPSIS/results/VencoPy_profiles/vencopy_week_chargingPower_N1000_smpGrid.csv'))

    print('end break')
