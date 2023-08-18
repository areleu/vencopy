
import sys
import numpy as np
import pandas as pd
import numpy as np
import pickle

from pathlib import Path
from itertools import product
from profilehooks import profile

# Needed to run in VSCode properties currently
sys.path.append('.')

from vencopy.core.diaryBuilders import WeekDiaryBuilder
from vencopy.core.dataParsers.dataParsers import ParseMiD
from vencopy.core.gridModelers import GridModeler
from vencopy.core.flexEstimators import WeekFlexEstimator
from vencopy.utils.globalFunctions import load_configs, createOutputFolders


__version__ = '0.2.X'
__maintainer__ = 'Niklas Wulff'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '11.01.2022'
__status__ = 'test'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'


# Columns for debugging purposes
# ['genericID', 'parkID', 'tripID', 'actID', 'nextActID', 'prevActID', 'dayActID', 'timestampStart', 'timestampEnd']

# If True do own calculations, otherwise read from pickle
CALC = True

if __name__ == '__main__':
    if CALC:
        # Set dataset and config to analyze, create output folders
        datasetID = 'MiD17'
        configNames = ('globalConfig', 'localPathConfig', 'parseConfig', 'gridConfig', 'flexConfig', 'evaluatorConfig')
        basePath = Path(__file__).parent.parent
        configDict = load_configs(configNames, basePath)
        createOutputFolders(configDict=configDict)

        vpData = ParseMiD(configDict=configDict, datasetID=datasetID)
        vpData.process(splitOvernightTrips=False)

        # Grid model application
        vpGrid = GridModeler(configDict=configDict, datasetID=datasetID, activities=vpData.activities,
                             gridModel='simple')
        vpGrid.assignGrid()

        # Week diary building
        vpWDB = WeekDiaryBuilder(activities=vpGrid.activities, catCols=['bundesland', 'areaType'])
        vpWDB.summarizeSamplingBases()
        vpWDB.composeWeekActivities(seed=42, nWeeks=500, replace=True)

        # Estimate charging flexibility based on driving profiles and charge connection
        vpWeFlex = WeekFlexEstimator(configDict=configDict,
                                     datasetID=datasetID,
                                     activities=vpWDB.weekActivities,
                                     threshold=0.8)
        vpWeFlex.estimateTechnicalFlexibility()
        acts = vpWeFlex.activities
        pickle.dump(acts, open('activities_T0.8_N500.p', 'wb'))

    else:
        acts = pickle.load(open('activities_T0.8_N500.p', 'rb'))

    def getWeeksDays(wdb: WeekDiaryBuilder) -> tuple[int, int]:
        """ Retrieve the number of weeks and the number of days used for sampling from the respective sample bases
        in the VencoPy week diary builder.

        Returns:
            tuple[int, int]: Returns the number of weeks as int and the number of days as int.
        """

        weeks = len(wdb.sampleBaseInAct) * wdb.sampleSize
        days = weeks * max(wdb.weekdayIDs)
        return weeks, days

    # CALCULATE AVERAGES FOR WHOLE DATA SET
    # Number of charging events (CE) per day and week
    print('Start')

    # Filtering before writing
    # Filter out weeks that require fuel
    # FIXME: Move this to a the flexEstimator or a separate filtering instance
    actsIdx = acts.set_index(['categoryID', 'weekID'])
    catWeekIDOut = acts.loc[~acts['maxResidualNeed'].isin([None, 0]), ['categoryID', 'weekID']]
    tplFilt = catWeekIDOut.apply(lambda x: tuple(x), axis=1).unique()
    actsFilt = actsIdx.loc[~actsIdx.index.isin(tplFilt), :]
    acts = actsFilt.reset_index()

    # Filter out activities that describe charging
    actsCE = acts.loc[acts['uncontrolledCharge'] > 0, :]

    # Writing
    actsCE.to_csv(Path('C:/repos/vencopy_paper/2022_EMPSIS/results/VencoPy_results_T0.8_N500/actsCE_T0.8_N500.csv'))
    acts.to_csv(Path('C:/repos/vencopy_paper/2022_EMPSIS/results/VencoPy_results_T0.8_N500/acts_T0.8_N500.csv'))
    totalNCE = len(actsCE)
    totalWeeks, totalDays = getWeeksDays(wdb=vpWDB)
    NCEPerDay = totalNCE / totalDays
    NCEPerWeek = totalNCE / totalWeeks
    print(f'Average number of charging events per day: {NCEPerDay}')
    print(f'Average number of charging events per week: {NCEPerWeek}')

    # Park duration in h
    avgParkDur = (acts.loc[~acts['parkID'].isna(), 'timedelta'] / pd.Timedelta('1 hour')).mean()

    # Park duration of charging events
    avgChargeDur = (actsCE['timedelta'] / pd.Timedelta('1 hour')).mean()

    print(f'Average park duration in general: {avgParkDur} h')
    print(f'Average park duration of CEs: {avgChargeDur} h')

    # Charge duration in h (currently not possible)

    # Charged energy in kWh / CE
    avgEnergy = actsCE['uncontrolledCharge'].mean()
    print(f'Average charged energy per charging event: {avgEnergy} kWh')

    # CALCULATE CATEGORY SPECIFIC
    # Number of charging events (CE) per day and week

    # Park duration in h

    # Charge duration in h

    # Charged energy in kWh / CE

    print('end break')
