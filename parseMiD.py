__version__ = '0.0.1'
__maintainer__ = 'Niklas Wulff'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '08.09.2020'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'


#----- imports & packages ------
from scripts.utilsParsing import *
from scripts.libPlotting import *
from scripts.libLogging import logger
import pathlib


if __name__ == '__main__':
    #----- data and config read-in -----
    linkConfig = pathlib.Path.cwd() / 'config' / 'config.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
    config = yaml.load(open(linkConfig), Loader=yaml.SafeLoader)
    # hhData_raw = pd.read_csv(pathlib.Path(config['linksAbsolute']['folderMiD2017']) / config['files']['MiD2017households'], sep=';')
    # personData_raw = pd.read_csv(pathlib.Path(config['linksAbsolute']['folderMiD2017']) / config['files']['MiD2017persons'], sep=';')
    tripData_raw = pd.read_csv(pathlib.Path(config['linksAbsolute']['folderMiD2017']) / config['files']['MiD2017trips'], sep=';', decimal=',')

    tripData = tripData_raw.loc[tripData_raw.loc[:, 'W_VM_G'] == 1, ['HP_ID_Reg', 'W_SZ', 'W_AZ', 'zweck', 'wegkm',
                                               'ST_JAHR', 'ST_MONAT', 'ST_WOCHE', 'ST_WOTAG', 'W_SZS', 'W_SZM', 'W_AZS',
                                                'W_AZM']]
    tripDataWDate = assignTSToColViaCWeek(tripData, 'ST_JAHR', 'ST_WOCHE', 'ST_WOTAG', 'W_SZS', 'W_SZM', 'timestamp_st')
    tripDataWDate = assignTSToColViaCWeek(tripDataWDate, 'ST_JAHR', 'ST_WOCHE', 'ST_WOTAG', 'W_AZS', 'W_AZM',
                                          'timestamp_en')

    tripDataWHourlyShares = calcHourlyShares(tripDataWDate, ts_st='timestamp_st', ts_en='timestamp_en')
    emptyDF = initiateHourDataframe(tripDataWHourlyShares.loc[:, 'HP_ID_Reg'], 24)
    fillDataframe(tripDataWHourlyShares, emptyDF)

    print(tripData.head())
