__version__ = '0.0.1'
__maintainer__ = 'Niklas Wulff'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '30.09.2020'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'


#----- imports & packages ------
from pathlib import Path
from scripts.libInput import *

# FIXME Add distributions and charging power ratings

linkConfig = pathlib.Path.cwd() / 'config' / 'config.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
config = yaml.load(open(linkConfig), Loader=yaml.SafeLoader)

# MID file resulting from Christoph Schimeczeks SQL-Parsing of MID2008
# Purposes
linkPurposes = Path(config['linksRelative']['input']) / config['files']['mid2008purposes']
# purposeDayData_raw = pd.read_csv(linkPurposes)
purposeDayData_raw = pd.read_excel(linkPurposes, sheet_name='Places')
chargeAvailability = purposeDayData_raw.replace(config['chargingInfrastructureDistributionsSchimmy'])
chargeAvailability.set_index(['VEHICLE', 'Day', 'Weight'], inplace=True)
chargeAvailability.iloc[~chargeAvailability.isin(['WAHR', 'FALSCH'])] = 'FALSCH'
chargeAvailability.to_csv(Path(config['linksRelative']['input']) / 'inputProfiles_Plug_MiD08.csv')
chargeAvailability = chargeAvailability.reset_index([1,2])

# Trips
linkTrips = Path(config['linksRelative']['input']) / config['files']['mid2008trips']
tripDayData_raw = pd.read_excel(linkTrips, sheet_name='DistancesInKm')
tripData = tripDayData_raw.set_index('VEHICLE')
tripData = pd.concat([tripData, chargeAvailability.loc[:, ['Day', 'Weight']]], axis=1)
tripData.to_csv(Path(config['linksRelative']['input']) / 'inputProfiles_Drive_MiD08.csv')
