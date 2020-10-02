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

linkPurposes = Path(config['linksRelative']['input']) / config['files']['mid2017purposes']
purposeDayData_raw = pd.read_csv(linkPurposes)
chargeAvailability = purposeDayData_raw.replace(config['chargingInfrastructureDistributions'])
chargeAvailability.to_csv(Path(config['linksRelative']['input']) / 'inputProfiles_Plug_MiD17.csv')