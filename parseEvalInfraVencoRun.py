__version__ = '0.0.1'
__maintainer__ = 'Niklas Wulff'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '23.10.2020'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'


#----- imports & packages ------
from scripts.utilsParsing import *
from scripts.libPlotting import *
from scripts.libLogging import logger
import pathlib
import time
from parseMiD import parseMiD
from assignChargeInfra import assignSimpleChargeInfra
from evaluateDriveProfiles import evaluateDriveProfiles
from evaluateTripPurposes import evaluateTripPurposes
from venco_main import vencoRun

# Set dataset and config to analyze
dataset = 'MiD08'
linkConfig = pathlib.Path.cwd() / 'config' / 'config.yaml'  # pathLib syntax for windows, max, linux compatibility, see https://realpython.com/python-pathlib/ for an intro
config = yaml.load(open(linkConfig), Loader=yaml.SafeLoader)

# Raw MiD2-017 dataset to hourly drive and purpose profiles
parseMiD(dataset='MiD08', config=config)
parseMiD(dataset='MiD17', config=config)
#
# # Assign charging infrastructure for both 2008 (from CS) and 2017 purpose profiles
# # In config under key chargingInfrastructureDistributions
assignSimpleChargeInfra(config=config, dataset='MiD08')
assignSimpleChargeInfra(config=config, dataset='MiD17')

# FixME: evaluate trip purposes
# FixME: Merging of variables of original dataset is possible
# Evaluate drive and trip purpose profiles
evaluateDriveProfiles(config)
# evaluateTripPurposes(config=config)

# FixME: Implement one VencoPy run for MiD08 and one for MiD17
# Estimate charging flexibility based on driving profiles and charge connection
vencoRun(config=config, dataset='MiD08')
vencoRun(config=config, dataset='MiD17')


