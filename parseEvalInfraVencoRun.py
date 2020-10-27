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

# Raw MiD2-017 dataset to hourly drive and purpose profiles
parseMiD()

# Assign charging infrastructure for both 2008 (from CS) and 2017 purpose profiles
# In config under key chargingInfrastructureDistributions
assignSimpleChargeInfra()

# FixME: evaluate trip purposes
# Evaluate drive and trip purpose profiles
evaluateDriveProfiles()
#evaluateTripPurposes()

# FixME: Implement one VencoPy run for MiD08 and one for MiD17
# Estimate charging flexibility based on driving profiles and charge connection
vencoRun()


