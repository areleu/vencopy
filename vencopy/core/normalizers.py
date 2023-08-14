__version__ = '0.4.X'
__maintainer__ = 'Niklas Wulff, Fabia Miorelli'
__birthdate__ = '12.07.2023'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'

if __package__ is None or __package__ == '':
    import sys
    from os import path
    sys.path.append(path.dirname(path.dirname(path.dirname(__file__))))

from pathlib import Path
import pandas as pd
import warnings

from vencopy.core.profileAggregators import ProfileAggregator
from vencopy.utils.globalFunctions import (createFileName, writeOut)


class Normalizer():
    def __init__(self, configDict: dict, profiles: ProfileAggregator):
        self.outputConfig = configDict['outputConfig']
        self.globalConfig = configDict['globalConfig']
        self.localPathConfig = configDict['localPathConfig']
        self.grid_config = configDict['gridConfig']
        self.rated_power_simple = self.grid_config['ratedPowerSimple']
        self.bat_cap = configDict['flexConfig']['Battery_capacity']
        self.drain = profiles.drain
        self.chargingPower = profiles.chargingPower
        self.uncontrolledCharge = profiles.uncontrolledCharge
        self.maxBatteryLevel = profiles.maxBatteryLevel
        self.minBatteryLevel = profiles.minBatteryLevel
        self.drain_norm = None
        self.charge_power_norm = None
        self.unc_charge_norm = None
        self.soc_max = None
        self.soc_min = None

    def normalize(self):
        # Normalization basis: Total annual energy
        self.drain_norm = self.drain / self.drain.sum()
        self.unc_charge_norm = self.uncontrolledCharge / self.uncontrolledCharge.sum()

        # Normalization basis: Rated power
        self.charge_power_norm = self.chargingPower / self.rated_power_simple
        if self.grid_config['gridModel'] != 'simple':
            warnings.warn(f"You selected a grid model where normalization is not meaningful. For normalization, the "
                          f"rated power of {self.rated_power_simple} was used.")

        # Normalization of battery level profiles using battery capacity
        self.soc_max = self.maxBatteryLevel / self.bat_cap
        self.soc_min = self.minBatteryLevel / self.bat_cap
