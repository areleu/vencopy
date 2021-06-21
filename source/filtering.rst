.. VencoPy documentation source file, created for sphinx

.. _filtering:


Filtering functionalities
===================================


In the following, filtering procedures in VencoPy for individual profiles are documented. Filtering occurs after
<<<<<<< HEAD
the completion of the main calculation steps using selectors. These are calculated based only on the four flow-related 
profiles (consumption, plugPower, uncontrolledCharge and auxilliaryFuelConsumption) in `calcProfileSelectors()` and 
applied to both flow-profiles and state-profiles. 

Four criteria are applied to select individual profiles that are eligible for load shifting.

1.  Profiles that depend on auxilliary fuel are excluded. These are profiles where consumption is higher than available
    battery SOC for at least one hour. This can also occur when vehicles drive only short distances but don't connect
    to the grid sufficiently.
    
2.  A minimum daily mileage in km can be set in the non-profile data (per default VencoPy_scalarInput.xlsx) to filter 
    out profiles where the mileage is below a specified threshold. In the shipped file, this value is set to 0. 

3.  In case, a fully charged battery doesn't suffice for the daily mileage of the respective profile, this profiles is
    excluded.

4.  Available charging throughout the day doesn't supply sufficient energy for the driven distance. This may occur even
    though the profile is eligible from criteria 3 e.g. when connection is never possible. 
=======
the completion of the main calculation steps using selectors. These are calculated for the four flow-related 
profiles (consumption, plugPower, uncontrolledCharge and auxilliaryFuelConsumption) in calcProfileSelectors().

Four criteria are applied to select individual profiles that are eligible for load shifting.
1.  Profiles that depend on auxilliary fuel are excluded. These are profiles where consumption is higher than available
battery SOC for at least one hour. This can also occur when vehicles drive only short distances but don't connect to the grid sufficiently.
2.  A minimum daily mileage can be set to filter out profiles that don't drive at all or only very little.
3.  A fully charged battery doesn't suffice for the daily mileage 
4.  Available charging throughout the day doesn't supply sufficient energy for the driven distance.

For the two state-based profiles SOCmax and SOCmin


>>>>>>> MiD2017
