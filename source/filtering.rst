.. VencoPy documentation source file, created for sphinx

.. _filtering:


Filtering functionalities
===================================


In the following, filtering procedures in VencoPy for individual profiles are documented. Filtering occurs after
the completion of the main calculation steps using selectors. These are calculated for the four flow-related 
profiles (consumption, plugPower, uncontrolledCharge and auxilliaryFuelConsumption) in calcProfileSelectors().

Four criteria are applied to select individual profiles that are eligible for load shifting.
1.  Profiles that depend on auxilliary fuel are excluded. These are profiles where consumption is higher than available
battery SOC for at least one hour. This can also occur when vehicles drive only short distances but don't connect to the grid sufficiently.
2.  A minimum daily mileage can be set to filter out profiles that don't drive at all or only very little.
3.  A fully charged battery doesn't suffice for the daily mileage 
4.  Available charging throughout the day doesn't supply sufficient energy for the driven distance.

For the two state-based profiles SOCmax and SOCmin


