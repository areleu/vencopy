.. VencoPy documentation source file, created for sphinx

.. _outputProfileSpecification:


Profile specification of output profiles
=========================================


This file documents the specifications of the output modules. This is important, because the output produced holds only
numbers without any units. These specifications describes how these values can be interpreted when VencoPy is configured
correctly. Different steps of filtering, aggregation, correction and normalization are performed for the six profiles.
Thus, it is important to understand what the numbers in the output files refer to. For all examples we assume 18000 
individual input profiles for illustrative purposes.

*************
Flow profiles
*************

Profile for connection capacity of the fleet `plugProfile`
############################################################
 
General description
*************************
This profile represents the hourly maximum charging capacity of the electric vehicle fleet. Charging can never be 
higher than this profile but may be lower since not all charging stations run on full capacity or it is attractive for
balancing renewable feed-in to not charge at full capacity. Currently, only one charging capacity per run of VencoPy can
be taken into account. 

Calculation steps
*************************
1. The profile is calculated based on each plug profile which is a basic input into VencoPy. The input profile is made 
up of boolean values describing if the respective vehicle is connected to the grid (1, Yes, True) or not (0, No, False). 
This hourly boolean profiles are multiplied with the charging capacity e.g. 3.7 kW for a simple home charger. 

We're left with 18000 hourly profiles in kW. Functions: `calcChargeProfiles()  <file:///C:/vencopy_repo/build/functions.html#scripts.libProfileCalculation.calcChargeProfiles>`_ in the library `libProfileCalculation.py`.

2. The profiles are filtered according to a specified selector. If 1000 profiles don't fulfill the selection criteria,
we're left with 17000 profiles still in hourly values of kW. Function: `filterConsProfiles()` in the library 
`libProfileCalculation.py`.

3. The filtered individual profiles are then aggregated by a simple averaging of each hourly value to calculate the 
average electricity consumption for one model vehicle for the complete EV fleet. We're left with one profile in hourly values of kW. Function: `aggregateProfiles()` in the library `libProfileCalculation.py`.
profile in hourly values of kW. Function: `aggregateProfilesMean()` in the library `libProfileCalculation.py`.


This profile may later be scaled by the number of vehicles in an EV fleet to calculate the average maximum hourly 
recharge capacity of the EV fleet. 


Profile for uncontrolled charging `chargeProfileUncontrolled`
#################################################################

General description
*************************

For each individual trip and plug profile, one uncontrolled charging profile is calculated. This describes the electric
flow of the grid to the battery under the assumption that the battery is fully charged at beginning of the day. If the 
battery SOC decreases through a trip and the vehicle is connected to the grid, charging occurs with full capacity until
the battery is fully charged. 

Calculation steps
*************************

1. The profile is calculated based on each maximum charge profile. It is equal to every positive difference between the 
maximum SOC in the current hour minus the SOC in the previous hour. Since the maximum SOC profiles adheres to the 
maximum charging capacity, uncontrolled charging may never overshoot the threshold of the defined maximum charging 
capacity. 

We're left with 18000 profiles in kW. Function: `calcChargeProfilesUncontrolled()` in the library 
`libProfileCalculation.py`.

2. The profiles are filtered according to a specified selector. If 1000 profiles don't fulfill the selection criteria,
we're left with 17000 profiles still in hourly values of kW. Function: `filterConsProfiles()` in the library 
`libProfileCalculation.py`.

3. The filtered individual profiles are then aggregated by a simple averaging of each hourly value to calculate the 
average uncontrolled charging power for one "representative" vehicle for the complete EV fleet. We're left with one 

profile in hourly values of kW. Function: `aggregateProfilesMean()` in the library `libProfileCalculation.py`.


4. The aggregated profile is then corrected according to more realistic specific electric consumption measurements. 
Function: `correctProfiles()` in the library `libProfileCalculation.py`.

This profile may later be scaled by the number of vehicles in an EV fleet to calculate the fleet uncontrolled 
charging electric flow. 


Profile for electric demand `electricPowerProfile`
#################################################################

General description
*************************

Each trip profile implies a specific electricity consumption that represents the time-specific electricity-outflow from
the battery to the electric motor for the purpose of propulsion of the vehicle. In the calculation of the electric 
consumption profile, a potential additional fuel demand for longer trips than feasible with the assumed battery capacity
is subtracted to result in the purely electric consumption.

Calculation steps
*************************

1. The profile is calculated based on each drive profile which is a basic input into VencoPy. The individual drive 
profiles are scaled with the electric consumption given in the technical vehicle characteristics. If the battery 
capacity doesn't suffice for the trip distance, additional fuel demand is subtracted to only account for electricity
consumption. 

We're left with 18000 hourly profiles in kW. Functions: `calcDrainProfiles()` and `calcElectricPowerProfiles()` in the library `libProfileCalculation.py`.

2. The profiles are filtered according to a specified selector. If 1000 profiles don't fulfill the selection criteria,
we're left with 17000 profiles still in hourly values of kW. Function: `filterConsProfiles()` in the library 
`libProfileCalculation.py`.

3. The filtered individual profiles are then aggregated by a simple averaging of each hourly value to calculate the 
average electricity consumption for one model vehicle for the complete EV fleet. We're left with one
profile in hourly values of kW. Function: `aggregateProfilesMean()` in the library `libProfileCalculation.py`.


4. The aggregated profile is then corrected according to more realistic specific electric consumption measurements. 
Function: `correctProfiles()` in the library `libProfileCalculation.py`.
This profile may later be scaled by the number of vehicles in an EV fleet to calculate the average electric flow leaving 
the EV fleet battery. 



Profile for additional fuel consumption `driveProfileFuelAux`
#################################################################

General description
*************************

This profile gives hourly values for fuel consumption in case a trip and plug profile cannot be supplied only from the 
vehicle battery. This profile is given in units of l of the specified fuel. 

Calculation steps
*************************

1. The profile is calculated based on the drive profile (basic input), the uncontrolled charging profile, the maximum 
SOC profile and vehicle specifications. It describes fuel consumption for the most optimistic case of uncontrolled 
charging and a fully charged battery at the beginning of the day. It is equal to the electric consumption for driving
minus the electric flow from the battery minus uncontrolled charging. Since all of these profiles are in units of kW, 
the resulting energy needs are then transferred from kWh to l of fuel. 

We're left with 18000 hourly profiles in l. 
Functions: `calcDriveProfilesFuelAux()` in the library `libProfileCalculation.py`.

2. The profiles are filtered according to a specified selector. If 1000 profiles don't fulfill the selection criteria,
we're left with 17000 profiles still in hourly values of l fuel. Function: `filterConsProfiles()` in the library 
`libProfileCalculation.py`.

3. The filtered individual profiles are then aggregated by a simple averaging of each hourly value to calculate the 
average fuel consumption for one model vehicle for the complete EV fleet. We're left with one profile in hourly values
of l fuel. Function: `aggregateProfilesMean()` in the library `libProfileCalculation.py`.


4. The aggregated profile is then corrected according to more realistic specific fuel consumption measurements. 
Function: `correctProfiles()` in the library `libProfileCalculation.py`.

This profile may later be scaled by the number of vehicles in an EV fleet to calculate the average fuel consumption 
needed by the hybrid electric vehicle fleet. 


**************
State profiles
**************

Maximum state-of-charge profile
#################################################################

Minimum state-of-charge profile
#################################################################