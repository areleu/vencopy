.. venco.py documentation source file, created for sphinx

.. _faq:


Frequently Asked Questions (FAQs)
===================================

1. What is the geographical resolution of venco.py? venco.py is currently based on the national travel survey for passenger vehicles in Germany
   (MiD2017) and, as such, intrinsically provides information on hourly demand and flexibility profile for EVs at national scale. As these profiles
   are provided with respect to the average vehicle, they can however be scaled to a higher geographical resolution (e.g. subnational, regional level)
   by multiplying them with the vehicle fleet details (number of vehicles).


2. Which aggregation methods are employed to go from single profiles to fleet profiles? Currently, to go from single profiles to a fleet profile,
   venco.py employs an averaging method and a security parameter *alpha*. The *alpha* value is a percentage value that selects the nth highest value
   for each hour of the minimum and maximum profiles. For example, if *alpha* equals 10 (10 is the default value), the 10% highest (respectively 10%
   smallest) values are disregarded in the calculation of the maximum and minimum state of charge of the fleet battery. Additional information on the
   influence of aggregation methods on EV fleet battery can be found here :ref:`Aggregating load shifting potentials of electric vehicles for energy
   system models <https://ieeexplore.ieee.org/document/9221974>`.


3. Is the the MiD dataset open-access? The MiD dataset(s) is not open-access but can be requested for scientific purposes :ref:`here
   <https://daten.clearingstelle-verkehr.de/279/>`.


4. Are seasonal differences accounted for in venco.py output profiles? (temperature dependency etc.) Currently, temperature dependency is not
   accounted for in the calculation of the electricity consumption.


5. Can representative profiles be derived in order not to have to do an aggregation to fleet level? It is possible to use indiviual profile by
   avoiding the aggregation step in the flexEstimator class.


6. Are multi-day SOC and plugging behavior accounted for? Plugging behaviour and multi-day SoC are currently not included.


7. Are charging availabilities and distributions modelled? The grid infrastructure is currently based on a simple binary True-False mapping based on
   purpose for which the trip was carried out. In the next release, charging availabilities and distribution will be modelled. A first non validated
   implementation is already provided in the current release.