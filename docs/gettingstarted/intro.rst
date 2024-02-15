..  venco.py introduction file created on February 11, 2020
    by Niklas Wulff
    Licensed under CC BY 4.0: https://creativecommons.org/licenses/by/4.0/deed.en

.. _intro:

Introduction
===================================


Future electric vehicle fleets pose both challenges and opportunities for power
systems. While increased power demand from road transport electrification
necessitates expansion of power supply, vehicle batteries can to a certain
degree shift their charging load to times of high availability of power. The
model-adequate description of power demand from future plug-in electric vehicle
fleets is a pre-requisite for modelling sector-coupled energy systems and
drawing respective policy-relevant conclusions. Vehicle Energy Consumption in
Python (venco.py) is a tool that provides boundary conditions for load shifting
and vehicle-to-grid potentials based on transport demand data and
techno-economic assumptions. The main structural assumption is that 
mobility patterns will remain similar to nowadays patterns. Profiles are created 
to be scalable with fleet scenario development data. Also, profiles are provided
following a perfect-foresight approach.

venco.py has so far been applied to the German, Dutch, 
UK and French national travel surveys to derive hourly load-shifting-
constraining profiles for the energy system optimization model
`REMix <https://gitlab.com/dlr-ve/esy/remix/framework>`_.


.. image:: ../figures/vencopy_usage_examples.png
	:width: 600
   	:align: center
..
