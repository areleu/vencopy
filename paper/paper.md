---
title: 'An open-source Python-based model to represent the charging flexibility and vehicle-to-grid potential of electric vehicles in energy systems models: venco.py'
tags:
  - electric vehicles modelling
  - demand-side flexibility
  - charging strategies
  - energy systems analysis 

authors:
  - name: Fabia Miorelli^[corresponding author]
    orcid: 0000-0001-5095-5401
    affiliation: 1
  - name: Niklas Wulff
    orcid: 0000-0002-4659-6984
    affiliation: 1
  - name: Benjamin Fuchs
    orcid: 0000-0002-7820-851X
    affiliation: 1
  - name: Hans Christian Gils
    orcid: 0000-0001-6745-6609
    affiliation: 1
  - name: Patrick Jochem
    orcid: 0000-0002-7486-4958
    affiliation: 1

affiliations:
 - name: German Aerospace Center (DLR), Institute of Networked Energy Systems, Curiestr. 4, 70563 Stuttgart, Germany
   index: 1


date: 29 February 2024
bibliography: paper.bib

---

# Summary

The bottom-up simulation model venco.py provides boundary conditions for load
shifting and vehicle-to-grid (V2G) potentials based on mobility demand data and
techno-economic assumptions. The tool allows the modelling of the energy demand
and flexibility potential of electric vehicle (EV) fleets within the field of
energy systems analysis. It can be used to model EV charging both in a
controlled (load shifting and V2G) and uncontrolled manner. Different charging
infrastructure assumptions as well as different technical characteristics for
the vehicle fleet can be considered, which allow the modelling of heterogeneous
fleets and various infrastructure deployment scenarios. The main modelling
outputs include battery drain profiles, charging capacity profiles, a minimum
and a maximum battery energy levels and uncontrolled charging profiles both at
single vehicle and at fleet level. The first four profiles can be used as
constraining boundaries in other models to determine optimal charging strategies
for the vehicles, representing this way an endogenous EV demand, whereas the
last profile simulates a case, where charging is uncontrolled and the vehicles
can charge as soon as a charging possibility is available. The very generic
nature of the model's resulting profiles allows their usage to answer a wide
range of research questions in multiple models, such as energy system
optimisation models [@Wetzel.2024; @Howells.2011; @Brown.2018] or agent-based
electricity market models [@Schimeczek.2023].


# Statement of need

Being able to estimate the electricity demand, the load shifting or V2G
potential of EV fleets is of interest in a wide range of research fields and
industry applications, be it to analyse future grid ancillary service demands,
the profit of aggregators managing EV fleets, to calculate the additional
electricity demand caused by an increased penetration of EVs or the demand-side
flexibility these could provide to the energy system.

Within the energy systems analysis field, two approaches to model EVs can be
identified, being either data-driven approaches based on empirically measured
data or bottom-up simulations. Data-driven approaches start from either measured
state of charge data on board of the vehicles or data collected at charging
stations to then scale these up to represent a fleet with respect to the
modelling scope [@Martz.2022]. On the other hand, bottom up simulation
models derive the flexibility and load profiles of EV fleets by taking a set of
assumptions related to the charging controllability and the technical parameters
of the vehicles [@GaeteMorales.2021]. These models usually use mobility
pattern data as a starting basis, which can be taken from National Travel
Surveys (NTSs) or be the simulation results from transport research models. A
few tools to calculate EV fleet profiles have been recently published, such as
Emobpy [@GaeteMorales.2021], RAMP-mobility [@Mangipinto.2022], OMOD
[@Strobel.2023], SimBEV [@simbev.2023] and MobiFlex [@Martz.2022].
Similarly to these models, venco.py was developed to provide demand and
flexibility potentials of future EV fleets. While the application focus of
venco.py to date was on plug-in battery electric vehicles, the novelty of the
framework in contrast to existing models consists in its upcoming capability to
also model plug-in hybrid electric vehicles (PHEV) and in ready to use
interfaces to several existing NTSs for European countries. Another novelty
consists in the possibility to carry out analyses for specific vehicle classes,
socio-economic groups or higher spatial resolutions than national level. As NTSs
contain a broad set of data in addition to the mere mobility patterns, it is
possible to generate EV profiles that can be linked to specific groups of EV
users.

The venco.py model is completely developed in Python and openly available on
Gitlab at https://gitlab.com/dlr-ve/esy/vencopy/vencopy.


# Modelling approach

The venco.py model is designed to allow the modelling of heterogeneous vehicle
fleets in a user-friendly and flexible way. While the model has so far been
applied on the German NTS ("Mobilität in Deutschland")
[@infasDLRIVTundinfas360.2018], the English NTS ("National Travel Survey:
England 2019") [@DepartmentforTransport.2019], the Dutch NTS ("Documentatie
onderzoek Onderweg in Nederland 2019 (ODiN2019)")
[@CentraalBureauvoordeStatistiek.2022] and the French NTS ("La mobilit{\'e}
locale et longue distance des Fran{\c{c}}ais - Enqu{\^e}te nationale sur la
mobilit{\'e} des personnes en 2019")
[@Leservicedesdonneesetetudesstatistiques.2023], it can accommodate any
input data representing mobility patterns of a fleet for the respective
modelling scope because of its flexible parsing approach. The model additionally
features a user-defined temporal and geographical resolution, and allows the
modelling both at an individual vehicle and at fleet level.

The model is based on the main building blocks shown in Fig. 1. Starting from a
parsing interface to mobility datasets, the data is first cleaned and filtered
for plausibility and variables are consolidated to model internal variables.
Following that, the charging infrastructure allocation takes place, which makes
use of a basic charging infrastructure model, that assumes the availability of
charging stations when vehicles are parked. Since the analytical focus of the
model lies on a regional level, the charging infrastructure availability is
allocated either based on binary mappings related to the respective trip
purpose, or it can be based on probability distributions for charging
availability. This way, different charging availability scenarios can be
distinguished. Subsequently, the demand and flexibility estimation are
calculated, which are based on techno-economic input assumptions related mostly
to the vehicle battery capacity and the power consumption. After an iteration
process, this yields the minimum and maximum battery constraints. The individual
trips at the survey day are afterwards consolidated into person-specific travel
diaries, which comprise multiple trips that are carried out by each vehicle
within a day. These daily activities are then discretised from a table to a time
series format. The last step in the framework is the aggregation from single
vehicle profiles to fleet level and the creation of annual profiles from the
daily and weekly samples, with an option to normalise the profiles or scale them
to a desired fleet size. 

The model output is furnished with an automatic metadata generation in line with
the metadata requirements of the Open Energy Platform (OEP)
[@Booshehri.2021], which enables the fulfilment of the FAIR (Findable,
Accessible, Interoperable, Reusable) principles [@Wilkinson.2016].

![Structure of venco.py.](vencopy_structure_simple.pdf)


# Projects and Publications

Earlier versions of venco.py have been used throughout different projects. In
the EVer project [@ever.2023], the model was, for example, used to perform a
comparison of different modelling approaches for demand-side flexibility assets
in the energy system optimisation model REMix [@Wetzel.2024]. A more
detailed assessment of the transport sector has been carried out in the BEniVer
[@beniver.2023] and UrMoDigital [@urmo.2023] projects, where the role of
synthetic fuels, EVs and new vehicle concepts have been analysed respectively.
The model has also been used within the framework of the SEDOS project
[@sedos.2023], whose aim is the creation of a an open-source national
reference energy system dataset and the development of a reference model for
three open-source frameworks (oemof [@Krien.2020], TIMES [@TIMES2022],
FINE [@Gro.2023]) with a focus on the German energy system. In the En4U
project [@en4u.2023] venco.py was used to create representative load
profiles based mobility patterns of different household clusters and on
residential time varying tariffs. In the European DriVe2X project the model is
being used to assess the impact of mass deployment of V2X technologies on the
energy systems as a whole [@drive2x.2023].


# Acknowledgements

The development of venco.py was financed through various third-party funded
projects supported by the German Federal Ministry of Economics and Climate
Protection (BMWK) including BEniVer ("Begleitforschung Energiewende im Verkehr",
FKZ 03EIV116F), SEDOS ("Die Bedeutung der Sektorintegration im Rahmen der
Energiewende in Deutschland - Modellierung mit einem nationalen Open Source
ReferenzEnergieSystem", FKZ 03EI1040D), and En4U ("Entwicklungspfade eines
dezentralen Energiesystems im Zusammenspiel der Entscheidungen privater und
kommerzieller Energieakteure unter Unsicherheit", FKZ 3026201). Additional
funding was provided by the German Aerospace Center (DLR) through the internal
projects UrMoDigital ("Wirkungen der Digitalisierung auf städtische
Verkehrssysteme") and EVer ("Energie und Verkehr"), by the European Union
through the DriVe2X project ("Delivering Renewal and Innovation to mass Vehicle
Electrification enabled by V2X technologies", Horizon Europe 101056934), and by
the Helmholtz Association's Energy System Design research program.


# References