..  VencoPy introdcution file created on September 15, 2020
    by Niklas Wulff
    Licensed under CC BY 4.0: https://creativecommons.org/licenses/by/4.0/deed.en
    
.. _architecture:

Architecture documentation
===================================


General framework and degree of object-orientation
---------------------------------------------------

VencoPy was specifically designed to not strictly follow object-oriented programming. E.g., so far there is no class SOCProfiles and respective instances. This was a design choice early on in the design phase and is rooted in the tools orientation on simplicity and easy accessibility. VencoPy's structure is very simple: There is a main script, calling functions defined in various libraries (inputLib, processingLib etc.) that in turn access the config and input data. After the data read-in, all functions build on the output of preceding function calls and every calculation can be transparently read in the main script. 

VencoPy is written in a compromise of object-oriented programming and functional programming. Mainly, we follow a 
master-script that calls functions defined in the respective libraries (that again possibly call sub-functions from the
same library). In its current version, VencoPy doesn't use class instances as it is a linear data calculation tool 
that takes input data and assumptions in the beginning and calculates estimated flexibility profiles in the end. Thus, 
a script structure seems intuitive. However, we make use of object-orientation features of libraries such as pandas.

Additionally, except for the SOC profile calculations, that are still subject to evaluation and development, all 
functions are quite easy and straightforward to comprehend. Thus, we mirror VencoPy's simplicity on the implementation
level.

Explicit prioritization of values are listed and described below. 


Quality values
---------------------------------------------------

.. list-table:: Quality values
   :widths: 50, 50
   :header-rows: 1

   * - Value prioriy
     - Description
   * - 1. Learnability
     - The highest priority of VencoPy is to provide a straight-forward easy-to-apply tool to estimate electric vehicle fleets' load shifting potential. 
   * - 2. Reproducability
     - Reproducibility is achieved by transparent versioning of the VencoPy releases as well as its well-defined input definition.
   * - 3. Reliability
     - Reliability is tested on various machines before release. However, in its alpha-version no special error catching routines are implemented.
   * - 4. Maintainability
     - Through its simple and straightforward design, VencoPy is easily maintained. Repeating methods or calls are unified where learnability is not threatened.
   * - 5. Performance
     - Performance is not a high priority, since runtimes are quite quick. However, basic coding principles are applied to not unnecessarily overload the code or methods in the code. 


Organizational information
---------------------------------------------------

.. list-table:: requirements
   :widths: 50, 50
   :header-rows: 1

   * - Requirement
     - Context
   * - Software Engineering Team (SET)
     - Niklas Wulff, Benjamin Fuchs
   * - Stakeholders
     - Hans Christian Gils, Department of energy system analysis at Institute of Networked Energy Systems, DLR
   * - Timeline
     - Alpha release in Q3 2020, Beta release in Q1 2021
   * - Open source ready
     - Features, dependencies and components which are contraindicative or at odds with an open source publication should not be used
   * - Development tools
     - Source code and all artefacts are located in the DLR GITLAB repository for VencoPy including the software documentation. For development, the PyCharm community edition IDE and gitbash are used. For graphical depictions of software components and similar documentation draw.io is used.





