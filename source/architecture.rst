..  VencoPy introdcution file created on September 15, 2020
    by Niklas Wulff
    Licensed under CC BY 4.0: https://creativecommons.org/licenses/by/4.0/deed.en
    
.. _architecture:

Architecture documentation
===================================


General framework and degree of object-orientation
---------------------------------------------------

VencoPy was specifically designed to not strictly follow object-oriented programming. E.g., so far there is no class SOCProfiles and respective instances. This was a design choice early on in the design phase and is rooted in the tools orientation on simplicity and easy accessibility. VencoPy's structure is very simple: There is a main script, calling functions defined in various libraries (inputLib, processingLib etc.) that in turn access the config and input data. After the data read-in, all functions build on the output of preceding function calls and every calculation can be transparently read in the main script. Explicit prioritization of values are listed and described below. 


Quality values
---------------------------------------------------

.. list-table:: Quality values
   :widths: 50, 50
   :header-rows: 1

   * - Value prioriy
     - Description
   * - 1. Learnability
     - The highest priority of VencoPy is to provide an easy-to-apply tool for scientists (not primarily developpers) to estimate electric vehicle fleets' load shifting potential. Hence, easy to understand approaches, code structures and explicit formulations are favored.
   * - 2. Readibility
     - The readability of the code, especially the linear flow structure of the main VencoPy file should be preserved. Source code and equations should be easy to read and understand. Splitting of statements is favored over convoluted one-liners. Significant learnability improvements (e.g. through an additional library), may motivate a deviation from this principle. 
   * - 3. Reproducibility
     - The model has to be deterministic and reproducible both on the scientific and on the technical level. Hence, all example and test artifacts have to be part of the git repository. Source code revisions should be used to reference reproducible states of the source code. 
   * - 4. Reliability
     - The software has to operate without crashes and should terminate early, if scientific requirements are not met. Testing and asserting expectations in code is encouraged. However, in its alpha-version no special error catching routines are implemented.
   * - 5. Performance
     - Performance is not a high priority, since runtimes are quite quick. However, basic performance considerations like efficient formulation and application of libraries and algorithms should be taken into account. 


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
     - Alpha release in Q3 2020, Beta release in Q2 2021
   * - Open source ready
     - Features, dependencies and components which are contraindicative or at odds with an open source publication should not be used
   * - Development tools
     - Source code and all artefacts are located in the DLR GITLAB repository for VencoPy including the software documentation. For development, the PyCharm community edition IDE and gitbash are used. For graphical depictions of software components and similar documentation draw.io is used.





