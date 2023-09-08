..  venco.py introdcution file created on September 15, 2020
    by Niklas Wulff
    Licensed under CC BY 4.0: https://creativecommons.org/licenses/by/4.0/deed.en

.. _architecture:

Architecture Documentation
===================================

General structure of the framework
---------------------------------------------------

The figure below shows the detailed venco.py components in a developer diagram. The four components - implemented as Python classes - DataParser,
TripDiaryBuilder, GridModeler and FlexEstimator can be clearly distinguished. A fifth component, Evaluator, used to create the plots for this work is
not shown in the figure as it is not needed to calculate results from venco.py. A brief description of the classes is presented below. For a more
detailed algebraic description of the tool please refer to the :ref:`publications` section.


.. image:: ../figures/vencopy_classes.png
	:width: 600
	:align: center


Quality values
---------------------------------------------------

.. list-table:: Quality values
   :widths: 35, 65
   :header-rows: 1

   * - Value priority
     - Description
   * - 1. Learnability
     - The highest priority of venco.py is to provide an easy-to-apply tool for scientists (not primarily developers) to estimate electric vehicle
       fleets' load shifting potential. Hence, easy to understand approaches, code structures and explicit formulations are favored.
   * - 2. Readability
     - The readability of the code, especially the linear flow structure of the main venco.py file should be preserved. Source code and equations
       should be easy to read and understand. Splitting of statements is favored over convoluted one-liners. Significant learnability improvements
       (e.g. through an additional library), may motivate a deviation from this principle.
   * - 3. Reproducibility
     - The model has to be deterministic and reproducible both on the scientific and on the technical level. Hence, all example and test artifacts
       have to be part of the git repository. Source code revisions should be used to reference reproducible states of the source code.
   * - 4. Reliability
     - The software has to operate without crashes and should terminate early, if scientific requirements are not met. Testing and asserting
       expectations in code is encouraged. However, in its alpha-version no special error catching routines are implemented.
   * - 5.Performance
     - Performance is not a high priority, since runtimes are quite quick. However, basic performance considerations like efficient formulation and
       application of libraries and algorithms should be considered.



Organizational information
---------------------------------------------------

.. list-table:: requirements
   :widths: 35, 65
   :header-rows: 1

   * - Requirement
     - Context
   * - Software Engineering Team
     - Niklas Wulff, Fabia Miorelli, Benjamin Fuchs
   * - Stakeholders
     - Hans Christian Gils, Department of Energy Systems Analysis at Institute of Networked Energy Systems, DLR
   * - Timeline
     - Alpha release in Q4 2020, Beta release in Q3 2021, for further releases planned, see :ref:`releaseTimeline`
   * - Open source ready
     - Features, dependencies and components which are contraindicative or at odds with an open source publication should not be used
   * - Development tools
     - Source code and all artefacts are located in the DLR GitLab repository for venco.py including the software documentation. For development, the
       PyCharm community edition IDE, VSCode and gitbash are used. For graphical depictions of software components and similar documentation draw.io
       and InkScape used.





