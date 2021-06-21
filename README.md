[![License: BSD 3-Clause](https://img.shields.io/badge/license-BSD%203--Clause-blue.svg)](https://github.com/TechSmith/hyde/blob/master/LICENSE.txt)



# VencoPy README File
- Author: Niklas Wulff
- Contact: niklas.wulff@dlr.de
- Version: 0.0.8 (pre-release)

Contents
========

 * [Description](#description)
 * [Installation](#installation)
 * [Codestyle](#codestyle)
 * [Documentation](#documentation)
 * [Want to contribute?](#want-to-contribute)

Description
---
A data processing tool offering hourly demand and flexibility profiles for future electric vehicle fleets in an aggregated manner.

Installation
---
Install using the environment management system conda, open the conda console navigate to your VencoPy folder and run the following commands

```python
conda create -f requirementsPreRelease.yml
[confirm]
```

An environment named "VencoPy_preRelease" will be created. Activate by entering
```python
conda activate VencoPy_preRelease 
```

In the same folder, run VencoPy by typing 
```python
python venco_main.py
```

Codestyle
---
We use PEP-8, with the exception of lowerCamelCase for method and variable names as well as UpperCamelCase for classes

Documentation
---
Build docu from a conda bash with activated environment typing

sphinx-build -b html ./source/ ./build/

Want to contribute?
---
Email
