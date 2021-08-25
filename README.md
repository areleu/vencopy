# Welcome to VencoPy!
[![PyPI version](https://badge.fury.io/py/vencopy.svg)](https://badge.fury.io/py/vencopy)

- Authors: Niklas Wulff, Fabia Miorelli
- Contact: niklas.wulff@dlr.de
- Version: 0.1.0

[![License: BSD 3-Clause](https://img.shields.io/badge/license-BSD%203--Clause-blue.svg)](https://github.com/TechSmith/hyde/blob/master/LICENSE.txt)
[![Documentation Status](https://readthedocs.org/projects/vencopy/badge/?version=latest)](https://vencopy.readthedocs.io/en/latest/?badge=latest)


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
conda env create -f requirements.yml
[confirm]
```

An environment named "VencoPy_env" will be created. Activate by entering
```python
conda activate VencoPy_env
```

Build your package locally (compared to from the package index PyPI) by navigating to the folder that you checked out 
and typing

```python
pip install .
```

Navigate to a parent directory where you want to create your VencoPy user folder in and type

```python
vencopy
```

You will be prompted for a userfolder name, type it and hit enter. Your VencoPy user folder will now be created. It
will look like this:



    FOLDERNAME
    ├── config
    │   ├── evaluatorConfig.yaml
    │   ├── flexConfig.yaml
    │   ├── globalConfig.yaml
    │   ├── gridConfig.yaml
    │   ├── localPathConfig.yaml
    │   └── parseConfig.yaml
    ├── output
    │   ├── tripConfig.yamldataParser
    │   ├── evaluator
    │   ├── flexEstimator
    │   ├── gridModeler
    │   └── tripDiaryBuilder 
    ├── tutorials          
    │   └── ..
    └── run.py

The configs in the config folder are the main interface between the user and the code. In order to learn more about 
them, check out our tutorials. For this you won't need any additional data.

To run VencoPy in full mode, you will need the data set Mobilität in Deutschland (German for mobility in Germany), you
can request it here from the clearingboard transport: https://daten.clearingstelle-verkehr.de/order-form.html Currently, 
VencoPy is only tested with the B2 data set.

In your localPathConfig.yaml, please enter the path to your local MiD STATA folder, it will end on .../B2/STATA/. Now
open your user folder in an IDE, configure your interpreter (environment) or type: 

```python
python run.py
``` 

and enjoy the tool!



Codestyle
---
We use PEP-8, with the exception of lowerCamelCase for method and variable names as well as UpperCamelCase for classes.

Documentation
---
The documentation can be found here: https://vencopy.readthedocs.io/en/latest/index.html
To build the documentation from a conda bash with an activated environment type:

```python
sphinx-build -b html ./docs/ ./build/
``` 

Want to contribute?
---
Great, welcome on the VP team! Please read our contribute section in the documentation and reach out to Niklas(niklas.wulff@dlr.de). 
