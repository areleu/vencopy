# Welcome to venco.py!

![PyPI](https://img.shields.io/pypi/v/vencopy)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/vencopy)
![Documentation Status](https://readthedocs.org/projects/vencopy/badge/?version=latest)
![PyPI - License](https://img.shields.io/pypi/l/vencopy)
[![Gitter](https://badges.gitter.im/vencopy/community.svg)](https://gitter.im/vencopy/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

- Authors: Niklas Wulff, Fabia Miorelli
- Contact: niklas.wulff@dlr.de

# Contents

- [Description](#description)
- [Installation](#installation)
- [Codestyle](#codestyle)
- [Documentation](#documentation)
- [Useful Links](#useful-links)
- [Want to contribute?](#want-to-contribute)

## Description

A data processing tool offering hourly demand and flexibility profiles for future electric vehicle fleets in an aggregated manner.

## Installation

Depending on if you want to use venco.py or if you want to contribute, there are
two different installation procedures described in venco.py's documentation:

[I want to apply the tool](https://vencopy.readthedocs.io/en/latest/gettingstarted/installation.html#installation-for-users)

[I want to contribute to the codebase, the documentation or the tutorials](https://vencopy.readthedocs.io/en/latest/gettingstarted/installation.html#installation-for-developers)

In order to start using venco.py, check out our [tutorials](https://vencopy.readthedocs.io/en/latest/gettingstarted/start.html#getting-started-and-tutorials). For this you won't need any additional data.

To run venco.py in full mode, you will need the data set Mobilität in Deutschland (German for mobility in Germany). You
can request it here from the clearingboard transport: https://daten.clearingstelle-verkehr.de/order-form.html Currently,
venco.py is so far only tested with the B2 data set of MiD 2008 and MiD 2017.

Enjoy the tool, we're happy to receive feedback!

## Codestyle

We use PEP-8, with the exception of lowerCamelCase for method and variable names as well as UpperCamelCase for classes.

## Documentation

The documentation can be found here: https://vencopy.readthedocs.io/en/latest/index.html
To build the documentation from a conda bash with an activated environment type:

```python
sphinx-build -b html ./docs/ ./build/
```

## Useful Links

- Documentation: https://vencopy.readthedocs.io/en/latest/index.html#
- Source code: https://gitlab.com/dlr-ve/vencopy
- PyPI release: https://pypi.org/project/vencopy
- Licence: https://opensource.org/licenses/BSD-3-Clause

## Want to contribute?

Great, welcome on the venco.py team! Please read our contribute section in the documentation and reach out to Niklas
(niklas.wulff@dlr.de). If experience difficulties on set up or have other technical questions, join our
[gitter community](https://gitter.im/vencopy/community)
