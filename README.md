# Welcome to venco.py!

- Authors: Niklas Wulff, Fabia Miorelli
- Contact: vencopy@dlr.de

# Contents

- [Description](#description)
- [Installation](#installation)
- [Codestyle](#codestyle)
- [Documentation](#documentation)
- [Useful Links](#useful-links)
- [Want to contribute?](#want-to-contribute)

## Description

A data processing tool estimating hourly electric demand and flexibility profiles for future 
electric vehicle fleets. Profiles are targeted to be scalable for the use in large-scale
energy system models. 

## Installation

After around 2 years after the initial public commit, we currently go through the release of 
our re-iteration of the tool providing more flexibility regarding temporal resolution. 
The documentation will be updatet and the PyPI package will be uploaded in the coming weeks. 

## Codestyle

We use PEP-8, with the exception of UpperCamelCase for classes.

## Documentation

The documentation of the previous release (0.1.5) can be found 
here: https://vencopy.readthedocs.io/en/latest/index.html.
To locally build the documentation from a conda bash with an activated 
environment type:

```python
sphinx-build -b html ./docs/ ./build/
```

## Useful Links

- Documentation: Available soon
- Source code: https://gitlab.com/dlr-ve/vencopy
- PyPI release: Available soon
- Licence: https://opensource.org/licenses/BSD-3-Clause

## Want to contribute?

Great, welcome on the venco.py team! Please read our contribute section in the documentation and reach out to Niklas
(niklas.wulff@dlr.de). If you experience difficulties on set up or have other technical questions, join our
[gitter community](https://gitter.im/vencopy/community)
