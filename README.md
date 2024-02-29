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

Depending on if you want to use venco.py or if you want to contribute, there are
two different installation procedures described in venco.py's documentation:

[I want to apply the tool](https://dlr-ve.gitlab.io/esy/vencopy/vencopy/gettingstarted/installation.html#installation-for-users)

[I want to contribute to the codebase, the documentation or the tutorials](https://dlr-ve.gitlab.io/esy/vencopy/vencopy/gettingstarted/installation.html#installation-for-developers)

In order to start using venco.py, check out our [tutorials](https://dlr-ve.gitlab.io/esy/vencopy/vencopy/gettingstarted/start.html). For this you won't need any additional data.

To run venco.py in full mode, you will need the data set Mobilität in Deutschland (German for mobility in Germany). You
can request it here from the clearingboard transport: https://daten.clearingstelle-verkehr.de/order-form.html 
Alternatively you can use venco.py with any National Travel Survey or mobility pattern dataset.


## Codestyle

We use PEP-8, with the exception of UpperCamelCase for class names.

## Documentation

The documentation can be found here: https://dlr-ve.gitlab.io/esy/vencopy/vencopy/
To build the documentation from a conda bash with an activated environment type:

```python
sphinx-build -b html ./docs/ ./build/
```

## Useful Links

- Documentation: https://dlr-ve.gitlab.io/esy/vencopy/vencopy/
- Source code: https://gitlab.com/dlr-ve/vencopy
- PyPI release: https://pypi.org/project/vencopy/
- Licence: https://opensource.org/licenses/BSD-3-Clause

## Want to contribute?

Great, welcome on the venco.py team! Please read our contribute section in the documentation and reach out to Niklas
(niklas.wulff@dlr.de). If you experience difficulties on set up or have other technical questions, join our
[gitter community](https://gitter.im/vencopy/community)
