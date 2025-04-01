# CORDEX-CMIP6 Compliance Checker Plugin

[![PyPI](https://img.shields.io/pypi/v/cc-plugin-cc6?label=PyPI&logo=pypi)](https://pypi.org/project/cc-plugin-cc6/)
[![Anaconda-Server Badge](https://anaconda.org/conda-forge/cc-plugin-cc6/badges/version.svg)](https://anaconda.org/conda-forge/cc-plugin-cc6)

This [ioos/compliance-checker](https://github.com/ioos/compliance-checker) plugin checks compliance with CORDEX-CMIP6 archive specifications:

| Standard                                                                                             | Checker Name |
| ---------------------------------------------------------------------------------------------------- | ------------ |
| [cordex-cmip6-cv](https://github.com/WCRP-CORDEX/cordex-cmip6-cv)         |  cc6         |
| [cordex-cmip6-cmor-tables](https://github.com/WCRP-CORDEX/cordex-cmip6-cmor-tables)|  cc6         |
| [CORDEX-CMIP6 Archive Specifications](https://doi.org/10.5281/zenodo.10961069) | cc6 |

## Installation

### Conda

```shell
$ conda install -c conda-forge cc-plugin-cc6
```

### Pip

```shell
$ pip install cc-plugin-cc6
```

See the [ioos/compliance-checker](https://github.com/ioos/compliance-checker#installation) for additional Installation notes

## Usage

```shell
$ compliance-checker -l
IOOS compliance checker available checker suites (code version):
  ...
  - cc6 (x.x.x)
  ...
$ compliance-checker -t cc6 [dataset_location]
```

See the [ioos/compliance-checker](https://github.com/ioos/compliance-checker) for additional Usage notes


## Summary of the Checks
This plugin shall check the suitability of a dataset to be published via the [ESGF](https://esgf-data.dkrz.de/projects/esgf-dkrz/) in the official CORDEX-CMIP6 project and checks the compliance with the CORDEX-CMIP6 CV(https://github.com/WCRP-CORDEX/cordex-cmip6-cv), the [CORDEX-CMIP6 CMOR tables](https://github.com/WCRP-CORDEX/cordex-cmip6-cmor-tables) as well as the [CORDEX-CMIP6 Archive Specifications](https://doi.org/10.5281/zenodo.10961069).

### High priority checks
Failures in these checks have to be addressed before submitting the data for publication via the ESGF project CORDEX-CMIP6!

- check


### Medium priority checks:
Failures in these checks should be addressed before submitting the data for publication via the ESGF project CORDEX-CMIP6!

- check


### Low priority checks

- check


## Environment variables
Path for the [CORDEX-CMIP6 CMOR tables](https://github.com/WCRP-CORDEX/cordex-cmip6-cmor-tables) (subdirectory Tables):
- `CORDEXCMIP6TABLESPATH`
