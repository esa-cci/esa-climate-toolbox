[![Build status](https://ci.appveyor.com/api/projects/status/ofws8cu11xpw89tu?svg=true)](https://ci.appveyor.com/project/bcdev/esa-climate-toolbox)
[![Documentation Status](https://readthedocs.org/projects/esa-climate-toolbox/badge/?version=latest)](https://esa-climate-toolbox.readthedocs.io/en/latest/?badge=latest)
[![Anaconda-Server Badge](https://anaconda.org/conda-forge/esa-climate-toolbox/badges/version.svg)](https://anaconda.org/conda-forge/esa-climate-toolbox)
[![Anaconda-Server Badge](https://anaconda.org/conda-forge/esa-climate-toolbox/badges/license.svg
)](https://anaconda.org/conda-forge/esa-climate-toolbox)

# ESA CCI Toolbox

The ESA CCI Toolbox is designed to provide access to CCI data from the ESA 
Open Data Portal. 
Also, it contains functions to operate on this data.

## Installation into a new environment with conda

The ESA CCI Toolbox and all necessary dependencies are available
on [conda-forge](https://conda-forge.org/), and can be installed using the
[conda package manager](https://docs.conda.io/projects/conda/en/latest/).
The conda package manager itself can be obtained in the [miniconda
distribution](https://docs.conda.io/en/latest/miniconda.html). 
Once conda is installed, the ESA CCI Toolbox can be installed like this:

```
$ conda create --name ect --channel conda-forge esa-climate-toolbox
$ conda activate ect
```

The name of the environment may be freely chosen.

#### Installation into an existing environment with conda

The ESA CCI Toolbox can also be installed into an existing conda 
environment.
To do so, execute this command with the existing environment activated:

```
$ conda install --channel conda-forge esa-climate-toolbox
```

Any necessary dependencies will be installed or updated if they are not already 
installed in a compatible version.

#### Installation into an existing environment from the repository

If you want to install the ESA CCI Toolbox directly from the git repository 
(for example in order to use an unreleased version or to modify the code), 
you can do so as follows:

```
$ git clone https://github.com/esa-cci/esa-climate-toolbox.git
$ cd esa-climate-toolbox
$ conda env create
$ conda activate ect
$ pip install -e .
```

## Testing

You can run the unit tests for the ESA CCI Toolbox by executing

```
$ pytest
```

in the `esa-climate-toolbox` repository. 

To create a test coverage report, you can use

```
coverage run --include='esa-climate-toolbox/**' --module pytest
coverage html
```

This will write a coverage report to `htmlcov/index.html`.

## Use

Jupyter notebooks demonstrating the use of the ESA CCI Toolbox can be found
in the `notebooks/` subdirectory of the repository.

## Releasing

To release the `esa-climate-toolbox`, please follow the steps outlined in the 
[xcube Developer Guide](https://github.com/dcs4cop/xcube/blob/master/docs/source/devguide.md#release-process).
