.. _conda-forge: https://conda-forge.org/
.. _conda package manager: https://docs.conda.io/projects/conda/en/latest/
.. -miniconda: https://docs.conda.io/projects/conda/en/latest/

==================
Installation Guide
==================

Installation into a new environment
====
The ESA CCI Toolbox and all necessary dependencies are available on `conda-forge`_ , and can be installed
using the `conda package manager`_. The conda package manager itself can be obtained in the `miniconda`_ distribution.
Once conda is installed, the ESA CCI Toolbox can be installed like this:

.. code-block::

    $ conda create --name ect --channel conda-forge esa-climate-toolbox
    $ conda activate ect
The name of the environment may be freely chosen.

Installation into an existing environment
====
The ESA CCI Toolbox can also be installed into an existing conda environment. To do so, execute this command with the existing environment activated:

.. code-block::

    $ mamba install -c conda-forge aiohttp  lxml nest-asyncio xcube "pydap==3.3"
    $ conda install --channel conda-forge esa-climate-toolbox

Installation into a new environment from the repository
====
If you want to install the ESA CCI Toolbox directly from the git repository (for example in order to use an unreleased version or to modify the code), you can do so as follows:

.. code-block::

    $ git clone https://github.com/esa-cci/esa-climate-toolbox.git
    $ cd esa-climate-toolbox
    $ conda env create
    $ conda activate ect
    $ pip install -e .

Testing
----
You can run the unit tests for the ESA CCI Toolbox by executing:

.. code-block::
    $ pytest

in the esa-climate-toolbox repository.