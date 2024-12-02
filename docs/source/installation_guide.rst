.. _conda-forge: https://conda-forge.org/
.. _conda package manager: https://docs.conda.io/projects/conda/en/latest/
.. _miniconda: https://docs.conda.io/projects/conda/en/latest/
.. _pypi: https://pypi.org/project/esa-climate-toolbox/#files
.. _xcube developer guide: https://github.com/dcs4cop/xcube/blob/master/docs/source/devguide.md#release-process

.. _installation_guide:

==================
Installation Guide
==================

Installation using pip
======================
The ESA CCI Toolbox is available on `pypi`_ and thereby can be installed using the pip
package manager.
To pursue this, you need to have pip installed in your Python environment.
If that is the case, the toolbox can be simply installed with

.. code-block::

    $ pip install esa-climate-toolbox

Installation into a new conda environment
=========================================
Alternatively, the toolbox and all necessary dependencies are available on
`conda-forge`_ and can be installed using the `conda package manager`_.
The conda package manager itself can be obtained in the `miniconda`_
distribution.
Once conda is installed, the ESA CCI Toolbox can be installed like this:

.. code-block::

    $ conda create --name ect --channel conda-forge esa-climate-toolbox
    $ conda activate ect

The name of the environment may be freely chosen.

Installation into an existing conda environment
===============================================
The ESA CCI Toolbox can also be installed into an existing conda environment.
To do so, execute this command with the existing environment activated:

.. code-block::

    $ conda install --channel conda-forge esa-climate-toolbox

Installation into a new environment from the repository
=======================================================
If you want to install the ESA CCI Toolbox directly from the git repository
(for example in order to use an unreleased version or to modify the code),
you can do so as follows:

.. code-block::

    $ git clone https://github.com/esa-cci/esa-climate-toolbox.git
    $ cd esa-climate-toolbox
    $ conda env create
    $ conda activate ect
    $ pip install -e .
