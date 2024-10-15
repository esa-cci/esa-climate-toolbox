.. _CCI Open Data Portal: https://climate.esa.int/en/odp/#/dashboard
.. _Conda: https://anaconda.org/conda-forge/esa-climate-toolbox
.. _geopandas: http://geopandas.org/
.. _GitHub: https://github.com/esa-cci/esa-climate-toolbox
.. _Helpdesk: https://climate.esa.int/helpdesk/
.. _kerchunk: https://fsspec.github.io/kerchunk/
.. _pandas: http://pandas.pydata.org/
.. _xarray: http://xarray.pydata.org/en/stable/
.. _xcube: https://xcube.readthedocs.io/en/latest/
.. _Zarr: https://zarr.readthedocs.io/en/stable/

===================
The ESA CCI Toolbox
===================

The CCI Toolbox is a python package that provides access and operations to CCI data.
It is available on `GitHub`_ and can be installed with `Conda`_.

Data Stores
===========

The CCI Toolbox comes with three pre-configured data stores,
built with the `xcube`_ Python package.
The `CCI Open Data Portal`_ data store (esa-cci) provides programmatic access to all
CCI data.
The `Zarr`_ data store (esa-cci-zarr) provides access to selected CCI data in Zarr
format for faster performance.
The `kerchunk`_ Data Store (esa-cci-kc) gives access to CCI datasets through a reference
file, thereby allowing similar performance as the Zarr Store

Datasets
========
Datasets are accessed through Data Stores.
By providing a dataset identifier the CCI Toolbox loads only the metadata and structure
of the dataset, with the full dataset loaded only when needed for operations.
Opened datasets are represented through data structures defined by Python packages
`xarray`_, `pandas`_, and `geopandas`_.

Operations
==========

The CCI Toolbox provides climate analyses operations geared to CCI data for
:ref:`coregistration`, :ref:`resampling`, spatial and temporal :ref:`subsetting`,
:ref:`aggregations`, :ref:`anomaly_detection`, :ref:`arithmetics`,
selected :ref:`data_frame_operations` and more.
In addition, the Python packages `xarray`_, `pandas`_, and `geopandas`_ provide a rich
and powerful low-level data processing interface for datasets opened through the
CCI Toolbox.
See the :ref:`api_reference` for details.

Installation
============

Method 1 - Install `Conda`_ and then run the following

.. code-block:: bash

    $ conda create --name ect --channel conda-forge esa-climate-toolbox
    $ conda activate ect

Method 2 - If you already have an existing `Conda`_ environment

.. code-block:: bash

    $ conda install --channel conda-forge esa-climate-toolbox

Method 3 - Install directly from the `GitHub`_ repository

.. code-block:: bash

    $ git clone https://github.com/esa-cci/esa-climate-toolbox.git
    $ cd esa-climate-toolbox
    $ conda env create
    $ conda activate ect
    $ pip install -e .

Getting Started
===============

Try our :ref:`jupyter_notebooks` on exploring CCI data, accessing data from the
Open Data Portal Store, from the Zarr Store, and from the Kerchunk Store;
on opening data subsets and opening data as dataframes, and on finding
and using operations on CCI data.

Helpdesk
========

For support with the CCI Toolbox, please visit our `Helpdesk`_.

.. toctree::
   :caption: Table of Contents
   :maxdepth: 2
   :numbered:

   introduction
   about
   installation_guide
   quick_start
   viewer
   api_reference
