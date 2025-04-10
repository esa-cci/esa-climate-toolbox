.. _GitHub Repository: https://github.com/esa-cci/esa-climate-toolbox

============
Quick Start
============

After executing the instructions given in :doc:`installation_guide` the ESA CCI
Toolbox is ready to use.
To show how to make first steps, a variety of notebooks has been compiled.
You may find them in the `notebooks` section in the `Github Repository`_, but
we have also assembled them here to provide you with an overview.

ESA CCI Toolbox Notebooks Setup
===============================
Before using Jupyter Lab for the first time install the :code:`jupyterlab` and
:code:`jupyterlab-geojson` packages.

.. code-block:: bash

    (ect) conda install -c conda-forge jupyterlab jupyterlab-geojson

Start Jupyter Lab:

.. code-block:: bash

    (ect) $ jupyter-lab

.. _jupyter_notebooks:

Jupyter Notebooks
=============================

Accessing Data
--------------

.. toctree::
   :maxdepth: 1

   notebooks/Accessing_Data/1-ECT_Finding_Data
   notebooks/Accessing_Data/2-ECT_General_Data_Access
   notebooks/Accessing_Data/3-ECT_Data_Access_with_Subsets
   notebooks/Accessing_Data/4-ECT_Access_Vector_Data
   notebooks/Accessing_Data/5-ECT_Zarr_Access
   notebooks/Accessing_Data/6-ECT_Kerchunk_Access
   notebooks/Accessing_Data/7-ECT_Access_Vector_Data_Cubes
   notebooks/Accessing_Data/8-ECT_Reading_WCS_data
   notebooks/Accessing_Data/9-ECT_Reading_WMS_data
   notebooks/Accessing_Data/10-ECT_Access_ESGF

Using Operations
----------------

.. toctree::
   :maxdepth: 1

   notebooks/Using_Operations/1-ECT_Finding_Operations
   notebooks/Using_Operations/2-ECT_Using_Operations
   notebooks/Using_Operations/3-ECT_Reprojecting_Data
   notebooks/Using_Operations/4-ECT_Filtering_Data
   notebooks/Using_Operations/5-ECT_Gap_Filling

Advanced Functionality
----------------------

.. toctree::
   :maxdepth: 1

   notebooks/Advanced_Functionality/1-ECT_Using_xcube_viewer
   notebooks/Advanced_Functionality/2-ECT_Failure-reporting
   notebooks/Advanced_Functionality/3-ECT_Performance
