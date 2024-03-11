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

.. toctree::
   :maxdepth: 1

   notebooks/1-ECT_Finding_Data
   notebooks/2-ECT_General_Data_Access
   notebooks/3-ECT_Data_Access_with_Subsets
   notebooks/4-ECT_Access_Vector_Data
   notebooks/5-ECT_Zarr_Access
   notebooks/6-ECT_Kerchunk_Access
   notebooks/7-ECT_Finding_Operations
   notebooks/8-ECT_Using_Operations


