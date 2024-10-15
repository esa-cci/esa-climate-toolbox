.. _xcube documentation: https://xcube.readthedocs.io/en/latest/cli/xcube_serve.html

===============================================
Using the ESA CCI Toolbox with the xcube Viewer
===============================================

The ESA CCI Toolbox does not come with a dedicated User Interface.
However, apart from the options to plot its data in Jupyter Notebooks
(as is extensively shown in the :doc:`jupyter_notebooks` section),
you can also open view datasets in an instance of xcube viewer.
There is a Notebook which explains how to do this in a Jupyter environment.
However, for some cases this might not be the ideal solution, as you might want to run
a server for a longer time than the lifespan of a Jupyter Notebook.
For these cases, you may set up a specific `xcube` server configuration.
If you are not familiar with these, we suggest you read the corresponding sections in
the `xcube documentation`_.

To access datasets provided by the toolbox, you may then design the data stores section
to look like this:

.. code-block:: yaml

    DataStores:
      - Identifier: cci
        StoreId: esa-cci
          Datasets:
            - Path: "esacci.AEROSOL.mon.L3.AAI.multi-sensor.multi-platform.MSAAI.1-7.r1"
              Title: "Monthly Aerosol"
              StoreOpenParams:
                variable_names: ['absorbing_aerosol_index']
                time_range: ['2005-01-01', '2005-12-31']
                bbox: [-23.4, -40.4, 57.4, 40.4]
      - Identifier: cci-zarr
        StoreId: esa-cci-zarr
          Datasets:
            - Path: "*SNOW*"

This section will load in the dataset with the identifier
`esacci.AEROSOL.mon.L3.AAI.multi-sensor.multi-platform.MSAAI.1-7.r1`,
subset to the given opening parameters.
From the zarr store, it will load in any datasets with the term `SNOW` in it.
