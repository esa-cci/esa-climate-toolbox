.. _api_reference:

=============
API Reference
=============

Datasets and Datastores
=======================

Exploring Data
--------------

.. autofunction:: esa_climate_toolbox.core.find_data_store

.. autofunction:: esa_climate_toolbox.core.get_store

.. autofunction:: esa_climate_toolbox.core.get_search_params

.. autofunction:: esa_climate_toolbox.core.list_datasets

.. autofunction:: esa_climate_toolbox.core.list_ecvs

.. autofunction:: esa_climate_toolbox.core.list_ecv_datasets

.. autofunction:: esa_climate_toolbox.core.list_stores

.. autofunction:: esa_climate_toolbox.core.search

Managing Data
-------------

.. autofunction:: esa_climate_toolbox.core.add_local_store

.. autofunction:: esa_climate_toolbox.core.add_store

.. autofunction:: esa_climate_toolbox.core.remove_store


Reading and Writing Data
------------------------

.. autofunction:: esa_climate_toolbox.core.get_output_store_id

.. autofunction:: esa_climate_toolbox.core.get_supported_formats

.. autofunction:: esa_climate_toolbox.core.open_data

.. autofunction:: esa_climate_toolbox.core.set_output_store

.. autofunction:: esa_climate_toolbox.core.write_data


Operations
==========

Coregistration
--------------

.. autofunction:: esa_climate_toolbox.ops.coregister


Resampling
----------

.. autofunction:: esa_climate_toolbox.ops.resample

.. autofunction:: esa_climate_toolbox.ops.resample_2d

.. autofunction:: esa_climate_toolbox.ops.downsample_2d

.. autofunction:: esa_climate_toolbox.ops.upsample_2d


Subsetting
----------

.. autofunction:: esa_climate_toolbox.ops.subset_spatial

.. autofunction:: esa_climate_toolbox.ops.subset_temporal

.. autofunction:: esa_climate_toolbox.ops.subset_temporal_index


Timeseries
----------

.. autofunction:: esa_climate_toolbox.ops.tseries_point

.. autofunction:: esa_climate_toolbox.ops.tseries_mean


Misc
----

.. autofunction:: esa_climate_toolbox.ops.detect_outliers

.. autofunction:: esa_climate_toolbox.ops.merge

.. autofunction:: esa_climate_toolbox.ops.normalize

.. autofunction:: esa_climate_toolbox.ops.adjust_spatial_attrs

.. autofunction:: esa_climate_toolbox.ops.adjust_temporal_attrs



Operation Registration API
==========================

.. autoclass:: esa_climate_toolbox.core.Operation
    :members:

.. autoclass:: esa_climate_toolbox.core.OpMetaInfo
    :members:

.. autofunction:: esa_climate_toolbox.core.op
    :noindex:

.. autofunction:: esa_climate_toolbox.core.op_input

.. autofunction:: esa_climate_toolbox.core.op_output

.. autofunction:: esa_climate_toolbox.core.op_return

.. autofunction:: esa_climate_toolbox.core.new_expression_op

.. autofunction:: esa_climate_toolbox.core.new_subprocess_op

Managing Operations
-------------------

.. autofunction:: esa_climate_toolbox.core.get_op

.. autofunction:: esa_climate_toolbox.core.get_op_meta_info

.. autofunction:: esa_climate_toolbox.core.list_operations

.. _api-monitoring:

Task Monitoring API
===================

.. autoclass:: esa_climate_toolbox.core.Monitor
    :members:

.. autoclass:: esa_climate_toolbox.core.ChildMonitor
    :members:
