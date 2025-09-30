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


Functions
=========

.. _output_arrays:

Output Images
-------------

.. autofunction:: esa_climate_toolbox.functions.outputarray.output_animation

.. autofunction:: esa_climate_toolbox.functions.outputarray.output_array_as_image

.. autofunction:: esa_climate_toolbox.functions.outputarray.output_dataset_as_geotiff

.. _regions:

Region
------

.. autofunction:: esa_climate_toolbox.functions.regions.get_land_mask

.. autofunction:: esa_climate_toolbox.functions.regions.get_regions_mask

.. autofunction:: esa_climate_toolbox.functions.regions.make_regions_dataset

.. autofunction:: esa_climate_toolbox.functions.regions.mask_dataset_by_land

.. autofunction:: esa_climate_toolbox.functions.regions.mask_dataset_by_regions


Operations
==========

.. _aggregations:

Aggregation
-----------

.. autofunction:: esa_climate_toolbox.ops.climatology

.. autofunction:: esa_climate_toolbox.ops.reduce

.. autofunction:: esa_climate_toolbox.ops.statistics

.. autofunction:: esa_climate_toolbox.ops.temporal_aggregation

.. _animate:

Animation
---------

.. autofunction:: esa_climate_toolbox.ops.animate_map

.. _anomaly_detection:

Anomalies
---------

.. autofunction:: esa_climate_toolbox.ops.anomaly_external

.. autofunction:: esa_climate_toolbox.ops.anomaly_internal

.. _arithmetics:

Arithmetics
-----------

.. autofunction:: esa_climate_toolbox.ops.arithmetics

.. autofunction:: esa_climate_toolbox.ops.diff

.. _coregistration:

Coregistration
--------------

.. autofunction:: esa_climate_toolbox.ops.coregister

.. _correlation:

Correlation
-----------

.. autofunction:: esa_climate_toolbox.ops.pairwise_var_correlation

.. autofunction:: esa_climate_toolbox.ops.pixelwise_group_correlation

.. _data_frame_operations:

Data Frame Operations
---------------------

.. autofunction:: esa_climate_toolbox.ops.add_dataset_values_to_geodataframe

.. autofunction:: esa_climate_toolbox.ops.aggregate_statistics

.. autofunction:: esa_climate_toolbox.ops.as_geodataframe

.. autofunction:: esa_climate_toolbox.ops.data_frame_max

.. autofunction:: esa_climate_toolbox.ops.data_frame_min

.. autofunction:: esa_climate_toolbox.ops.data_frame_subset

.. autofunction:: esa_climate_toolbox.ops.find_closest

.. autofunction:: esa_climate_toolbox.ops.query

.. autofunction:: esa_climate_toolbox.ops.to_dataframe

.. autofunction:: esa_climate_toolbox.ops.to_dataset

.. _gapfilling:

Gap Filling
-----------

.. autofunction:: esa_climate_toolbox.ops.gapfill

.. _plotting:

Plotting
--------

.. autofunction:: esa_climate_toolbox.ops.plot

.. autofunction:: esa_climate_toolbox.ops.plot_contour

.. autofunction:: esa_climate_toolbox.ops.plot_hist

.. autofunction:: esa_climate_toolbox.ops.plot_line

.. autofunction:: esa_climate_toolbox.ops.plot_map

.. autofunction:: esa_climate_toolbox.ops.plot_scatter

.. _resampling:

Resampling
----------

.. autofunction:: esa_climate_toolbox.ops.resample

.. _subsetting:

Subsetting
----------

.. autofunction:: esa_climate_toolbox.ops.select_features

.. autofunction:: esa_climate_toolbox.ops.select_var

.. autofunction:: esa_climate_toolbox.ops.subset_spatial

.. autofunction:: esa_climate_toolbox.ops.subset_temporal

.. autofunction:: esa_climate_toolbox.ops.subset_temporal_index

.. _time_series_extraction:

Timeseries
----------

.. autofunction:: esa_climate_toolbox.ops.fourier_analysis

.. autofunction:: esa_climate_toolbox.ops.tseries_point

.. autofunction:: esa_climate_toolbox.ops.tseries_mean

Misc
----

.. autofunction:: esa_climate_toolbox.ops.detect_outliers

.. autofunction:: esa_climate_toolbox.ops.merge

.. autofunction:: esa_climate_toolbox.ops.normalize

.. autofunction:: esa_climate_toolbox.ops.adjust_spatial_attrs

.. autofunction:: esa_climate_toolbox.ops.adjust_temporal_attrs

.. autofunction:: esa_climate_toolbox.ops.normalise_vars

.. autofunction:: esa_climate_toolbox.ops.standardise_vars





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

Operation Execution API
=======================

.. autofunction:: esa_climate_toolbox.core.execute_operations

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

