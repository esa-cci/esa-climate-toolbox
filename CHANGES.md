## Changes in 0.3.1 (in development)

## Changes in 0.3

* Integrated operation framework
* Added operations:
  * `adjust_spatial_attrs`: Adjusts global spatial attributes
  * `adjust_temporal_attrs`: Adjusts global temporal attributes
  * `coregister`: Coregisters two datasets
  * `detect_outliers`: Detect outliers in a dataset
  * `merge`: Merges up to four datasets into one
  * `normalize`: Normalises geo- and time-coding
  * `resample`: Resamples a dataset to a new resolution
  * `subset_spatial`: Creates a spatial subset
  * `subset_temporal`: Creates a temporal subset
  * `subset_temporal_index`: Creates a temporal subset based on indices
  * `tseries_point`: Extracts a time series for a point
  * `tseries_mean`: Extracts a time series for a dataset's mean
* Added functionality to manage operations:
  * `get_op`
  * `get_op_meta_info`
  * `list_operations`
* Added functionality to manage data:
  * `add_local_store`
  * `add_store`
  * `find_data_store`
  * `get_output_store_id`
  * `get_store`
  * `get_search_params`
  * `get_supported_formats`
  * `list_datasets`
  * `list_ecvs`
  * `list_ecv_datasets`
  * `list_stores`
  * `open_data`
  * `remove_store`
  * `search`
  * `set_output_store`
  * `write_data`

* Integrated implementation of data stores into ESA Climate Toolbox
* Extended Documentation with descriptions of newly added API

## Changes in 0.2.1

* Minor packaging update

## Changes in 0.2

* Added documentation
* Added example notebooks
* Fixed registration of xcube plugins
* Adjusted names

## Changes in 0.1

* Added registrations of `esa-cdc` and  `esa-climate-data-centre` to access 
  data store with data from ESA Climate Data Centre

