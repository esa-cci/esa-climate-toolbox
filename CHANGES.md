## Changes in 0.3.1 (in development)

* Added new data store `cci-kerchunk-store` 
  (and corresponding xcube data store `esa-cdc-kc`) that allows performant 
  accessing of selected datasets of the ESA Climate Data Centre using a 
  Zarr view of the original NetCDF files. This approach is made possible by 
  using the [kerchunk](https://fsspec.github.io/kerchunk/) package. Also 
  added new Notebook that demonstrates usage of the new data store.

* Added operations:
  * `aggregate_statistics`: 
    Aggregates data frame columns into statistical measures
  * `anomaly_external`: Calculates anomaly with external reference data
  * `anomaly_internal`: Calculates anomaly using a dataset's mean
  * `arithmetics`: Applies arithmetic operations
  * `climatology`: Creates a 'mean over years' dataset
  * `data_frame_max`: Selects a data frame's maximum record
  * `data_frame_min`: Selects a data frame's minimum record
  * `data_frame_subset`: Creates a variable or spatial subset of a dataframe
  * `diff`: Calculates the difference of two datasets
  * `find_closest`: Find data frame records closest to a given location
  * `query`: Query records from a dataframe
  * `reduce`: Reduces a dataset's variables
  * `temporal_aggregation`: Aggregates a dataset

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

