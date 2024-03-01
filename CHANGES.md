## Changes in 0.4

* Added store aliases `esa-cci` and `esa-cci-zarr` and renamed default stores to have
  the same names.

* Ensure default stores will always be included in store registry.

* Updated documentation

* Added opener `"datafame:geojson:esa-cdc"` to `esa-cci` data store.
  The following datasets can now be opened:
  * esacci.GHG.satellite-orbit-frequency.L2.CH4.SCIAMACHY.Envisat.IMAP.v7-2.r1
  * esacci.GHG.satellite-orbit-frequency.L2.CH4.TANSO-FTS.GOSAT.EMMA.ch4_v1-2.r1
  * esacci.GHG.satellite-orbit-frequency.L2.CH4.TANSO-FTS.GOSAT.OCFP.v2-1.r1
  * esacci.GHG.satellite-orbit-frequency.L2.CH4.TANSO-FTS.GOSAT.OCPR.v7-0.r1
  * esacci.GHG.satellite-orbit-frequency.L2.CH4.TANSO-FTS.GOSAT.SRFP.v2-3-8.r1
  * esacci.GHG.satellite-orbit-frequency.L2.CO2.SCIAMACHY.Envisat.BESD.v02-01-02.r1
  * esacci.GHG.satellite-orbit-frequency.L2.CO2.SCIAMACHY.Envisat.WFMD.v4-0.r1
  * esacci.GHG.satellite-orbit-frequency.L2.CO2.TANSO-FTS.GOSAT.EMMA.v2-2c.r1
  * esacci.GHG.satellite-orbit-frequency.L2.CO2.TANSO-FTS.GOSAT.SRFP.v2-3-8.r1
  * esacci.SEAICE.satellite-orbit-frequency.L2P.SITHICK.RA-2.Envisat.NH.2-0.r1
  * esacci.SEAICE.satellite-orbit-frequency.L2P.SITHICK.RA-2.Envisat.SH.2-0.r1
  * esacci.SEAICE.satellite-orbit-frequency.L2P.SITHICK.SIRAL.CryoSat-2.NH.2-0.r1
  * esacci.SEAICE.satellite-orbit-frequency.L2P.SITHICK.SIRAL.CryoSat-2.SH.2-0.r1
  * esacci.SEALEVEL.satellite-orbit-frequency.L1.UNSPECIFIED.AltiKa.SARAL.UNSPECIFIED.v2-0.r1
  * esacci.SEALEVEL.satellite-orbit-frequency.L1.UNSPECIFIED.GFO-RA.GFO.UNSPECIFIED.v2-0.r1
  * esacci.SEALEVEL.satellite-orbit-frequency.L1.UNSPECIFIED.Poseidon-2.Jason-1.UNSPECIFIED.v2-0.r1
  * esacci.SEALEVEL.satellite-orbit-frequency.L1.UNSPECIFIED.Poseidon-3.Jason-2.UNSPECIFIED.v2-0.r1
  * esacci.SEALEVEL.satellite-orbit-frequency.L1.UNSPECIFIED.RA-2.Envisat.UNSPECIFIED.v2-0.r1
  * esacci.SEALEVEL.satellite-orbit-frequency.L1.UNSPECIFIED.RA.ERS-1.UNSPECIFIED.v2-0.r1
  * esacci.SEALEVEL.satellite-orbit-frequency.L1.UNSPECIFIED.RA.ERS-2.UNSPECIFIED.v2-0.r1
  * esacci.SEALEVEL.satellite-orbit-frequency.L1.UNSPECIFIED.SIRAL.CryoSat-2.UNSPECIFIED.v2-0.r1
  * esacci.SEALEVEL.satellite-orbit-frequency.L1.UNSPECIFIED.SSALT.Topex-Poseidon.UNSPECIFIED.v2-0.r1
  Also added notebook that opens one of these datasets.

* Added new data store `esa-cci-kc` 
  (and corresponding xcube data store `esa-cci-kc`) that allows performant 
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

