## Changes in 1.5

### Operations

* Added operation `normalise_vars` to normalise variables of a dataset
* Added operation `standardise_vars` to standardise variables of a dataset
* Added operation `pairwise_var_correlation` to compute correlation between 
  single variables of two datasets
* Added operation `pixelwise_group_correlation` to compute correlation between
  variable_ensembles from two datasets

All new operations are documented in the documentation's API reference.
  
### Data Access

* Added support for reading from and writing to KML

## Changes in 1.4

### Operations
* Added function `execute_operations` which allows to run one or more 
  input datasets on one or more operations, effectively allowing to
  set up an operation chain.
* Added operation for gapfilling
* Added operation for performing fourier analysis on time series
* Provided example for reprojection as notebook
* Provided example for filtering as notebook

### Data Access
* Provided guideline for accessing OGC Web Services as notebooks
* Provided guideline for accessing CMIP and Obs4MIPs data from ESGF
* Support reading and writing vectordatacubes

### Enhancements
* Allow searching for operations with specific tags
* Allow writing xarray dataarrays as png or geotiff images
* Allow writing xarray datarrays as sequence of images
* Allow writing xarray datasets as single geotiff image
* Added reporting module to allow reporting errors (including demo notebook)

## Changes in 1.3.2
* Maintenance release to handle changes from external package

## Changes in 1.3.1
* Added new operation `statistics` to compute statistics for a dataset 

## Changes in 1.3
* Made Python 3.10 the minimal supported version
* Improved error handling of coregistration op
* Added function to list ecv datasets of titles
* Revised list of operations
* Fixed bug where list_ecv_datasets would return false positives
* Updated installation guide in documentation

## Changes in 1.2

* Added dockerfile to run the `ESA CCI Toolbox` in a GitHub Codespace,
  allowing users to use the toolbox and in particular Jupyter Notebooks
  on GitHub. A dedicated readme file with instructions is in the `.devcontainer`
  sub-folder.
* Introduced `user_examples` section in notebooks folder, where example 
  notebooks from users can be collected.
* Changed toolbox to use external `xcube-cci` package for access to the ODP and
  removed redundant modules in the toolbox. This change has no functional effect
  on the toolbox, as together with the `xcube-cci` package it can be used as
  before.

## Changes in 1.1

* Support numpy >= 2.0 and pydap >= 3.4
* Automatically publish packages on pypi at release
* Improved support of vector data cubes
* Support HR LC data
* Added user functions for regions:
  * `get_land_mask`:  Gets a land mask for a dataset.
  * `get_regions_mask`: Gets a regions mask for a dataset.
  * `make_regions_dataset`: Rasterize country and continent polygons into a 
grid provided by a template dataset.
  * `mask_dataset_by_land`: Masks out non-land-pixels of a dataset.
  * `mask_dataset_by_regions`: Masks out pixels of a dataset which are not in 
one of the specified regions.
* Added notebooks:
  * `ESA CCI Toolbox Vector Data Cube Access`: Explaining the handling of 
vector data cubes
  * `Using the ESA CCI Toolbox with the xcube Viewer`: Explaining how to access
the xcube viewer from within a Jupyter Notebook
* Extended documentation by a section on how to configure an xcube server to 
serve data from the ESA CCI Toolbox

## Changes in 1.0.2

* Updated list of dataset states
* Improved dependencies
* Adapted coregistration to work with latest version of xarray

## Changes in 1.0.1

* Pinned numpy to <2.0, as numpy 2.0 causes errors

## Changes in 1.0

* Added operations:
  * `add_dataset_values_to_geodataframe`: 
    Adds values from a dataset to a geodataframe
  * `as_geodataframe`: Ensures a dataframe is a geodataframe
  * `to_dataframe`: Converts a dataset to a dataframe
  * `to_dataset`: Converts a dataframe to a dataset
  * `animate_map`: Creates a geographic map animation
  * `plot`: Create a 1D/line or 2D/image plot
  * `plot_contour`: Create a contour plot
  * `plot_hist`: Creates a histogram of a variable
  * `plot_line`: Creates a 1D/line plot
  * `plot_map`: Creates a geographic map plot
  * `plot_scatter`: Creates a scatter plot of two variables
* The ESA CCI Toolbox now also affords opening the following datasets:
  * As xarray datasets:
    * esacci.ICESHEETS.unspecified.L4.SEC.multi-sensor.multi-platform.UNSPECIFIED.0-1.greenland_sec_saral_altika
    * esacci.ICESHEETS.yr.Unspecified.SEC.SIRAL.CryoSat-2.UNSPECIFIED.2-2.greenland_sec_cryosat_2yr  
    * esacci.ICESHEETS.yr.Unspecified.SEC.SIRAL.CryoSat-2.UNSPECIFIED.2-2.greenland_sec_cryosat_5yr  
    * esacci.SEALEVEL.mon.IND.MSL.multi-sensor.multi-platform.MERGED.2-0.r1
    * esacci.SEALEVEL.mon.L4.MSLA.multi-sensor.multi-platform.MERGED.2-0.r1
  * As geodataframes:
    * esacci.AEROSOL.satellite-orbit-frequency.L2P.AER_PRODUCTS.AATSR.Envisat.AATSR-ENVISAT-ENS.v2-6.r1
    * esacci.AEROSOL.satellite-orbit-frequency.L2P.AER_PRODUCTS.AATSR.Envisat.ADV.2-31.r1
    * esacci.AEROSOL.satellite-orbit-frequency.L2P.AER_PRODUCTS.AATSR.Envisat.ORAC.04-01.r1
    * esacci.AEROSOL.satellite-orbit-frequency.L2P.AER_PRODUCTS.AATSR.Envisat.SU.4-3.r1
    * esacci.AEROSOL.satellite-orbit-frequency.L2P.AER_PRODUCTS.ATSR-2.ERS-2.ADV.2-31.r1
    * esacci.AEROSOL.satellite-orbit-frequency.L2P.AER_PRODUCTS.ATSR-2.ERS-2.ORAC.04-01.r1
    * esacci.AEROSOL.satellite-orbit-frequency.L2P.AER_PRODUCTS.ATSR-2.ERS-2.SU.4-3.r1
    * esacci.AEROSOL.satellite-orbit-frequency.L2P.AER_PRODUCTS.multi-sensor.multi-platform.ATSR2-ENVISAT-ENS.v2-6.r1
    * esacci.AEROSOL.satellite-orbit-frequency.L2P.AOD.MERIS.Envisat.MERIS_ENVISAT.2-2.r1
    * esacci.ICESHEETS.unspecified.Unspecified.CFL.multi-sensor.multi-platform.UNSPECIFIED.v3-0.greenland
    * esacci.ICESHEETS.unspecified.Unspecified.GLL.multi-sensor.multi-platform.UNSPECIFIED.v1-3.greenland
  * As vector data cubes:
    * esacci.ICESHEETS.yr.Unspecified.GMB.GRACE-instrument.GRACE.UNSPECIFIED.1-2.greenland_gmb_mass_trends
    * esacci.ICESHEETS.yr.Unspecified.GMB.GRACE-instrument.GRACE.UNSPECIFIED.1-3.greenland_gmb_mass_trends
    * esacci.ICESHEETS.yr.Unspecified.GMB.GRACE-instrument.GRACE.UNSPECIFIED.1-4.greenland_gmb_mass_trends
    * esacci.ICESHEETS.yr.Unspecified.GMB.GRACE-instrument.GRACE.UNSPECIFIED.1-5.greenland_gmb_mass_trends  
    * esacci.SEALEVEL.mon.IND.MSLTR.multi-sensor.multi-platform.MERGED.2-0.r1
    * esacci.SEALEVEL.mon.IND.MSLTR.multi-sensor.multi-platform.MERGED.2-2.ASA
    * esacci.SEALEVEL.mon.IND.MSLTR.multi-sensor.multi-platform.MERGED.2-2.BENGUELA
    * esacci.SEALEVEL.mon.IND.MSLTR.multi-sensor.multi-platform.MERGED.2-2.CARIBBEAN
    * esacci.SEALEVEL.mon.IND.MSLTR.multi-sensor.multi-platform.MERGED.2-2.GULFSTREAM
    * esacci.SEALEVEL.mon.IND.MSLTR.multi-sensor.multi-platform.MERGED.2-2.HUMBOLDT
    * esacci.SEALEVEL.mon.IND.MSLTR.multi-sensor.multi-platform.MERGED.2-2.MED_SEA
    * esacci.SEALEVEL.mon.IND.MSLTR.multi-sensor.multi-platform.MERGED.2-2.NE_ATL
    * esacci.SEALEVEL.mon.IND.MSLTR.multi-sensor.multi-platform.MERGED.2-2.N_INDIAN
    * esacci.SEALEVEL.mon.IND.MSLTR.multi-sensor.multi-platform.MERGED.2-2.SE_AFRICA
    * esacci.SEALEVEL.mon.IND.MSLTR.multi-sensor.multi-platform.MERGED.2-2.SE_ASIA
    * esacci.SEALEVEL.mon.IND.MSLTR.multi-sensor.multi-platform.MERGED.2-2.S_AUSTRALIA
    * esacci.SEALEVEL.mon.IND.MSLTR.multi-sensor.multi-platform.MERGED.2-2.WAFRICA
    * esacci.SEALEVEL.mon.IND.MSLTR.multi-sensor.multi-platform.MERGED.2-2.r1     
  * In addition, the FIRE pixel datasets can be read. However, as they are 
    subdivided by regions, they do not appear in the toolbox as single datasets,
    but as one dataset per region:
    * esacci.FIRE.mon.L3S.BA.MODIS.Terra.MODIS_TERRA.v5-1.pixel~AREA_1
    * esacci.FIRE.mon.L3S.BA.MODIS.Terra.MODIS_TERRA.v5-1.pixel~AREA_2
    * esacci.FIRE.mon.L3S.BA.MODIS.Terra.MODIS_TERRA.v5-1.pixel~AREA_3
    * esacci.FIRE.mon.L3S.BA.MODIS.Terra.MODIS_TERRA.v5-1.pixel~AREA_4
    * esacci.FIRE.mon.L3S.BA.MODIS.Terra.MODIS_TERRA.v5-1.pixel~AREA_5
    * esacci.FIRE.mon.L3S.BA.MODIS.Terra.MODIS_TERRA.v5-1.pixel~AREA_6
    * esacci.FIRE.mon.L3S.BA.multi-sensor.multi-platform.SYN.v1-1.pixel~AREA_1
    * esacci.FIRE.mon.L3S.BA.multi-sensor.multi-platform.SYN.v1-1.pixel~AREA_2
    * esacci.FIRE.mon.L3S.BA.multi-sensor.multi-platform.SYN.v1-1.pixel~AREA_3
    * esacci.FIRE.mon.L3S.BA.multi-sensor.multi-platform.SYN.v1-1.pixel~AREA_4
    * esacci.FIRE.mon.L3S.BA.multi-sensor.multi-platform.SYN.v1-1.pixel~AREA_5
    * esacci.FIRE.mon.L3S.BA.multi-sensor.multi-platform.SYN.v1-1.pixel~AREA_6
    As for the Sentinel-2 FIRE pixel dataset 
    esacci.FIRE.mon.L3S.BA.MSI-(Sentinel-2).Sentinel-2A.MSI.v1-1.pixel, it is 
    split into one dataset per region, e.g.,   
    esacci.FIRE.mon.L3S.BA.MSI-(Sentinel-2).Sentinel-2A.MSI.v1-1.pixel~h45v23
    As for esacci.FIRE.mon.L3S.BA.MSI-(Sentinel-2).Sentinel-2A.MSI.2-0.pixel,
    it is additionally split per variable:
      * esacci.FIRE.mon.L3S.BA.MSI-(Sentinel-2).Sentinel-2A.MSI.2-0.pixel~h32v14-fv2.0-CL
      * esacci.FIRE.mon.L3S.BA.MSI-(Sentinel-2).Sentinel-2A.MSI.2-0.pixel~h32v14-fv2.0-JD
      * esacci.FIRE.mon.L3S.BA.MSI-(Sentinel-2).Sentinel-2A.MSI.2-0.pixel~h32v14-fv2.0-LC
  
## Changes in 0.4.1

* Updated list of dataset states
* Fixed internal build process

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

