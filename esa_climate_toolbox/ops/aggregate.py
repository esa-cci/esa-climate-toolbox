# The MIT License (MIT)
# Copyright (c) 2024-2026 ESA Climate Change Initiative
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""
Description
===========

This module provides operations to create climatologies and other aggregations

Components
==========
"""

from datetime import timezone
from collections.abc import Sequence

import dask.array as da
import geopandas as gpd
import numpy as np
import pandas as pd
from typing import List
from typing import Union

import scipy.stats
import xarray as xr
from xarray.core.resample import DatasetResample

from esa_climate_toolbox.core.op import op
from esa_climate_toolbox.core.op import op_input
from esa_climate_toolbox.core.op import op_return
from esa_climate_toolbox.core.types import DatasetLike
from esa_climate_toolbox.core.types import DimNamesLike
from esa_climate_toolbox.core.types import ValidationError
from esa_climate_toolbox.core.types import VarNamesLike
from esa_climate_toolbox.ops.normalize import adjust_temporal_attrs
from esa_climate_toolbox.ops.select import select_var
from esa_climate_toolbox.ops._datatree_aggregate import aggregate_datatree
from esa_climate_toolbox.util.monitor import Monitor


@op(tags=['aggregate', 'temporal', 'climatology'], version='1.5')
@op_input('ds', data_type=DatasetLike)
@op_input('var', value_set_source='ds', data_type=VarNamesLike)
@op_return(add_history=True)
def climatology(ds: DatasetLike.TYPE,
                var: VarNamesLike.TYPE = None,
                monitor: Monitor = Monitor.NONE) -> xr.Dataset:
    """
    Create a 'mean over years' dataset by averaging the values of the given input
    dataset over all years. The output is a climatological dataset with the same
    resolution as the input dataset. E.g. a daily input dataset will create a daily
    climatology consisting of 365 days, a monthly input dataset will create a monthly
    climatology, etc.

    Seasonal input datasets must have matching seasons over all years denoted by the
    same date each year. E.g., first date of each quarter. The output dataset will
    then be a seasonal climatology where each season is denoted with the same date
    as in the input dataset.

    For further information on climatological datasets, see
    http://cfconventions.org/cf-conventions/v1.6.0/cf-conventions.html#climatological-statistics

    :param ds: A dataset to average
    :param var: If given,
        only these variables will be preserved in the resulting dataset
    :param monitor: A progress monitor
    :return: A climatological long term average dataset
    """
    ds = DatasetLike.convert(ds)
    # Check if time dtype is what we want
    if 'datetime64[ns]' != ds.time.dtype:
        raise ValidationError('Long term average operation expects a dataset with the '
                              'time coordinate of type datetime64[ns], but received '
                              '{}. Running the normalize operation on this '
                              'dataset may help'.format(ds.time.dtype))

    try:
        t_resolution = ds.attrs['time_coverage_resolution']
    except KeyError:
        raise ValidationError('Could not determine temporal resolution. Running '
                              'the adjust_temporal_attrs operation beforehand may '
                              'help.')

    var = VarNamesLike.convert(var)
    # Shallow

    if var:
        ds = select_var(ds, var)

    if t_resolution == 'P1D':
        return _lta_daily(ds)
    elif t_resolution == 'P1M':
        return _lta_monthly(ds, monitor)
    else:
        return _lta_general(ds, monitor)


def _lta_monthly(ds: xr.Dataset, monitor: Monitor):
    """
    Carry out a long term average on a monthly dataset

    :param ds: Dataset to aggregate
    :param monitor: Progress monitor
    :return: Aggregated dataset
    """
    time_min = pd.Timestamp(ds.time.values[0], tzinfo=timezone.utc)
    time_max = pd.Timestamp(ds.time.values[-1], tzinfo=timezone.utc)
    total_work = 100
    retset = ds

    with monitor.starting('LTA', total_work=total_work):
        monitor.progress(work=0)
        step = total_work / 12
        kwargs = {'monitor': monitor, 'step': step}
        retset = retset.groupby('time.month', squeeze=False).map(_mean, **kwargs)

    # Make the return dataset CF compliant
    retset = retset.rename({'month': 'time'})
    retset['time'] = pd.date_range('{}-01-01'.format(time_min.year),
                                   freq='MS',
                                   periods=12)

    climatology_bounds = xr.DataArray(data=da.tile(np.array([time_min, time_max],
                                                            dtype=np.datetime64),
                                                   (12, 1)),
                                      dims=['time', 'nv'],
                                      name='climatology_bounds')
    retset['climatology_bounds'] = climatology_bounds
    retset.time.attrs = ds.time.attrs
    retset.time.attrs['climatology'] = 'climatology_bounds'

    return retset


def _lta_daily(ds: xr.Dataset):
    """
    Carry out a long term average of a daily dataset

    :param ds: Dataset to aggregate
    :return: Aggregated dataset
    """
    return ds.groupby('time.dayofyear', squeeze=False).mean('time')


def _lta_general(ds: xr.Dataset, monitor: Monitor):
    """
    Try to carry out a long term average in a general case, notably
    in the case of having seasonal datasets

    :param ds: Dataset to aggregate
    :param monitor: Progress monitor
    :return: Aggregated dataset
    """
    time_min = pd.Timestamp(ds.time.values[0], tzinfo=timezone.utc)
    time_max = pd.Timestamp(ds.time.values[-1], tzinfo=timezone.utc)
    total_work = 100
    retset = ds

    # The dataset should feature time periods consistent over years
    # and denoted with the same dates each year
    if not _is_seasonal(ds.time):
        raise ValidationError("A long term average dataset can not be created for"
                              " a dataset with inconsistent seasons.")

    # Get 'representative year'
    c = 0
    rep_year = 0
    for group in ds.time.groupby('time.year'):
        c = c + 1
        if c == 1:
            rep_year = group[1].time
            continue
        if c == 2 and len(group[1].time) > len(rep_year):
            rep_year = group[1].time
            break

    with monitor.starting('LTA', total_work=total_work):
        monitor.progress(work=0)
        step = total_work / len(rep_year.time)
        kwargs = {'monitor': monitor, 'step': step}
        retset = retset.groupby(
            'time.month', squeeze=False
        ).map(_groupby_day, **kwargs)

    # Make the return dataset CF compliant
    retset = retset.stack(time=('month', 'day'))

    # Turn month, day coordinates to time
    retset = retset.reset_index('time')
    retset = retset.drop_vars(['month', 'day'])
    retset['time'] = rep_year.time

    climatology_bounds = xr.DataArray(data=da.tile(np.array([time_min, time_max],
                                                            dtype=np.datetime64),
                                                   (len(rep_year), 1)),
                                      dims=['time', 'nv'],
                                      name='climatology_bounds')
    retset['climatology_bounds'] = climatology_bounds
    retset.time.attrs = ds.time.attrs
    retset.time.attrs['climatology'] = 'climatology_bounds'

    for var in retset.data_vars:
        try:
            retset[var].attrs['cell_methods'] = \
                retset[var].attrs['cell_methods'] + ' time: mean over years'
        except KeyError:
            retset[var].attrs['cell_methods'] = 'time: mean over years'

    return retset


def _groupby_day(ds: xr.Dataset, monitor: Monitor, step: float):
    """
    Groupby the given dataset by day of month and apply mean to it

    :param ds: Dataset to aggregate
    :param monitor: Progress monitor
    :param step: Progress step
    """
    kwargs = {'monitor': monitor, 'step': step}
    return ds.groupby('time.day', squeeze=False).map(_mean, **kwargs)


def _mean(ds: xr.Dataset, monitor: Monitor, step: float):
    """
    Calculate mean of the given dataset and update the given monitor.

    :param ds: Dataset to take the mean of
    :param monitor: Monitor to update
    :param step: Work step
    """
    dataset = ds.mean(dim='time', keep_attrs=True)
    monitor.progress(work=step)
    return dataset


def _is_seasonal(time: xr.DataArray):
    """
    Check if the given timestamp dataarray features consistent
    seasons. E.g. Each year has the same date-month values in it.
    """
    c = 0
    test = None
    first_days = 0
    first_months = 0
    for group in time.groupby('time.year'):
        # Test (month, day) dates of all years against
        # (month, day) dates of the first year, or second
        # year in case the first year is not full
        c = c + 1
        np_time = group[1].time.values
        months = pd.DatetimeIndex(np_time).month
        days = pd.DatetimeIndex(np_time).day
        if c == 1:
            first_months = months
            first_days = days
            continue
        elif c == 2:
            second_months = months
            second_days = days

            if len(second_months) > len(first_months):
                test = list(zip(second_months, second_days))
                for date in zip(first_months, first_days):
                    if date not in test:
                        return False
            else:
                test = list(zip(first_months, first_days))
                for date in zip(second_months, second_days):
                    if date not in test:
                        return False
            continue

        for date in zip(months, days):
            if date not in test:
                return False

    return True


# noinspection PyIncorrectDocstring
@op(tags=['aggregate', 'temporal'], version='2.0')
@op_input('ds', data_type=DatasetLike)
@op_input('method', value_set=['mean', 'max', 'median', 'prod', 'sum', 'std',
                               'var', 'argmax', 'argmin', 'first', 'last'])
@op_return(add_history=True)
def temporal_aggregation(ds: DatasetLike.TYPE,
                         method: str = 'mean',
                         period: str = 'MS',
                         monitor: Monitor = Monitor.NONE) -> xr.Dataset:
    """
    Perform aggregation of dataset according to the given
    aggregation *method* and time period *period*.

    Note that the operation does not perform weighting. Depending on the
    combination of input and output resolutions, as well as aggregation
    method, the resulting dataset might yield unexpected results.

    The possible values if *period* are the offset-aliases
    supported by the Pandas package:
    http://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases

    Some examples for *period* values:

    * 'QS-DEC' will result in a dataset aggregated to DJF, MAM, JJA, SON
      seasons, each denoted by the first date of the season.
    * 'QS-JUN' produces an output dataset on a quarterly resolution where the
      year ends in 1st of June and each quarter is denoted by its first date.
    * '8MS' produces an output dataset on an eight-month resolution where each
      period is denoted by the first date. Note that such periods will not be
      consistent over years.
    * '8D' produces a dataset on an eight day resolution.

    :param ds: Dataset to aggregate
    :param method: Aggregation method
    :param period: Aggregation time period
    :return: Aggregated dataset
    """
    ds = DatasetLike.convert(ds)

    if "time" not in ds.coords:
        raise ValidationError('Temporal aggregation operation expects a dataset with a '
                              '"time" coordinate variable. '
                              'Running the normalize operation on this '
                              'dataset may help')

    # Validate output frequency as a valid offset string
    try:
        pd.date_range('2000-01-01', periods=2, freq=period)
    except ValueError:
        raise ValidationError(f'Invalid freq value "{period}".'
                              ' Please check operation documentation.')

    try:
        agg_function = getattr(DatasetResample, method)
    except AttributeError:
        raise ValidationError(f'Provided aggregation method {method} is not valid.')

    with monitor.observing("Resample dataset"):
        dataset = agg_function(ds.resample(time=period))

    return adjust_temporal_attrs(dataset)


@op(tags=['aggregate', 'datatree'], version='1.0')
@op_input('tree', data_type=xr.DataTree)
@op_input('var', data_type=VarNamesLike)
@op_input('dim', data_type=DimNamesLike)
@op_input('method', value_set=['mean', 'min', 'max', 'sum', 'median'])
@op_input('aggregate_nodes', data_type=bool, default_value=True)
@op_input('output_format', value_set=['datatree', 'vector_data_cube', 'dataframe'])
@op_input('vector_data', data_type=gpd.GeoDataFrame, nullable=True)
@op_input('vector_path', data_type=str, nullable=True)
@op_return()
def reduce_datatree(
        tree: xr.DataTree,
        var: VarNamesLike.TYPE = None,
        dim: DimNamesLike.TYPE = None,
        method: str = 'mean',
        apply_spatial_weighting: bool = True,
        nodes: Sequence[str] | None = None,
        aggregate_nodes: bool = True,
        output_format: str = 'datatree',
        vector_data: gpd.GeoDataFrame | None = None,
        vector_path: str | None = None,
        node_column: str = 'path',
        monitor: Monitor = Monitor.NONE,
) -> xr.DataTree | xr.Dataset | pd.DataFrame:
    """Reduce the selected DataTree as a whole or reduce its leaves separately.

    Without vector input, output geometry is the spherical centroid of all
    pixels valid in at least one selected spatial variable at any time or level.
    Each spatial pixel counts once. Statistics use each variable's own valid
    observations. DataFrame output also includes this geometry.

    :param tree: Input tree with spatial datasets using ``lat``/``lon``.
    :param var: Variables which to reduce; None selects all data variables.
    :param dim: Dimensions along which to reduce; None selects all variable dimensions.
    :param method: Reduction method,
        must be one of ``mean``, ``m̀in``, ``max``, ``sum``, ``median``.
    :param apply_spatial_weighting: Whether to apply spatial weighting to cancel
        the effects of grid cells closer to the equator cover more area than
        grid cells closer to the poles. Only applicable for datasets with a
        latitude column named 'lat'. Has no effect for methods 'median', 'min',
        and 'max'. Default is True.
    :param nodes: Paths of dataset leaves to process, with or without a leading
        slash, e.g. 'lake_2' or '/lake_2'. For nested leaves include the parent
        groups, e.g. 'Europe/lake_2'. None selects all leaves.
        Unselected nodes are excluded from the result. Ancestor groups of
        selected leaves are retained but not reduced.
    :param aggregate_nodes: If True (default), reduce across selected datasets
        as well as the dimensions specified by dim, producing a shared result.
        All spatial dimensions must be reduced. Remaining coordinates are
        aligned by their union; absent observations do not contribute.
        Overlapping pixels in different nodes count as separate observations.
        Means use the number of valid observations per variable and remaining
        coordinate, or their weight sums when spatial weighting is enabled.
        Other statistics must combine observations according to their method,
        rather than averaging per-node statistics. If False, reduce each
        dataset independently and retain its identity.
        The exact median pools individual values and may require substantial
        memory. Other methods combine reduced per-node quantities lazily.
    :param output_format: ``datatree`` stores the shared result at the root
        when aggregate_nodes=True; otherwise it preserves the selected leaves
        and their ancestor groups. The following per-node conversion formats require
        aggregate_nodes=False:
        ``vector_data_cube`` produces an xarray Dataset with feature and time
        dimensions and feature geometries; requires only time to remain in
        each result variable. ``dataframe`` produces one row per
        selected node and one column per result variable; applicable when all
        dimensions have been reduced.
        Full node paths identify features/rows. With vector input, the frame
        is a GeoDataFrame. Output format does not determine aggregation scope.
    :param vector_data: Optional polygons with a declared CRS, used both for
        spatial masking and output geometries. Supply either this or
        vector_path, or neither. Pixels are selected by their centres.
    :param vector_path: Optional vector file to read instead of vector_data.
    :param node_column: Name of the column in vector_data or the vector file
        containing full DataTree node paths, e.g. '/lake_1'. The column values
        associate each selected dataset node with exactly one geometry,
        independently of vector row order. Only considered when vector_data
        or vector_path is provided; ignored otherwise. Defaults to 'path'.
    :param monitor: Progress monitor.
    :return: Reduced DataTree, vector data cube, or (Geo)DataFrame.
    """
    return aggregate_datatree(
        tree, var, dim, [method], apply_spatial_weighting, nodes, aggregate_nodes,
        output_format, vector_data, vector_path, node_column, monitor,
    )


@op(tags=['aggregate', 'statistics', 'datatree'], version='1.0')
@op_input('tree', data_type=xr.DataTree)
@op_input('var', data_type=VarNamesLike)
@op_input('dim', data_type=DimNamesLike)
@op_input('methods', any_of=['count', 'mean', 'median', 'sum', 'std', 'min', 'max'])
@op_input('aggregate_nodes', data_type=bool, default_value=True)
@op_input('output_format', value_set=['datatree', 'vector_data_cube', 'dataframe'])
@op_input('vector_data', data_type=gpd.GeoDataFrame, nullable=True)
@op_input('vector_path', data_type=str, nullable=True)
@op_return()
def statistics_datatree(
        tree: xr.DataTree,
        var: VarNamesLike.TYPE = None,
        apply_spatial_weighting: bool = True,
        dim: DimNamesLike.TYPE = None,
        methods: Union[str, List[str]] = 'mean',
        nodes: Sequence[str] | None = None,
        aggregate_nodes: bool = True,
        output_format: str = 'datatree',
        vector_data: gpd.GeoDataFrame | None = None,
        vector_path: str | None = None,
        node_column: str = 'path',
        monitor: Monitor = Monitor.NONE,
) -> xr.DataTree | xr.Dataset | pd.DataFrame:
    """Compute statistics over the selected tree or its individual leaves.

    Tree selection, masking, geometry and output-format parameters have the
    same meaning as in ``reduce_datatree``. Selected output variables are named
    ``<variable>_<method>``. Output conversion uses these statistic variables,
    rather than unreduced input variables. By default, statistics describe all
    selected datasets together; aggregate_nodes=False keeps results separate.

    Without vector input, output geometry uses the union of pixels valid in
    any selected spatial variable at any time or level, as in ``reduce_datatree``.
    Each spatial pixel counts once; statistics retain per-variable validity.

    :param tree: Input DataTree.
    :param var: Variables to analyse; None selects all data variables.
    :param apply_spatial_weighting: Use cosine-latitude weights for spatial
        mean, sum and standard deviation; otherwise valid pixels weigh equally.
    :param dim: Dimensions to reduce; None selects all variable dimensions.
    :param methods: One or more of count, mean, median, sum, std, min and max.
        Count counts valid observations, including valid zero values.
    :param nodes: Leaf paths with or without a leading slash, e.g. 'lake_2'
        or '/lake_2'. Include parent groups for nested leaves, e.g.
        'Europe/lake_2'. None selects all leaves. Unselected nodes are excluded
        from the result; ancestor groups of selected leaves are retained.
    :param aggregate_nodes: Aggregate across selected datasets (default True),
        with the validity/weighting semantics of ``reduce_datatree``. Count and
        sum combine additively, min/max take extrema, and std/median describe
        the pooled observations, not averages of per-node std/median values.
        Spatial dimensions must be reduced; remaining coordinates use their
        union. Overlapping observations are counted for each contributing node.
        Exact median computation may require substantial memory.
    :param output_format: datatree, vector_data_cube or dataframe.
    :param vector_data: Optional masking polygons and output geometries.
    :param vector_path: Optional vector file instead of vector_data.
    :param node_column: Name of the column in vector_data or the vector file
        containing full DataTree node paths, e.g. '/lake_1'. The column values
        associate each selected dataset node with exactly one geometry,
        independently of vector row order. Only considered when vector_data
        or vector_path is provided; ignored otherwise. Defaults to 'path'.
    :param monitor: Progress monitor.
    :return: Statistics DataTree, vector data cube, or (Geo)DataFrame.
    """
    methods = [methods] if isinstance(methods, str) else list(methods)
    return aggregate_datatree(
        tree, var, dim, methods, apply_spatial_weighting, nodes, aggregate_nodes,
        output_format, vector_data, vector_path, node_column, monitor, statistics=True,
    )


@op(tags=['aggregate'], version='1.1')
@op_input('ds', data_type=DatasetLike)
@op_input('var', value_set_source='ds', data_type=VarNamesLike)
@op_input('dim', value_set_source='ds', data_type=DimNamesLike)
@op_input('method', value_set=['mean', 'min', 'max', 'sum', 'median'])
@op_input('apply_spatial_weighting', default_value=True)
@op_return(add_history=True)
def reduce(ds: DatasetLike.TYPE,
           var: VarNamesLike.TYPE = None,
           dim: DimNamesLike.TYPE = None,
           method: str = 'mean',
           apply_spatial_weighting: bool = True,
           monitor: Monitor = Monitor.NONE) -> xr.Dataset:
    """
    Reduce the given variables of the given dataset along the given dimensions.
    If no variables are given, all variables of the dataset will be reduced. If
    no dimensions are given, all dimensions will be reduced. If no variables
    have been given explicitly, it can be set that only variables featuring numeric
    values should be reduced.

    :param ds: Dataset to reduce
    :param var: Variables in the dataset to reduce
    :param dim: Dataset dimensions along which to reduce
    :param method: reduction method
    :param apply_spatial_weighting: Whether to apply spatial weighting to cancel
        the effects of grid cells closer to the equator cover more area than
        grid cells closer to the poles. Only applicable for datasets with a
        latitude column named 'lat'. Has no effect for methods 'median', 'min',
        and 'max'. Default is True.
    :param monitor: A progress monitor
    """
    ufuncs = {'min': da.nanmin, 'max': da.nanmax, 'mean': da.nanmean,
              'median': da.nanmedian, 'sum': da.nansum}

    weightable_funcs = ["mean", "sum"]

    ds = DatasetLike.convert(ds)

    if not var:
        var = list(ds.data_vars.keys())
    var_names = VarNamesLike.convert(var)

    if not dim:
        dim = list(ds.coords.keys())
    else:
        dim = DimNamesLike.convert(dim)

    retset = ds.copy()

    if apply_spatial_weighting:
        if not 'lat' in retset.coords:
            raise ValueError(
                "To apply spatial weighting, the dataset must have a 'lat' coordinate."
                "Consider to either set parameter 'apply_spatial_weighting' to False"
                "or run the 'normalize' operation on the dataset first."
            )
        weights = da.cos(da.deg2rad(retset.lat))

    for var_name in var_names:
        intersection = [value for value in dim if value in retset[var_name].dims]
        with monitor.starting("Reduce dataset", total_work=100):
            monitor.progress(5)
            with monitor.child(95).observing("Reduce"):
                if method in weightable_funcs and apply_spatial_weighting and "lat" in intersection:
                    m = getattr(retset[var_name].weighted(weights), method)
                    retset[var_name] = m(dim="lat", keep_attrs=True)
                    intersection.remove("lat")
                retset[var_name] = retset[var_name].reduce(
                    ufuncs[method], dim=intersection, keep_attrs=True
                )

    return retset


@op(tags=['aggregate', 'statistics'], version='1.1')
@op_input('ds', data_type=DatasetLike)
@op_input('var', value_set_source='ds', data_type=VarNamesLike)
@op_input('dim', value_set_source='ds', data_type=DimNamesLike)
@op_input('methods', any_of=["count", "mean", "median", "sum", "std", "min", "max"])
@op_input('apply_spatial_weighting', default_value=True)
@op_return(add_history=True)
def statistics(ds: DatasetLike.TYPE,
               var: VarNamesLike.TYPE = None,
               apply_spatial_weighting: bool = True,
               dim: DimNamesLike.TYPE = None,
               methods: Union[str, List[str]] = 'mean',
               monitor: Monitor = Monitor.NONE) -> xr.Dataset:
    """
    Computes statistics for the given variables of the given dataset along the
    given dimensions for the given methods.
    If no variables are given, all variables of the dataset will be reduced. If
    no dimensions are given, all dimensions will be reduced.

    :param ds: Dataset to aggregate statistics for
    :param var: Variables in the dataset to aggregate
    :param dim: Dataset dimensions along which to aggregate
    :param methods: One or more methods to create statistics from. Must be one or more
        of the following: 'count', 'mean', 'median', 'sum', 'std', 'min', 'max'
    :param apply_spatial_weighting: Whether to apply spatial weighting to cancel
        the effects of grid cells closer to the equator cover more area than
        grid cells closer to the poles. Only applicable for datasets with a
        latitude column named 'lat'. Has no effect for methods 'count', 'median',
        'min', and 'max'. Default is True.
    :param monitor: A progress monitor
    """
    if isinstance(methods, str):
        methods = [methods]
    ufuncs = {'min': da.nanmin, 'max': da.nanmax, 'mean': da.nanmean,
              'median': np.nanmedian, 'sum': da.nansum, "count": da.count_nonzero,
              "std": da.std}
    weightable_funcs = ["mean", "sum", "std"]
    funcs = dict()
    for k, v in ufuncs.items():
        if k in methods:
            funcs[k] = v

    ds = DatasetLike.convert(ds)

    if not var:
        var = list(ds.data_vars.keys())
    var_names = VarNamesLike.convert(var)

    if not dim:
        dim = list(ds.coords.keys())
    else:
        dim = DimNamesLike.convert(dim)

    retset = ds.copy()

    if apply_spatial_weighting:
        if not 'lat' in retset.coords:
            raise ValueError(
                "To apply spatial weighting, the dataset must have a 'lat' coordinate."
                "Consider to either set parameter 'apply_spatial_weighting' to False"
                "or run the 'normalize' operation on the dataset first."
            )
        weights = da.cos(da.deg2rad(retset.lat))

    with monitor.starting("Aggregating statistics", total_work=100):
        work = int(100 / (len(var_names) + len(funcs)))
        for var_name in var_names:
            for func_name, func in funcs.items():
                intersection = [value for value in dim if value in retset[var_name].dims]
                with monitor.child(work).observing("Reduce"):
                    retvar = retset[var_name]
                    if func_name in weightable_funcs and apply_spatial_weighting and "lat" in intersection:
                        m = getattr(retset[var_name].weighted(weights), func_name)
                        retvar = m(dim="lat", keep_attrs=True)
                        intersection.remove("lat")
                    retset[f"{var_name}_{func_name}"] = retvar.reduce(
                        func, dim=intersection, keep_attrs=True
                    )
    return retset
