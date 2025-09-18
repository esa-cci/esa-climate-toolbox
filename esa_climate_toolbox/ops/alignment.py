# The MIT License (MIT)
# Copyright (c) 2025 ESA Climate Change Initiative
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

Provides various resampling methods including up- and downsampling for temporal resampling.

Components
==========
"""

import cftime
import pandas as pd
from datetime import timedelta
import dask
import dask.array as da
from enum import Enum
import numpy as np
from typing import Any, Dict, Optional, Sequence, Tuple, Union
import xarray as xr
from xarray.coding.cftime_offsets import Day
from xarray.coding.cftime_offsets import Hour
from xarray.coding.cftime_offsets import MonthBegin
from xarray.coding.cftime_offsets import QuarterBegin
from xarray.coding.cftime_offsets import YearBegin

from xcube.core.resampling import resample_in_time

from esa_climate_toolbox.core.op import op
from esa_climate_toolbox.core.op import op_input
from esa_climate_toolbox.core.op import op_return
from esa_climate_toolbox.util.monitor import Monitor
from esa_climate_toolbox.core.types import DatasetLike, ValidationError


# DOWNSAMPLING_METHODS = ['count', 'first', 'last', 'min', 'max', 'sum', 'prod',
#                         'mean', 'median', 'std', 'var', 'percentile']
# UPSAMPLING_METHODS = ['asfreq', 'ffill', 'bfill', 'pad', 'nearest',
#                       'interpolate']
# ALL_METHODS = DOWNSAMPLING_METHODS + UPSAMPLING_METHODS
# SPLINE_INTERPOLATION_KINDS = ['zero', 'slinear', 'quadratic', 'cubic']
# OTHER_INTERPOLATION_KINDS = ['linear', 'nearest', 'previous', 'next']
# INTERPOLATION_KINDS = SPLINE_INTERPOLATION_KINDS + OTHER_INTERPOLATION_KINDS
RESAMPLING_METHODS = [
    "all", "any", "argmax", "argmin", "count", "first", "last", "max", "min", "nearest",
    "mean", "median", "std", "sum", "var"
]


class Offset(Enum):
    PREVIOUS = 'previous'
    NONE = 'none'
    NEXT = 'next'

class SampleType(Enum):
    DOWN = "down"
    UP = "up"
    EITHER = "either"

def _determine_frequency(ds):
    index = ds.time.to_index()
    freq = pd.infer_freq(index)
    if freq is None:
        # test_periods = ["Y", "Q", "M"]
        test_periods = ["M", "Y"]
        for test_period in test_periods:
            period_index = index.to_period(test_period)
            diffs = np.diff(period_index)
            # first_diff = diffs[1] - diffs[0]
            freq = test_period
            for diff in diffs:
                if diff != diffs[0]:
                    freq = None
                    break
            if freq is not None:
                break
            # if np.all(diffs == first_diff):
            #     freq = f"{first_diff}{test_period}"
            #     break
    return freq

def _offset_in_days(freq: str) -> str:
    offset_to_days = {
        "D": 1,
        "W": 7,
        "M": 30,
        "Q": 91,
        "Y": 365
    }
    return f"{offset_to_days.get(freq)}D"

def _half_offset_in_days(freq: str) -> str:
    _half_offset_to_days = {
        "D": "12H",
        "W": "2D",
        "M": "2D",
        "Q": "5D",
        "Y": "5D"
    }
    return _half_offset_to_days.get(freq)

def _determine_required_sampling_type(primary_freq: str, replica_freq: str):
    factors = {
        "D": 1,
        "W": 7,
        "M": 30,
        "Q": 91,
        "Y": 365
    }
    primary_factor = primary_freq[:-1]
    primary_multiplier = primary_freq[-1]
    primary_harm = int(primary_factor) * factors.get(primary_multiplier)
    replica_factor = replica_freq[:-1]
    replica_multiplier = replica_freq[-1]
    replica_harm = int(replica_factor) * factors.get(replica_multiplier)
    if replica_harm < primary_harm:
        return SampleType.UP
    if replica_harm > primary_harm:
        return SampleType.DOWN
    return SampleType.EITHER


def _find_time_bounds(ds: xr.Dataset, freq: str = None) -> Tuple[xr.Dataset, Optional[str]]:
    default_time_bounds_name = "time_bnds"
    time_bounds_names = [default_time_bounds_name, "time_bounds"]
    for time_bounds_name in time_bounds_names:
        if time_bounds_name in ds.data_vars or time_bounds_name in ds.coords:
            return ds, time_bounds_name
    if freq is None:
        freq = _determine_frequency(ds)
        if freq is None:
            return ds, None
    index = ds.time.to_index()
    period = index.to_period(freq)
    time_bounds = xr.DataArray(
        name=default_time_bounds_name,
        data=np.array([period.start_time.values, period.end_time.values]).transpose(),
        dims=("time", "bnds")
    )
    ds = ds.assign({default_time_bounds_name: time_bounds})
    return ds, default_time_bounds_name


@op(tags=['temporal', 'alignment'], version='1.0')
@op_input('method', value_set=RESAMPLING_METHODS, default_value="mean")
@op_return(add_history=True)
def temporal_alignment(
        ds_primary: xr.Dataset,
        ds_replica: xr.Dataset,
        method: str = None
) -> xr.Dataset:
    """
    Temporally aligns a dataset (ds_replica) with another dataset (ds_primary),
    so that both datasets will have the same time coordinate.

    :param ds_primary: The dataset whose time coordinate shall be the reference
    :param ds_replica: The dataset whose time coordinate shall be changed to
        match the one of the primary dataset.
    :param method: The method by which the values from the replica dataset shall
        be resampled. Must be one of the following: "all", "any", "argmax",
        "argmin", "count", "first", "last", "max", "min", "nearest", "mean",
        "median", "std", "sum", "var".

    :return: A dataset where the data variables of the replica coordinate have been
        resampled to match the time coordinate of the primary dataset.
    """

    # check whether datasets have a time dimension
    if "time" not in ds_primary.dims:
        raise ValidationError(
            "The primary dataset grid does not have a dimension named 'time'. "
            "Running the normalize operation may help."
        )
    if "time" not in ds_replica.dims:
        raise ValidationError(
            "The replica dataset grid does not have a dimension named 'time'. "
            "Running the normalize operation may help."
        )
    # check whether primary has a regular time
    # retrieve resampling parameters from primary dataset
    primary_freq = _determine_frequency(ds_primary)
    if primary_freq is None:
        raise ValidationError(
            'The time coordinate of the primary dataset is not equidistant, '
            'can not perform temporal alignment.'
        )
    # ds_primary, prim_time_bounds = _find_time_bounds(ds_primary, primary_freq)

    # expected_resampled_date_range =

    offset_in_days = _offset_in_days(primary_freq)
    adjusted_start_time = ds_primary.time.values[0] - pd.Timedelta(offset_in_days)
    expected_resampled_date_range = pd.date_range(
        start=adjusted_start_time, periods=len(ds_primary.time.values) + 1, freq=primary_freq
        # start=adjusted_start_time, periods=len(ds_primary.time.values) , freq=primary_freq
    )
    # estimated_time_deviations = ds_primary.time.values - expected_resampled_date_range
    estimated_time_deviations = ds_primary.time.values - expected_resampled_date_range[:-1]
    # cut dataset
    if cut:
        replica_freq = _determine_frequency(ds_replica)
        if replica_freq is not None:
            times = pd.to_datetime(ds_replica.time.values)
            periods = times.to_period(replica_freq)
            starts = periods.start_time
            ends = periods.end_time
            mask = (starts <= expected_resampled_date_range[-1]) & (ends >= expected_resampled_date_range[0])
            ds_replica = ds_replica.sel(time=mask)
        # ds_replica = ds_replica.sel(time=slice(ds_primary.time[0], ds_primary.time[-1]))
        else:
            ds_replica = ds_replica.sel(time=slice(expected_resampled_date_range[0], expected_resampled_date_range[-1]))
    if (len(ds_replica.time) == 0):
        raise ValidationError("Could not determine an overlap between primary and replica dataset. "
                              "Temporal alignment was not possible.")
    # resample using xcube
    # ds = resample_in_time(ds_replica, frequency=primary_freq, method=method, cube_asserted=False)
    # ds = resample_in_time(ds_replica, frequency=primary_freq, method=method, tolerance=_half_offset_in_days(replica_freq))
    ds = resample_in_time(ds_replica, frequency=primary_freq, method="mean")
    ds = ds.assign(time=ds.time.values + estimated_time_deviations[0:len(ds.time)])
    ds = ds.reindex(time=ds_primary.time, method="nearest", tolerance=_half_offset_in_days(primary_freq))
    var_renamings = {var_name: "_".join(var_name.split("_")[0]) for var_name in ds.data_vars.keys()}
    ds = ds.rename(var_renamings)
    time_bounds_names = ["time_bnds", "time_bounds"]
    for time_bounds_name in time_bounds_names:
        if time_bounds_name in ds_primary.coords or time_bounds_name in ds_primary.data_vars:
    # if prim_time_bounds is not None:
            ds = ds.assign({time_bounds_name: ds_primary[time_bounds_name]})
        # ds = ds.assign({prim_time_bounds: ds_primary[prim_time_bounds]})
    # for var_name, var in ds.data_vars.keys():
        # ds.rename()
    # ds = ds.reindex(time=ds_primary.time, method="nearest", tolerance="12H")
    return ds
