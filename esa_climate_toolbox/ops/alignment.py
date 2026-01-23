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

Provides methods for aligning datasets temporally.

Components
==========
"""

import bisect
import pandas as pd
from enum import Enum
import numpy as np
from typing import Optional, Tuple
import xarray as xr

from xcube.core.resampling import resample_in_time

from esa_climate_toolbox.core.op import op
from esa_climate_toolbox.core.op import op_input
from esa_climate_toolbox.core.op import op_return
from esa_climate_toolbox.core.types import ValidationError


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
    if freq is not None:
        return freq
    period_candidates = ["Y", "Q", "M"]
    for period_candidate in period_candidates:
        period_index = index.to_period(period_candidate)
        diffs = np.diff(period_index)
        num_units = diffs[0].freqstr.replace(diffs[0].name, "")
        if num_units == "0":
            continue
        for diff in diffs:
            if diff != diffs[0]:
                diffs = None
                break
        if diffs is not None:
            break
    if diffs is None:
        return None
    num_units = diff.freqstr.replace(diff.name, "")
    num_units = 1 if num_units == "" else int(num_units)
    return f"{num_units}{period_candidate}S"

def _half_offset(freq: str):
    return _offset(freq, half=True)

def _offset(freq: str, half: bool=False):
    sub_freq = freq[:-1] if freq.endswith("S") else freq
    sub_freq = sub_freq.split("-")[0]
    _base_offsets = {
        "h": (60, "min"),
        "H": (60, "min"),
        "D": (24, "H"),
        "W": (168, "H"),
        "M": (30, "D"),
        "Q": (90, "D"),
        "Y": (364, "D")
    }
    if len(sub_freq) == 1:
        num_units = 1
    else:
        num_units = int(sub_freq[:-1])
    offsets, offset_units = _base_offsets.get(sub_freq[-1])
    num_offset_units = num_units * offsets
    num_offset_units = int(num_offset_units / 2) if half else num_offset_units
    return f"{num_offset_units}{offset_units}"


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
    if "MS" in freq or "QS" in freq or "YS" in freq:
        index = ds.time.to_index()
        period = index.to_period(freq[:-1])
        time_bounds = xr.DataArray(
            name=default_time_bounds_name,
            data=np.array([period.start_time.values, period.end_time.values]).transpose(),
            dims=("time", "bnds")
        )
    else:
        time_delta = ds.time[1] - ds.time[0]
        start_times = ds.time - time_delta // 2
        end_times = ds.time + time_delta // 2
        time_bounds = xr.DataArray(
            name=default_time_bounds_name,
            data=np.array([start_times, end_times]).transpose(),
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
    replica_freq = _determine_frequency(ds_replica)
    upsampling = ds_primary.time[1] - ds_primary.time[0] > ds_replica.time[1] - ds_replica.time[0]
    if upsampling:
        offset_for_adjusting = _half_offset(primary_freq)
        method= "mean"
    else:
        offset_for_adjusting = _half_offset(replica_freq)
        method = "interpolate"

    if primary_freq is None:
        raise ValidationError(
            'The time coordinate of the primary dataset is not equidistant, '
            'can not perform temporal alignment.'
        )

    ds_primary, prim_time_bounds = _find_time_bounds(ds_primary, primary_freq)
    if prim_time_bounds is None:
        raise ValidationError(
            'Could not determine bounds of primary dataset, '
            'cannot perform temporal alignment.'
        )
    ds_replica, repl_time_bounds = _find_time_bounds(ds_replica)

    bounds_start = ds_primary[prim_time_bounds].values[0][0]
    bounds_end = ds_primary[prim_time_bounds].values[-1][1]

    repl_start_bounds = repl_end_bounds = ds_replica["time"]
    if repl_time_bounds is not None:
        repl_start_bounds = ds_replica[repl_time_bounds][:, 0]
        repl_end_bounds = ds_replica[repl_time_bounds][:, 1]

    start_cut_index = bisect.bisect_right(repl_end_bounds, bounds_start)
    end_cut_index = bisect.bisect_left(repl_start_bounds, bounds_end)

    ds_replica = ds_replica.isel(time=slice(start_cut_index, end_cut_index))

    if len(ds_replica.time) == 0:
        raise ValidationError("Could not determine an overlap between primary and replica dataset. "
                              "Temporal alignment was not possible.")

    if len(ds_replica.time) == 1:
        method= "mean"

    if primary_freq.endswith("S") or "W-" in primary_freq:
        offset_in_days = _offset(primary_freq)
        adjusted_start_time = ds_primary.time.values[0] - pd.Timedelta(offset_in_days)
    elif primary_freq.endswith("h"):
        if repl_time_bounds is None:
            adjusted_start_time = ds_replica.time.values[0]
        else:
            adjusted_start_time = ds_replica[repl_time_bounds][:, 0].values[0]
    else:
        # pad ds_replica in front so start time corresponds to start time of ds_primary
        estimated_offset = max(
            0,
            bisect.bisect_left(ds_primary[prim_time_bounds][:,0], ds_replica.time.values[0]) - 1
        )
        adjusted_start_time = ds_primary[prim_time_bounds][:,0].values[estimated_offset]

    expected_resampled_date_range = pd.date_range(
        start=adjusted_start_time, end=bounds_end, freq=primary_freq
    )
    resampling_offset = ds_replica.time.values[0] - expected_resampled_date_range[0]

    if repl_time_bounds is not None:
        # we need to remove replica time bounds because they cannot be resampled in time
        ds_replica = ds_replica.drop_vars(repl_time_bounds)

    # resample using xcube
    ds = resample_in_time(
        ds_replica,
        frequency=primary_freq,
        method=method,
        interp_kind="nearest",
        tolerance=primary_freq,
        offset=resampling_offset
    )
    non_dropped_time_length = len(ds.time)
    ds = ds.dropna(dim="time")

    if upsampling or len(ds.time) is not non_dropped_time_length:
        ds = ds.assign(time=ds.time.values + pd.Timedelta(_half_offset(primary_freq)))

    ds = ds.reindex(time=ds_primary.time, method="nearest", tolerance=offset_for_adjusting)
    var_renamings = {var_name: "_".join(var_name.split("_")[0]) for var_name in ds.data_vars.keys()}
    ds = ds.rename(var_renamings)
    time_bounds_names = ["time_bnds", "time_bounds"]
    for time_bounds_name in time_bounds_names:
        if time_bounds_name in ds_primary.coords or time_bounds_name in ds_primary.data_vars:
            ds = ds.assign({time_bounds_name: ds_primary[time_bounds_name]})
    return ds
