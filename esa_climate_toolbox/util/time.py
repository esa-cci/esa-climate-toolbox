# The MIT License (MIT)
# Copyright (c) 2023 ESA Climate Change Initiative
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

import numpy as np
import pandas as pd
from typing import Optional, Tuple
import xarray as xr

from xcube.core.timecoord import get_timestamps_from_string

from ..constants import MONTHS


def get_time_strings_from_string(string: str) -> (str, str):
    first_time, second_time = get_timestamps_from_string(string)
    if first_time:
        first_time = first_time.isoformat()
    if second_time:
        second_time = second_time.isoformat()
    if not first_time:
        for i, month in enumerate(MONTHS):
            if month in string:
                first_time = i + 1
                break
    return first_time, second_time

def determine_frequency(ds):
    index = ds.time.to_index()
    freq = pd.infer_freq(index)
    if freq is not None:
        index = 0
        try:
            while(True):
                int(freq[index])
                index+=1
        except:
            if index > 0:
                freq = freq[0:index+1]
            else:
                freq = f"1{freq[0]}"
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

def half_offset(freq: str):
    return offset(freq, half=True)

def offset(freq: str, half: bool=False):
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


def find_time_bounds(ds: xr.Dataset, freq: str = None) -> Tuple[xr.Dataset, Optional[str]]:
    default_time_bounds_name = "time_bnds"
    time_bounds_names = [default_time_bounds_name, "time_bounds"]
    for time_bounds_name in time_bounds_names:
        if time_bounds_name in ds.data_vars or time_bounds_name in ds.coords:
            return ds, time_bounds_name
    if freq is None:
        freq = determine_frequency(ds)
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
