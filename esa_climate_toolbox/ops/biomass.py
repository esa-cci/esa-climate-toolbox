# The MIT License (MIT)
# Copyright (c) 2026 ESA Climate Change Initiative
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

"""Operations for operations specific to the biomass ecv."""

from datetime import datetime
from scipy import ndimage
from typing import Union

import dask.array as da
from dask_image import ndfilters
import numpy as np
import pandas as pd
import xarray as xr

from esa_climate_toolbox.core import open_data
from esa_climate_toolbox.core.op import op
from esa_climate_toolbox.core.op import op_input
from esa_climate_toolbox.core.op import op_return
from esa_climate_toolbox.core.types import DatasetLike, TimeLike
from esa_climate_toolbox.core.types import ValidationError
from esa_climate_toolbox.util.time import determine_frequency
from esa_climate_toolbox.util.time import half_offset

_AGB_DIFF_NAME = "diff"
_AGB_DIFF_ATTRS = {
    "long_name": "AGB Difference",
    "units": "Mg/ha",
    "valid_min":  -10000.,
    "valid_max": 10000.
}
_AGB_DIFF_SD_NAME = "diff_sd"
_AGB_DIFF_SD_ATTRS = {
    "long_name": "Standard Deviation",
    "units": "Mg/ha",
    "valid_min":  0.,
    "valid_max": 10000.
}
_AGB_DIFF_QF_NAME = "diff_qf"
_AGB_DIFF_QF_ATTRS = {
    "long_name": "Quality Flag",
    "valid_min":  0,
    "valid_max": 5,
    "flag_values": [0, 1, 2, 3, 4, 5],
    "flag_meanings": [
        "AGB=0 t/ha in both maps", "significant loss", "potential loss",
        "non significant change", "potential gain", "significant gain"
    ]
}


@op(tags=['biomass'], version='1.0')
@op_input('ds')
@op_input('reference')
@op_input('difference')
@op_input('agb_name', data_type=str, default_value="agb")
@op_input('agb_sd_name', data_type=str, default_value="agb_sd")
@op_return()
def agb_change(
        ds: Union[str, DatasetLike.TYPE],
        reference: Union[int, TimeLike.TYPE],
        difference: Union[int, TimeLike.TYPE],
        agb_name: str = "agb",
        agb_sd_name: str = "agb_sd"
) -> xr.Dataset:
    """Computes the difference of above-ground biomass between two time stamps of
    a dataset of the BIOMASS ECV. This operation expects an above-ground biomass
    and a standard deviation to be present. It will also output the standard deviation
    of the difference and a quality flag band.

    :param ds: Dataset to split into regional subsets.
        One may also specify the id of a dataset provided by the Toolbox.
    :param reference: The timestep serving as reference for computing the difference.
        May also be given as year.
    :param difference: The timestep serving as difference for computing the difference.
        May also be given as year.
    :param agb_name: The name of the agb variable Default is 'agb'.
    :param agb_sd_name: The name of the agb standard deviation variable Default is 'agb_sd'.
    :return: A Dataset with variables agb difference, agb difference standard deviation and
        quality flag.
    """
    if isinstance(ds, str):
        ds, _ = open_data(ds)
    ds = DatasetLike.convert(ds)

    try:
        ds_frequency = determine_frequency(ds)
    except ValueError:
        ds_frequency = "1Y"
    half_ds_frequency = half_offset(ds_frequency)

    if isinstance(reference, int):
        reference = datetime(reference, 1, 1)
    reference = np.datetime64(TimeLike.convert(reference))

    if isinstance(difference, int):
        difference = datetime(difference, 1, 1)
    difference = np.datetime64(TimeLike.convert(difference))

    try:
        reference_ds = ds.sel(time=reference, method="nearest", tolerance=half_ds_frequency)
    except KeyError as ke:
        raise ValidationError("Reference time not in dataset")
    try:
        difference_ds = ds.sel(time=difference, method="nearest", tolerance=half_ds_frequency)
    except KeyError as ke:
        raise ValidationError("Difference time not in dataset")

    try:
        reference_agb = reference_ds[agb_name]
        difference_agb = difference_ds[agb_name]
    except KeyError:
        raise ValidationError(f"Variable '{agb_name}' not in dataset")

    try:
        reference_agb_sd = reference_ds[agb_sd_name]
        difference_agb_sd = difference_ds[agb_sd_name]
    except KeyError:
        raise ValidationError(f"Variable '{agb_sd_name}' not in dataset")

    agb_diff = (difference_agb - reference_agb).astype(np.int16)
    std_diff = da.sqrt(
        da.power(reference_agb_sd, 2) + da.power(difference_agb_sd, 2)
    ).astype(np.int16)

    reference_agb_lower_bound = reference_agb - reference_agb_sd
    reference_agb_upper_bound = reference_agb + reference_agb_sd
    difference_agb_lower_bound = difference_agb - difference_agb_sd
    difference_agb_upper_bound = difference_agb + difference_agb_sd

    agb_prob = xr.full_like(reference_agb, 3, dtype=np.int16)
    agb_prob = agb_prob.where(~(difference_agb < reference_agb_lower_bound), 2)
    agb_prob = agb_prob.where(~(difference_agb > reference_agb_upper_bound), 4)
    agb_prob = agb_prob.where(~(difference_agb_lower_bound > reference_agb_upper_bound), 5)
    agb_prob = agb_prob.where(~(difference_agb_upper_bound < reference_agb_lower_bound), 1)
    agb_prob = agb_prob.where(~((difference_agb == 0) & (reference_agb == 0)), 0)

    # Accounting for the fact that forests cannot grow more than its potential
    # If it happens, re-label to no significant change
    annual_increment = 10    # Highest value of AGB increment for the natural forest
                             # and also including most plantations (IPCC guidelines)
    year_diff = pd.Timestamp(difference).year - pd.Timestamp(reference).year
    agb_prob = agb_prob.where(~(agb_diff > (year_diff * annual_increment)), 3)

    # Filtering using a plain median filter.
    # First: create a bitmap of 0 and 1 (change no change) to re-label potential/significant
    # changes to no changes if isolated
    mask = xr.ones_like(reference_agb, dtype=np.int16)
    mask = mask.where(agb_prob != 3, 2)
    mask = mask.where(agb_prob != 0, 0)

    if isinstance(mask.data, da.Array):
        mask_filtered = ndfilters.median_filter(mask.data, size=3)
    else:
        mask_filtered = ndimage.median_filter(mask.data, size=3)
    mask = xr.DataArray(mask_filtered, coords=mask.coords, dims=mask.dims)

    agb_prob_ref = agb_prob.where(mask != 2, 3)
    agb_prob_ref = agb_prob_ref.where(agb_prob != 0, 0)
    agb_prob_ref = agb_prob_ref.astype(np.int16)

    agb_diff.attrs = _AGB_DIFF_ATTRS
    std_diff.attrs = _AGB_DIFF_SD_ATTRS
    agb_prob_ref.attrs = _AGB_DIFF_QF_ATTRS

    ds_out = xr.Dataset(
        data_vars={
            _AGB_DIFF_NAME: agb_diff,
            _AGB_DIFF_SD_NAME: std_diff,
            _AGB_DIFF_QF_NAME: agb_prob_ref
        }
    )

    return ds_out
