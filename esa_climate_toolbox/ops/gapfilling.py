import numpy as np
from numba import jit
import xarray as xr

from esa_climate_toolbox.core.op import op
from esa_climate_toolbox.core.op import op_input
from esa_climate_toolbox.core.op import op_return
from esa_climate_toolbox.core.types import DatasetLike
from esa_climate_toolbox.util.monitor import Monitor


DEFAULT_KERNEL = np.array([[0.5, 0.7, 0.5],
                           [0.7, 1.0, 0.7],
                           [0.5, 0.7, 0.5]])


@op(tags=['gapfill'], version='1.0')
@op_input('ds', data_type=DatasetLike)
@op_return(add_history=True)
def gapfill(ds: DatasetLike, monitor: Monitor = Monitor.NONE):
    """
    Removes non-finite values within the variables of a data set by applying a
    low-pass filter.

    :param ds: The dataset whose gaps are to be filled.
    :param monitor: A progress monitor.

    :return: A new dataset without gaps in its data.
    """
    ds = DatasetLike.convert(ds)
    retset = ds
    das = {}
    with monitor.starting('Fill gaps', total_work=len(retset.data_vars)):
        for data_var_name, data_var in retset.data_vars.items():
            data_filled = data_var.data.map_blocks(fillgaps_lowpass_2d)
            das[data_var_name] = xr.DataArray(
                data_filled,
                coords=data_var.coords,
                dims=data_var.dims,
                attrs=data_var.attrs
            )
            monitor.progress(1)
    retset = retset.assign(das)
    return retset


def fillgaps_lowpass_2d(src, kernel=DEFAULT_KERNEL, threshold=1):
    w = src.shape[-1]
    h = src.shape[-2]
    pixel_count = w * h
    gap_count = 1
    out = src
    while 0 < gap_count < pixel_count:
        out, gap_count = _apply_low_pass_filter(out, kernel, threshold)
    return out


@jit(nopython=True)
def _apply_low_pass_filter(data, kernel, threshold):
    w = data.shape[-1]
    h = data.shape[-2]
    out = data.copy()
    kw = kernel.shape[-1]
    kh = kernel.shape[-2]
    kx0 = kw // 2
    ky0 = kh // 2
    gap_count = 0
    for y in range(h):
        for x in range(w):
            v = data[y, x]
            if is_gap(v):
                v_sum = 0.
                k_sum = 0.
                for ky in range(kh):
                    yy = y + ky - ky0
                    if 0 <= yy < h:
                        for kx in range(kw):
                            xx = x + kx - kx0
                            if 0 <= xx < w:
                                v = data[yy, xx]
                                if not is_gap(v):
                                    k = kernel[ky, kx]
                                    v_sum += k * v
                                    k_sum += k
                if k_sum != 0 and k_sum >= threshold:
                    out[y, x] = v_sum / k_sum
                else:
                    gap_count += 1

    return out, gap_count


@jit(nopython=True)
def is_gap(v):
    return not np.isfinite(v)

@jit(nopython=True)
def count_gaps(data):
    w = data.shape[-1]
    h = data.shape[-2]
    gap_count = 0
    for y in range(h):
        for x in range(w):
            if is_gap(data[y, x]):
                gap_count += 1
    return gap_count