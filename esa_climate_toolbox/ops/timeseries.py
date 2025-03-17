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

"""
Description
===========

Simple time-series extraction operation.

Functions
=========
"""

import dask.array as da
import pandas as pd
import scipy
import xarray as xr

from esa_climate_toolbox.core.op import op
from esa_climate_toolbox.core.op import op_input
from esa_climate_toolbox.core.op import op_return
from esa_climate_toolbox.ops.select import select_var
from esa_climate_toolbox.core.types import PointLike
from esa_climate_toolbox.core.types import VarNamesLike
from esa_climate_toolbox.util.monitor import Monitor


@op(tags=['timeseries', 'temporal', 'filter', 'point'], version='1.0')
@op_input('point', data_type=PointLike)
@op_input('method', value_set=['nearest', 'ffill', 'bfill'])
@op_input('var', value_set_source='ds', data_type=VarNamesLike)
@op_return(add_history=True)
def tseries_point(ds: xr.Dataset,
                  point: PointLike.TYPE,
                  var: VarNamesLike.TYPE = None,
                  method: str = 'nearest') -> xr.Dataset:
    """
    Extract time-series from *ds* at given *lon*, *lat* position using
    interpolation *method* for each *var* given in a comma separated list of
    variables.

    The operation returns a new timeseries dataset, that contains the point
    timeseries for all required variables with original variable
    meta-information preserved.

    If a variable has more than three dimensions, the resulting timeseries
    variable will preserve all other dimensions except for lon/lat.

    :param ds: The dataset from which to perform timeseries extraction.
    :param point: Point to extract, e.g. (lon,lat)
    :param var: Variable(s) for which to perform the timeseries selection
                if none is given, all variables in the dataset will be used.
    :param method: Interpolation method to use.
    :return: A timeseries dataset
    """
    point = PointLike.convert(point)
    lon = point.x
    lat = point.y

    if not var:
        var = '*'

    retset = select_var(ds, var=var)
    indexers = {'lat': lat, 'lon': lon}
    retset = retset.sel(method=method, **indexers)

    # The dataset is no longer a spatial dataset -> drop associated global
    # attributes
    drop = ['geospatial_bounds_crs', 'geospatial_bounds_vertical_crs',
            'geospatial_vertical_min', 'geospatial_vertical_max',
            'geospatial_vertical_positive', 'geospatial_vertical_units',
            'geospatial_vertical_resolution', 'geospatial_lon_min',
            'geospatial_lat_min', 'geospatial_lon_max', 'geospatial_lat_max']

    for key in drop:
        retset.attrs.pop(key, None)

    return retset


@op(tags=['timeseries', 'temporal'], version='1.0')
@op_input('ds')
@op_input('var', value_set_source='ds', data_type=VarNamesLike)
@op_return(add_history=True)
def tseries_mean(ds: xr.Dataset,
                 var: VarNamesLike.TYPE = None,
                 mean_suffix: str = '_mean',
                 std_suffix: str = '_std',
                 monitor: Monitor = Monitor.NONE) -> xr.Dataset:
    """
    Extract spatial mean timeseries of the provided variables, return the
    dataset that in addition to all the information in the given dataset
    contains also timeseries data for the provided variables, following
    naming convention 'var_name1_ts_mean'. In addition, the standard deviation
    is computed.

    If a data variable with more dimensions than time/lat/lon is provided,
    the data will be reduced by taking the mean of all data values at a single
    time position resulting in one dimensional timeseries data variable.

    :param ds: The dataset from which to perform timeseries extraction.
    :param var: Variables for which to perform timeseries extraction
    :param mean_suffix: Mean suffix to use for resulting datasets
    :param std_suffix: Std suffix to use for resulting datasets
    :param monitor: a progress monitor.
    :return: Dataset with timeseries variables
    """
    if not var:
        var = '*'

    retset = select_var(ds, var)
    names = list(retset.data_vars.keys())

    with monitor.starting("Calculate mean and standard deviation",
                          total_work=len(names)):
        for name in names:
            dims = list(ds[name].dims)
            dims.remove('time')
            mean_name = name + mean_suffix
            with monitor.child(1).observing("Calculate mean"):
                retset[mean_name] = retset[name].mean(dim=dims, keep_attrs=True)
            retset[mean_name].attrs['ESA_Climate_Toolbox_Description'] = \
                'Mean aggregated over {} at each point in time.'.format(dims)
            std_name = name + std_suffix
            retset[std_name] = ds[name].std(dim=dims)
            retset[std_name].attrs['ESA_Climate_Toolbox_Description'] = \
                'Accompanying std values for variable \'{}\''.format(name)
    retset = retset.drop_vars(names)

    return retset


@op(tags=['timeseries', 'temporal', 'fourier', 'analysis'], version='1.0')
@op_input('ds')
@op_input('var', value_set_source='ds', data_type=VarNamesLike)
@op_return(add_history=True)
def fourier_analysis(ds: xr.Dataset,
                 var: VarNamesLike.TYPE = None,
                 compute_frequencies: bool = True,
                 monitor: Monitor = Monitor.NONE) -> xr.Dataset:
    """
    Performs a fourier analysis on a 1-dimensional dataset, determining
    phase and amplitude, and, if requested, frequencies.

    :param ds: The dataset for which to perform a discrete fourier transform.
    :param var: Variables for which to perform discrete fourier transform.
    :param monitor: a progress monitor.
    :return: Dataset with timeseries variables
    """
    if not var:
        var = '*'

    retset = select_var(ds, var)
    names = list(retset.data_vars.keys())

    with monitor.starting("Calculate phases and amplitudes",
                          total_work=len(names)):
        num_time_steps = len(ds["time"])
        if compute_frequencies:
            d = ds["time"].diff(dim="time")
            frequency = scipy.fft.fftfreq(
                len(ds["time"]), pd.to_timedelta(d[0].values).total_seconds()
            )
            retset = retset.assign({"frequency": frequency})
                                       # scipy.fft.fftfreq(len(ds["time"]),
                                       #         pd.to_timedelta(d[0].values).total_seconds())}
            # )
        for name in names:
            var_fft = scipy.fft.fft(ds[name])
            ampl_name = f"{name}_ampl"
            phase_name = f"{name}_phase"
            amplitude = (1 / num_time_steps) * da.abs(var_fft)
            # amplitude = (2 / num_time_steps) * da.abs(var_fft[:num_time_steps // 2])
            phase = da.angle(var_fft)
            retset = retset.assign({
                ampl_name: xr.DataArray(amplitude, dims="time"),
                # ampl_name: xr.DataArray(amplitude, dims="time"),
                phase_name: xr.DataArray(phase, dims="time")
                # phase_name: da.angle(var_fft[:num_time_steps // 2])
            })
            retset[ampl_name].attrs['ESA_Climate_Toolbox_Description'] = \
                'Amplitude of Discrete Fourier Transform of \'{}\''.format(name)
            retset[phase_name].attrs['ESA_Climate_Toolbox_Description'] = \
                'Phase of Discrete Fourier Transform ov \'{}\''.format(name)
        retset = retset.drop_vars(names)

        return retset


