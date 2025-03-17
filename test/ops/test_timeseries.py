"""
Tests for timeseries operations
"""

from unittest import TestCase

import numpy as np
import xarray as xr

from xcube.core.new import new_cube

from esa_climate_toolbox.core.op import OP_REGISTRY
from esa_climate_toolbox.util.misc import object_to_qualified_name

from esa_climate_toolbox.ops.timeseries import fourier_analysis
from esa_climate_toolbox.ops.timeseries import tseries_mean
from esa_climate_toolbox.ops.timeseries import tseries_point


def assertDatasetEqual(expected, actual):
    # this method is functionally equivalent to
    # `assert expected == actual`, but it checks each aspect of
    # equality separately for easier debugging
    assert expected.equals(actual), (expected, actual)


class TimeSeriesPoint(TestCase):

    def test_tseries_point(self):
        # Test general functionality
        dataset = xr.Dataset({
            'abs': (['lat', 'lon', 'time'], np.ones([4, 8, 6])),
            'bbs': (['lat', 'lon', 'time'], np.ones([4, 8, 6])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': ['2000-01-01', '2000-02-01', '2000-03-01', '2000-04-01',
                     '2000-05-01', '2000-06-01']})

        actual = tseries_point(dataset, point='10, 5', var='*bs')
        expected = xr.Dataset({
            'abs': (['time'], np.ones(6)),
            'bbs': (['time'], np.ones(6)),
            'lat': 22.5,
            'lon': 22.5,
            'time': ['2000-01-01', '2000-02-01', '2000-03-01', '2000-04-01',
                     '2000-05-01', '2000-06-01']})
        expected = expected.set_coords(['lat', 'lon'])
        assertDatasetEqual(expected, actual)

        actual = tseries_point(dataset, point=(10, 5), var='')
        assertDatasetEqual(expected, actual)

    def registered(self):
        """
        Test tseries_point as a registered operation
        """
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(tseries_point))
        dataset = xr.Dataset({
            'abs': (['lat', 'lon', 'time'], np.ones([4, 8, 6])),
            'bbs': (['lat', 'lon', 'time'], np.ones([4, 8, 6])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': ['2000-01-01', '2000-02-01', '2000-03-01', '2000-04-01',
                     '2000-05-01', '2000-06-01']})

        actual = reg_op(ds=dataset, point='10, 5', var='*bs')
        expected = xr.Dataset({
            'abs': (['time'], np.ones(6)),
            'bbs': (['time'], np.ones(6)),
            'lat': 22.5,
            'lon': 22.5,
            'time': ['2000-01-01', '2000-02-01', '2000-03-01', '2000-04-01',
                     '2000-05-01', '2000-06-01']})
        expected = expected.set_coords(['lat', 'lon'])
        assertDatasetEqual(expected, actual)


class TimeSeriesMean(TestCase):

    def test_tseries_mean(self):
        # Test general functionality
        dataset = xr.Dataset({
            'abs': (['lat', 'lon', 'time'], np.ones([4, 8, 6])),
            'bbs': (['lat', 'lon', 'time'], np.ones([4, 8, 6])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': ['2000-01-01', '2000-02-01', '2000-03-01', '2000-04-01',
                     '2000-05-01', '2000-06-01']})
        actual = tseries_mean(dataset, var='*bs')
        expected = xr.Dataset({
            'abs_mean': (['time'], np.ones([6])),
            'bbs_mean': (['time'], np.ones([6])),
            'abs_std': (['time'], np.zeros([6])),
            'bbs_std': (['time'], np.zeros([6])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': ['2000-01-01', '2000-02-01', '2000-03-01', '2000-04-01',
                     '2000-05-01', '2000-06-01']})
        assertDatasetEqual(expected, actual)

        actual = tseries_mean(dataset, var='')
        assertDatasetEqual(actual, expected)

    def registered(self):
        """
        Test tseries_point as a registered operation
        """
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(tseries_mean))
        dataset = xr.Dataset({
            'abs': (['lat', 'lon', 'time'], np.ones([4, 8, 6])),
            'bbs': (['lat', 'lon', 'time'], np.ones([4, 8, 6])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': ['2000-01-01', '2000-02-01', '2000-03-01', '2000-04-01',
                     '2000-05-01', '2000-06-01']})
        actual = reg_op(ds=dataset, var='*bs')
        expected = xr.Dataset({
            'abs': (['time'], np.ones([6])),
            'bbs': (['time'], np.ones([6])),
            'abs_std': (['time'], np.zeros([6])),
            'bbs_std': (['time'], np.zeros([6])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': ['2000-01-01', '2000-02-01', '2000-03-01', '2000-04-01',
                     '2000-05-01', '2000-06-01']})
        assertDatasetEqual(expected, actual)

class FourierAnalysisTest(TestCase):

    def test_fourier_analysis(self):
        dataset = new_cube(
            width=4,
            height=8,
            y_start=-67.5,
            y_res=6,
            x_start=157.5,
            x_res=8,
            time_periods=6,
            variables=(dict(abs=1, bbs=1))
        )

        tseries = tseries_mean(dataset, var='*bs')

        actual = fourier_analysis(tseries, compute_frequencies=False)

        expected = new_cube(
            width=4,
            height=8,
            y_start=-67.5,
            y_res=6,
            x_start=157.5,
            x_res=8,
            time_periods=6,
        )
        expected = expected.assign(
            abs_mean_ampl=xr.DataArray([1., 0., 0., 0., 0., 0.], dims="time"),
            abs_mean_phase=xr.DataArray([0., 0., 0., 0., 0., 0.], dims="time"),
            abs_std_ampl=xr.DataArray([0., 0., 0., 0., 0., 0.], dims="time"),
            abs_std_phase=xr.DataArray([0., 0., 0., 0., 0., 0.], dims="time"),
            bbs_mean_ampl=xr.DataArray([1., 0., 0., 0., 0., 0.], dims="time"),
            bbs_mean_phase=xr.DataArray([0., 0., 0., 0., 0., 0.], dims="time"),
            bbs_std_ampl=xr.DataArray([0., 0., 0., 0., 0., 0.], dims="time"),
            bbs_std_phase=xr.DataArray([0., 0., 0., 0., 0., 0.], dims="time")
        )
        assertDatasetEqual(expected, actual)
