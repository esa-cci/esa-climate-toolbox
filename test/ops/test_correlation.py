"""
Tests for correlation operations
"""

import numpy as np
import unittest
import xarray as xr
from xcube.core.new import new_cube

from esa_climate_toolbox.ops import pixelwise_group_correlation

def get_test_dataset(time_periods: int, time_freq: str, time_start: str, var_name: str, offset: float):
    inner_grid = np.arange(2 * 2).reshape(2, 2) * offset
    outer_offsets = np.arange(time_periods).reshape(time_periods, 1, 1) * 2
    array = xr.DataArray(inner_grid + outer_offsets, dims=("time", "lat", "lon"), name=var_name).chunk()
    return new_cube(
        time_periods=time_periods,
        time_freq=time_freq,
        width=2,
        height=2,
        time_start=time_start,
        variables={
            var_name: array
        },
        drop_bounds=True
    )

class PixelwiseGroupCorrelationTest(unittest.TestCase):

    def test_pixelwise_group_correlation(self):
        ds_1 = get_test_dataset(time_periods=5, time_freq="YE", time_start="2009-01-01T00:00:00", var_name="a",
                                offset=2.3)
        ds_2 = get_test_dataset(time_periods=5, time_freq="YE", time_start="2009-01-01T00:00:00", var_name="b",
                                offset=1.3)
        ds_a = xr.merge([ds_1, ds_2])

        ds_3 = get_test_dataset(time_periods=5, time_freq="YE", time_start="2009-01-01T00:00:00", var_name="x",
                                offset=0.4)
        ds_4 = get_test_dataset(time_periods=5, time_freq="YE", time_start="2009-01-01T00:00:00", var_name="y",
                                offset=3.5)
        ds_b = xr.merge([ds_3, ds_4])

        ds = pixelwise_group_correlation(ds_a, ds_b, ["a", "b"], ["x","y"])

        self.assertIsNotNone(ds)
        self.assertIn("canonical_correlation", ds.data_vars)
        self.assertEqual({'lat': 2, 'lon': 2}, ds.dims)
        self.assertEqual(('lat', 'lon'), ds.canonical_correlation.dims)
        self.assertEqual(1.0, ds.canonical_correlation.values[0, 1])
        self.assertEqual(1.0, ds.canonical_correlation.values[1, 0])
        self.assertEqual(1.0, ds.canonical_correlation.values[1, 1])
