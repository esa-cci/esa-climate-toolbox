"""
Test coregistration, checks if the values seem as expected
when using default upsampling/downsampling methods.

"""

from unittest import TestCase

import numpy as np
import xarray as xr
from numpy.testing import assert_almost_equal, assert_array_equal

from esa_climate_toolbox.core.op import OP_REGISTRY
from esa_climate_toolbox.util.misc import object_to_qualified_name

from esa_climate_toolbox.ops import coregister
from esa_climate_toolbox.ops.coregistration import _find_intersection
from ..util.test_monitor import RecordingMonitor


class TestCoregistration(TestCase):
    """
    Test coregistration
    """

    def test_nominal(self):
        """
        Test nominal execution
        """
        ds_fine = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.array([np.eye(4, 8), np.eye(4, 8)])),
            'second': (['time', 'lat', 'lon'], np.array([np.eye(4, 8), np.eye(4, 8)])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.array([1, 2])
        }).chunk(chunks={'lat': 2, 'lon': 4})

        ds_coarse = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.array([np.eye(3, 6), np.eye(3, 6)])),
            'second': (['time', 'lat', 'lon'], np.array([np.eye(3, 6), np.eye(3, 6)])),
            'lat': np.linspace(-60, 60, 3),
            'lon': np.linspace(-150, 150, 6),
            'time': np.array([1, 2])
        }).chunk(chunks={'lat': 3, 'lon': 3})

        # Test that the coarse dataset has been resampled onto the grid
        # of the finer dataset.
        rm = RecordingMonitor()
        ds_coarse_resampled = coregister(ds_fine, ds_coarse, monitor=rm)
        self.assertGreaterEqual(len(rm.records), 3)
        self.assertIn('start', rm.records[0])
        self.assertIn('done', rm.records[-1])
        self.assertIn('progress', rm.records[1])

        expected = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.array(
                [[[1., 0.28571429, 0., 0., 0., 0., 0., 0.],
                  [0.33333333, 0.57142857, 0.38095238, 0., 0., 0., 0., 0.],
                  [0., 0.47619048, 0.52380952, 0.28571429, 0.04761905, 0., 0., 0.],
                  [0., 0., 0.42857143, 0.85714286, 0.14285714, 0., 0., 0.]],
                 [[1., 0.28571429, 0., 0., 0., 0., 0., 0.],
                  [0.33333333, 0.57142857, 0.38095238, 0., 0., 0., 0., 0.],
                  [0., 0.47619048, 0.52380952, 0.28571429, 0.04761905, 0., 0., 0.],
                  [0., 0., 0.42857143, 0.85714286, 0.14285714, 0., 0., 0.]]])),
            'second': (['time', 'lat', 'lon'], np.array(
                [[[1., 0.28571429, 0., 0., 0., 0., 0., 0.],
                  [0.33333333, 0.57142857, 0.38095238, 0., 0., 0., 0., 0.],
                  [0., 0.47619048, 0.52380952, 0.28571429, 0.04761905, 0., 0., 0.],
                  [0., 0., 0.42857143, 0.85714286, 0.14285714, 0., 0., 0.]],
                 [[1., 0.28571429, 0., 0., 0., 0., 0., 0.],
                  [0.33333333, 0.57142857, 0.38095238, 0., 0., 0., 0., 0.],
                  [0., 0.47619048, 0.52380952, 0.28571429, 0.04761905, 0., 0., 0.],
                  [0., 0., 0.42857143, 0.85714286, 0.14285714, 0., 0., 0.]]])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.array([1, 2])
        })
        assert_almost_equal(
            ds_coarse_resampled['first'].values, expected['first'].values
        )

        # Test that the fine dataset has been resampled (aggregated)
        # onto the grid of the coarse dataset.
        ds_fine_resampled = coregister(ds_coarse, ds_fine)
        expected = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.array(
                [[[0.625, 0.125, 0., 0., 0., 0.],
                  [0.125, 0.5, 0.125, 0., 0., 0.],
                  [0., 0.125, 0.625, 0., 0., 0.]],
                 [[0.625, 0.125, 0., 0., 0., 0.],
                  [0.125, 0.5, 0.125, 0., 0., 0.],
                  [0., 0.125, 0.625, 0., 0., 0.]]])),
            'second': (['time', 'lat', 'lon'], np.array(
                [[[0.625, 0.125, 0., 0., 0., 0.],
                  [0.125, 0.5, 0.125, 0., 0., 0.],
                  [0., 0.125, 0.625, 0., 0., 0.]],
                 [[0.625, 0.125, 0., 0., 0., 0.],
                  [0.125, 0.5, 0.125, 0., 0., 0.],
                  [0., 0.125, 0.625, 0., 0., 0.]]])),
            'lat': np.linspace(-60, 60, 3),
            'lon': np.linspace(-150, 150, 6),
            'time': np.array([1, 2])
        })

        assert_almost_equal(ds_fine_resampled['first'].values, expected['first'].values)

    def test_registered(self):
        """
        Test registered operation execution execution
        """
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(coregister))
        ds_fine = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.array([np.eye(4, 8), np.eye(4, 8)])),
            'second': (['time', 'lat', 'lon'], np.array([np.eye(4, 8), np.eye(4, 8)])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.array([1, 2])
        })

        ds_coarse = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.array([np.eye(3, 6), np.eye(3, 6)])),
            'second': (['time', 'lat', 'lon'], np.array([np.eye(3, 6), np.eye(3, 6)])),
            'lat': np.linspace(-60, 60, 3),
            'lon': np.linspace(-150, 150, 6),
            'time': np.array([1, 2])
        })

        # Test that the coarse dataset has been resampled onto the grid
        # of the finer dataset.
        ds_coarse_resampled = reg_op(ds_primary=ds_fine, ds_replica=ds_coarse)
        expected = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.array(
                [[[1., 0.28571429, 0., 0., 0., 0., 0., 0.],
                  [0.33333333, 0.57142857, 0.38095238, 0., 0., 0., 0., 0.],
                  [0., 0.47619048, 0.52380952, 0.28571429, 0.04761905, 0., 0., 0.],
                  [0., 0., 0.42857143, 0.85714286, 0.14285714, 0., 0., 0.]],
                 [[1., 0.28571429, 0., 0., 0., 0., 0., 0.],
                  [0.33333333, 0.57142857, 0.38095238, 0., 0., 0., 0., 0.],
                  [0., 0.47619048, 0.52380952, 0.28571429, 0.04761905, 0., 0., 0.],
                  [0., 0., 0.42857143, 0.85714286, 0.14285714, 0., 0., 0.]]]
            )),
            'second': (['time', 'lat', 'lon'], np.array(
                [[[1., 0.28571429, 0., 0., 0., 0., 0., 0.],
                  [0.33333333, 0.57142857, 0.38095238, 0., 0., 0., 0., 0.],
                  [0., 0.47619048, 0.52380952, 0.28571429, 0.04761905, 0., 0., 0.],
                  [0., 0., 0.42857143, 0.85714286, 0.14285714, 0., 0., 0.]],
                 [[1., 0.28571429, 0., 0., 0., 0., 0., 0.],
                  [0.33333333, 0.57142857, 0.38095238, 0., 0., 0., 0., 0.],
                  [0., 0.47619048, 0.52380952, 0.28571429, 0.04761905, 0., 0., 0.],
                  [0., 0., 0.42857143, 0.85714286, 0.14285714, 0., 0., 0.]]]
            )),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.array([1, 2])})
        assert_almost_equal(
            ds_coarse_resampled['first'].values, expected['first'].values
        )

        # Test that the fine dataset has been resampled (aggregated)
        # onto the grid of the coarse dataset.
        ds_fine_resampled = reg_op(ds_primary=ds_coarse, ds_replica=ds_fine)
        expected = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.array(
                [[[0.625, 0.125, 0., 0., 0., 0.],
                  [0.125, 0.5, 0.125, 0., 0., 0.],
                  [0., 0.125, 0.625, 0., 0., 0.]],
                 [[0.625, 0.125, 0., 0., 0., 0.],
                  [0.125, 0.5, 0.125, 0., 0., 0.],
                  [0., 0.125, 0.625, 0., 0., 0.]]]
            )),
            'second': (['time', 'lat', 'lon'], np.array(
                [[[0.625, 0.125, 0., 0., 0., 0.],
                  [0.125, 0.5, 0.125, 0., 0., 0.],
                  [0., 0.125, 0.625, 0., 0., 0.]],
                 [[0.625, 0.125, 0., 0., 0., 0.],
                  [0.125, 0.5, 0.125, 0., 0., 0.],
                  [0., 0.125, 0.625, 0., 0., 0.]]]
            )),
            'lat': np.linspace(-60, 60, 3),
            'lon': np.linspace(-150, 150, 6),
            'time': np.array([1, 2])})

        assert_almost_equal(ds_fine_resampled['first'].values, expected['first'].values)

    def test_error(self):
        """
        Test error conditions
        """
        # Test unexpected global bounds
        ds_fine = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.array([np.eye(4, 8), np.eye(4, 8)])),
            'second': (['time', 'lat', 'lon'], np.array([np.eye(4, 8), np.eye(4, 8)])),
            'lat': np.linspace(67.5, 135, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.array([1, 2])})

        ds_coarse = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.array([np.eye(3, 6), np.eye(3, 6)])),
            'second': (['time', 'lat', 'lon'], np.array([np.eye(3, 6), np.eye(3, 6)])),
            'lat': np.linspace(-60, 60, 3),
            'lon': np.linspace(-150, 150, 6),
            'time': np.array([1, 2])})

        with self.assertRaises(ValueError) as err:
            coregister(ds_fine, ds_coarse)
        self.assertIn('(67.5, 135.0)', str(err.exception))

        # Test non-equidistant dataset
        ds_fine = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.array([np.eye(4, 8), np.eye(4, 8)])),
            'second': (['time', 'lat', 'lon'], np.array([np.eye(4, 8), np.eye(4, 8)])),
            'lat': [-67.5, -20, 20, 67.5],
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.array([1, 2])})

        ds_coarse = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.array([np.eye(3, 6), np.eye(3, 6)])),
            'second': (['time', 'lat', 'lon'], np.array([np.eye(3, 6), np.eye(3, 6)])),
            'lat': np.linspace(-60, 60, 3),
            'lon': np.linspace(-150, 150, 6),
            'time': np.array([1, 2])})

        with self.assertRaises(ValueError) as err:
            coregister(ds_fine, ds_coarse)
        self.assertIn('not equidistant', str(err.exception))

        # Test non-pixel registered dataset
        ds_fine = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.array([np.eye(4, 8), np.eye(4, 8)])),
            'second': (['time', 'lat', 'lon'], np.array([np.eye(4, 8), np.eye(4, 8)])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.array([1, 2])})

        ds_coarse_err = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.zeros([2, 5, 10])),
            'second': (['time', 'lat', 'lon'], np.zeros([2, 5, 10])),
            'lat': np.linspace(-90, 90, 5),
            'lon': np.linspace(-162, 162, 10),
            'time': np.array([1, 2])})

        with self.assertRaises(ValueError) as err:
            coregister(ds_fine, ds_coarse_err)
        self.assertIn('not pixel-registered', str(err.exception))

        ds_coarse_err = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.zeros([2, 5, 10])),
            'second': (['time', 'lat', 'lon'], np.zeros([2, 5, 10])),
            'lat': np.linspace(-72, 72, 5),
            'lon': np.linspace(-180, 180, 10),
            'time': np.array([1, 2])})

        with self.assertRaises(ValueError) as err:
            coregister(ds_fine, ds_coarse_err)
        self.assertIn('not pixel-registered', str(err.exception))

        # Test unexpected dimensionality
        ds_fine = xr.Dataset({
            'first': (['lat', 'longertude'], np.eye(4, 8)),
            'second': (['lat', 'longertude'], np.eye(4, 8)),
            'lat': np.linspace(-67.5, 67.5, 4),
            'longertude': np.linspace(-157.5, 157.5, 8)})

        ds_coarse = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.array([np.eye(3, 6), np.eye(3, 6)])),
            'second': (['time', 'lat', 'lon'], np.array([np.eye(3, 6), np.eye(3, 6)])),
            'lat': np.linspace(-60, 60, 3),
            'lon': np.linspace(-150, 150, 6),
            'time': np.array([1, 2])})

        with self.assertRaises(ValueError) as err:
            coregister(ds_fine, ds_coarse)
        self.assertIn('longertude', str(err.exception))

        ds_fine = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.array([np.eye(4, 8), np.eye(4, 8)])),
            'second': (['time', 'lat', 'lon'], np.array([np.eye(4, 8), np.eye(4, 8)])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.array([1, 2])})

        ds_coarse = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.array([np.eye(3, 6), np.eye(3, 6)])),
            'second': (['time', 'lon'], np.eye(2, 6)),
            'lat': np.linspace(-60, 60, 3),
            'lon': np.linspace(-150, 150, 6),
            'time': np.array([1, 2])})

        with self.assertRaises(ValueError) as err:
            coregister(ds_fine, ds_coarse)
        self.assertIn('select_var', str(err.exception))

    def test_find_intersection(self):
        """
        Test the _find_intersection method
        """

        # Test =======
        #          =========
        a = np.linspace(0.5, 9.5, 10)
        b = np.linspace(5.5, 14.5, 10)
        result = _find_intersection(a, b, (0, 15))
        self.assertEqual((5, 10), result)

        # Test   =======
        #    =========
        a = np.linspace(0.5, 9.5, 10)
        b = np.linspace(5.5, 14.5, 10)
        result = _find_intersection(b, a, (0, 15))
        self.assertEqual((5, 10), result)

        # Test   =======
        #     ==============
        a = np.linspace(5.5, 14.5, 10)
        b = np.linspace(0.5, 19.5, 20)
        result = _find_intersection(a, b, (0, 20))
        self.assertEqual((5, 15), result)

        # Test ==================
        #          ========
        a = np.linspace(0.5, 19.5, 20)
        b = np.linspace(5.5, 14.5, 10)
        result = _find_intersection(a, b, (0, 20))
        self.assertEqual((5, 15), result)

        # Test ============
        #                    ========
        a = np.linspace(0.5, 9.5, 10)
        b = np.linspace(10.5, 19.5, 10)
        with self.assertRaises(ValueError) as err:
            _find_intersection(a, b, (0, 20))
        self.assertIn('valid intersection', str(err.exception))

        # Test       ============
        #  ========
        a = np.linspace(0.5, 9.5, 10)
        b = np.linspace(10.5, 19.5, 10)
        with self.assertRaises(ValueError) as err:
            _find_intersection(b, a, (0, 20))
        self.assertIn('valid intersection', str(err.exception))

        # Test misaligned origins
        a = np.linspace(0.5, 9.5, 10)
        b = np.linspace(1, 9, 10)
        with self.assertRaises(ValueError) as err:
            _find_intersection(a, b, (0, 10))
        self.assertIn('valid intersection', str(err.exception))

        # Test differing pixel sizes
        a = np.linspace(0.5, 9.5, 10)
        b = np.linspace(5.25, 14.75, 20)
        result = _find_intersection(b, a, (0, 20))
        self.assertEqual((5, 10), result)

    def test_subset(self):
        """
        Test coregistration being run on a subset
        """
        ds_fine = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.array([np.eye(4, 8), np.eye(4, 8)])),
            'second': (['time', 'lat', 'lon'], np.array([np.eye(4, 8), np.eye(4, 8)])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.array([1, 2])})

        ds_coarse = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.array([np.eye(3, 6), np.eye(3, 6)])),
            'second': (['time', 'lat', 'lon'], np.array([np.eye(3, 6), np.eye(3, 6)])),
            'lat': np.linspace(-60, 60, 3),
            'lon': np.linspace(-150, 150, 6),
            'time': np.array([1, 2])})

        lat_slice = slice(-70, 70)
        lon_slice = slice(-40, 40)
        ds_coarse = ds_coarse.sel(lat=lat_slice, lon=lon_slice)

        # Test that the coarse dataset has been resampled onto the grid
        # of the finer dataset.
        ds_coarse_resampled = coregister(ds_fine, ds_coarse)
        assert_array_equal([-67.5, -22.5, 22.5, 67.5], ds_coarse_resampled.lat.values)
        assert_array_equal([-22.5, 22.5],
                           ds_coarse_resampled.lon.values)

        # Check if the geospatial attributes have been correctly set
        self.assertEqual(ds_coarse_resampled.lat.values[0] - 45 * 0.5,
                         ds_coarse_resampled.attrs['geospatial_lat_min'])
        self.assertEqual(ds_coarse_resampled.lat.values[-1] + 45 * 0.5,
                         ds_coarse_resampled.attrs['geospatial_lat_max'])
        self.assertEqual(ds_coarse_resampled.lon.values[0] - 45 * 0.5,
                         ds_coarse_resampled.attrs['geospatial_lon_min'])
        self.assertEqual(ds_coarse_resampled.lon.values[-1] + 45 * 0.5,
                         ds_coarse_resampled.attrs['geospatial_lon_max'])
        self.assertEqual(45.0,
                         ds_coarse_resampled.attrs['geospatial_lat_resolution'])
        self.assertEqual(45.0,
                         ds_coarse_resampled.attrs['geospatial_lon_resolution'])

    def test_recursive(self):
        """
        Test coregistration with more dimensions than lat/lon/time
        """
        slice_fine = np.eye(4, 8)
        slice_coarse = np.eye(3, 6)
        ndarr_fine = np.zeros([2, 2, 2, 4, 8])
        ndarr_coarse = np.zeros([2, 2, 2, 3, 6])
        ndarr_fine_l1 = np.zeros([2, 2, 4, 8])
        ndarr_coarse_l1 = np.zeros([2, 2, 3, 6])
        ndarr_fine_l2 = np.zeros([2, 2, 4, 8])
        ndarr_coarse_l2 = np.zeros([2, 2, 3, 6])
        ndarr_fine[:] = slice_fine
        ndarr_coarse[:] = slice_coarse
        ndarr_fine_l1[:] = slice_fine
        ndarr_coarse_l1[:] = slice_coarse
        ndarr_fine_l2[:] = slice_fine
        ndarr_coarse_l2[:] = slice_coarse

        ds_fine = xr.Dataset({
            'first': (['time', 'layer', 'layer2', 'lat', 'lon'], ndarr_fine),
            'second': (['time', 'layer', 'layer2', 'lat', 'lon'], ndarr_fine),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'layer': np.array([1, 2]),
            'layer2': np.array([1, 2]),
            'time': np.array([1, 2])}).chunk(chunks={'lat': 2, 'lon': 4})

        ds_coarse = xr.Dataset({
            'first': (['time', 'layer', 'layer2', 'lat', 'lon'], ndarr_coarse),
            'second': (['time', 'layer', 'layer2', 'lat', 'lon'], ndarr_coarse),
            'lat': np.linspace(-60, 60, 3),
            'lon': np.linspace(-150, 150, 6),
            'time': np.array([1, 2]),
            'layer': np.array([1, 2]),
            'layer2': np.array([1, 2])}).chunk(chunks={'lat': 3, 'lon': 3})

        # Test that the coarse dataset has been resampled onto the grid
        # of the finer dataset.
        rm = RecordingMonitor()
        ds_coarse_resampled = coregister(ds_fine, ds_coarse, monitor=rm)
        self.assertGreaterEqual(len(rm.records), 3)
        self.assertIn('start', rm.records[0])
        self.assertIn('done', rm.records[-1])
        self.assertIn('progress', rm.records[1])

        slice_exp = np.array(
            [[1., 0.28571429, 0., 0., 0., 0., 0., 0.],
             [0.33333333, 0.57142857, 0.38095238, 0., 0., 0., 0., 0.],
             [0., 0.47619048, 0.52380952, 0.28571429, 0.04761905, 0., 0., 0.],
             [0., 0., 0.42857143, 0.85714286, 0.14285714, 0., 0., 0.]]
        )
        ndarr_fine_exp = np.zeros([2, 2, 2, 4, 8])
        ndarr_fine_exp[:] = slice_exp

        expected = xr.Dataset({
            'first': (['time', 'layer', 'layer2', 'lat', 'lon'], ndarr_fine_exp),
            'second': (['time', 'layer', 'layer2', 'lat', 'lon'], ndarr_fine_exp),
            'layer': np.array([1, 2]),
            'layer2': np.array([1, 2]),
            'time': np.array([1, 2])})
        assert_almost_equal(
            ds_coarse_resampled['first'].values, expected['first'].values
        )

        # Test that the fine dataset has been resampled (aggregated)
        # onto the grid of the coarse dataset.
        ds_fine_resampled = coregister(ds_coarse, ds_fine)

        slice_exp = np.array([[0.625, 0.125, 0., 0., 0., 0.],
                              [0.125, 0.5, 0.125, 0., 0., 0.],
                              [0., 0.125, 0.625, 0., 0., 0.]])
        ndarr_coarse_exp = np.zeros([2, 2, 2, 3, 6])
        ndarr_coarse_exp[:] = slice_exp

        expected = xr.Dataset({
            'first': (['time', 'layer', 'layer2', 'lat', 'lon'], ndarr_coarse_exp),
            'second': (['time', 'layer', 'layer2', 'lat', 'lon'], ndarr_coarse_exp),
            'lat': np.linspace(-60, 60, 3),
            'lon': np.linspace(-150, 150, 6),
            'layer': np.array([1, 2]),
            'layer2': np.array([1, 2]),
            'time': np.array([1, 2])})

        assert_almost_equal(ds_fine_resampled['first'].values, expected['first'].values)

        # Test that coregistering with data arrays with less than all possible
        # dimensions works
        ds_fine = xr.Dataset({
            'first': (['time', 'layer', 'lat', 'lon'], ndarr_fine_l1),
            'second': (['time', 'layer2', 'lat', 'lon'], ndarr_fine_l2),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'layer': np.array([1, 2]),
            'layer2': np.array([1, 2]),
            'time': np.array([1, 2])}).chunk(chunks={'lat': 2, 'lon': 4})

        ds_coarse = xr.Dataset({
            'first': (['time', 'layer', 'lat', 'lon'], ndarr_coarse_l1),
            'second': (['time', 'layer2', 'lat', 'lon'], ndarr_coarse_l2),
            'lat': np.linspace(-60, 60, 3),
            'lon': np.linspace(-150, 150, 6),
            'time': np.array([1, 2]),
            'layer': np.array([1, 2]),
            'layer2': np.array([1, 2])}).chunk(chunks={'lat': 3, 'lon': 3})

        ds_fine_resampled = coregister(ds_coarse, ds_fine)
        ndarr_coarse_exp = np.zeros([2, 2, 3, 6])
        ndarr_coarse_exp[:] = slice_exp

        expected = xr.Dataset({
            'first': (['time', 'layer', 'lat', 'lon'], ndarr_coarse_exp),
            'second': (['time', 'layer2', 'lat', 'lon'], ndarr_coarse_exp),
            'lat': np.linspace(-60, 60, 3),
            'lon': np.linspace(-150, 150, 6),
            'layer': np.array([1, 2]),
            'layer2': np.array([1, 2]),
            'time': np.array([1, 2])})

        assert_almost_equal(ds_fine_resampled['first'].values, expected['first'].values)

    def test_2D(self):
        """
        Test a case where a 2D lat/lon dataset is resampled or used for
        resampling
        """

        # Primary dataset is 2D
        ds_fine = xr.Dataset({
            'first': (['lat', 'lon'], np.eye(4, 8)),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8)}).chunk()

        ds_coarse = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.array([np.eye(3, 6), np.eye(3, 6)])),
            'second': (['time', 'lat', 'lon'], np.array([np.eye(3, 6), np.eye(3, 6)])),
            'lat': np.linspace(-60, 60, 3),
            'lon': np.linspace(-150, 150, 6),
            'time': np.array([1, 2])}).chunk(chunks={'lat': 3, 'lon': 3})

        ds_coarse_resampled = coregister(ds_fine, ds_coarse)

        slice_exp = np.array(
            [[1., 0.28571429, 0., 0., 0., 0., 0., 0.],
             [0.33333333, 0.57142857, 0.38095238, 0., 0., 0., 0., 0.],
             [0., 0.47619048, 0.52380952, 0.28571429, 0.04761905, 0., 0., 0.],
             [0., 0., 0.42857143, 0.85714286, 0.14285714, 0., 0., 0.]]
        )
        exp_arr = np.zeros([2, 4, 8])
        exp_arr[:] = slice_exp

        expected = xr.Dataset({
            'first': (['time', 'lat', 'lon'], exp_arr),
            'second': (['time', 'lat', 'lon'], exp_arr),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.array([1, 2])})

        assert_almost_equal(
            ds_coarse_resampled['first'].values, expected['first'].values
        )

        # replica dataset contains a 2D variable
        ds_coarse = xr.Dataset({
            'first': (['lat', 'lon'], np.eye(3, 6)),
            'second': (['time', 'lat', 'lon'], np.array([np.eye(3, 6), np.eye(3, 6)])),
            'lat': np.linspace(-60, 60, 3),
            'lon': np.linspace(-150, 150, 6),
            'time': np.array([1, 2])}).chunk(chunks={'lat': 3, 'lon': 3})

        ds_coarse_resampled = coregister(ds_fine, ds_coarse)

        assert_almost_equal(ds_coarse_resampled['first'].values, slice_exp)

    def test_int_array(self):
        """
        Test coregistration on integer arrays
        """
        ds_fine = xr.Dataset({
            'first': (['time', 'lat', 'lon'],
                      np.array([np.eye(4, 8), np.eye(4, 8)], dtype='int32')),
            'second': (['time', 'lat', 'lon'],
                       np.array([np.eye(4, 8), np.eye(4, 8,)])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.array([1, 2])}).chunk(chunks={'lat': 2, 'lon': 4})

        ds_coarse = xr.Dataset({
            'first': (['time', 'lat', 'lon'],
                      np.array([np.eye(3, 6), np.eye(3, 6)], dtype='int32')),
            'second': (['time', 'lat', 'lon'],
                       np.array([np.eye(3, 6), np.eye(3, 6)])),
            'lat': np.linspace(-60, 60, 3),
            'lon': np.linspace(-150, 150, 6),
            'time': np.array([1, 2])}).chunk(chunks={'lat': 3, 'lon': 3})

        # Test that the coarse dataset has been resampled onto the grid
        # of the finer dataset.
        ds_coarse_resampled = coregister(ds_fine, ds_coarse, method_us='nearest')

        expected = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.array([[[1, 1, 0, 0, 0, 0, 0, 0],
                                                         [1, 1, 0, 0, 0, 0, 0, 0],
                                                         [0, 0, 1, 0, 0, 0, 0, 0],
                                                         [0, 0, 0, 1, 0, 0, 0, 0]],
                                                        [[1, 1, 0, 0, 0, 0, 0, 0],
                                                         [1, 1, 0, 0, 0, 0, 0, 0],
                                                         [0, 0, 1, 0, 0, 0, 0, 0],
                                                         [0, 0, 0, 1, 0, 0, 0, 0]]])),
            'second': (['time', 'lat', 'lon'], np.array([[[1, 1, 0, 0, 0, 0, 0, 0],
                                                         [1, 1, 0, 0, 0, 0, 0, 0],
                                                         [0, 0, 1, 0, 0, 0, 0, 0],
                                                         [0, 0, 0, 1, 0, 0, 0, 0]],
                                                         [[1, 1, 0, 0, 0, 0, 0, 0],
                                                         [1, 1, 0, 0, 0, 0, 0, 0],
                                                         [0, 0, 1, 0, 0, 0, 0, 0],
                                                         [0, 0, 0, 1, 0, 0, 0, 0]]])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.array([1, 2])})
        assert_almost_equal(
            ds_coarse_resampled['first'].values, expected['first'].values
        )

        # Test that the fine dataset has been resampled (aggregated)
        # onto the grid of the coarse dataset.
        ds_fine_resampled = coregister(ds_coarse, ds_fine, method_ds='mode')
        expected = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.array([[[1, 0, 0, 0, 0, 0],
                                                         [0, 1, 0, 0, 0, 0],
                                                         [0, 0, 1, 0, 0, 0]],

                                                        [[1, 0, 0, 0, 0, 0],
                                                         [0, 1, 0, 0, 0, 0],
                                                         [0, 0, 1, 0, 0, 0]]])),
            'second': (['time', 'lat', 'lon'], np.array([[[1, 0, 0, 0, 0, 0],
                                                         [0, 1, 0, 0, 0, 0],
                                                         [0, 0, 1, 0, 0, 0]],

                                                        [[1, 0, 0, 0, 0, 0],
                                                         [0, 1, 0, 0, 0, 0],
                                                         [0, 0, 1, 0, 0, 0]]])),
            'lat': np.linspace(-60, 60, 3),
            'lon': np.linspace(-150, 150, 6),
            'time': np.array([1, 2])})

        assert_almost_equal(ds_fine_resampled['first'].values, expected['first'].values)

    def test_same_grid(self):
        """
        Test the case when both datasets already have the same geospatial definition
        """
        ds_fine = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.array([np.eye(4, 8), np.eye(4, 8)])),
            'second': (['time', 'lat', 'lon'], np.array([np.eye(4, 8), np.eye(4, 8)])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.array([1, 2])}).chunk(chunks={'lat': 2, 'lon': 4})

        rm = RecordingMonitor()
        ds_same = coregister(ds_fine, ds_fine, monitor=rm)
        # Make sure it returned the input as opposed to going through with
        # coregistration
        self.assertEqual([], rm.records)

        assert_almost_equal(ds_same['first'].values, ds_fine['first'].values)

        # Test that a subset is performed, but no coregistration done
        lat_slice = slice(-70, 70)
        lon_slice = slice(-40, 40)
        ds_subset = ds_fine.sel(lat=lat_slice, lon=lon_slice)

        ds_coreg = coregister(ds_subset, ds_fine, monitor=rm)
        self.assertEqual([], rm.records)
        assert_almost_equal(ds_coreg['first'].values, ds_subset['first'].values)
