"""
Test for the normalisation operation
"""

import calendar
from datetime import datetime
from unittest import TestCase

import cftime
import numpy as np
import pandas as pd
import xarray as xr
from jdcal import gcal2jd
from numpy.testing import assert_array_almost_equal

from esa_climate_toolbox.core.op import OP_REGISTRY
from esa_climate_toolbox.ops.normalize import adjust_spatial_attrs
from esa_climate_toolbox.ops.normalize import adjust_temporal_attrs
from esa_climate_toolbox.ops.normalize import normalize
from esa_climate_toolbox.util.misc import object_to_qualified_name

from xcube.core.normalize import normalize_coord_vars
from xcube.core.normalize import normalize_missing_time


# noinspection PyPep8Naming
def assertDatasetEqual(expected, actual):
    # this method is functionally equivalent to
    # `assert expected == actual`, but it
    # checks each aspect of equality separately for easier debugging
    assert expected.equals(actual), (expected, actual)


class TestNormalize(TestCase):

    def test_normalize_lon_lat_2d(self):
        """
        Test nominal execution
        """
        dims = ('time', 'y', 'x')
        attributes = {'valid_min': 0., 'valid_max': 1.}

        t_size = 2
        y_size = 3
        x_size = 4

        a_data = np.random.random_sample((t_size, y_size, x_size))
        b_data = np.random.random_sample((t_size, y_size, x_size))
        time_data = [1, 2]
        lat_data = [[10., 10., 10., 10.],
                    [20., 20., 20., 20.],
                    [30., 30., 30., 30.]]
        lon_data = [[-10., 0., 10., 20.],
                    [-10., 0., 10., 20.],
                    [-10., 0., 10., 20.]]
        dataset = xr.Dataset({'a': (dims, a_data, attributes),
                              'b': (dims, b_data, attributes)
                              },
                             {'time': (('time',), time_data),
                              'lat': (('y', 'x'), lat_data),
                              'lon': (('y', 'x'), lon_data)
                              },
                             {'geospatial_lon_min': -15.,
                              'geospatial_lon_max': 25.,
                              'geospatial_lat_min': 5.,
                              'geospatial_lat_max': 35.
                              }
                             )

        new_dims = ('time', 'lat', 'lon')
        expected = xr.Dataset({'a': (new_dims, a_data, attributes),
                               'b': (new_dims, b_data, attributes)},
                              {'time': (('time',), time_data),
                               'lat': (('lat',), [10., 20., 30.]),
                               'lon': (('lon',), [-10., 0., 10., 20.]),
                               },
                              {'geospatial_lon_min': -15.,
                               'geospatial_lon_max': 25.,
                               'geospatial_lat_min': 5.,
                               'geospatial_lat_max': 35.})

        actual = normalize(dataset)
        xr.testing.assert_equal(actual, expected)

    def test_normalize_lon_lat(self):
        """
        Test nominal execution
        """
        dataset = xr.Dataset({'first': (['latitude',
                                         'longitude'], [[1, 2, 3],
                                                        [2, 3, 4]])})
        expected = xr.Dataset({'first': (['lat', 'lon'], [[1, 2, 3],
                                                          [2, 3, 4]])})
        actual = normalize(dataset)
        assertDatasetEqual(actual, expected)

        dataset = xr.Dataset({'first': (['lat', 'long'], [[1, 2, 3],
                                                          [2, 3, 4]])})
        expected = xr.Dataset({'first': (['lat', 'lon'], [[1, 2, 3],
                                                          [2, 3, 4]])})
        actual = normalize(dataset)
        assertDatasetEqual(actual, expected)

        dataset = xr.Dataset({'first': (['latitude',
                                         'spacetime'], [[1, 2, 3],
                                                        [2, 3, 4]])})
        expected = xr.Dataset({'first': (['lat', 'spacetime'], [[1, 2, 3],
                                                                [2, 3, 4]])})
        actual = normalize(dataset)
        assertDatasetEqual(actual, expected)

        dataset = xr.Dataset({'first': (['zef', 'spacetime'], [[1, 2, 3],
                                                               [2, 3, 4]])})
        expected = xr.Dataset({'first': (['zef', 'spacetime'], [[1, 2, 3],
                                                                [2, 3, 4]])})
        actual = normalize(dataset)
        assertDatasetEqual(actual, expected)

    def test_normalize_inverted_lat(self):
        first = np.zeros([3, 45, 90])
        first[0, :, :] = np.eye(45, 90)
        ds = xr.Dataset({
            'first': (['time', 'lat', 'lon'], first),
            'second': (['time', 'lat', 'lon'], np.zeros([3, 45, 90])),
            'lat': np.linspace(88, -88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1, 4)]
        }).chunk(chunks={'time': 1})

        first = np.zeros([3, 45, 90])
        first[0, :, :] = np.flip(np.eye(45, 90), axis=0)
        expected = xr.Dataset({
            'first': (['time', 'lat', 'lon'], first),
            'second': (['time', 'lat', 'lon'], np.zeros([3, 45, 90])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1, 4)]
        }).chunk(chunks={'time': 1})

        actual = normalize(ds)
        xr.testing.assert_equal(actual, expected)

    def test_normalize_with_missing_time_dim(self):
        ds = xr.Dataset({'first': (['lat', 'lon'], np.zeros([90, 180])),
                         'second': (['lat', 'lon'], np.zeros([90, 180]))},
                        coords={'lat': np.linspace(-89.5, 89.5, 90),
                                'lon': np.linspace(-179.5, 179.5, 180)},
                        attrs={'time_coverage_start': '20120101',
                               'time_coverage_end': '20121231'})
        norm_ds = normalize(ds)
        self.assertIsNot(norm_ds, ds)
        self.assertEqual(len(norm_ds.coords), 4)
        self.assertIn('lon', norm_ds.coords)
        self.assertIn('lat', norm_ds.coords)
        self.assertIn('time', norm_ds.coords)
        self.assertIn('time_bnds', norm_ds.coords)

        self.assertEqual(norm_ds.first.shape, (1, 90, 180))
        self.assertEqual(norm_ds.second.shape, (1, 90, 180))
        self.assertEqual(norm_ds.coords['time'][0],
                         xr.DataArray(pd.to_datetime('2012-07-01T12:00:00')))
        self.assertEqual(norm_ds.coords['time_bnds'][0][0],
                         xr.DataArray(pd.to_datetime('2012-01-01')))
        self.assertEqual(norm_ds.coords['time_bnds'][0][1],
                         xr.DataArray(pd.to_datetime('2012-12-31')))

    def test_normalize_with_time_called_t(self):
        ds = xr.Dataset(
            {'first': (['time', 'lat', 'lon'], np.zeros([4, 90, 180])),
             'second': (['time', 'lat', 'lon'], np.zeros([4, 90, 180])),
             't': ('time', np.array(['2005-07-02T00:00:00.000000000',
                                     '2006-07-02T12:00:00.000000000',
                                     '2007-07-03T00:00:00.000000000',
                                     '2008-07-02T00:00:00.000000000'],
                                    dtype='datetime64[ns]'))
             },
            coords={'lat': np.linspace(-89.5, 89.5, 90),
                    'lon': np.linspace(-179.5, 179.5, 180)},
            attrs={'time_coverage_start': '2005-01-17',
                   'time_coverage_end': '2008-08-17'}
        )
        norm_ds = normalize(ds)
        self.assertIsNot(norm_ds, ds)
        self.assertEqual(len(norm_ds.coords), 3)
        self.assertIn('lon', norm_ds.coords)
        self.assertIn('lat', norm_ds.coords)
        self.assertIn('time', norm_ds.coords)

        self.assertEqual(norm_ds.first.shape, (4, 90, 180))
        self.assertEqual(norm_ds.second.shape, (4, 90, 180))
        self.assertEqual(norm_ds.coords['time'][0],
                         xr.DataArray(pd.to_datetime('2005-07-02T00:00')))

    def test_normalize_julian_day(self):
        """
        Test Julian Day -> Datetime conversion
        """
        tuples = [gcal2jd(2000, x, 1) for x in range(1, 13)]

        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [x[0] + x[1] for x in tuples]})
        ds.time.attrs['long_name'] = 'time in julian days'

        expected = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.zeros([12, 45, 90])),
            'second': (['time', 'lat', 'lon'], np.zeros([12, 45, 90])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1, 13)]})
        expected.time.attrs['long_name'] = 'time'

        actual = normalize(ds)

        assertDatasetEqual(actual, expected)

    def test_registered(self):
        """
        Test as a registered operation
        """
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(normalize))
        dataset = xr.Dataset({'first': (['latitude',
                                         'longitude'], [[1, 2, 3],
                                                        [2, 3, 4]])})
        expected = xr.Dataset({'first': (['lat', 'lon'], [[1, 2, 3],
                                                          [2, 3, 4]])})
        actual = reg_op(ds=dataset)
        assertDatasetEqual(actual, expected)


class TestAdjustSpatial(TestCase):
    def test_nominal(self):
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1, 13)]})

        ds.lon.attrs['units'] = 'degrees_east'
        ds.lat.attrs['units'] = 'degrees_north'

        ds1 = adjust_spatial_attrs(ds)

        # Make sure original dataset is not altered
        with self.assertRaises(KeyError):
            # noinspection PyStatementEffect
            ds.attrs['geospatial_lat_min']

        # Make sure expected values are in the new dataset
        self.assertEqual(ds1.attrs['geospatial_lat_min'], -90)
        self.assertEqual(ds1.attrs['geospatial_lat_max'], 90)
        self.assertEqual(ds1.attrs['geospatial_lat_units'], 'degrees_north')
        self.assertEqual(ds1.attrs['geospatial_lat_resolution'], 4)
        self.assertEqual(ds1.attrs['geospatial_lon_min'], -180)
        self.assertEqual(ds1.attrs['geospatial_lon_max'], 180)
        self.assertEqual(ds1.attrs['geospatial_lon_units'], 'degrees_east')
        self.assertEqual(ds1.attrs['geospatial_lon_resolution'], 4)
        self.assertEqual(ds1.attrs['geospatial_bounds'],
                         'POLYGON((-180.0 -90.0, -180.0 90.0, 180.0 90.0,'
                         ' 180.0 -90.0, -180.0 -90.0))')

        # Test existing attributes update
        lon_min, lat_min, lon_max, lat_max = -20, -40, 60, 40
        indexers = {'lon': slice(lon_min, lon_max),
                    'lat': slice(lat_min, lat_max)}
        ds2 = ds1.sel(**indexers)
        ds2 = adjust_spatial_attrs(ds2)

        self.assertEqual(ds2.attrs['geospatial_lat_min'], -42)
        self.assertEqual(ds2.attrs['geospatial_lat_max'], 42)
        self.assertEqual(ds2.attrs['geospatial_lat_units'], 'degrees_north')
        self.assertEqual(ds2.attrs['geospatial_lat_resolution'], 4)
        self.assertEqual(ds2.attrs['geospatial_lon_min'], -20)
        self.assertEqual(ds2.attrs['geospatial_lon_max'], 60)
        self.assertEqual(ds2.attrs['geospatial_lon_units'], 'degrees_east')
        self.assertEqual(ds2.attrs['geospatial_lon_resolution'], 4)
        self.assertEqual(ds2.attrs['geospatial_bounds'],
                         'POLYGON((-20.0 -42.0, -20.0 42.0, 60.0 42.0, 60.0'
                         ' -42.0, -20.0 -42.0))')

    def test_nominal_inverted(self):
        # Inverted lat
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'lat': np.linspace(88, -88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1, 13)]})

        ds.lon.attrs['units'] = 'degrees_east'
        ds.lat.attrs['units'] = 'degrees_north'

        ds1 = adjust_spatial_attrs(ds)

        # Make sure original dataset is not altered
        with self.assertRaises(KeyError):
            # noinspection PyStatementEffect
            ds.attrs['geospatial_lat_min']

        # Make sure expected values are in the new dataset
        self.assertEqual(ds1.attrs['geospatial_lat_min'], -90)
        self.assertEqual(ds1.attrs['geospatial_lat_max'], 90)
        self.assertEqual(ds1.attrs['geospatial_lat_units'], 'degrees_north')
        self.assertEqual(ds1.attrs['geospatial_lat_resolution'], 4)
        self.assertEqual(ds1.attrs['geospatial_lon_min'], -180)
        self.assertEqual(ds1.attrs['geospatial_lon_max'], 180)
        self.assertEqual(ds1.attrs['geospatial_lon_units'], 'degrees_east')
        self.assertEqual(ds1.attrs['geospatial_lon_resolution'], 4)
        self.assertEqual(ds1.attrs['geospatial_bounds'],
                         'POLYGON((-180.0 -90.0, -180.0 90.0, 180.0 90.0,'
                         ' 180.0 -90.0, -180.0 -90.0))')

        # Test existing attributes update
        lon_min, lat_min, lon_max, lat_max = -20, -40, 60, 40
        indexers = {'lon': slice(lon_min, lon_max),
                    'lat': slice(lat_max, lat_min)}
        ds2 = ds1.sel(**indexers)
        ds2 = adjust_spatial_attrs(ds2)

        self.assertEqual(ds2.attrs['geospatial_lat_min'], -42)
        self.assertEqual(ds2.attrs['geospatial_lat_max'], 42)
        self.assertEqual(ds2.attrs['geospatial_lat_units'], 'degrees_north')
        self.assertEqual(ds2.attrs['geospatial_lat_resolution'], 4)
        self.assertEqual(ds2.attrs['geospatial_lon_min'], -20)
        self.assertEqual(ds2.attrs['geospatial_lon_max'], 60)
        self.assertEqual(ds2.attrs['geospatial_lon_units'], 'degrees_east')
        self.assertEqual(ds2.attrs['geospatial_lon_resolution'], 4)
        self.assertEqual(ds2.attrs['geospatial_bounds'],
                         'POLYGON((-20.0 -42.0, -20.0 42.0, 60.0 42.0, 60.0'
                         ' -42.0, -20.0 -42.0))')

    def test_bnds(self):
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1, 13)]})

        ds.lon.attrs['units'] = 'degrees_east'
        ds.lat.attrs['units'] = 'degrees_north'

        lat_bnds = np.empty([len(ds.lat), 2])
        lon_bnds = np.empty([len(ds.lon), 2])
        ds['nv'] = [0, 1]

        lat_bnds[:, 0] = ds.lat.values - 2
        lat_bnds[:, 1] = ds.lat.values + 2
        lon_bnds[:, 0] = ds.lon.values - 2
        lon_bnds[:, 1] = ds.lon.values + 2

        ds['lat_bnds'] = (['lat', 'nv'], lat_bnds)
        ds['lon_bnds'] = (['lon', 'nv'], lon_bnds)

        ds.lat.attrs['bounds'] = 'lat_bnds'
        ds.lon.attrs['bounds'] = 'lon_bnds'

        ds1 = adjust_spatial_attrs(ds)

        # Make sure original dataset is not altered
        with self.assertRaises(KeyError):
            # noinspection PyStatementEffect
            ds.attrs['geospatial_lat_min']

        # Make sure expected values are in the new dataset
        self.assertEqual(ds1.attrs['geospatial_lat_min'], -90)
        self.assertEqual(ds1.attrs['geospatial_lat_max'], 90)
        self.assertEqual(ds1.attrs['geospatial_lat_units'], 'degrees_north')
        self.assertEqual(ds1.attrs['geospatial_lat_resolution'], 4)
        self.assertEqual(ds1.attrs['geospatial_lon_min'], -180)
        self.assertEqual(ds1.attrs['geospatial_lon_max'], 180)
        self.assertEqual(ds1.attrs['geospatial_lon_units'], 'degrees_east')
        self.assertEqual(ds1.attrs['geospatial_lon_resolution'], 4)
        self.assertEqual(ds1.attrs['geospatial_bounds'],
                         'POLYGON((-180.0 -90.0, -180.0 90.0, 180.0 90.0,'
                         ' 180.0 -90.0, -180.0 -90.0))')

        # Test existing attributes update
        lon_min, lat_min, lon_max, lat_max = -20, -40, 60, 40
        indexers = {'lon': slice(lon_min, lon_max),
                    'lat': slice(lat_min, lat_max)}
        ds2 = ds1.sel(**indexers)
        ds2 = adjust_spatial_attrs(ds2)

        self.assertEqual(ds2.attrs['geospatial_lat_min'], -42)
        self.assertEqual(ds2.attrs['geospatial_lat_max'], 42)
        self.assertEqual(ds2.attrs['geospatial_lat_units'], 'degrees_north')
        self.assertEqual(ds2.attrs['geospatial_lat_resolution'], 4)
        self.assertEqual(ds2.attrs['geospatial_lon_min'], -20)
        self.assertEqual(ds2.attrs['geospatial_lon_max'], 60)
        self.assertEqual(ds2.attrs['geospatial_lon_units'], 'degrees_east')
        self.assertEqual(ds2.attrs['geospatial_lon_resolution'], 4)
        self.assertEqual(ds2.attrs['geospatial_bounds'],
                         'POLYGON((-20.0 -42.0, -20.0 42.0, 60.0 42.0, 60.0'
                         ' -42.0, -20.0 -42.0))')

    def test_bnds_inverted(self):
        # Inverted lat
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'lat': np.linspace(88, -88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1, 13)]})

        ds.lon.attrs['units'] = 'degrees_east'
        ds.lat.attrs['units'] = 'degrees_north'

        lat_bnds = np.empty([len(ds.lat), 2])
        lon_bnds = np.empty([len(ds.lon), 2])
        ds['nv'] = [0, 1]

        lat_bnds[:, 0] = ds.lat.values + 2
        lat_bnds[:, 1] = ds.lat.values - 2
        lon_bnds[:, 0] = ds.lon.values - 2
        lon_bnds[:, 1] = ds.lon.values + 2

        ds['lat_bnds'] = (['lat', 'nv'], lat_bnds)
        ds['lon_bnds'] = (['lon', 'nv'], lon_bnds)

        ds.lat.attrs['bounds'] = 'lat_bnds'
        ds.lon.attrs['bounds'] = 'lon_bnds'

        ds1 = adjust_spatial_attrs(ds)

        # Make sure original dataset is not altered
        with self.assertRaises(KeyError):
            # noinspection PyStatementEffect
            ds.attrs['geospatial_lat_min']

        # Make sure expected values are in the new dataset
        self.assertEqual(ds1.attrs['geospatial_lat_min'], -90)
        self.assertEqual(ds1.attrs['geospatial_lat_max'], 90)
        self.assertEqual(ds1.attrs['geospatial_lat_units'], 'degrees_north')
        self.assertEqual(ds1.attrs['geospatial_lat_resolution'], 4)
        self.assertEqual(ds1.attrs['geospatial_lon_min'], -180)
        self.assertEqual(ds1.attrs['geospatial_lon_max'], 180)
        self.assertEqual(ds1.attrs['geospatial_lon_units'], 'degrees_east')
        self.assertEqual(ds1.attrs['geospatial_lon_resolution'], 4)
        self.assertEqual(ds1.attrs['geospatial_bounds'],
                         'POLYGON((-180.0 -90.0, -180.0 90.0, 180.0 90.0,'
                         ' 180.0 -90.0, -180.0 -90.0))')

        # Test existing attributes update
        lon_min, lat_min, lon_max, lat_max = -20, -40, 60, 40
        indexers = {'lon': slice(lon_min, lon_max),
                    'lat': slice(lat_max, lat_min)}
        ds2 = ds1.sel(**indexers)
        ds2 = adjust_spatial_attrs(ds2)

        self.assertEqual(ds2.attrs['geospatial_lat_min'], -42)
        self.assertEqual(ds2.attrs['geospatial_lat_max'], 42)
        self.assertEqual(ds2.attrs['geospatial_lat_units'], 'degrees_north')
        self.assertEqual(ds2.attrs['geospatial_lat_resolution'], 4)
        self.assertEqual(ds2.attrs['geospatial_lon_min'], -20)
        self.assertEqual(ds2.attrs['geospatial_lon_max'], 60)
        self.assertEqual(ds2.attrs['geospatial_lon_units'], 'degrees_east')
        self.assertEqual(ds2.attrs['geospatial_lon_resolution'], 4)
        self.assertEqual(
            ds2.attrs['geospatial_bounds'],
            'POLYGON((-20.0 -42.0, -20.0 42.0, 60.0 42.0, '
            '60.0 -42.0, -20.0 -42.0))'
        )

    def test_once_cell_with_bnds(self):
        # Only one cell in lat/lon
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([1, 1, 12])),
            'second': (['lat', 'lon', 'time'], np.zeros([1, 1, 12])),
            'lat': np.array([52.5]),
            'lon': np.array([11.5]),
            'lat_bnds': (['lat', 'bnds'], np.array([[52.4, 52.6]])),
            'lon_bnds': (['lon', 'bnds'], np.array([[11.4, 11.6]])),
            'time': [datetime(2000, x, 1) for x in range(1, 13)]})
        ds.lon.attrs['units'] = 'degrees_east'
        ds.lat.attrs['units'] = 'degrees_north'

        ds1 = adjust_spatial_attrs(ds)
        self.assertAlmostEqual(ds1.attrs['geospatial_lat_resolution'], 0.2)
        self.assertAlmostEqual(ds1.attrs['geospatial_lat_min'], 52.4)
        self.assertAlmostEqual(ds1.attrs['geospatial_lat_max'], 52.6)
        self.assertEqual(ds1.attrs['geospatial_lat_units'], 'degrees_north')
        self.assertAlmostEqual(ds1.attrs['geospatial_lon_resolution'], 0.2)
        self.assertAlmostEqual(ds1.attrs['geospatial_lon_min'], 11.4)
        self.assertAlmostEqual(ds1.attrs['geospatial_lon_max'], 11.6)
        self.assertEqual(ds1.attrs['geospatial_lon_units'], 'degrees_east')
        self.assertEqual(ds1.attrs['geospatial_bounds'],
                         'POLYGON((11.4 52.4, 11.4 52.6, 11.6 52.6, '
                         '11.6 52.4, 11.4 52.4))')

    def test_once_cell_without_bnds(self):
        # Only one cell in lat/lon
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([1, 1, 12])),
            'second': (['lat', 'lon', 'time'], np.zeros([1, 1, 12])),
            'lat': np.array([52.5]),
            'lon': np.array([11.5]),
            'time': [datetime(2000, x, 1) for x in range(1, 13)]})
        ds.lon.attrs['units'] = 'degrees_east'
        ds.lat.attrs['units'] = 'degrees_north'

        ds2 = adjust_spatial_attrs(ds)
        # Datasets should be the same --> not modified
        self.assertIs(ds2, ds)


class AdjustTemporalAttrsTest(TestCase):
    def test_nominal(self):

        time_datas = [
            [datetime(2000, x, 1) for x in range(1, 13)],
            [np.datetime64(datetime(2000, x, 1)) for x in range(1, 13)],
            [cftime.DatetimeGregorian(2000, x, 1) for x in range(1, 13)],
        ]

        for time_data in time_datas:
            print(time_data)
            ds = xr.Dataset({
                'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
                'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
                'lat': np.linspace(-88, 88, 45),
                'lon': np.linspace(-178, 178, 90),
                'time': time_data
            })

            ds1 = adjust_temporal_attrs(ds)

            # Make sure original dataset is not altered
            with self.assertRaises(KeyError):
                # noinspection PyStatementEffect
                ds.attrs['time_coverage_start']

            # Make sure expected values are in the new dataset
            self.assertEqual(ds1.attrs['time_coverage_start'],
                             '2000-01-01T00:00:00')
            self.assertEqual(ds1.attrs['time_coverage_end'],
                             '2000-12-01T00:00:00')
            self.assertEqual(ds1.attrs['time_coverage_resolution'],
                             'P1M')
            self.assertEqual(ds1.attrs['time_coverage_duration'],
                             'P336D')

            # Test existing attributes update
            # noinspection PyTypeChecker
            indexers = {
                'time': slice(datetime(2000, 2, 15), datetime(2000, 6, 15))
            }
            ds2 = ds1.sel(**indexers)
            ds2 = adjust_temporal_attrs(ds2)

            self.assertEqual(ds2.attrs['time_coverage_start'],
                             '2000-03-01T00:00:00')
            self.assertEqual(ds2.attrs['time_coverage_end'],
                             '2000-06-01T00:00:00')
            self.assertEqual(ds2.attrs['time_coverage_resolution'],
                             'P1M')
            self.assertEqual(ds2.attrs['time_coverage_duration'],
                             'P93D')

    def test_wrong_type(self):
        ds = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.zeros([12, 45, 90])),
            'second': (['time', 'lat', 'lon'], np.zeros([12, 45, 90])),
            'lon': (['lon'], np.linspace(-178, 178, 90)),
            'lat': (['lat'], np.linspace(-88, 88, 45)),
            'time': (['time'], np.linspace(0, 1, 12))})

        ds1 = adjust_temporal_attrs(ds)

        self.assertIs(ds1, ds)
        self.assertNotIn('time_coverage_start', ds1)
        self.assertNotIn('time_coverage_end', ds1)
        self.assertNotIn('time_coverage_resolution', ds1)
        self.assertNotIn('time_coverage_duration', ds1)

    def test_bnds(self):
        """Test a case when time_bnds is available"""
        time = [datetime(2000, x, 1) for x in range(1, 13)]
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'nv': [0, 1],
            'time': time})

        month_ends = list()
        for x in ds.time.values:
            year = int(str(x)[0:4])
            month = int(str(x)[5:7])
            day = calendar.monthrange(year, month)[1]
            month_ends.append(datetime(year, month, day))

        ds['time_bnds'] = (['time', 'nv'], list(zip(time, month_ends)))
        ds.time.attrs['bounds'] = 'time_bnds'

        ds1 = adjust_temporal_attrs(ds)

        # Make sure original dataset is not altered
        with self.assertRaises(KeyError):
            # noinspection PyStatementEffect
            ds.attrs['time_coverage_start']

        # Make sure expected values are in the new dataset
        self.assertEqual(ds1.attrs['time_coverage_start'],
                         '2000-01-01T00:00:00')
        self.assertEqual(ds1.attrs['time_coverage_end'],
                         '2000-12-31T00:00:00')
        self.assertEqual(ds1.attrs['time_coverage_resolution'],
                         'P1M')
        self.assertEqual(ds1.attrs['time_coverage_duration'],
                         'P366D')

    def test_single_slice(self):
        """Test a case when the dataset is a single time slice"""
        # With bnds
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 1])),
            'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 1])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'nv': [0, 1],
            'time': [datetime(2000, 1, 1)]})
        ds.time.attrs['bounds'] = 'time_bnds'
        ds['time_bnds'] = (['time', 'nv'],
                           [(datetime(2000, 1, 1), datetime(2000, 1, 31))])

        ds1 = adjust_temporal_attrs(ds)

        # Make sure original dataset is not altered
        with self.assertRaises(KeyError):
            # noinspection PyStatementEffect
            ds.attrs['time_coverage_start']

        # Make sure expected values are in the new dataset
        self.assertEqual(ds1.attrs['time_coverage_start'],
                         '2000-01-01T00:00:00')
        self.assertEqual(ds1.attrs['time_coverage_end'],
                         '2000-01-31T00:00:00')
        self.assertEqual(ds1.attrs['time_coverage_duration'],
                         'P31D')
        with self.assertRaises(KeyError):
            # Resolution is not defined for a single slice
            # noinspection PyStatementEffect
            ds.attrs['time_coverage_resolution']

        # Without bnds
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 1])),
            'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 1])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, 1, 1)]})

        ds1 = adjust_temporal_attrs(ds)

        self.assertEqual(ds1.attrs['time_coverage_start'],
                         '2000-01-01T00:00:00')
        self.assertEqual(ds1.attrs['time_coverage_end'],
                         '2000-01-01T00:00:00')
        with self.assertRaises(KeyError):
            # noinspection PyStatementEffect
            ds.attrs['time_coverage_resolution']
        with self.assertRaises(KeyError):
            # noinspection PyStatementEffect
            ds.attrs['time_coverage_duration']


class TestNormalizeCoordVars(TestCase):

    def test_ds_with_potential_coords(self):
        ds = xr.Dataset({'first': (['lat', 'lon'], np.zeros([90, 180])),
                         'second': (['lat', 'lon'], np.zeros([90, 180])),
                         'lat_bnds': (['lat', 'bnds'], np.zeros([90, 2])),
                         'lon_bnds': (['lon', 'bnds'], np.zeros([180, 2]))},
                        coords={'lat': np.linspace(-89.5, 89.5, 90),
                                'lon': np.linspace(-179.5, 179.5, 180)})

        new_ds = normalize_coord_vars(ds)

        self.assertIsNot(ds, new_ds)
        self.assertEqual(len(new_ds.coords), 4)
        self.assertIn('lon', new_ds.coords)
        self.assertIn('lat', new_ds.coords)
        self.assertIn('lat_bnds', new_ds.coords)
        self.assertIn('lon_bnds', new_ds.coords)

        self.assertEqual(len(new_ds.data_vars), 2)
        self.assertIn('first', new_ds.data_vars)
        self.assertIn('second', new_ds.data_vars)

    def test_ds_with_potential_coords_and_bounds(self):
        ds = xr.Dataset({'first': (['lat', 'lon'], np.zeros([90, 180])),
                         'second': (['lat', 'lon'], np.zeros([90, 180])),
                         'lat_bnds': (['lat', 'bnds'], np.zeros([90, 2])),
                         'lon_bnds': (['lon', 'bnds'], np.zeros([180, 2])),
                         'lat': (['lat'], np.linspace(-89.5, 89.5, 90)),
                         'lon': (['lon'], np.linspace(-179.5, 179.5, 180))})

        new_ds = normalize_coord_vars(ds)

        self.assertIsNot(ds, new_ds)
        self.assertEqual(len(new_ds.coords), 4)
        self.assertIn('lon', new_ds.coords)
        self.assertIn('lat', new_ds.coords)
        self.assertIn('lat_bnds', new_ds.coords)
        self.assertIn('lon_bnds', new_ds.coords)

        self.assertEqual(len(new_ds.data_vars), 2)
        self.assertIn('first', new_ds.data_vars)
        self.assertIn('second', new_ds.data_vars)

    def test_ds_with_no_potential_coords(self):
        ds = xr.Dataset({'first': (['lat', 'lon'], np.zeros([90, 180])),
                         'second': (['lat', 'lon'], np.zeros([90, 180]))},
                        coords={'lat': np.linspace(-89.5, 89.5, 90),
                                'lon': np.linspace(-179.5, 179.5, 180)},
                        attrs={'time_coverage_start': '20120101'})
        new_ds = normalize_coord_vars(ds)
        self.assertIs(ds, new_ds)


class TestNormalizeMissingTime(TestCase):
    def test_ds_without_time(self):
        ds = xr.Dataset({'first': (['lat', 'lon'], np.zeros([90, 180])),
                         'second': (['lat', 'lon'], np.zeros([90, 180]))},
                        coords={'lat': np.linspace(-89.5, 89.5, 90),
                                'lon': np.linspace(-179.5, 179.5, 180)},
                        attrs={'time_coverage_start': '20120101',
                               'time_coverage_end': '20121231'})

        new_ds = normalize_missing_time(ds)

        self.assertIsNot(ds, new_ds)
        self.assertEqual(len(new_ds.coords), 4)
        self.assertIn('lon', new_ds.coords)
        self.assertIn('lat', new_ds.coords)
        self.assertIn('time', new_ds.coords)
        self.assertIn('time_bnds', new_ds.coords)

        self.assertEqual(new_ds.coords['time'].attrs.get('long_name'), 'time')
        self.assertEqual(new_ds.coords['time'].attrs.get('bounds'), 'time_bnds')

        self.assertEqual(new_ds.first.shape, (1, 90, 180))
        self.assertEqual(new_ds.second.shape, (1, 90, 180))
        self.assertEqual(new_ds.coords['time'][0],
                         xr.DataArray(pd.to_datetime('2012-07-01T12:00:00')))
        self.assertEqual(new_ds.coords['time'].attrs.get('long_name'), 'time')
        self.assertEqual(new_ds.coords['time'].attrs.get('bounds'), 'time_bnds')
        self.assertEqual(new_ds.coords['time_bnds'][0][0],
                         xr.DataArray(pd.to_datetime('2012-01-01')))
        self.assertEqual(new_ds.coords['time_bnds'][0][1],
                         xr.DataArray(pd.to_datetime('2012-12-31')))
        self.assertEqual(
            new_ds.coords['time_bnds'].attrs.get('long_name'), 'time'
        )

    def test_ds_without_bounds(self):
        ds = xr.Dataset({'first': (['lat', 'lon'], np.zeros([90, 180])),
                         'second': (['lat', 'lon'], np.zeros([90, 180]))},
                        coords={'lat': np.linspace(-89.5, 89.5, 90),
                                'lon': np.linspace(-179.5, 179.5, 180)},
                        attrs={'time_coverage_start': '20120101'})

        new_ds = normalize_missing_time(ds)

        self.assertIsNot(ds, new_ds)
        self.assertEqual(len(new_ds.coords), 3)
        self.assertIn('lon', new_ds.coords)
        self.assertIn('lat', new_ds.coords)
        self.assertIn('time', new_ds.coords)
        self.assertNotIn('time_bnds', new_ds.coords)

        self.assertEqual(new_ds.first.shape, (1, 90, 180))
        self.assertEqual(new_ds.second.shape, (1, 90, 180))
        self.assertEqual(
            new_ds.coords['time'][0], xr.DataArray(pd.to_datetime('2012-01-01'))
        )
        self.assertEqual(new_ds.coords['time'].attrs.get('long_name'), 'time')
        self.assertEqual(new_ds.coords['time'].attrs.get('bounds'), None)

    def test_ds_without_time_attrs(self):
        ds = xr.Dataset({'first': (['lat', 'lon'], np.zeros([90, 180])),
                         'second': (['lat', 'lon'], np.zeros([90, 180]))},
                        coords={'lat': np.linspace(-89.5, 89.5, 90),
                                'lon': np.linspace(-179.5, 179.5, 180)})

        new_ds = normalize_missing_time(ds)
        self.assertIs(ds, new_ds)

    def test_ds_with_cftime(self):
        time_data = xr.cftime_range(start='2010-01-01T00:00:00',
                                    periods=6,
                                    freq='D',
                                    calendar='gregorian').values
        ds = xr.Dataset(
            {'first': (['time', 'lat', 'lon'], np.zeros([6, 90, 180])),
             'second': (['time', 'lat', 'lon'], np.zeros([6, 90, 180]))},
            coords={'lat': np.linspace(-89.5, 89.5, 90),
                    'lon': np.linspace(-179.5, 179.5, 180),
                    'time': time_data},
            attrs={'time_coverage_start': '20120101',
                   'time_coverage_end': '20121231'}
        )
        new_ds = normalize_missing_time(ds)
        self.assertIs(ds, new_ds)


class Fix360Test(TestCase):

    def test_fix_360_lon(self):
        # The following simulates a strangely geo-coded soil moisture dataset
        # we found at ...
        #
        lon_size = 360
        lat_size = 130
        time_size = 12
        ds = xr.Dataset({
            'first': (['time', 'lat', 'lon'],
                      np.random.random_sample([time_size, lat_size, lon_size])),
            'second': (['time', 'lat', 'lon'],
                       np.random.random_sample([time_size, lat_size, lon_size]))
        },
            coords={'lon': np.linspace(1., 360., lon_size),
                    'lat': np.linspace(-64., 65., lat_size),
                    'time':
                        [datetime(2000, x, 1) for x in range(1, time_size + 1)]
                    },
            attrs=dict(geospatial_lon_min=0.,
                       geospatial_lon_max=360.,
                       geospatial_lat_min=-64.5,
                       geospatial_lat_max=+65.5,
                       geospatial_lon_resolution=1.,
                       geospatial_lat_resolution=1.))

        new_ds = normalize(ds)
        self.assertIsNot(ds, new_ds)
        self.assertEqual(ds.dims, new_ds.dims)
        self.assertEqual(ds.sizes, new_ds.sizes)
        assert_array_almost_equal(new_ds.lon, np.linspace(-179.5, 179.5, 360))
        assert_array_almost_equal(new_ds.lat, np.linspace(-64., 65., 130))
        assert_array_almost_equal(new_ds.first[..., :180], ds.first[..., 180:])
        assert_array_almost_equal(new_ds.first[..., 180:], ds.first[..., :180])
        assert_array_almost_equal(new_ds.second[..., :180],
                                  ds.second[..., 180:])
        assert_array_almost_equal(new_ds.second[..., 180:],
                                  ds.second[..., :180])
        self.assertEqual(-180., new_ds.attrs['geospatial_lon_min'])
        self.assertEqual(+180., new_ds.attrs['geospatial_lon_max'])
        self.assertEqual(-64.5, new_ds.attrs['geospatial_lat_min'])
        self.assertEqual(+65.5, new_ds.attrs['geospatial_lat_max'])
        self.assertEqual(1., new_ds.attrs['geospatial_lon_resolution'])
        self.assertEqual(1., new_ds.attrs['geospatial_lat_resolution'])


class NormalizeDimOrder(TestCase):
    """
    Test normalize_cci_sea_level operation
    """

    def test_no_change(self):
        """
        Test nominal operation
        """
        lon_size = 360
        lat_size = 130
        time_size = 12
        ds = xr.Dataset(
            {
                'first': (
                    ['time', 'lat', 'lon'],
                    np.random.random_sample([time_size, lat_size, lon_size])),
                'second': (
                    ['time', 'lat', 'lon'],
                    np.random.random_sample([time_size, lat_size, lon_size]))
            },
            coords={
                'lon': np.linspace(-179.5, -179.5, lon_size),
                'lat': np.linspace(-64., 65., lat_size),
                'time': [datetime(2000, x, 1) for x in range(1, time_size + 1)]
            }
        )
        ds2 = normalize(ds)
        self.assertIs(ds2, ds)

    def test_nominal(self):
        """
        Test nominal operation
        """
        ds = self.new_cci_sea_level_ds()
        ds2 = normalize(ds)
        self.assertIsNot(ds2, ds)
        self.assertIn('ampl', ds2)
        self.assertIn('phase', ds2)
        self.assertIn('time', ds2.coords)
        self.assertIn('time_bnds', ds2.coords)
        self.assertNotIn('time_step', ds2.coords)
        self.assertEqual(['time', 'period', 'lat', 'lon'], list(ds2.ampl.dims))
        self.assertEqual(['time', 'period', 'lat', 'lon'], list(ds2.phase.dims))

    def new_cci_sea_level_ds(self):
        period_size = 2
        lon_size = 4
        lat_size = 2

        dataset = xr.Dataset(dict(
            ampl=(['lat', 'lon', 'period'],
                  np.ones(shape=(lat_size, lon_size, period_size))),
            phase=(['lat', 'lon', 'period'],
                   np.zeros(shape=(lat_size, lon_size, period_size)))),
            coords=dict(
                lon=np.array([-135, -45., 45., 135.]),
                lat=np.array([-45., 45.]),
                time=pd.to_datetime(
                    ['1993-01-15T00:00:00.000000000',
                     '1993-02-15T00:00:00.000000000',
                     '2015-11-15T00:00:00.000000000',
                     '2015-12-15T00:00:00.000000000'])
            )
        )

        dataset.coords['time'].encoding.update(
            units='days since 1950-01-01',
            dtype=np.dtype(np.float32)
        )
        dataset.coords['time'].attrs.update(
            long_name='time',
            standard_name='time'
        )

        dataset.attrs['time_coverage_start'] = '1993-01-01 00:00:00'
        dataset.attrs['time_coverage_end'] = '2015-12-31 23:59:59'

        return dataset
