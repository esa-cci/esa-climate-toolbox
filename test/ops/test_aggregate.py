"""
Tests for aggregation operations
"""
from unittest import TestCase
from tempfile import TemporaryDirectory
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr
from geopandas.testing import assert_geodataframe_equal
from shapely.geometry import box

from xcube.core.new import new_cube

from esa_climate_toolbox.core.op import OP_REGISTRY
from esa_climate_toolbox.core.types import ValidationError
from esa_climate_toolbox.ops import adjust_temporal_attrs
from esa_climate_toolbox.ops import climatology
from esa_climate_toolbox.ops import reduce
from esa_climate_toolbox.ops import reduce_datatree
from esa_climate_toolbox.ops import statistics
from esa_climate_toolbox.ops import statistics_datatree
from esa_climate_toolbox.ops import temporal_aggregation
from esa_climate_toolbox.util.misc import object_to_qualified_name
from esa_climate_toolbox.util.monitor import ConsoleMonitor


class TestClimatology(TestCase):
    """
    Test long term averaging
    """

    def test_nominal(self):
        """
        Test nominal execution
        """
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', freq='MS', periods=24)})

        ds = adjust_temporal_attrs(ds)

        # Test monitor
        m = ConsoleMonitor()
        actual = climatology(ds, monitor=m)
        self.assertEqual(m._percentage, 100)

        # Test CF attributes
        self.assertEqual(actual.dims, {'time': 12,
                                       'nv': 2,
                                       'lat': 45,
                                       'lon': 90})
        self.assertEqual(actual.time.attrs['climatology'],
                         'climatology_bounds')

        # Test variable selection
        actual = climatology(ds, var='first')
        with self.assertRaises(KeyError):
            actual['second']

    # @unittest.skip("Daily aggregation does do weird things. Skipping for the moment")
    def test_daily(self):
        """
        Test creating a daily LTA dataset
        """
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 730])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 730])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2001-01-01', '2002-12-31')})
        ds = adjust_temporal_attrs(ds)
        actual = climatology(ds)

        self.assertEqual(actual.dims, {'dayofyear': 365,
                                       'lat': 45,
                                       'lon': 90})

    def test_general(self):
        """
        Test creating a 'general' LTA dataset
        """
        # Test seasonal
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 5])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 5])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('1999-12-01', freq='QS-DEC', periods=5)})
        ds = adjust_temporal_attrs(ds)
        actual = climatology(ds)
        self.assertEqual(actual['first'].attrs['cell_methods'],
                         'time: mean over years')
        self.assertEqual(actual.dims, {'time': 4,
                                       'nv': 2,
                                       'lat': 45,
                                       'lon': 90})
        self.assertEqual(actual.time.attrs['climatology'],
                         'climatology_bounds')

        # Test irregular seasons
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 6])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 6])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', freq='5M', periods=6)})
        ds = adjust_temporal_attrs(ds)
        with self.assertRaises(ValueError) as err:
            climatology(ds)
        self.assertIn('inconsistent seasons', str(err.exception))

        # Test 8D
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 137])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 137])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', '2002-12-31', freq='8D')})
        ds = adjust_temporal_attrs(ds)
        with self.assertRaises(ValueError) as err:
            climatology(ds)
        self.assertIn('inconsistent seasons', str(err.exception))

    def test_registered(self):
        """
        Test registered operation execution
        """
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(climatology))
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', freq='MS', periods=24)})

        ds = adjust_temporal_attrs(ds)

        reg_op(ds=ds)

    def test_validation(self):
        """
        Test input validation
        """
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        ds = adjust_temporal_attrs(ds)

        with self.assertRaises(ValueError) as err:
            climatology(ds)
        self.assertIn('normalize', str(err.exception))


class TestTemporalAggregation(TestCase):
    """
    Test temporal aggregation
    """

    def test_nominal(self):
        """
        Test nominal exeuction
        """
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 366])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 366])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', '2000-12-31')})
        ds = adjust_temporal_attrs(ds)

        ex = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', freq='MS', periods=12)})
        ex.first.attrs['cell_methods'] = 'time: mean within years'
        ex.second.attrs['cell_methods'] = 'time: mean within years'

        m = ConsoleMonitor()
        actual = temporal_aggregation(ds, monitor=m)

        self.assertTrue(actual.broadcast_equals(ex))

    def test_seasonal(self):
        """
        Test aggregation to a seasonal dataset
        """
        # daily -> seasonal
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 366])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 366])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', '2000-12-31')})
        ds = adjust_temporal_attrs(ds)

        ex = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 5])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 5])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('1999-12-01', freq='QS-DEC', periods=5)})
        m = ConsoleMonitor()
        actual = temporal_aggregation(ds, period='QS-DEC', monitor=m)
        self.assertTrue(actual.broadcast_equals(ex))

        # TODO: uncomment once we have a fix for
        #  esa_climate_toolbox.core.types.ValidationError:
        #  Units 'M', 'Y', and 'y' are no longer supported,
        #  as they do not represent unambiguous timedelta values durations.

        # # monthly -> seasonal
        # ds = xr.Dataset({
        #     'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
        #     'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
        #     'lat': np.linspace(-88, 88, 45),
        #     'lon': np.linspace(-178, 178, 90),
        #     'time': pd.date_range('2000-01-01', freq='MS', periods=12)})
        # ds = adjust_temporal_attrs(ds)
        # actual = temporal_aggregation(ds, output_resolution='season', monitor=m)
        # self.assertTrue(actual.broadcast_equals(ex))

    def test_custom(self):
        """
        Test aggergating to custom temporal resolutions
        """
        # daily -> 8 days
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 366])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 366])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', '2000-12-31')})
        ds = adjust_temporal_attrs(ds)

        ex = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 46])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 46])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', '2000-12-31', freq='8D')})
        ex.first.attrs['cell_methods'] = 'time: mean within years'
        ex.second.attrs['cell_methods'] = 'time: mean within years'
        m = ConsoleMonitor()
        actual = temporal_aggregation(ds, period='8D', monitor=m)
        self.assertTrue(actual.broadcast_equals(ex))

        # daily -> weekly
        ex = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 53])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 53])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', '2000-12-31', freq='W')})
        ex.first.attrs['cell_methods'] = 'time: mean within years'
        ex.second.attrs['cell_methods'] = 'time: mean within years'
        actual = temporal_aggregation(ds, period='W', monitor=m)
        self.assertTrue(actual.broadcast_equals(ex))

        # TODO: uncomment once we have a fix for
        #  esa_climate_toolbox.core.types.ValidationError:
        #  Units 'M', 'Y', and 'y' are no longer supported,
        #  as they do not represent unambiguous timedelta values durations.

        # # monthly -> 4 month seasons
        # ds = xr.Dataset({
        #     'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
        #     'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
        #     'lat': np.linspace(-88, 88, 45),
        #     'lon': np.linspace(-178, 178, 90),
        #     'time': pd.date_range('2000-01-01', freq='MS', periods=12)})
        # ds = adjust_temporal_attrs(ds)
        # ex = xr.Dataset({
        #     'first': (['lat', 'lon', 'time'], np.ones([45, 90, 4])),
        #     'second': (['lat', 'lon', 'time'], np.ones([45, 90, 4])),
        #     'lat': np.linspace(-88, 88, 45),
        #     'lon': np.linspace(-178, 178, 90),
        #     'time': pd.date_range('2000-01-01', freq='4M', periods=4)})
        # ex.first.attrs['cell_methods'] = 'time: mean within years'
        # ex.second.attrs['cell_methods'] = 'time: mean within years'
        # actual = temporal_aggregation(ds, custom_resolution='4M', monitor=m)
        # self.assertTrue(actual.broadcast_equals(ex))

    def test_8days(self):
        """
        Test nominal execution with a 8 Days frequency input dataset
        """
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 46])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 46])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', '2000-12-31', freq='8D')})
        ds = adjust_temporal_attrs(ds)

        ex = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', freq='MS', periods=12)})
        ex.first.attrs['cell_methods'] = 'time: mean within years'
        ex.second.attrs['cell_methods'] = 'time: mean within years'

        m = ConsoleMonitor()
        actual = temporal_aggregation(ds, monitor=m)

        self.assertTrue(actual.broadcast_equals(ex))

    def test_registered(self):
        """
        Test registered operation execution
        """
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(temporal_aggregation))
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 366])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 366])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', '2000-12-31')})

        ex = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', freq='MS', periods=12)})

        actual = reg_op(ds=ds)
        self.assertTrue(actual.broadcast_equals(ex))

    def test_validation(self):
        """
        Test input validation
        """
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})
        ds = adjust_temporal_attrs(ds)

        with self.assertRaises(ValueError) as err:
            temporal_aggregation(ds)
        self.assertIn('normalize', str(err.exception))

        # invalid custom resolution
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 366])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 366])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', '2000-12-31')})
        ds = adjust_temporal_attrs(ds)

        with self.assertRaises(ValueError) as err:
            temporal_aggregation(ds, period='SDFG')
        self.assertEqual('Invalid freq value "SDFG". '
                         'Please check operation documentation.', str(err.exception))


class TestReduce(TestCase):
    """
    Test reduce() operation
    """

    def test_nominal(self):
        """
        Test nominal execution
        """
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 366])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 366])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', '2000-12-31')})
        ds = adjust_temporal_attrs(ds)

        ex = xr.Dataset({
            'first': 1.0,
            'second': 1.0,
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', '2000-12-31')})

        m = ConsoleMonitor()
        actual = reduce(ds, monitor=m)

        xr.testing.assert_allclose(actual, ex)

        actual = reduce(ds, dim=['lat', 'time', 'there_is_no_spoon'], monitor=m)
        ex = xr.Dataset({
            'first': (['lon'], np.ones([90])),
            'second': (['lon'], np.ones([90])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', '2000-12-31')})

        xr.testing.assert_allclose(actual, ex)


def _new_aggregation_datatree():
    rng = np.random.default_rng(42)
    time = pd.date_range('2000-01-01', periods=5)
    datasets = {}
    geometries = []
    paths = []

    for index in range(3):
        path = f'/lake_{index + 1}'
        lat = np.arange(10, dtype=float) + 30
        lon = np.arange(10, dtype=float) + index * 20
        # Different valid areas, bounded halfway between pixel centres.
        geometry = box(lon[1] - 0.5, lat[2] - 0.5,
                       lon[6 + index] + 0.5, lat[7] + 0.5)
        spatial_mask = np.zeros((10, 10), dtype=bool)
        spatial_mask[2:8, 1:7 + index] = True

        first = rng.normal(size=(5, 10, 10))
        second = rng.normal(size=(5, 10, 10, 3))
        first[:, ~spatial_mask] = np.nan
        second[:, ~spatial_mask, :] = np.nan

        # Sporadic missing observations. At t=0 every pixel inside the polygon
        # is valid, so the union of valid pixels over time matches the polygon.
        first[1:] = np.where(rng.random(first[1:].shape) < 0.1,
                             np.nan, first[1:])
        second[1:] = np.where(rng.random(second[1:].shape) < 0.1,
                              np.nan, second[1:])
        if index == 1:
            # One complete time step is missing in both variables/all levels.
            first[2] = np.nan
            second[2] = np.nan

        datasets[path] = xr.Dataset(
            data_vars={
                'first': (('time', 'lat', 'lon'), first),
                'second': (('time', 'lat', 'lon', 'level'), second),
            },
            coords={'time': time, 'lat': lat, 'lon': lon, 'level': [0, 1, 2]},
        )
        paths.append(path)
        geometries.append(geometry)

    regions = gpd.GeoDataFrame(
        {'path': paths}, geometry=geometries, crs='EPSG:4326'
    )
    return xr.DataTree.from_dict(datasets), regions


def _expected_spatial_statistic(variable, function, reduce_dims=('lat', 'lon')):
    """Calculate each remaining coordinate's statistic from valid input values."""
    dims = tuple(dim for dim in variable.dims if dim not in reduce_dims)
    shape = tuple(variable.sizes[dim] for dim in dims)
    values = np.empty(shape)
    for index in np.ndindex(shape):
        pixels = variable.isel(dict(zip(dims, index))).values.ravel()
        valid = pixels[~np.isnan(pixels)]
        if valid.size or function in (np.sum, np.size):
            values[index] = function(valid)
        else:
            values[index] = np.nan
    return xr.DataArray(values, dims=dims,
                        coords={dim: variable.coords[dim] for dim in dims})


def _invalid_datatree_parameters(regions):
    """Independent invalid combinations, with the relevant parameter in the error."""
    return [
        ('both vector sources',
         {'vector_data': regions, 'vector_path': 'unused.gpkg'},
         r'vector_data|vector_path'),
        ('missing node mapping',
         {'vector_data': regions.iloc[:2].copy()}, r'lake_3|mapping|geometry'),
        ('non-scalar frame',
         {'output_format': 'dataframe', 'dim': ['lat', 'lon']}, r'dimension|scalar|dataframe'),
        ('global frame',
         {'aggregate_nodes': True, 'output_format': 'dataframe'}, r'aggregate_nodes'),
        ('global vector cube',
         {'aggregate_nodes': True, 'output_format': 'vector_data_cube',
          'dim': ['lat', 'lon', 'level']},
         r'aggregate_nodes'),
    ]


def _expected_weighted_spatial(variable, method):
    dims = tuple(d for d in variable.dims if d not in ('lat', 'lon'))
    values = np.empty(tuple(variable.sizes[d] for d in dims))
    weights = np.broadcast_to(np.cos(np.deg2rad(variable.lat.values))[:, None],
                              (variable.sizes['lat'], variable.sizes['lon']))
    for index in np.ndindex(values.shape):
        pixels = variable.isel(dict(zip(dims, index))).transpose('lat', 'lon').values
        valid = ~np.isnan(pixels)
        data, weight = pixels[valid], weights[valid]
        if method == 'sum':
            value = np.sum(data * weight)
        elif not valid.any():
            value = np.nan
        else:
            mean = np.sum(data * weight) / np.sum(weight)
            value = mean if method == 'mean' else np.sqrt(
                np.sum(weight * (data - mean) ** 2) / np.sum(weight)
            )
        values[index] = value
    return xr.DataArray(values, dims=dims, coords={d: variable[d] for d in dims})


class _DataTreeAggregationTests:
    """Shared aggregation contract, exercised through both public operators."""

    def _aggregate(self, tree=None, method='mean', **kwargs):
        parameters = dict(aggregate_nodes=True, apply_spatial_weighting=False)
        parameters.update(kwargs)
        if self._aggregation_statistics:
            return statistics_datatree(
                self.datatree if tree is None else tree,
                methods=method, **parameters,
            )
        return reduce_datatree(
            self.datatree if tree is None else tree, method=method, **parameters,
        )

    def _assert_aggregate(self, actual, variables, method='mean'):
        if self._aggregation_statistics:
            variables = {f'{name}_{method}': value for name, value in variables.items()}
        # A shared result belongs at the root, without the original leaf datasets.
        xr.testing.assert_allclose(actual, xr.DataTree(xr.Dataset(variables)))

    def _assert_pooled_method(self, method, function):
        expected = {}
        for name in ('first', 'second'):
            values = np.concatenate([
                node[name].values.ravel() for node in self.datatree.leaves
            ])
            expected[name] = function(values[~np.isnan(values)])
        self._assert_aggregate(self._aggregate(method=method), expected, method)

    def test_aggregation_mean(self):
        self._assert_pooled_method('mean', np.mean)

    def test_aggregation_min(self):
        self._assert_pooled_method('min', np.min)

    def test_aggregation_max(self):
        self._assert_pooled_method('max', np.max)

    def test_aggregation_sum(self):
        self._assert_pooled_method('sum', np.sum)

    def test_aggregation_median(self):
        self._assert_pooled_method('median', np.median)

    def test_aggregation_coordinate_union(self):
        """Align remaining times and levels by labels, using their union."""
        times = pd.date_range('2000-01-01', periods=3)
        datasets = {}
        for i, (days, levels, values) in enumerate([
            ([0, 2], [0, 2], [[1, 3], [5, 7]]),
            ([2, 1], [2, 1], [[9, 11], [13, 15]]),
            ([1], [0], [[17]]),
        ]):
            datasets[f'/lake_{i + 1}'] = xr.Dataset(
                {'second': (('time', 'lat', 'lon', 'level'),
                            np.asarray(values, dtype=float)[:, None, None, :])},
                coords={'time': times[days], 'lat': [30], 'lon': [i * 20],
                        'level': levels},
            )
        expected = xr.DataArray(
            [[1, np.nan, 3], [17, 15, 13], [5, 11, 8]],
            dims=('time', 'level'), coords={'time': times, 'level': [0, 1, 2]},
        )
        actual = self._aggregate(
            xr.DataTree.from_dict(datasets), var='second', dim=['lat', 'lon']
        )
        self._assert_aggregate(actual, {'second': expected})

    def test_aggregation_node_selection(self):
        """Unselected nodes must not contribute to the shared result."""
        paths = ['/lake_1', '/lake_3']
        expected = {}
        for name in ('first', 'second'):
            values = np.concatenate([self.datatree[path][name].values.ravel()
                                     for path in paths])
            expected[name] = np.mean(values[~np.isnan(values)])
        self._assert_aggregate(self._aggregate(nodes=paths), expected)

    def test_node_selection_preserves_ancestor_groups(self):
        tree = xr.DataTree.from_dict({
            '/Europe': xr.Dataset(attrs={'continent': 'Europe'}),
            '/Europe/lake_1': self.datatree['lake_1'].to_dataset(),
            '/Europe/lake_2': self.datatree['lake_2'].to_dataset(),
            '/Africa/lake_3': self.datatree['lake_3'].to_dataset(),
        })
        reduced = self._aggregate(tree, aggregate_nodes=False, nodes=['Europe/lake_2'])
        self.assertEqual(list(reduced.groups), ['/', '/Europe', '/Europe/lake_2'])
        self.assertEqual(reduced['Europe'].attrs, {'continent': 'Europe'})
        self.assertEqual(list(tree['Europe'].children), ['lake_1', 'lake_2'])

    def test_aggregation_all_missing(self):
        """No valid contributions give zero for sum/count, NaN otherwise."""
        tree = self.datatree.copy(deep=True)
        for node in tree.leaves:
            for name in ('first', 'second'):
                node[name].values[:] = np.nan
        methods = ['mean', 'sum', 'min', 'max', 'median']
        if self._aggregation_statistics:
            methods += ['count', 'std']
        for method in methods:
            with self.subTest(method=method):
                value = 0 if method in ('sum', 'count') else np.nan
                self._assert_aggregate(
                    self._aggregate(tree, method=method),
                    {'first': value, 'second': value}, method,
                )

    def _assert_weighted_aggregation(self, method, actual=None):
        expected = {}
        for name in ('first', 'second'):
            data, weights = [], []
            for node in self.datatree.leaves:
                variable = node[name]
                shape = [1] * variable.ndim
                shape[variable.dims.index('lat')] = variable.sizes['lat']
                weight = np.broadcast_to(
                    np.cos(np.deg2rad(variable.lat.values)).reshape(shape),
                    variable.shape,
                )
                valid = ~np.isnan(variable.values)
                data.append(variable.values[valid])
                weights.append(weight[valid])
            data, weights = np.concatenate(data), np.concatenate(weights)
            numerator = np.sum(data * weights)
            mean = numerator / np.sum(weights)
            expected[name] = (
                numerator if method == 'sum' else mean if method == 'mean'
                else np.sqrt(np.sum(weights * (data - mean) ** 2) / np.sum(weights))
            )
        if actual is None:
            actual = self._aggregate(method=method, apply_spatial_weighting=True)
        self._assert_aggregate(actual, expected, method)

    def test_aggregation_dask(self):
        """Chunking must not affect results or trigger eager computation."""
        from dask.base import is_dask_collection
        from dask.callbacks import Callback

        reference = self._aggregate()
        methods = ['mean', 'sum', 'min', 'max', 'median']
        if self._aggregation_statistics:
            methods += ['count', 'std']
        for method in methods:
            with self.subTest(method=method):
                tree = self.datatree.chunk({'time': 2, 'lat': 4, 'lon': 3, 'level': 1})
                tasks = []
                with Callback(pretask=lambda *args: tasks.append(args[0])):
                    actual = self._aggregate(tree, method=method)
                self.assertEqual(tasks, [])
                self.assertTrue(all(is_dask_collection(v.data)
                                    for v in actual.dataset.data_vars.values()))
                expected = reference if method == 'mean' else self._aggregate(method=method)
                xr.testing.assert_allclose(actual.compute(), expected)

    def test_aggregation_weighted_mean(self):
        self._assert_weighted_aggregation('mean')

    def test_aggregation_weighted_sum(self):
        self._assert_weighted_aggregation('sum')

    def test_aggregation_missing_variable(self):
        datasets = {node.path: node.to_dataset().copy(deep=True)
                    for node in self.datatree.leaves}
        datasets['/lake_2'] = datasets['/lake_2'].drop_vars('second')
        with self.assertRaisesRegex(ValidationError, r'second'):
            reduce_datatree(xr.DataTree.from_dict(datasets))

    def test_per_node_dask(self):
        methods = ['mean', 'sum', 'min', 'max', 'median']
        if self._aggregation_statistics:
            methods += ['count', 'std']
        tree = self.datatree.chunk({'time': 2, 'lat': 4, 'lon': 3, 'level': 1})
        for method in methods:
            with self.subTest(method=method):
                actual = self._aggregate(tree, method=method, aggregate_nodes=False)
                expected = self._aggregate(method=method, aggregate_nodes=False)
                xr.testing.assert_allclose(actual.compute(), expected)

    def test_mask_with_projected_crs(self):
        regions = self.regions.to_crs('EPSG:3857')
        actual = self._aggregate(aggregate_nodes=False, output_format='dataframe',
                                 vector_data=regions)
        expected = self._aggregate(aggregate_nodes=False, output_format='dataframe',
                                   vector_data=self.regions).to_crs(regions.crs)
        assert_geodataframe_equal(actual, expected)

    def test_centroid_across_antimeridian(self):
        ds = xr.Dataset({'first': (('lat', 'lon'), [[1., 2.]])},
                        coords={'lat': [0.], 'lon': [179., -179.]})
        actual = self._aggregate(xr.DataTree.from_dict({'/lake': ds}),
                                 aggregate_nodes=False, output_format='dataframe')
        point = actual.loc['/lake'].geometry
        self.assertAlmostEqual(abs(point.x), 180.)
        self.assertAlmostEqual(point.y, 0.)


class ReduceDatatreeTest(_DataTreeAggregationTests, TestCase):
    _aggregation_statistics = False

    def setUp(self):
        self.datatree, self.regions = _new_aggregation_datatree()

    def _assert_spatial_method(self, method, function):
        expected = {}
        for node in self.datatree.leaves:
            ds = node.to_dataset()
            result = ds.copy()
            for name in ('first', 'second'):
                result[name] = _expected_spatial_statistic(ds[name], function)
            expected[node.path] = result

        actual = reduce_datatree(
            self.datatree, method=method, dim=['lat', 'lon'],
            aggregate_nodes=False, apply_spatial_weighting=False,
        )
        xr.testing.assert_allclose(actual, xr.DataTree.from_dict(expected))

    def test_defaults(self):
        self._assert_weighted_aggregation('mean', actual=reduce_datatree(self.datatree))

    def test_mean(self):
        self._assert_spatial_method('mean', np.mean)

    def test_min(self):
        self._assert_spatial_method('min', np.min)

    def test_max(self):
        self._assert_spatial_method('max', np.max)

    def test_sum(self):
        self._assert_spatial_method('sum', np.sum)

    def test_median(self):
        self._assert_spatial_method('median', np.median)

    def test_variable_selection(self):
        reduced = reduce_datatree(self.datatree, var="first")
        for node in reduced.leaves:
            self.assertEqual({"first"}, set(node.to_dataset().data_vars))

        reduced = reduce_datatree(self.datatree, var="second")
        for node in reduced.leaves:
            self.assertEqual({"second"}, set(node.to_dataset().data_vars))

    def test_dimension_selection(self):
        for dims in (['time'], ['lat', 'lon'], ['lat', 'lon', 'level']):
            with self.subTest(dim=dims):
                expected = {}
                for node in self.datatree.leaves:
                    ds = node.to_dataset()
                    result = ds.copy()
                    for name in ('first', 'second'):
                        result[name] = _expected_spatial_statistic(
                            ds[name], np.mean, reduce_dims=dims
                        )
                    expected[node.path] = result

                actual = reduce_datatree(
                    self.datatree, dim=dims, aggregate_nodes=False,
                    apply_spatial_weighting=False,
                )
                xr.testing.assert_allclose(actual, xr.DataTree.from_dict(expected))
                for node_name in ["lake_1", "lake_2", "lake_3"]:
                    for var_name in ("first", "second"):
                        self.assertEqual(
                            actual.get(node_name).to_dataset()[var_name].dims,
                            tuple(t for t in self.datatree.get(node_name).to_dataset()[var_name].dims if t not in dims)
                        )

    def test_spatial_weighting(self):
        for method in ['mean', 'sum']:
            with self.subTest(method=method):
                expected = {}
                for node in self.datatree.leaves:
                    ds = node.to_dataset()
                    local = ds.copy()
                    for name in ('first', 'second'):
                        local[name] = _expected_weighted_spatial(ds[name], method)
                    expected[node.path] = local
                actual = self._aggregate(method=method, aggregate_nodes=False,
                                         dim=['lat', 'lon'], apply_spatial_weighting=True)
                xr.testing.assert_allclose(actual, xr.DataTree.from_dict(expected))
        # Latitude weights cancel when only time is reduced.
        weighted = self._aggregate(aggregate_nodes=False, dim=['time'],
                                   apply_spatial_weighting=True)
        plain = self._aggregate(aggregate_nodes=False, dim=['time'])
        xr.testing.assert_allclose(weighted, plain)

    def test_node_selection(self):
        reduced = reduce_datatree(self.datatree, aggregate_nodes=False, nodes=["lake_2"])
        self.assertEqual(list(reduced.children), ["lake_2"])

        reduced = reduce_datatree(self.datatree, aggregate_nodes=False, nodes=["lake_1", "lake_3"])
        self.assertEqual(list(reduced.children), ["lake_1", "lake_3"])

    def test_vector_data_cube(self):
        reduced = reduce_datatree(
            self.datatree, output_format="vector_data_cube", dim=['lat', 'lon', 'level'],
            aggregate_nodes=False
        )
        self.assertEqual(reduced.xindexes['geometry'].crs.to_epsg(), 4326)
        for geometry, polygon in zip(reduced.geometry.values, self.regions.geometry):
            self.assertEqual(geometry.geom_type, 'Point')
            self.assertTrue(polygon.contains(geometry))

    def test_dataframe(self):
        reduced = reduce_datatree(
            self.datatree, output_format="dataframe", dim=['lat', 'lon', 'time', 'level'],
            aggregate_nodes=False, apply_spatial_weighting=False
        )
        self.assertIsInstance(reduced, gpd.GeoDataFrame)
        self.assertEqual(list(reduced.index), list(self.regions.path))
        self.assertEqual(set(reduced.columns), {'first', 'second', 'geometry'})
        for node in self.datatree.leaves:
            for name in ('first', 'second'):
                data = node[name].values
                self.assertAlmostEqual(reduced.loc[node.path, name],
                                       np.mean(data[~np.isnan(data)]))
        self.assertEqual(reduced.crs.to_epsg(), 4326)

    def test_vector_data_mask(self):
        regions = self.regions.copy()
        regions.geometry = [box(i * 20 + 0.5, 31.5, i * 20 + 2.5, 33.5)
                            for i in range(3)]
        actual = self._aggregate(aggregate_nodes=False, output_format='dataframe',
                                 vector_data=regions)
        expected = {}
        for node in self.datatree.leaves:
            row = {}
            for name in ('first', 'second'):
                data = node[name].isel(lat=slice(2, 4), lon=slice(1, 3)).values
                row[name] = np.mean(data[~np.isnan(data)])
            expected[node.path] = row
        expected = gpd.GeoDataFrame.from_dict(expected, orient='index')
        expected.index.name = 'path'
        expected = expected.set_geometry(gpd.GeoSeries(list(regions.geometry),
                                                       index=expected.index, crs=regions.crs))
        assert_geodataframe_equal(actual, expected)

    def test_vector_path(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / 'regions.geojson'
            self.regions.to_file(path, driver='GeoJSON')
            actual = reduce_datatree(self.datatree, aggregate_nodes=False, output_format='dataframe',
                                     vector_path=str(path))
        expected = reduce_datatree(self.datatree, aggregate_nodes=False, output_format='dataframe',
                                   vector_data=self.regions)
        assert_geodataframe_equal(actual, expected)

    def test_node_column(self):
        regions = self.regions.rename(columns={'path': 'lake_path'}).iloc[::-1]
        regions = regions.rename_geometry('shape')
        actual = reduce_datatree(self.datatree, aggregate_nodes=False, output_format='dataframe',
                                 vector_data=regions, node_column='lake_path')
        expected = reduce_datatree(self.datatree, aggregate_nodes=False, output_format='dataframe',
                                   vector_data=self.regions)
        assert_geodataframe_equal(actual, expected)

    def test_invalid_parameter_combinations(self):
        for label, overrides, message in _invalid_datatree_parameters(self.regions):
            with self.subTest(case=label):
                parameters = dict(aggregate_nodes=False, apply_spatial_weighting=False)
                parameters.update(overrides)
                with self.assertRaisesRegex(ValidationError, message):
                    reduce_datatree(self.datatree, **parameters)

    def test_monitor(self):
        monitor = ConsoleMonitor()
        actual = self._aggregate(monitor=monitor)
        self.assertEqual(monitor._percentage, 100)
        self._assert_aggregate(actual, {
            name: np.nanmean(np.concatenate([node[name].values.ravel()
                                            for node in self.datatree.leaves]))
            for name in ('first', 'second')
        })


class StatisticsDatatreeTest(_DataTreeAggregationTests, TestCase):
    _aggregation_statistics = True

    def test_aggregation_count(self):
        self.datatree['/lake_1']['first'][0, 2, 1] = 0.0
        self._assert_pooled_method('count', np.size)

    def test_aggregation_std(self):
        self._assert_pooled_method('std', np.std)

    def test_aggregation_weighted_std(self):
        self._assert_weighted_aggregation('std')

    def setUp(self):
        self.datatree, self.regions = _new_aggregation_datatree()

    def _assert_spatial_method(self, method, function):
        expected = {}
        for node in self.datatree.leaves:
            ds = node.to_dataset()
            result = ds.copy()
            for name in ('first', 'second'):
                result[f'{name}_{method}'] = _expected_spatial_statistic(
                    ds[name], function
                )
            expected[node.path] = result

        actual = statistics_datatree(
            self.datatree, methods=method, dim=['lat', 'lon'],
            aggregate_nodes=False, apply_spatial_weighting=False,
        )
        xr.testing.assert_allclose(actual, xr.DataTree.from_dict(expected))

    def test_defaults(self):
        self._assert_weighted_aggregation('mean', actual=statistics_datatree(self.datatree))

    def test_count(self):
        self.datatree['/lake_1']['first'][0, 2, 1] = 0.0
        self.datatree['/lake_1']['second'][0, 2, 1, :] = 0.0
        self._assert_spatial_method('count', np.size)

    def test_mean(self):
        self._assert_spatial_method('mean', np.mean)

    def test_median(self):
        self._assert_spatial_method('median', np.median)

    def test_sum(self):
        self._assert_spatial_method('sum', np.sum)

    def test_std(self):
        self._assert_spatial_method('std', np.std)

    def test_min(self):
        self._assert_spatial_method('min', np.min)

    def test_max(self):
        self._assert_spatial_method('max', np.max)

    def test_multiple_methods(self):
        methods = ['count', 'mean', 'std']
        expected = {}
        for node in self.datatree.leaves:
            ds = node.to_dataset()
            local = ds.copy()
            for name in ('first', 'second'):
                for method, function in zip(methods, [np.size, np.mean, np.std]):
                    local[f'{name}_{method}'] = _expected_spatial_statistic(ds[name], function)
            expected[node.path] = local
        actual = statistics_datatree(self.datatree, methods=methods, dim=['lat', 'lon'],
                                     aggregate_nodes=False, apply_spatial_weighting=False)
        xr.testing.assert_allclose(actual, xr.DataTree.from_dict(expected))

    def test_variable_selection(self):
        reduced = statistics_datatree(self.datatree, var="first")
        for node in reduced.leaves:
            self.assertEqual({"first_mean"}, set(node.to_dataset().data_vars))

        reduced = statistics_datatree(self.datatree, var="second")
        for node in reduced.leaves:
            self.assertEqual({"second_mean"}, set(node.to_dataset().data_vars))

    def test_dimension_selection(self):
        """Check exact remaining axes, including level on second but not first."""
        for dims in (['time'], ['lat', 'lon'], ['lat', 'lon', 'level']):
            with self.subTest(dim=dims):
                expected = {}
                for node in self.datatree.leaves:
                    ds = node.to_dataset()
                    result = ds.copy()
                    for name in ('first', 'second'):
                        result[f'{name}_mean'] = _expected_spatial_statistic(
                            ds[name], np.mean, reduce_dims=dims
                        )
                    expected[node.path] = result

                actual = statistics_datatree(
                    self.datatree, dim=dims, aggregate_nodes=False,
                    apply_spatial_weighting=False,
                )
                xr.testing.assert_allclose(actual, xr.DataTree.from_dict(expected))
                for node_name in ["lake_1", "lake_2", "lake_3"]:
                    for var_name in ("first", "second"):
                        self.assertEqual(
                            actual.get(node_name).to_dataset()[f"{var_name}_mean"].dims,
                            tuple(t for t in self.datatree.get(node_name).to_dataset()[var_name].dims if t not in dims)
                        )

    def test_spatial_weighting(self):
        for method in ['mean', 'sum', 'std']:
            with self.subTest(method=method):
                expected = {}
                for node in self.datatree.leaves:
                    ds = node.to_dataset()
                    local = ds.copy()
                    for name in ('first', 'second'):
                        local[f'{name}_{method}'] = _expected_weighted_spatial(ds[name], method)
                    expected[node.path] = local
                actual = self._aggregate(method=method, aggregate_nodes=False,
                                         dim=['lat', 'lon'], apply_spatial_weighting=True)
                xr.testing.assert_allclose(actual, xr.DataTree.from_dict(expected))
        # Latitude weights cancel when only time is reduced.
        weighted = self._aggregate(aggregate_nodes=False, dim=['time'],
                                   apply_spatial_weighting=True)
        plain = self._aggregate(aggregate_nodes=False, dim=['time'])
        xr.testing.assert_allclose(weighted, plain)

    def test_node_selection(self):
        reduced = statistics_datatree(self.datatree, aggregate_nodes=False, nodes=["lake_2"])
        self.assertEqual(list(reduced.children), ["lake_2"])

        reduced = statistics_datatree(self.datatree, aggregate_nodes=False, nodes=["lake_1", "lake_3"])
        self.assertEqual(list(reduced.children), ["lake_1", "lake_3"])

    def test_vector_data_cube(self):
        reduced = statistics_datatree(
            self.datatree, output_format="vector_data_cube", dim=['lat', 'lon', 'level'],
            aggregate_nodes=False
        )
        self.assertEqual(reduced.xindexes['geometry'].crs.to_epsg(), 4326)
        for geometry, polygon in zip(reduced.geometry.values, self.regions.geometry):
            self.assertEqual(geometry.geom_type, 'Point')
            self.assertTrue(polygon.contains(geometry))

    def test_dataframe(self):
        reduced = statistics_datatree(
            self.datatree, output_format="dataframe", dim=['lat', 'lon', 'time', 'level'],
            aggregate_nodes=False, apply_spatial_weighting=False
        )
        self.assertIsInstance(reduced, gpd.GeoDataFrame)
        self.assertEqual(list(reduced.index), list(self.regions.path))
        self.assertEqual(set(reduced.columns), {'first_mean', 'second_mean', 'geometry'})
        for node in self.datatree.leaves:
            for name in ('first', 'second'):
                data = node[name].values
                self.assertAlmostEqual(reduced.loc[node.path, f'{name}_mean'],
                                       np.mean(data[~np.isnan(data)]))
        self.assertEqual(reduced.crs.to_epsg(), 4326)

    def test_vector_data_mask(self):
        regions = self.regions.copy()
        regions.geometry = [box(i * 20 + 0.5, 31.5, i * 20 + 2.5, 33.5)
                            for i in range(3)]
        actual = self._aggregate(aggregate_nodes=False, output_format='dataframe',
                                 vector_data=regions)
        expected = {}
        for node in self.datatree.leaves:
            row = {}
            for name in ('first', 'second'):
                data = node[name].isel(lat=slice(2, 4), lon=slice(1, 3)).values
                row[f'{name}_mean'] = np.mean(data[~np.isnan(data)])
            expected[node.path] = row
        expected = gpd.GeoDataFrame.from_dict(expected, orient='index')
        expected.index.name = 'path'
        expected = expected.set_geometry(gpd.GeoSeries(list(regions.geometry),
                                                       index=expected.index, crs=regions.crs))
        assert_geodataframe_equal(actual, expected)

    def test_vector_path(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / 'regions.geojson'
            self.regions.to_file(path, driver='GeoJSON')
            actual = statistics_datatree(self.datatree, aggregate_nodes=False, output_format='dataframe',
                                     vector_path=str(path))
        expected = statistics_datatree(self.datatree, aggregate_nodes=False, output_format='dataframe',
                                   vector_data=self.regions)
        assert_geodataframe_equal(actual, expected)

    def test_node_column(self):
        regions = self.regions.rename(columns={'path': 'lake_path'}).iloc[::-1]
        regions = regions.rename_geometry('shape')
        actual = statistics_datatree(self.datatree, aggregate_nodes=False, output_format='dataframe',
                                 vector_data=regions, node_column='lake_path')
        expected = statistics_datatree(self.datatree, aggregate_nodes=False, output_format='dataframe',
                                   vector_data=self.regions)
        assert_geodataframe_equal(actual, expected)

    def test_invalid_parameter_combinations(self):
        for label, overrides, message in _invalid_datatree_parameters(self.regions):
            with self.subTest(case=label):
                parameters = dict(aggregate_nodes=False, apply_spatial_weighting=False)
                parameters.update(overrides)
                with self.assertRaisesRegex(ValidationError, message):
                    statistics_datatree(self.datatree, **parameters)

    def test_monitor(self):
        monitor = ConsoleMonitor()
        actual = self._aggregate(monitor=monitor)
        self.assertEqual(monitor._percentage, 100)
        self._assert_aggregate(actual, {
            name: np.nanmean(np.concatenate([node[name].values.ravel()
                                            for node in self.datatree.leaves]))
            for name in ('first', 'second')
        })


class TestStatistics(TestCase):

    @staticmethod
    def _sst_func(t, y, x):
        return t * 2.0 + x * 0.1

    @staticmethod
    def _chl_func(t, y, x):
        return y * 1.0 + x * 0.1

    @staticmethod
    def _aot_func(t, y, x):
        return y * 0.1 + x * 1.0

    def test_statistics_mean(self):
        cube = new_cube(
            width=16, height=8, variables=dict(sst=self._sst_func, chl=self._chl_func, aot=self._aot_func)
        )

        res = statistics(
            cube, var=["chl", "aot"],
            methods=["count", "mean", "median", "sum", "std", "min", "max"],
            apply_spatial_weighting=False
        )

        self.assertIsNotNone(res)
        self.assertIn("chl_count", res.data_vars)
        self.assertEqual(635, res["chl_count"].values)
        self.assertIn("chl_mean", res.data_vars)
        self.assertAlmostEqual(4.25, res["chl_mean"].values, 6)
        self.assertIn("chl_median", res.data_vars)
        self.assertAlmostEqual(4.25, res["chl_median"].values, 6)
        self.assertIn("chl_sum", res.data_vars)
        self.assertAlmostEqual(2720, res["chl_sum"].values, 6)
        self.assertIn("chl_std", res.data_vars)
        self.assertAlmostEqual(2.337199, res["chl_std"].values, 6)
        self.assertIn("chl_min", res.data_vars)
        self.assertAlmostEqual(0, res["chl_min"].values, 6)
        self.assertIn("chl_max", res.data_vars)
        self.assertAlmostEqual(8.5, res["chl_max"].values, 8)
        self.assertIn("aot_count", res.data_vars)
        self.assertEqual(635, res["aot_count"].values, 6)
        self.assertIn("aot_mean", res.data_vars)
        self.assertAlmostEqual(7.85, res["aot_mean"].values, 6)
        self.assertIn("aot_median", res.data_vars)
        self.assertAlmostEqual(7.85, res["aot_median"].values, 6)
        self.assertIn("aot_sum", res.data_vars)
        self.assertAlmostEqual(5024, res["aot_sum"].values, 6)
        self.assertIn("aot_std", res.data_vars)
        self.assertAlmostEqual(4.615463, res["aot_std"].values, 6)
        self.assertIn("aot_min", res.data_vars)
        self.assertAlmostEqual(0, res["aot_min"].values, 6)
        self.assertIn("aot_max", res.data_vars)
        self.assertAlmostEqual(15.7, res["aot_max"].values, 6)

    def test_statistics_mean_apply_spatial_weighting(self):
        cube = new_cube(
            width=16, height=8, variables=dict(sst=self._sst_func, chl=self._chl_func, aot=self._aot_func)
        )

        res = statistics(
            cube, var=["chl", "aot"],
            methods=["count", "mean", "median", "sum", "std", "min", "max"]
        )

        self.assertIsNotNone(res)
        self.assertIn("chl_count", res.data_vars)
        self.assertEqual(635, res["chl_count"].values)
        self.assertIn("chl_mean", res.data_vars)
        self.assertAlmostEqual(5.56079961, res["chl_mean"].values, 6)
        self.assertIn("chl_median", res.data_vars)
        self.assertAlmostEqual(4.25, res["chl_median"].values, 6)
        self.assertIn("chl_sum", res.data_vars)
        self.assertAlmostEqual(248.05866912, res["chl_sum"].values, 6)
        self.assertIn("chl_std", res.data_vars)
        self.assertAlmostEqual(0, res["chl_std"].values, 6)
        self.assertIn("chl_min", res.data_vars)
        self.assertAlmostEqual(0, res["chl_min"].values, 6)
        self.assertIn("chl_max", res.data_vars)
        self.assertAlmostEqual(8.5, res["chl_max"].values, 8)
        self.assertIn("aot_count", res.data_vars)
        self.assertEqual(635, res["aot_count"].values, 6)
        self.assertIn("aot_mean", res.data_vars)
        self.assertAlmostEqual(7.98107996, res["aot_mean"].values, 6)
        self.assertIn("aot_median", res.data_vars)
        self.assertAlmostEqual(7.85, res["aot_median"].values, 6)
        self.assertIn("aot_sum", res.data_vars)
        self.assertAlmostEqual(356.02363189, res["aot_sum"].values, 6)
        self.assertIn("aot_std", res.data_vars)
        self.assertAlmostEqual(0, res["aot_std"].values, 6)
        self.assertIn("aot_min", res.data_vars)
        self.assertAlmostEqual(0, res["aot_min"].values, 6)
        self.assertIn("aot_max", res.data_vars)
        self.assertAlmostEqual(15.7, res["aot_max"].values, 6)
